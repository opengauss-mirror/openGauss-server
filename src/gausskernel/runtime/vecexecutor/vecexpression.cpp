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
 * vecexpression.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/vecexecutor.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "vecexecutor/vecexpression.h"
#include "vecexecutor/vectorbatch.h"
#include "nodes/execnodes.h"
#include "vecexecutor/vecsubplan.h"
#include "access/nbtree.h"
#include "access/tupconvert.h"
#include "catalog/pg_type.h"
#include "commands/typecmds.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeSubplan.h"
#include "executor/node/nodeAgg.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "utils/date.h"
#include "vecexecutor/vecfunc.h"
#include "catalog/pg_proc.h"
#include "utils/syscache.h"
#include "access/hash.h"
#ifdef PGXC
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#endif
#include "postmaster/fencedudf.h"
#include "catalog/pg_proc_fn.h"

extern void initVectorFcache(Oid foid, Oid input_collation, FuncExprState* fcache, MemoryContext fcacheCxt);

static void DispatchVectorFunction(Oid foid, Oid input_collation, FuncExprState* fcache, ExprContext* econtext);

static ExprDoneCond ExecEvalVecFuncArgs(
    FunctionCallInfo fcinfo, List* argList, bool* pSelection, ExprContext* econtext);
static Datum ExecEvalVecRow(
    RowExprState* rstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone);

static ScalarVector* ExecEvalVecGroupingFuncExpr(GroupingFuncExprState* gstate, ExprContext* econtext,
    const bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone);

typedef ScalarVector* (*VecCoerceIOFunc)(
    CoerceViaIOState* caseExpr, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone);

static void BindingGenericFunction(FunctionCallInfo finfo, MemoryContext fcacheCxt);

static ScalarVector* ExecEvalVecGroupingIdExpr(GroupingIdExprState* gstate, ExprContext* econtext,
    const bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone);

template <bool batchMode>
extern Datum RPCFencedUDF(FunctionCallInfo fcinfo);

static ScalarVector* ExecEvalVecVar(
    ExprState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Var* variable = (Var*)exprstate->expr;
    VectorBatch* batch = NULL;
    AttrNumber attnum = variable->varattno;

    /* Get the input slot and attribute number we want */
    switch (variable->varno) {
        case INNER_VAR: /* get the tuple from the inner node */
            batch = econtext->ecxt_innerbatch;
            break;

        case OUTER_VAR: /* get the tuple from the outer node */
            batch = econtext->ecxt_outerbatch;
            break;

        default: /* get the tuple from the relation being
                  * scanned */
            batch = econtext->ecxt_scanbatch;
            break;
    }

    /* Sys column branch */
    Assert(batch != NULL);
    if (attnum < 0) {
        ScalarVector* pVec = batch->GetSysVector(attnum);

        ScalarValue* pSrc = pVec->m_vals;
        ScalarValue* pDest = pVector->m_vals;

        int rows = batch->m_rows;

        for (int i = 0; i < rows; i++)
            pDest[i] = pSrc[i];

        pVector->m_rows = rows;
        pVector->m_desc = pVec->m_desc;

        return pVector;
    }

    // Fast path. Use the vector reference instead
    //
    pVector = &batch->m_arr[variable->varattno - 1];

    Assert(pVector->m_rows == econtext->align_rows);

    return pVector;
}

/* ----------------------------------------------------------------
 *      ExecEvalConst
 *
 *      Returns the value of a constant.
 *
 *      Note that for pass-by-ref datatypes, we return a pointer to the
 *      actual constant node.  This is one of the reasons why functions
 *      must treat their input arguments as read-only.
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecConst(
    ExprState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Const* con = (Const*)exprstate->expr;
    ScalarValue val;
    ScalarVector* contVector = &exprstate->tmpVector;
    ScalarDesc desc;

    if (contVector->m_rows == 0) {
        AutoContextSwitch tempContext(econtext->ecxt_estate->es_query_cxt);

        desc.typeId = con->consttype;
        desc.encoded = COL_IS_ENCODE(con->consttype);
        contVector->init(CurrentMemoryContext, desc);

        val = ScalarVector::DatumToScalar(con->constvalue, con->consttype, con->constisnull);

        ScalarValue* pValues = contVector->m_vals;
        uint8* pFlag = contVector->m_flag;

        if (con->constisnull == false) {
            for (int i = 0; i < BatchMaxSize; i++) {
                pValues[i] = val;
                SET_NOTNULL(pFlag[i]);
            }
        } else {
            for (int i = 0; i < BatchMaxSize; i++) {
                SET_NULL(pFlag[i]);
            }
        }
    }

    contVector->m_const = true;
    contVector->m_rows = econtext->align_rows;

    return contVector;
}

static ScalarVector* ExecEvalVecAggref(
    AggrefExprState* aggref, ExprContext* econtext, bool* pSelection, ScalarVector* pVectorm, ExprDoneCond* isDone)
{
    VectorBatch* pBatch = econtext->ecxt_aggbatch;

    Assert(aggref->m_htbOffset < pBatch->m_cols);
    return &pBatch->m_arr[aggref->m_htbOffset];
}

static ScalarVector* ExecEvalVecNot(
    BoolExprState* notclause, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    ExprState* clause = (ExprState*)linitial(notclause->args);
    ScalarValue* pDest = NULL;
    ScalarVector* ptmp = NULL;

    Assert(pSelection != NULL);
    ptmp = VectorExprEngine(clause, econtext, pSelection, pVector, isDone);
    pDest = pVector->m_vals;

    // Flip the results
    //
    if (econtext->m_fUseSelection) {
        for (int i = 0; i < econtext->align_rows; i++) {
            /* the value of pDest[i] need not */
            if (pSelection[i]) {
                if (!IS_NULL(ptmp->m_flag[i])) {
                    SET_NOTNULL(pVector->m_flag[i]);
                    pDest[i] = !(bool)ptmp->m_vals[i];
                } else
                    SET_NULL(pVector->m_flag[i]);
            }
        }
    } else {
        for (int i = 0; i < econtext->align_rows; i++) {
            /* the value of pDest[i] need not */
            if (!IS_NULL(ptmp->m_flag[i])) {
                SET_NOTNULL(pVector->m_flag[i]);
                pDest[i] = !(bool)ptmp->m_vals[i];
            } else
                SET_NULL(pVector->m_flag[i]);
        }
    }

    pVector->m_rows = econtext->align_rows;

    return pVector;
}

static ScalarVector* ExecEvalVecRownum(
    RownumState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Assert(pSelection != NULL);
    ScalarValue* pDest = pVector->m_vals;
    int64 ps_rownum = exprstate->ps->ps_rownum;
    
    for (int i = 0; i < econtext->align_rows; i++) {
        SET_NOTNULL(pVector->m_flag[i]);
        pDest[i] = ++ps_rownum ;
    }

    pVector->m_rows = econtext->align_rows;

    return pVector;
}
// TRUE means we deal with or
// false means we deal with and
template <bool AndOrFLag>
ScalarVector* ExecEvalProcessAndOrLogic(
    BoolExprState* boolExpr, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    List* clauses = boolExpr->args;
    ListCell* clause = NULL;
    ScalarVector* tmpVec = &boolExpr->xprstate.tmpVector;
    ScalarVector* vec = NULL;
    int nrows = 0;
    int i;
    bool continueCompare = false;
    bool* tmpSelection = &boolExpr->tmpSelection[0];
    bool firstEnter = true;
    bool* pSel = pSelection;
    uint8* pFlag = NULL;
    ScalarValue* pVals = NULL;
    uint8* pDstFlag = pVector->m_flag;
    ScalarValue* pDstVal = pVector->m_vals;
    bool savedUseSelection = econtext->m_fUseSelection;

    tmpVec->m_rows = 0;  // reset the tmp scalar vector rows;
    Assert(pSelection != NULL);

    foreach (clause, clauses) {
        ExprState* clausestate = (ExprState*)lfirst(clause);

        vec = VectorExprEngine(clausestate, econtext, pSel, tmpVec, isDone);
        continueCompare = false;
        pVals = vec->m_vals;
        pFlag = vec->m_flag;

        if (!firstEnter) {
            Assert(nrows != 0);
            for (i = 0; i < nrows; i++) {
                if (pSel[i]) {
                    if (NOT_NULL(pFlag[i]) && pVals[i] == AndOrFLag) {
                        SET_NOTNULL(pDstFlag[i]);

                        // no need  further compare
                        pDstVal[i] = AndOrFLag;
                        pSel[i] = false;
                        continue;
                    } else if (IS_NULL(pFlag[i]))
                        SET_NULL(pDstFlag[i]);

                    continueCompare = true;
                }
            }
        } else {
            nrows = econtext->align_rows;
            for (i = 0; i < nrows; i++) {
                // copy the selection value.
                tmpSelection[i] = pSel[i];

                if (pSel[i]) {
                    if (NOT_NULL(pFlag[i]) && pVals[i] == AndOrFLag) {
                        SET_NOTNULL(pDstFlag[i]);
                        pDstVal[i] = AndOrFLag;

                        // no need to further compare
                        // flip the selection vector
                        tmpSelection[i] = false;
                        continue;
                    } else if (IS_NULL(pFlag[i]))
                        SET_NULL(pDstFlag[i]);
                    else {
                        pDstFlag[i] = pFlag[i];
                        pDstVal[i] = pVals[i];
                    }

                    continueCompare = true;
                }
            }

            firstEnter = false;
            pSel = tmpSelection;               // use the tmp selection now
            econtext->m_fUseSelection = true;  // When doing multi qualification, unqualified results should not be eval
        }

        if (!continueCompare)
            break;
    }

    econtext->m_fUseSelection = savedUseSelection;

    pVector->m_rows = nrows;
    return pVector;
}

void buildOneRowVector(ScalarVector* oneRowVector, Datum elem, Oid type)
{
    Assert(OidIsValid(type));

    ScalarValue val = ScalarVector::DatumToScalar(elem, type, false);

    oneRowVector->m_vals[0] = val;
    SET_NOTNULL(oneRowVector->m_flag[0]);

    oneRowVector->m_rows = 1;
    oneRowVector->m_desc.typeId = type;
}

/*
 * ExecEvalScalarArrayOp
 *
 * Evaluate "scalar op ANY/ALL (array)".  The operator always yields boolean,
 * and we combine the results across all array elements using OR and AND
 * (for ANY and ALL respectively).	Of course we short-circuit as soon as
 * the result is known.
 */
static ScalarVector* ExecEvalVecScalarArrayOp(ScalarArrayOpExprState* sstate, ExprContext* econtext, bool* pSelection,
    ScalarVector* pVector, ExprDoneCond* isDone)
{
    ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)sstate->fxprstate.xprstate.expr;
    bool useOr = opexpr->useOr;
    int i;
    int j;
    FunctionCallInfo fcinfo;
    ExprDoneCond argDone;
    ArrayType* arr = NULL;
    int nitems;
    int16 typlen;
    bool typbyval = false;
    char typalign;
    ScalarVector* thisResult = NULL;
    ScalarValue* vals = NULL;
    char* s = NULL;
    bits8* bitmap = NULL;
    uint32 bitmask;
    PgStat_FunctionCallUsage fcusage;
    bool elemNull = false;
    ScalarVector* arg0 = NULL;
    ScalarVector* arg1 = NULL;

    bool* pSel = sstate->pSel;
    bool savedUseSelection = econtext->m_fUseSelection;

    Assert(pSelection != NULL);
    if (sstate->fxprstate.func.fn_oid == InvalidOid) {
        DispatchVectorFunction(opexpr->opfuncid, opexpr->inputcollid, &sstate->fxprstate, econtext);
    }

    fcinfo = &sstate->fxprstate.fcinfo_data;
    argDone = ExecEvalVecFuncArgs(fcinfo, sstate->fxprstate.args, pSelection, econtext);
    Assert(argDone == ExprSingleResult);
    arg0 = (ScalarVector*)fcinfo->arg[0];
    arg1 = (ScalarVector*)fcinfo->arg[1];

    /* We expect that eval expr result could be empty vector, but could not be null */
    Assert(arg0 != NULL && arg1 != NULL);

    int rows = econtext->align_rows;

    for (i = 0; i < rows; i++) {
        pVector->m_vals[i] = BoolGetDatum(!useOr);
        SET_NOTNULL(pVector->m_flag[i]);
    }

    for (i = 0; i < BatchMaxSize; i++) {
        pSel[i] = true;
    }

    ScalarVector* argLeft = sstate->tmpVecLeft;
    ScalarVector* argRight = sstate->tmpVecRight;

    pVector->m_rows = rows;
    for (i = 0; i < rows; i++) {
        if (!pSelection[i]) {
            continue;
        }

        /*
         * If the array is NULL then we return NULL --- it's not very meaningful
         * to do anything else, even if the operator isn't strict.
         */
        if (IS_NULL(arg1->m_flag[i])) {
            SET_NULL(pVector->m_flag[i]);
            continue;
        }

        arr = DatumGetArrayTypeP(arg1->m_vals[i]);
        if (arr == NULL) {
            SET_NULL(pVector->m_flag[i]);
            continue;
        }

        nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
        if (nitems <= 0) {
            continue;
        } else {
            econtext->m_fUseSelection = true;
        }

        /*
         * If the scalar is NULL, and the function is strict, return NULL; no
         * point in iterating the loop.
         */
        if (IS_NULL(arg0->m_flag[i]) && sstate->fxprstate.func.fn_strict) {
            SET_NULL(pVector->m_flag[i]);
            pSel[i] = false;
            continue;
        }

        /* Get array infomation */
        if (sstate->element_type != ARR_ELEMTYPE(arr)) {
            get_typlenbyvalalign(ARR_ELEMTYPE(arr), &sstate->typlen, &sstate->typbyval, &sstate->typalign);
            sstate->element_type = ARR_ELEMTYPE(arr);
        }

        typlen = sstate->typlen;
        typbyval = sstate->typbyval;
        typalign = sstate->typalign;

        /* left argument shallow copy one row from arg0 */
        argLeft->m_flag[0] = arg0->m_flag[i];
        argLeft->m_vals[0] = arg0->m_vals[i];
        argLeft->m_rows = 1;
        fcinfo->arg[0] = (Datum)argLeft;

        /* Loop over the array elements */
        s = (char*)ARR_DATA_PTR(arr);
        bitmap = ARR_NULLBITMAP(arr);
        bitmask = 1;

        for (j = 0; j < nitems; j++) {
            Datum elt;

            /* Get array element, checking for NULL */
            if (bitmap && (*bitmap & bitmask) == 0) {
                elemNull = true;
                SET_NULL(argRight->m_flag[0]);
            } else {
                elemNull = false;
                elt = fetch_att(s, typbyval, typlen);
                s = att_addlength_pointer(s, typlen, s);
                s = (char*)att_align_nominal(s, typalign);
                buildOneRowVector(argRight, elt, sstate->element_type);
            }

            /* right argument build from array element */
            fcinfo->arg[1] = (Datum)argRight;

            /* Call comparison function */
            if (elemNull && sstate->fxprstate.func.fn_strict) {
                fcinfo->isnull = true;
            } else {
                fcinfo->isnull = false;
                if (econtext->m_fUseSelection) {
                    if (!pSel[i]) {
                        break;
                    }
                }
                pgstat_init_function_usage(fcinfo, &fcusage);
                fcinfo->arg[fcinfo->nargs] = 1;
                fcinfo->arg[fcinfo->nargs + 1] = PointerGetDatum(sstate->tmpVec);
                fcinfo->arg[fcinfo->nargs + 2] = (Datum)0;
                fcinfo->nargs += EXTRA_NARGS;

                thisResult = VecFunctionCallInvoke(fcinfo);
                fcinfo->nargs -= EXTRA_NARGS;
                pgstat_end_function_usage(&fcusage, true);
            }

            if (fcinfo->isnull) {
                if (econtext->m_fUseSelection) {
                    if (pSel[i])
                        SET_NULL(pVector->m_flag[i]);
                } else {
                    SET_NULL(pVector->m_flag[i]);
                }
            } else {
                /* Do some short circuit operation for "AND" and "OR" */
                vals = thisResult->m_vals;
                if (econtext->m_fUseSelection) {
                    if (useOr) {
                        if (pSel[i] && DatumGetBool(vals[0])) {
                            pVector->m_vals[i] = BoolGetDatum(true);
                            SET_NOTNULL(pVector->m_flag[i]);
                            pSel[i] = false;

                            /* needn't look at any more elements */
                            break;
                        }
                    } else {
                        if (pSel[i] && !DatumGetBool(vals[0])) {
                            pVector->m_vals[i] = BoolGetDatum(false);
                            SET_NOTNULL(pVector->m_flag[i]);
                            pSel[i] = false;

                            /* needn't look at any more elements */
                            break;
                        }
                    }
                } else {
                    if (useOr) {
                        if (DatumGetBool(vals[0])) {
                            pVector->m_vals[i] = BoolGetDatum(true);
                            SET_NOTNULL(pVector->m_flag[i]);
                            pSel[i] = false;

                            /* needn't look at any more elements */
                            break;
                        }
                    } else {
                        if (!DatumGetBool(vals[0])) {
                            pVector->m_vals[i] = BoolGetDatum(false);
                            SET_NOTNULL(pVector->m_flag[i]);
                            pSel[i] = false;

                            /* needn't look at any more elements */
                            break;
                        }
                    }
                }
            }

            /* advance bitmap pointer if any */
            if (bitmap != NULL) {
                bitmask <<= 1;
                if (bitmask == 0x100) {
                    bitmap++;
                    bitmask = 1;
                }
            }
        }
    }

    econtext->m_fUseSelection = savedUseSelection;
    pVector->m_desc.typeId = BOOLOID;
    return pVector;
}

/*
 * ExecEvalVecCaseTestExpr
 *
 * Return the value stored by CASE.
 */
static ScalarVector* ExecEvalVecCaseTestExpr(
    ExprState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    return econtext->caseValue_vector;
}

/* ----------------------------------------------------------------
 *		ExecEvalVecCase
 *
 *		Evaluate a CASE clause. Will have boolean expressions
 *		inside the WHEN clauses, and will have expressions
 *		for results.
 * ----------------------------------------------------------------
 */
template <bool hasCaseExpr>
ScalarVector* ExecEvalVecCase(
    CaseExprState* caseExpr, ExprContext* econtext, const bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    List* clauses = caseExpr->args;
    ListCell* clause = NULL;
    int i;
    ScalarVector* results = NULL;
    bool* sel = caseExpr->localSel;
    bool savedUseSelection = econtext->m_fUseSelection;
    bool* matchedResult = caseExpr->matchedResult;
    ScalarVector* save_vector = caseExpr->save_vector;
    ScalarVector* tmp_vector = &(caseExpr->xprstate.tmpVector);
    ScalarVector* vec = NULL;
    ScalarValue* pVal = NULL;
    uint8* pFlag = NULL;
    errno_t rc;
    bool all_false = true;

    Assert(pSelection != NULL);
    Assert(econtext->align_rows != 0);
    rc = memcpy_s(sel, BatchMaxSize * sizeof(bool), pSelection, econtext->align_rows * sizeof(bool));
    securec_check(rc, "\0", "\0");
    rc = memset_s(matchedResult, BatchMaxSize * sizeof(bool), 0, econtext->align_rows * sizeof(bool));
    securec_check(rc, "\0", "\0");

    // When eval CASE, we use selection to fillter matched results.
    econtext->m_fUseSelection = true;

    /*
     * If there's a test expression, we have to evaluate it and save the value
     * where the CaseTestExpr placeholders can find it. We must save and
     * restore prior setting of econtext's caseValue fields, in case this node
     * is itself within a larger CASE. Furthermore, don't assign to the
     * econtext fields until after returning from evaluation of the test
     * expression.  We used to pass &econtext->caseValue_vector to the
     * recursive call, but that leads to aliasing that variable within said
     * call, which can (and did) produce bugs when the test expression itself
     * contains a CASE.
     *
     * If there's no test expression, we don't actually need to save and
     * restore these fields; but it's less code to just do so unconditionally.
     */
    if (hasCaseExpr)
        save_vector->copy(econtext->caseValue_vector);

    if (hasCaseExpr) {
        tmp_vector->m_rows = econtext->align_rows;
        vec = VectorExprEngine(caseExpr->arg, econtext, sel, tmp_vector, isDone);
        econtext->caseValue_vector->copy(vec);
    }

    // We evaluate each of the WHEN clauses in turn, as soon as one is true we
    // mark the corresponding selection vector. If none are true then we return the
    // value of the default clause, or NULL if there is none.
    //
    foreach (clause, clauses) {
        CaseWhenState* wclause = (CaseWhenState*)lfirst(clause);

        tmp_vector->m_rows = econtext->align_rows;

        results = VectorExprEngine(wclause->expr, econtext, sel, tmp_vector, isDone);

        // The result shall be copy back to selection vector to compute the expression.
        // We borrow qual_results vector to save next applied selection vector.
        //
        pVal = results->m_vals;
        pFlag = results->m_flag;
        all_false = true;
        for (i = 0; i < econtext->align_rows; i++) {
            if (NOT_NULL(pFlag[i]))
                sel[i] = pVal[i] && sel[i];
            else
                sel[i] = false;

            matchedResult[i] = matchedResult[i] || sel[i];  // remember the pass one
            if (all_false && matchedResult[i])
                all_false = false;
        }

        // For each true test, we evaluate the corresponding result. A NULL result
        // from the test is not considered true. If none of the tuples has been
        // selected, we do not pass the results into the next level.
        //
        if (all_false) {
            for (i = 0; i < econtext->align_rows; i++) {
                sel[i] = pSelection[i] && (!matchedResult[i]);
            }
            continue;
        }

        vec = VectorExprEngine(wclause->result, econtext, sel, pVector, isDone);
        if (vec == NULL) {
            pVector->m_rows = econtext->align_rows = 0;
        } else if (vec != pVector) {
            pVector->m_rows = econtext->align_rows;
            pVector->copy(vec, sel);
        }

        // Flip the selection vector so these values are not affected
        //
        for (i = 0; i < econtext->align_rows; i++) {
            // when a result matched, we should set pSelection to 0, to avoid
            // eval again. note that we can not move it into last "if" because
            // when a result is matched, we want to change pSelection[i] of
            // every level, but when we move to up level, the sel is last eval
            // result, maybe 0, made pSelection not set to 0.
            sel[i] = pSelection[i] && (!matchedResult[i]);
        }
    }

    // only when not all of the tuples mathed, we need to calculate
    // the default expression.
    bool not_all_select = false;
    for (i = 0; i < econtext->align_rows; i++) {
        if (sel[i]) {
            not_all_select = true;
            break;
        }
    }

    if (hasCaseExpr)
        econtext->caseValue_vector->copy(save_vector);

    // Set to default value or NULL
    //
    if (caseExpr->defresult) {
        if (not_all_select) {
            vec = VectorExprEngine(caseExpr->defresult, econtext, sel, pVector, isDone);
            if (vec == NULL) {
                pVector->m_rows = econtext->align_rows = 0;
            } else if (vec != pVector) {
                pVector->m_rows = econtext->align_rows;
                pVector->copy(vec, sel);
            }
        }
    } else {
        for (i = 0; i < econtext->align_rows; i++) {
            if (sel[i])
                SET_NULL(pVector->m_flag[i]);
        }
    }

    econtext->m_fUseSelection = savedUseSelection;
    return pVector;
}

static ScalarVector* ExecEvalVecParamExtern(
    ExprState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Param* expression = (Param*)exprstate->expr;
    int thisParamId = expression->paramid;
    ParamListInfo paramInfo = econtext->ecxt_param_list_info;
    ParamExternData* prm = NULL;
    ScalarValue val;

    Assert(pSelection != NULL);
    Assert(econtext->align_rows != 0);
    /*
     * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
     */
    if (paramInfo && thisParamId > 0 && thisParamId <= paramInfo->numParams) {
        prm = &paramInfo->params[thisParamId - 1];

        /* give hook a chance in case parameter is dynamic */
        if (!OidIsValid(prm->ptype) && paramInfo->paramFetch != NULL)
            (*paramInfo->paramFetch)(paramInfo, thisParamId);

        if (OidIsValid(prm->ptype)) {
            /* safety check in case hook did something unexpected */

            if (prm->ptype != expression->paramtype)
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("Type of parameter %d (%s) does not match that when preparing the plan (%s)",
                            thisParamId,
                            format_type_be(prm->ptype),
                            format_type_be(expression->paramtype))));
        }

        /*
         * Extend the result to a const vector and return.
         * We are probably in a short-lived expression-evaluation context. Switch
         * to the per-query context for manipulating the const value is valid.
         */
        MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_estate->es_query_cxt);
        val = ScalarVector::DatumToScalar(prm->value, prm->ptype, prm->isnull);
        (void)MemoryContextSwitchTo(oldContext);
        if (econtext->m_fUseSelection) {
            if (prm->isnull) {
                for (int i = 0; i < econtext->align_rows; i++) {
                    if (pSelection[i])
                        SET_NULL(pVector->m_flag[i]);
                }
            } else {
                for (int i = 0; i < econtext->align_rows; i++) {
                    if (pSelection[i]) {
                        pVector->m_vals[i] = val;
                        SET_NOTNULL(pVector->m_flag[i]);
                    }
                }
            }
        } else {
            if (prm->isnull) {
                for (int i = 0; i < econtext->align_rows; i++) {
                    SET_NULL(pVector->m_flag[i]);
                }
            } else {
                for (int i = 0; i < econtext->align_rows; i++) {
                    pVector->m_vals[i] = val;
                    SET_NOTNULL(pVector->m_flag[i]);
                }
            }
        }

        pVector->m_rows = econtext->align_rows;

        return pVector;
    }

    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("No value found for parameter %d", thisParamId)));

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecEvalVecParamExec
 *
 *		Returns the value of a PARAM_EXEC parameter.
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecParamExec(
    ExprState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Param* expression = (Param*)exprstate->expr;
    int thisParamId = expression->paramid;
    ParamExecData* prm = NULL;
    ScalarValue val;
    MemoryContext oldContext = NULL;
    ScalarDesc desc;
    ScalarVector* paramVec = NULL;
    Assert(econtext->align_rows != 0);

    // PARAM_EXEC params (internal executor parameters) are stored in the
    // ecxt_param_exec_vals array, and can be accessed by array index.
    //
    prm = &(econtext->ecxt_param_exec_vals[thisParamId]);

    prm->valueType = expression->paramtype;

    if (unlikely(prm->paramVector == NULL)) {
        oldContext = MemoryContextSwitchTo(econtext->ecxt_estate->es_query_cxt);
        paramVec = New(CurrentMemoryContext) ScalarVector();
        desc.typeId = prm->valueType;
        desc.encoded = COL_IS_ENCODE(desc.typeId);
        paramVec->init(CurrentMemoryContext, desc);
        prm->paramVector = paramVec;
        (void)MemoryContextSwitchTo(oldContext);
    }
    // if the parameter is changed
    // or parameter is const but the vector is not initialized
    // or init plan has not been evaluated
    if (prm->isChanged || (prm->isConst && prm->paramVector == NULL) || prm->execPlan != NULL ||
        (prm->isConst == false && prm->execPlan == NULL)) {
        paramVec = (ScalarVector*)prm->paramVector;

        if (prm->execPlan != NULL) {
            // Invoke the row interface to get a parameter evaluated
            //
            ExecSetParamPlan((SubPlanState*)prm->execPlan, econtext);

            // ExecSetParamPlan should have processed this param
            //
            prm->isConst = true;
            prm->valueType = expression->paramtype;
            Assert(prm->execPlan == NULL);
        }
        paramVec->m_desc.typeId = prm->valueType;
        paramVec->m_desc.encoded = COL_IS_ENCODE(paramVec->m_desc.typeId);

        /*
         * Extend the result to a const vector and return.
         * We are probably in a short-lived expression-evaluation context. Switch
         * to the per-query context for manipulating the const value is valid.
         */
        oldContext = MemoryContextSwitchTo(econtext->ecxt_estate->es_query_cxt);
        val = ScalarVector::DatumToScalar(prm->value, prm->valueType, prm->isnull);
        (void)MemoryContextSwitchTo(oldContext);
        if (prm->isnull) {
            for (int i = 0; i < BatchMaxSize; i++)
                SET_NULL(paramVec->m_flag[i]);
        } else {
            for (int i = 0; i < BatchMaxSize; i++) {
                paramVec->m_vals[i] = val;
                SET_NOTNULL(paramVec->m_flag[i]);
            }
        }

        paramVec->m_rows = BatchMaxSize;
        prm->isChanged = false;
        prm->isConst = true;
    }

    ((ScalarVector*)prm->paramVector)->m_rows = econtext->align_rows;
    return (ScalarVector*)prm->paramVector;
}
/*
 * Evaluate arguments for a function.
 */
static ExprDoneCond ExecEvalVecFuncArgs(FunctionCallInfo fcinfo, List* argList, bool* pSelection, ExprContext* econtext)
{
    int i;
    ListCell* arg = NULL;
    ScalarVector* colVector = NULL;

    i = 0;
    foreach (arg, argList) {
        ExprState* argstate = (ExprState*)lfirst(arg);
        colVector = &fcinfo->argVector[i];
        colVector->m_rows = econtext->align_rows;
        fcinfo->arg[i] = (Datum)VectorExprEngine(argstate, econtext, pSelection, colVector, NULL);
        fcinfo->argTypes[i] = argstate->resultType;
        i++;
    }

    Assert(i == fcinfo->nargs);
    return ExprSingleResult;
}

/*
 * Evaluate arguments for a vector set function.
 */
static ExprDoneCond ExecEvalVecSetFuncArgs(
    FunctionCallInfo fcinfo, List* argList, bool* pSelection, ExprContext* econtext)
{
    int i;
    ListCell* arg = NULL;
    ScalarVector* colVector = NULL;

    ExprDoneCond argIsDone = ExprSingleResult; /* default assumption */

    i = 0;
    foreach (arg, argList) {
        ExprState* argstate = (ExprState*)lfirst(arg);
        colVector = &fcinfo->argVector[i];
        colVector->m_rows = econtext->align_rows;

        ExprDoneCond thisArgIsDone = ExprSingleResult;

        fcinfo->arg[i] = (Datum)VectorExprEngine(argstate, econtext, pSelection, colVector, &thisArgIsDone);
        fcinfo->argTypes[i] = argstate->resultType;
        i++;

        if (thisArgIsDone != ExprSingleResult) {
            /*
             * We allow only one argument to have a set value; we'd need much
             * more complexity to keep track of multiple set arguments (cf.
             * ExecTargetList) and it doesn't seem worth it.
             */
            if (argIsDone != ExprSingleResult)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("functions and operators can take at most one set argument")));
            argIsDone = thisArgIsDone;
        }
    }

    Assert(i == fcinfo->nargs);
    return argIsDone;
}

/*
 * @Description: Evaluate a least clause or greatest clause.
 * @param[IN] minmaxExpr: MinMaxExprState node for vec executor.
 * @param[IN] econtext: ExprContext for current clause.
 * @param[IN] pSelection: Mark each rows of batch have been selected.
 * @param[IN] pVector: Store the final results.
 * @Return: return the final results - pVector.
 */
static ScalarVector* ExecEvalVecMinMax(
    MinMaxExprState* minmaxExpr, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Assert(pSelection != NULL);

    MinMaxExpr* minmax = (MinMaxExpr*)minmaxExpr->xprstate.expr;
    FunctionCallInfoData* locfcinfo = &minmaxExpr->cinfo;
    MinMaxOp op = minmax->op;
    ScalarVector* vec = NULL;
    ScalarVector* result = NULL;
    ListCell* arg = NULL;
    int cmpresult, i;

    locfcinfo->argnull[0] = false;
    locfcinfo->argnull[1] = false;

    Assert(econtext->align_rows != 0);

    /*
     * Set minmaxExpr->cmpresult to be NULL
     * Set pVector to be NULL by pSelection
     */
    if (econtext->m_fUseSelection) {
        for (i = 0; i < econtext->align_rows; i++) {
            if (pSelection[i])
                SET_NULL(pVector->m_flag[i]);

            SET_NULL(minmaxExpr->cmpresult->m_flag[i]);
        }
    } else {
        for (i = 0; i < econtext->align_rows; i++) {
            SET_NULL(pVector->m_flag[i]);
            SET_NULL(minmaxExpr->cmpresult->m_flag[i]);
        }
    }

    /* Calculate each arg of minmaxExpr->args */
    foreach (arg, minmaxExpr->args) {
        ExprState* e = (ExprState*)lfirst(arg);
        minmaxExpr->argvec->m_rows = 0;
        vec = VectorExprEngine(e, econtext, pSelection, minmaxExpr->argvec, isDone);

        /*
         * Copy the vec data to pVector, avoid
         * real value compare with NULL value.
         */
        pVector->m_rows = econtext->align_rows;
        if (econtext->m_fUseSelection) {
            for (i = 0; i < econtext->align_rows; i++) {
                if (pSelection[i] && NOT_NULL(vec->m_flag[i]) && IS_NULL(pVector->m_flag[i])) {
                    pVector->m_vals[i] = vec->m_vals[i];
                    SET_NOTNULL(pVector->m_flag[i]);
                }
            }
        } else {
            for (i = 0; i < econtext->align_rows; i++) {
                if (NOT_NULL(vec->m_flag[i]) && IS_NULL(pVector->m_flag[i])) {
                    pVector->m_vals[i] = vec->m_vals[i];
                    SET_NOTNULL(pVector->m_flag[i]);
                }
            }
        }

        /* Call the cmp function to get the result */
        locfcinfo->arg[0] = (Datum)pVector;
        locfcinfo->arg[1] = (Datum)vec;
        locfcinfo->isnull = false;
        locfcinfo->arg[locfcinfo->nargs] = econtext->align_rows;
        locfcinfo->arg[locfcinfo->nargs + 1] = (Datum)minmaxExpr->cmpresult;
        locfcinfo->arg[locfcinfo->nargs + 2] = PointerGetDatum(pSelection);
        locfcinfo->nargs += EXTRA_NARGS;
        result = VecFunctionCallInvoke(locfcinfo);
        locfcinfo->nargs -= EXTRA_NARGS;

        /* Refresh pVector value by result and vec */
        if (econtext->m_fUseSelection) {
            for (i = 0; i < econtext->align_rows; i++) {
                if (pSelection[i] && NOT_NULL(result->m_flag[i])) {
                    cmpresult = DatumGetInt32(result->m_vals[i]);
                    if (cmpresult > 0 && op == IS_LEAST) {
                        SET_NOTNULL(pVector->m_flag[i]);
                        pVector->m_vals[i] = vec->m_vals[i];
                    } else if (cmpresult < 0 && op == IS_GREATEST) {
                        SET_NOTNULL(pVector->m_flag[i]);
                        pVector->m_vals[i] = vec->m_vals[i];
                    }
                }
            }
        } else {
            for (i = 0; i < econtext->align_rows; i++) {
                if (NOT_NULL(result->m_flag[i])) {
                    cmpresult = DatumGetInt32(result->m_vals[i]);
                    if (cmpresult > 0 && op == IS_LEAST) {
                        SET_NOTNULL(pVector->m_flag[i]);
                        pVector->m_vals[i] = vec->m_vals[i];
                    } else if (cmpresult < 0 && op == IS_GREATEST) {
                        SET_NOTNULL(pVector->m_flag[i]);
                        pVector->m_vals[i] = vec->m_vals[i];
                    }
                }
            }
        }
    }
    return pVector;
}

/* ----------------------------------------------------------------
 *		ExecEvalRowCompare - ROW() comparison-op ROW()
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecRowCompare(RowCompareExprState* rstate, ExprContext* econtext, const bool* pSelection,
    ScalarVector* pVector, ExprDoneCond* isDone)
{
    ScalarVector* result = NULL;
    RowCompareType rctype = ((RowCompareExpr*)rstate->xprstate.expr)->rctype;
    ListCell* l = NULL;
    ListCell* r = NULL;
    int i;
    ScalarVector* left_vec = NULL;
    ScalarVector* right_vec = NULL;
    bool* pSel = rstate->pSel;
    errno_t rc = 0;

    if (econtext->m_fUseSelection)
        rc = memcpy_s(pSel, sizeof(bool) * BatchMaxSize, pSelection, sizeof(bool) * econtext->align_rows);
    else
        rc = memset_s(pSel, sizeof(bool) * BatchMaxSize, 1, sizeof(bool) * econtext->align_rows);
    securec_check(rc, "\0", "\0");
    Assert(econtext->align_rows != 0);

    i = 0;
    forboth(l, rstate->largs, r, rstate->rargs)
    {
        ExprState* le = (ExprState*)lfirst(l);
        ExprState* re = (ExprState*)lfirst(r);
        FunctionCallInfoData* locfcinfo = &rstate->cinfo[i];

        left_vec = VectorExprEngine(le, econtext, pSel, rstate->left_argvec, isDone);
        right_vec = VectorExprEngine(re, econtext, pSel, rstate->right_argvec, isDone);
        locfcinfo->arg[0] = (Datum)left_vec;
        locfcinfo->arg[1] = (Datum)right_vec;
        locfcinfo->isnull = false;

        locfcinfo->arg[locfcinfo->nargs] = Min(left_vec->m_rows, right_vec->m_rows);
        locfcinfo->arg[locfcinfo->nargs + 1] = (Datum)rstate->cmpresult;
        locfcinfo->arg[locfcinfo->nargs + 2] = PointerGetDatum(pSel);
        locfcinfo->nargs += EXTRA_NARGS;
        result = VecFunctionCallInvoke(locfcinfo);
        locfcinfo->nargs -= EXTRA_NARGS;

        pVector->m_rows = econtext->align_rows;

        for (int k = 0; k < econtext->align_rows; k++) {
            if (pSel[k]) {
                if (IS_NULL(result->m_flag[k])) {
                    SET_NULL(pVector->m_flag[k]);
                    pSel[k] = false;
                    continue;
                } else {
                    int cmpresult = DatumGetInt32(result->m_vals[k]);
                    bool bool_result = false;
                    if (cmpresult != 0)
                        pSel[k] = false;
                    switch (rctype) {
                            /* EQ and NE cases aren't allowed here */
                        case ROWCOMPARE_LT:
                            bool_result = (cmpresult < 0);
                            break;
                        case ROWCOMPARE_LE:
                            bool_result = (cmpresult <= 0);
                            break;
                        case ROWCOMPARE_GE:
                            bool_result = (cmpresult >= 0);
                            break;
                        case ROWCOMPARE_GT:
                            bool_result = (cmpresult > 0);
                            break;
                        default:
                            ereport(ERROR,
                                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                    errmodule(MOD_VEC_EXECUTOR),
                                    errmsg("Unrecognized RowCompareType: %d", (int)rctype)));
                            result = 0; /* keep compiler quiet */
                            break;
                    }
                    SET_NOTNULL(pVector->m_flag[k]);
                    pVector->m_vals[k] = BoolGetDatum(bool_result);
                }
            }
        }
        i++;
    }
    return pVector;
}

/* ----------------------------------------------------------------
 *		ExecEvalVecCoalesce
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecCoalesce(CoalesceExprState* coalesceExpr, ExprContext* econtext, const bool* pSelection,
    ScalarVector* pVector, ExprDoneCond* isDone)
{
    ListCell* arg = NULL;
    ScalarVector* vec = NULL;
    int i;
    bool* pSel = coalesceExpr->pSel;
    bool savedUseSelection = econtext->m_fUseSelection;
    errno_t rc;
    bool breakForward = true;
    econtext->m_fUseSelection = true;

    Assert(pSelection != NULL);
    rc = memcpy_s(pSel, sizeof(bool) * BatchMaxSize, pSelection, sizeof(bool) * econtext->align_rows);
    securec_check(rc, "\0", "\0");

    foreach (arg, coalesceExpr->args) {
        ExprState* e = (ExprState*)lfirst(arg);
        vec = VectorExprEngine(e, econtext, pSel, pVector, isDone);
        breakForward = true;

        // If eval result is not put in pVector(i.e. ExecEvalVecVar), do a copy
        // Note that we should not copy val whose pSel is false, which means it
        // has been evaled and should not be touched any more and we only deal
        // with the actual number of rows.
        if (vec != pVector)
            pVector->copy(vec, pSel);

        if (econtext->m_fUseSelection) {
            for (i = 0; i < pVector->m_rows; i++) {
                if (pSelection[i]) {
                    if (!IS_NULL(pVector->m_flag[i]))
                        pSel[i] = false;
                    else
                        breakForward = false;
                }
            }
        } else {
            for (i = 0; i < pVector->m_rows; i++) {
                if (!IS_NULL(pVector->m_flag[i]))
                    pSel[i] = false;
                else
                    breakForward = false;
            }
        }
        /* all data of pVector is not NULL, no need to calculate remaining args */
        if (breakForward)
            break;
    }

    econtext->m_fUseSelection = savedUseSelection;
    return pVector;
}

/* Compute hash value according to every column */
template <bool bUseSelection>
static void get_distinct_value(
    ScalarVector* result, ScalarVector* v1, ScalarVector* v2, ScalarVector* pVector, const bool* pSelection)
{
    for (int i = 0; i < result->m_rows; i++) {
        /* If bUseSelection is true, we need use pSelection. */
        if (bUseSelection == false || pSelection[i]) {
            bool isnull1 = IS_NULL(v1->m_flag[i]);
            bool isnull2 = IS_NULL(v2->m_flag[i]);
            if (isnull1 && isnull2) {
                /* Both NULL, no distinct ... */
                pVector->m_vals[i] = BoolGetDatum(FALSE);
            } else if (isnull1 || isnull2) {
                /* Only One NULL, is distinct */
                pVector->m_vals[i] = BoolGetDatum(TRUE);
            } else {
                /* Must invert result of "=" */
                pVector->m_vals[i] = BoolGetDatum(!DatumGetBool(result->m_vals[i]));
            }
            SET_NOTNULL(pVector->m_flag[i]);

        } else
            SET_NULL(pVector->m_flag[i]);
    }
}

static ScalarVector* ExecEvalVecDistinct(
    FuncExprState* fcache, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    ScalarVector* result = NULL;
    FunctionCallInfo fcinfo;
    ScalarVector* v1 = NULL;
    ScalarVector* v2 = NULL;
    int actualSize;
    List* arguments = NIL;
    PgStat_FunctionCallUsage fcusage;

    Assert(pSelection != NULL);

    /*
     * Initialize function cache if first time through
     */
    if (fcache->func.fn_oid == InvalidOid) {
        DistinctExpr* op = (DistinctExpr*)fcache->xprstate.expr;

        DispatchVectorFunction(op->opfuncid, op->inputcollid, fcache, econtext);

        Assert(!fcache->func.fn_retset);
    }

    fcinfo = &fcache->fcinfo_data;
    arguments = fcache->args;
    Assert(fcinfo->nargs == 2);
    (void)ExecEvalVecFuncArgs(fcinfo, arguments, pSelection, econtext);
    v1 = (ScalarVector*)fcinfo->arg[0];
    v2 = (ScalarVector*)fcinfo->arg[1];
    actualSize = econtext->align_rows;

    Assert(actualSize == Min(v1->m_rows, v2->m_rows));

    pgstat_init_function_usage(fcinfo, &fcusage);
    fcinfo->arg[fcinfo->nargs] = actualSize;
    fcinfo->arg[fcinfo->nargs + 1] = PointerGetDatum(pVector);
    fcinfo->arg[fcinfo->nargs + 2] = (Datum)(econtext->m_fUseSelection ? PointerGetDatum(pSelection) : 0);
    fcinfo->nargs += EXTRA_NARGS;
    fcinfo->isnull = false;

    result = VecFunctionCallInvoke(fcinfo);
    fcinfo->nargs -= EXTRA_NARGS;
    pgstat_end_function_usage(&fcusage, true);

    pVector->m_rows = result->m_rows;

    if (econtext->m_fUseSelection)
        get_distinct_value<true>(result, v1, v2, pVector, pSelection);
    else
        get_distinct_value<false>(result, v1, v2, pVector, pSelection);

    return pVector;
}

static ScalarVector* ExecEvalVecNullIf(
    FuncExprState* fcache, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    List* arguments = NIL;
    ScalarVector* result = NULL;
    FunctionCallInfo fcinfo;
    ScalarVector* v1 = NULL;
    int actualSize;
    PgStat_FunctionCallUsage fcusage;
    ScalarVector* tmpVec = fcache->tmpVec;
    Assert(econtext->align_rows != 0);

    Assert(pSelection != NULL);
    /*
     * Initialize function cache if first time through
     */
    if (fcache->func.fn_oid == InvalidOid) {
        NullIfExpr* op = (NullIfExpr*)fcache->xprstate.expr;

        DispatchVectorFunction(op->opfuncid, op->inputcollid, fcache, econtext);

        Assert(!fcache->func.fn_retset);
    }

    fcinfo = &fcache->fcinfo_data;
    arguments = fcache->args;
    Assert(fcinfo->nargs == 2);
    (void)ExecEvalVecFuncArgs(fcinfo, arguments, pSelection, econtext);
    v1 = (ScalarVector*)fcinfo->arg[0];
    actualSize = econtext->align_rows;

    Assert(actualSize >= 0);

    pgstat_init_function_usage(fcinfo, &fcusage);
    fcinfo->arg[fcinfo->nargs] = actualSize;
    fcinfo->arg[fcinfo->nargs + 1] = PointerGetDatum(tmpVec);
    fcinfo->arg[fcinfo->nargs + 2] = (Datum)(econtext->m_fUseSelection ? PointerGetDatum(pSelection) : 0);
    fcinfo->nargs += EXTRA_NARGS;
    fcinfo->isnull = false;

    result = VecFunctionCallInvoke(fcinfo);
    fcinfo->nargs -= EXTRA_NARGS;
    pgstat_end_function_usage(&fcusage, true);

    if (econtext->m_fUseSelection)
        pVector->copy(v1, pSelection);
    else
        pVector->copy(v1);

    if (econtext->m_fUseSelection) {
        for (int i = 0; i < result->m_rows; i++) {
            if (pSelection[i]) {
                if (NOT_NULL(result->m_flag[i]) && result->m_vals[i] == 1)
                    SET_NULL(pVector->m_flag[i]);
            }
        }
    } else {
        for (int i = 0; i < result->m_rows; i++) {
            if (NOT_NULL(result->m_flag[i]) && result->m_vals[i] == 1)
                SET_NULL(pVector->m_flag[i]);
        }
    }
    return pVector;
}

static ScalarVector* ExecEvalVecNullTest(
    NullTestState* nstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{

    NullTest* ntest = (NullTest*)nstate->xprstate.expr;
    ScalarVector* results = NULL;
    int i;
    bool postiveFlag = false;
    bool negativeFlag = false;
    Assert(econtext->align_rows != 0);

    Assert(pSelection != NULL);
    switch (ntest->nulltesttype) {
        case IS_NULL:
            postiveFlag = true;
            negativeFlag = false;
            break;
        case IS_NOT_NULL:
            postiveFlag = false;
            negativeFlag = true;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
    }

    results = VectorExprEngine(nstate->arg, econtext, pSelection, pVector, isDone);

    if (results == NULL) {
       econtext->align_rows = 0;
    }

    if (econtext->m_fUseSelection) {
        for (i = 0; i < econtext->align_rows; i++) {
            if (pSelection[i]) {
                if (IS_NULL(results->m_flag[i]))
                    pVector->m_vals[i] = postiveFlag;
                else
                    pVector->m_vals[i] = negativeFlag;

                SET_NOTNULL(pVector->m_flag[i]);
            }
        }
    } else {
        for (i = 0; i < econtext->align_rows; i++) {
            if (IS_NULL(results->m_flag[i]))
                pVector->m_vals[i] = postiveFlag;
            else
                pVector->m_vals[i] = negativeFlag;

            SET_NOTNULL(pVector->m_flag[i]);
        }
    }

    pVector->m_rows = econtext->align_rows;
    pVector->m_desc.typeId = BOOLOID;
    pVector->m_desc.encoded = false;
    return pVector;
}

/* Compute hash value according to every column */
template <bool bUseSelection>
static void get_hash_value(Oid vartype, ScalarVector* results, ScalarVector* pVector, const bool* pSelection,
    bool* isAllNull, Datum* hashValue)
{
    int rowindex;
    Datum val = 0;

    for (rowindex = 0; rowindex < results->m_rows; rowindex++) {
        /* If bUseSelection is true, we need use pSelection. */
        if (bUseSelection == false || pSelection[rowindex]) {
            /* If get val is null and the current pgxc node id is node1, we should return the val. */
            if (NOT_NULL(results->m_flag[rowindex])) {
                val = results->m_vals[rowindex];

                /* For vector data, the data types (INTERVALOID and TIMETZOID)
                 * adds a byte to store the length of the data
                 */
                if (vartype == INTERVALOID || vartype == TIMETZOID) {
                    val = PointerGetDatum((char*)val + VARHDRSZ_SHORT);
                }

                /* If there are more than one column combine to compute hash value, the method of compute for
                 * the first column is different with followed column.
                 */
                hashValue[rowindex] =
                    hashValueCombination(hashValue[rowindex], vartype, (Datum)val, isAllNull[rowindex]);
                isAllNull[rowindex] = false;
            }
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecEvalVecHashFilter
 *
 *		Evaluate a HashFilter node.
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecHashFilter(
    HashFilterState* hfstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    HashFilter* htest = (HashFilter*)hfstate->xprstate.expr;
    ScalarVector* results = NULL;
    int modulo = 0;
    int nodeIndex = 0;
    int rowindex = 0;
    int ret = 0;
    ListCell* distkey = NULL;
    ListCell* vartypes = NULL;
    Datum hashValue[BatchMaxSize] = {0};
    bool isAllNull[BatchMaxSize];
    Assert(econtext->align_rows != 0);

    ret = memset_s(isAllNull, BatchMaxSize * sizeof(bool), true, econtext->align_rows * sizeof(bool));
    securec_check(ret, "\0", "\0");

    /* Get every column value. */
    forboth(distkey, hfstate->arg, vartypes, htest->typeOids)
    {
        ExprState* e = (ExprState*)lfirst(distkey);
        Oid vartype = (Oid)lfirst_oid(vartypes);

        /* Get Scalar row value for a distribute key */
        pVector->m_rows = econtext->align_rows;
        results = VectorExprEngine(e, econtext, pSelection, pVector, isDone);

        if (econtext->m_fUseSelection)
            get_hash_value<true>(vartype, results, pVector, pSelection, (bool*)isAllNull, (Datum*)hashValue);
        else
            get_hash_value<false>(vartype, results, pVector, pSelection, (bool*)isAllNull, (Datum*)hashValue);
    }

    int null_value_dn_index = (NULL != hfstate->nodelist) ? hfstate->nodelist[0]
                                                          : /* fetch first dn in group's dn list */
                                  0;                        /* fetch first dn index */

    /* Get hash value for each row, and deside whether it filter or not. */
    for (rowindex = 0; rowindex < econtext->align_rows; rowindex++) {
        if (isAllNull[rowindex]) {
            if (null_value_dn_index == u_sess->pgxc_cxt.PGXCNodeId) {
                pVector->m_vals[rowindex] = true;
                SET_NOTNULL(pVector->m_flag[rowindex]);
            } else {
                /* Others the val should be reject */
                pVector->m_vals[rowindex] = false;
                SET_NULL(pVector->m_flag[rowindex]);
            }
        } else {
            // pick up exec node based on bucket list in pgxc_group
            modulo = hfstate->bucketMap[(unsigned int)abs((int)hashValue[rowindex]) & 
                                        (uint32)(hfstate->bucketCnt - 1)];
            nodeIndex = hfstate->nodelist[modulo];

            SET_NOTNULL(pVector->m_flag[rowindex]);
            /* Look into the handles and return correct position in array */
            if (nodeIndex == u_sess->pgxc_cxt.PGXCNodeId)
                pVector->m_vals[rowindex] = true;
            else
                pVector->m_vals[rowindex] = false;
        }
    }

    pVector->m_rows = econtext->align_rows;
    pVector->m_desc.typeId = BOOLOID;
    pVector->m_desc.encoded = false;
    return pVector;
}

template <BoolTestType booltesttype>
static ScalarVector* ExecEvalVecBooleanTest(
    GenericExprState* bstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    BooleanTest* btest = (BooleanTest*)bstate->xprstate.expr;
    ScalarVector* vec = NULL;
    uint8* resultFlag = NULL;
    uint8* outFlag = pVector->m_flag;
    ScalarValue* resultValues = NULL;
    ScalarValue* outValues = pVector->m_vals;
    int i;
    Assert(econtext->align_rows != 0);

    Assert(pSelection != NULL);
    vec = VectorExprEngine(bstate->arg, econtext, pSelection, pVector, isDone);
    resultFlag = vec->m_flag;
    resultValues = vec->m_vals;
    pVector->m_rows = econtext->align_rows;

    if (econtext->m_fUseSelection) {
        for (i = 0; i < pVector->m_rows; i++) {
            if (pSelection[i]) {
                switch (booltesttype) {
                    case IS_TRUE:
                        if (IS_NULL(resultFlag[i]))
                            outValues[i] = false;
                        else
                            outValues[i] = resultValues[i];

                        break;
                    case IS_NOT_TRUE:
                        if (IS_NULL(resultFlag[i]))
                            outValues[i] = true;
                        else
                            outValues[i] = !resultValues[i];

                        break;
                    case IS_FALSE:
                        if (IS_NULL(resultFlag[i]))
                            outValues[i] = false;
                        else
                            outValues[i] = !resultValues[i];

                        break;
                    case IS_NOT_FALSE:
                        if (IS_NULL(resultFlag[i]))
                            outValues[i] = true;
                        else
                            outValues[i] = resultValues[i];

                        break;
                    case IS_UNKNOWN:
                        outValues[i] = IS_NULL(resultFlag[i]);
                        break;
                    case IS_NOT_UNKNOWN:
                        outValues[i] = !IS_NULL(resultFlag[i]);
                        break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmodule(MOD_VEC_EXECUTOR),
                                errmsg("unrecognized booltesttype: %d", (int)btest->booltesttype)));
                }

                SET_NOTNULL(outFlag[i]);
            }
        }
    } else {
        for (i = 0; i < pVector->m_rows; i++) {
            switch (booltesttype) {
                case IS_TRUE:
                    if (IS_NULL(resultFlag[i]))
                        outValues[i] = false;
                    else
                        outValues[i] = resultValues[i];

                    break;
                case IS_NOT_TRUE:
                    if (IS_NULL(resultFlag[i]))
                        outValues[i] = true;
                    else
                        outValues[i] = !resultValues[i];

                    break;
                case IS_FALSE:
                    if (IS_NULL(resultFlag[i]))
                        outValues[i] = false;
                    else
                        outValues[i] = !resultValues[i];

                    break;
                case IS_NOT_FALSE:
                    if (IS_NULL(resultFlag[i]))
                        outValues[i] = true;
                    else
                        outValues[i] = resultValues[i];

                    break;
                case IS_UNKNOWN:
                    outValues[i] = IS_NULL(resultFlag[i]);
                    break;
                case IS_NOT_UNKNOWN:
                    outValues[i] = !IS_NULL(resultFlag[i]);
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_VEC_EXECUTOR),
                            errmsg("unrecognized booltesttype: %d", (int)btest->booltesttype)));
            }

            SET_NOTNULL(outFlag[i]);
        }
    }

    return pVector;
}

template <bool fnStrict, Oid retType, bool fenced>
static ScalarVector* GenericFunctionT(PG_FUNCTION_ARGS)
{
    int32 i, j;
    int32 cArgs = fcinfo->nargs - EXTRA_NARGS;
    int32 nvalues = PG_GETARG_INT32(cArgs);
    ScalarValue* presult = PG_GETARG_VECVAL(cArgs + 1);
    ScalarVector* presultVector = PG_GETARG_VECTOR(cArgs + 1);
    bool* pselection = PG_GETARG_SELECTION(cArgs + 2);
    uint8* presultFlag = presultVector->m_flag;
    Datum result;
    PGFunction RowFunction;
    GenericFunRuntime* genericRuntime = fcinfo->flinfo->genericRuntime;
    GenericFunRuntimeArg* genericRuntimeArgs = genericRuntime->args;
    FunctionCallInfoData* rowcinfo = genericRuntime->internalFinfo;
    ScalarVector* pArgVector = NULL;
    bool* restricFlag = &genericRuntime->restrictFlag[0];
    Oid returnType;
    int matchedRows = 0;

    rowcinfo->udfInfo.argBatchRows = 0;
    RowFunction = fcinfo->flinfo->fn_addr;
    returnType = fcinfo->flinfo->fn_rettype;

    if (nvalues > BatchMaxSize) {
        ereport(ERROR,
            (errmodule(MOD_UDF),
                errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                errmsg("nvalues: %d is invalid", nvalues)));
    }

    if (fnStrict) {
        for (i = 0; i < nvalues; i++) {
            restricFlag[i] = true;
            for (j = 0; j < cArgs; j++) {
                pArgVector = *(genericRuntimeArgs[j].arg);
                if (IS_NULL(pArgVector->m_flag[i])) {
                    restricFlag[i] = false;
                    break;
                }
            }
        }
    }
    // Invoke a row function for each value
    //
    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (fnStrict && restricFlag[i] == false)
                    SET_NULL(presultFlag[i]);
                else {
                    for (j = 0; j < cArgs; j++) {
                        pArgVector = *(genericRuntimeArgs[j].arg);
                        if (NOT_NULL(pArgVector->m_flag[i])) {
                            rowcinfo->arg[j] = genericRuntimeArgs[j].getArgFun(&pArgVector->m_vals[i]);
                            rowcinfo->argnull[j] = false;
                        } else {
                            // keep the same with row function for non strict function.
                            if (fnStrict == false)
                                rowcinfo->arg[j] = 0;

                            rowcinfo->argnull[j] = true;
                        }
                        if (fenced) {
                            if (matchedRows >= rowcinfo->udfInfo.allocRows)
                                ereport(ERROR,
                                    (errmodule(MOD_UDF),
                                        errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                                        errmsg("udfInfo->allocRows: %d is not enough", rowcinfo->udfInfo.allocRows)));
                            rowcinfo->udfInfo.arg[matchedRows][j] = rowcinfo->arg[j];
                            rowcinfo->udfInfo.null[matchedRows][j] = rowcinfo->argnull[j];
                        }
                    }
                    /* Just cache argument value for Fenced mode, So skip it */
                    if (!fenced) {
                        rowcinfo->isnull = false;
                        result = RowFunction(rowcinfo);
                        if (rowcinfo->isnull == false) {
                            presult[i] = ScalarVector::DatumToScalarT<retType>(result, false);
                            SET_NOTNULL(presultFlag[i]);
                        } else
                            SET_NULL(presultFlag[i]);
                    }
                    matchedRows++;
                    Assert(matchedRows <= nvalues);
                }
            }
        }

        /* Batch run and get result */
        if (fenced) {
            rowcinfo->udfInfo.argBatchRows = matchedRows;
            RowFunction(rowcinfo);
            Datum* resultPtr = rowcinfo->udfInfo.result;
            bool* resultIsNull = rowcinfo->udfInfo.resultIsNull;
            int pos = 0;
            for (i = 0; i < nvalues; i++) {
                if (pselection[i]) {
                    if (!(fnStrict && restricFlag[i] == false)) {
                        if (resultIsNull[pos] == false) {
                            presult[i] = ScalarVector::DatumToScalarT<retType>(resultPtr[pos], false);
                            SET_NOTNULL(presultFlag[i]);
                        } else
                            SET_NULL(presultFlag[i]);
                        pos++;
                    }
                }
            }
            Assert(matchedRows == pos);
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (fnStrict && restricFlag[i] == false)
                SET_NULL(presultFlag[i]);
            else {
                for (j = 0; j < cArgs; j++) {
                    pArgVector = *(genericRuntimeArgs[j].arg);
                    if (NOT_NULL(pArgVector->m_flag[i])) {
                        rowcinfo->arg[j] = genericRuntimeArgs[j].getArgFun(&pArgVector->m_vals[i]);
                        rowcinfo->argnull[j] = false;
                    } else {
                        // keep the same with row function for non strict function.
                        if (fnStrict == false)
                            rowcinfo->arg[j] = 0;

                        rowcinfo->argnull[j] = true;
                    }
                    if (fenced) {
                        rowcinfo->udfInfo.arg[matchedRows][j] = rowcinfo->arg[j];
                        rowcinfo->udfInfo.null[matchedRows][j] = rowcinfo->argnull[j];
                    }
                }

                if (!fenced) {
                    rowcinfo->isnull = false;
                    result = RowFunction(rowcinfo);
                    if (rowcinfo->isnull == false) {
                        presult[i] = ScalarVector::DatumToScalarT<retType>(result, false);
                        SET_NOTNULL(presultFlag[i]);
                    } else
                        SET_NULL(presultFlag[i]);
                }
                matchedRows++;
                Assert(matchedRows <= nvalues);
            }
        }
        /* Batch run and get result */
        if (fenced) {
            rowcinfo->udfInfo.argBatchRows = matchedRows;
            RowFunction(rowcinfo);
            Datum* resultPtr = rowcinfo->udfInfo.result;
            bool* resultIsNull = rowcinfo->udfInfo.resultIsNull;
            int pos = 0;
            for (i = 0; i < nvalues; i++) {
                if (!(fnStrict && restricFlag[i] == false)) {
                    if (resultIsNull[pos] == false) {
                        presult[i] = ScalarVector::DatumToScalarT<retType>(resultPtr[pos], false);
                        SET_NOTNULL(presultFlag[i]);
                    } else
                        SET_NULL(presultFlag[i]);
                    pos++;
                }
            }
            Assert(matchedRows == pos);
        }
    }

    // Return results
    //
    // we do not use template arg retType, as we may change the real type outside
    // in order to reduce template number.
    presultVector->m_desc.typeId = returnType;
    presultVector->m_desc.encoded = COL_IS_ENCODE(returnType);
    presultVector->m_rows = nvalues;
    return presultVector;
}

GenericArgExtract ChooseExtractFun(Oid Dtype, Oid fn_oid)
{
    if (COL_IS_ENCODE(Dtype)) {
        switch (Dtype) {
            case MACADDROID:
            case TIMETZOID:
            case TINTERVALOID:
            case INTERVALOID:
            case NAMEOID:
            case ARRAYTIMETZOID:
            case ARRAYTINTERVALOID:
            case UUIDOID:
                return ExtractFixedType;
            case ARRAYINTERVALOID: {
                if (fn_oid == INTERVALAVGFUNCOID || fn_oid == INTERVALACCUMFUNCOID)
                    return ExtractVarType;
                else
                    return ExtractFixedType;
            }
            case UNKNOWNOID:
            case CSTRINGOID:
                return ExtractCstringType;
            default:
                return ExtractVarType;
        }
    } else if (Dtype == TIDOID) {
        return ExtractAddrType;
    } else {
        return ExtractVarType;
    }

    return NULL;
}

template <Oid retType>
void DispatchGenericFunction(FunctionCallInfo finfo, int nargs, bool strict)
{
    if (strict) {
        if (!finfo->flinfo->fn_fenced)
            finfo->flinfo->vec_fn_addr = GenericFunctionT<true, retType, false>;
        else
            finfo->flinfo->vec_fn_addr = GenericFunctionT<true, retType, true>;
    } else {
        if (!finfo->flinfo->fn_fenced)
            finfo->flinfo->vec_fn_addr = GenericFunctionT<false, retType, false>;
        else
            finfo->flinfo->vec_fn_addr = GenericFunctionT<false, retType, true>;
    }
}

static void BindingGenericFunction(FunctionCallInfo finfo, MemoryContext fcacheCxt)
{
    int nargs;
    bool strict = false;
    VecFuncCacheEntry* entry = NULL;
    bool found = false;

    AutoContextSwitch autoS(fcacheCxt);  // use a long-live context

    nargs = finfo->nargs;

    strict = finfo->flinfo->fn_strict;

    GenericFunRuntime* funcRuntime = finfo->flinfo->genericRuntime;

    Assert(funcRuntime != NULL);

    funcRuntime->internalFinfo = (FunctionCallInfoData*)palloc0(sizeof(FunctionCallInfoData));

    errno_t rc =
        memcpy_s(funcRuntime->internalFinfo, sizeof(FunctionCallInfoData), finfo, sizeof(FunctionCallInfoData));
    securec_check(rc, "", "");

    funcRuntime->internalFinfo->arg = &funcRuntime->inputargs[0];
    funcRuntime->internalFinfo->argnull = &funcRuntime->nulls[0];

    for (int i = 0; i < nargs; i++) {
        funcRuntime->args[i].arg = (ScalarVector**)&finfo->arg[i];
        Assert(funcRuntime->args[i].argType != InvalidOid);
        funcRuntime->args[i].getArgFun = ChooseExtractFun(funcRuntime->args[i].argType, finfo->flinfo->fn_oid);
    }

    if (COL_IS_ENCODE(finfo->flinfo->fn_rettype)) {
        switch (finfo->flinfo->fn_rettype) {
            case MACADDROID:
                DispatchGenericFunction<MACADDROID>(finfo, nargs, strict);
                break;

            case TIMETZOID:
            case TINTERVALOID:
                DispatchGenericFunction<TIMETZOID>(finfo, nargs, strict);
                break;

            case INTERVALOID:
                DispatchGenericFunction<INTERVALOID>(finfo, nargs, strict);
                break;

            case UUIDOID:
                DispatchGenericFunction<UUIDOID>(finfo, nargs, strict);
                break;

            case NAMEOID:
                DispatchGenericFunction<NAMEOID>(finfo, nargs, strict);
                break;

            case UNKNOWNOID:
            case CSTRINGOID:
                DispatchGenericFunction<CSTRINGOID>(finfo, nargs, strict);
                break;

            case NUMERICOID:
                DispatchGenericFunction<VARCHAROID>(finfo, nargs, strict);
                /* replace some numeric function to bi function */
                if (u_sess->attr.attr_sql.enable_fast_numeric) {
                    /* find the corresponding function in hash table: g_instance.vec_func_hash */
                    entry = (VecFuncCacheEntry*)hash_search(
                        g_instance.vec_func_hash, &finfo->flinfo->fn_oid, HASH_FIND, &found);
                    /* if found, replace it */
                    if (found)
                        finfo->flinfo->fn_addr = entry->vec_transform_function[0];
                    /* not found, report this log */
                    else {
                        const FmgrBuiltin* fbp = fmgr_isbuiltin(finfo->flinfo->fn_oid);
                        /* UnSupported vector function */
                        if (fbp != NULL) {
                            elog(DEBUG1, "non-optimized big integer function: %s", fbp->funcName);
                        } else {
                            elog(DEBUG1, "non-optimized big integer function :%u", finfo->flinfo->fn_oid);
                        }
                    }
                }
                break;

            default:
                DispatchGenericFunction<VARCHAROID>(finfo, nargs, strict);
                break;
        }
    } else {
        DispatchGenericFunction<INT4OID>(finfo, nargs, strict);
    }
}

void InitVectorFunction(FunctionCallInfo finfo, MemoryContext fcacheCxt)
{
    VecFuncCacheEntry* entry = NULL;
    bool found = false;

    Oid foid = finfo->flinfo->fn_oid;

    struct HTAB* vec_func_hash = NULL;

#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
    if (u_sess->attr.attr_sql.dolphin &&
        g_instance.plugin_vec_func_cxt.vec_func_plugin[DOLPHIN_VEC] != NULL) {
        vec_func_hash = g_instance.plugin_vec_func_cxt.vec_func_plugin[DOLPHIN_VEC];
    } else if (u_sess->attr.attr_sql.whale &&
        g_instance.plugin_vec_func_cxt.vec_func_plugin[WHALE_VEC] != NULL) {
        vec_func_hash = g_instance.plugin_vec_func_cxt.vec_func_plugin[WHALE_VEC];
    } else
#endif
        vec_func_hash = g_instance.vec_func_hash;

    entry = (VecFuncCacheEntry*)hash_search(vec_func_hash, &foid, HASH_FIND, &found);

    if (found && entry->vec_fn_cache[0] != NULL) {
        finfo->flinfo->vec_fn_cache = &entry->vec_fn_cache[0];
        finfo->flinfo->vec_fn_addr = entry->vec_fn_cache[0];
    } else {
        const FmgrBuiltin* fbp = NULL;
        fbp = fmgr_isbuiltin(foid);
        /* UnSupported vector function */
        if (found && entry->vec_fn_cache[0] == NULL && entry->vec_transform_function[0] == NULL) {
            if (fbp != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_VEC_EXECUTOR),
                        errmsg("UnSupported vector function %s", fbp->funcName)));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_VEC_EXECUTOR),
                        errmsg("UnSupported vector function %u", foid)));
        } else {
            BindingGenericFunction(finfo, fcacheCxt);
            if (fbp != NULL) {
                elog(DEBUG5, "Unvectorized function %s encountered, fall back to row version", fbp->funcName);
            } else {
                elog(DEBUG5, "Unvectorized function oid %u encountered, fall back to row version", foid);
            }
        }
    }
}

void DispatchVectorFunction(Oid foid, Oid input_collation, FuncExprState* fcache, ExprContext* econtext)
{
    FunctionCallInfo finfo;
    initVectorFcache(foid, input_collation, fcache, econtext->ecxt_per_query_memory);

    finfo = &fcache->fcinfo_data;

    Assert(finfo->flinfo->vec_fn_addr == NULL);

    InitVectorFunction(finfo, econtext->ecxt_per_query_memory);
}

/*
 *      ExecMakeVecFunctionResult
 *
 * Evaluate the arguments to a function and then the function itself.
 * init_fcache is presumed already run on the FuncExprState.
 *
 */
static ScalarVector* ExecMakeVecFunctionResult(
    FuncExprState* fcache, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    List* arguments = NIL;
    ScalarVector* result = NULL;
    FunctionCallInfo fcinfo;
    ExprDoneCond argDone;
    PgStat_FunctionCallUsage fcusage;
    int actualSize;

    // Vector Engine do not deal with return-set function.
    //
    Assert(!fcache->funcResultStore);
    Assert(!fcache->setArgsValid);

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    if (isDone != NULL)
        *isDone = ExprSingleResult;

    fcinfo = &fcache->fcinfo_data;
    arguments = fcache->args;
    argDone = ExecEvalVecFuncArgs(fcinfo, arguments, pSelection, econtext);
    Assert(argDone == ExprSingleResult);
    Assert(econtext->align_rows != 0);

    actualSize = econtext->align_rows;
    Assert(actualSize >= 0);

    pgstat_init_function_usage(fcinfo, &fcusage);
    fcinfo->arg[fcinfo->nargs] = actualSize;
    fcinfo->arg[fcinfo->nargs + 1] = PointerGetDatum(pVector);
    fcinfo->arg[fcinfo->nargs + 2] = econtext->m_fUseSelection ? PointerGetDatum(pSelection) : (Datum)0;
    fcinfo->nargs += EXTRA_NARGS;
    fcinfo->isnull = false;

    result = VecFunctionCallInvoke(fcinfo);
    fcinfo->nargs -= EXTRA_NARGS;
    pgstat_end_function_usage(&fcusage, true);

    Assert(result->m_rows == actualSize);
    result->m_desc.typeId = fcinfo->flinfo->fn_rettype;
    result->m_desc.encoded = COL_IS_ENCODE(fcinfo->flinfo->fn_rettype);

    return result;
}

/*
 *      ExecMakeVecFunctionResultWithSets
 *
 * Evaluate the arguments to a function and then the function itself.
 * init_fcache is presumed already run on the FuncExprState.
 *
 */
static ScalarVector* ExecMakeVecFunctionResultWithSets(
    FuncExprState* fcache, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    List* arguments = NULL;
    ScalarVector* result = NULL;
    FunctionCallInfo fcinfo = NULL;
    ExprDoneCond argDone;
    PgStat_FunctionCallUsage fcusage;
    int actualSize;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    fcinfo = &fcache->fcinfo_data;
    arguments = fcache->args;
    bool hasSetArg = false;
    if (!fcache->setArgsValid) {
        argDone = ExecEvalVecSetFuncArgs(fcinfo, arguments, pSelection, econtext);
        if (argDone == ExprEndResult) {
            /* input is an empty set, so return an empty set. */
            if (isDone != NULL)
                *isDone = ExprEndResult;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
            return NULL;
        }
        hasSetArg = (argDone != ExprSingleResult);
    } else {
        /* Re-use callinfo from previous evaluation */
        hasSetArg = fcache->vec_setHasSetArg;
        /* Reset flag (we may set it again below) */
        fcache->setArgsValid = false;
    }

    Assert(econtext->align_rows != 0);

    actualSize = econtext->align_rows;
    Assert(actualSize >= 0);

    fcinfo->arg[fcinfo->nargs] = actualSize;
    fcinfo->arg[fcinfo->nargs + 1] = PointerGetDatum(pVector);
    fcinfo->arg[fcinfo->nargs + 2] = econtext->m_fUseSelection ? PointerGetDatum(pSelection) : (Datum)0;
    fcinfo->isnull = false;

    if (fcache->func.fn_retset || hasSetArg) {
        /*
         * We need to return a set result.	Complain if caller not ready to
         * accept one.
         */
        if (isDone == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("set-valued function called in context that cannot accept a set")));

        /*
         * for functions returning sets
         */
        GenericFunRuntime* genericRuntime = fcinfo->flinfo->genericRuntime;
        FunctionCallInfoData* rowcinfo = genericRuntime->internalFinfo;
        ReturnSetInfo rsinfo;
        if (fcache->func.fn_retset)
            rowcinfo->resultinfo = (Node*)&rsinfo;
        rsinfo.type = T_ReturnSetInfo;
        rsinfo.econtext = econtext;
        rsinfo.expectedDesc = fcache->funcResultDesc;
        rsinfo.allowedModes = (int)(SFRM_ValuePerCall);
        rsinfo.returnMode = SFRM_ValuePerCall;
        rsinfo.setResult = NULL;
        rsinfo.setDesc = NULL;

        for (;;) {
            fcinfo->nargs += EXTRA_NARGS;
            if (fcache->func.fn_retset) {
                rsinfo.isDone = ExprEndResult;
            } else {
                rsinfo.isDone = ExprSingleResult;
            }

            pgstat_init_function_usage(fcinfo, &fcusage);

            result = VecFunctionCallInvoke(fcinfo);
            fcinfo->nargs -= EXTRA_NARGS;

            pgstat_end_function_usage(&fcusage, true);
            *isDone = rsinfo.isDone;

            if (*isDone != ExprEndResult) {
                /*
                 * Got a result from current argument. If function itself
                 * returns set, save the current argument values to re-use
                 * on the next call.
                 */
                if (fcache->func.fn_retset && *isDone == ExprMultipleResult) {
                    fcache->vec_setHasSetArg = hasSetArg;
                    fcache->setArgsValid = true;
                }

                /*
                 * Make sure we say we are returning a set, even if the
                 * function itself doesn't return sets.
                 */
                if (hasSetArg) {
                    *isDone = ExprMultipleResult;
                }
                break;
            }

            if (!hasSetArg) {
                break; /* input not a set, so done */
            }

            argDone = ExecEvalVecSetFuncArgs(fcinfo, arguments, pSelection, econtext);

            if (argDone != ExprMultipleResult) {
                *isDone = ExprEndResult;
                break;
            }
        }
    } else {
        fcinfo->nargs += EXTRA_NARGS;
        /*
         * Non-set case: much easier.
         *
         * In common cases, this code path is unreachable because we'd have
         * selected ExecMakeFunctionResultNoSets instead.  However, it's
         * possible to get here if an argument sometimes produces set results
         * and sometimes scalar results.  For example, a CASE expression might
         * call a set-returning function in only some of its arms.
         */
        if (isDone != NULL)
            *isDone = ExprSingleResult;

        pgstat_init_function_usage(fcinfo, &fcusage);

        result = VecFunctionCallInvoke(fcinfo);
        fcinfo->nargs -= EXTRA_NARGS;
        pgstat_end_function_usage(&fcusage, true);
    }

    Assert(result->m_rows == actualSize);
    result->m_desc.typeId = fcinfo->flinfo->fn_rettype;
    result->m_desc.encoded = COL_IS_ENCODE(fcinfo->flinfo->fn_rettype);

    CHECK_FOR_INTERRUPTS();

    return result;
}

/* ----------------------------------------------------------------
 *      ExecEvalOper
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecOper(
    FuncExprState* fcache, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    /* This is called only the first time through */
    OpExpr* op = (OpExpr*)fcache->xprstate.expr;
    DispatchVectorFunction(op->opfuncid, op->inputcollid, fcache, econtext);

    /* Go directly to ExecMakeFunctionResult on subsequent uses */
    fcache->xprstate.vecExprFun = (VectorExprFun)ExecMakeVecFunctionResult;

    return ExecMakeVecFunctionResult(fcache, econtext, pSelection, pVector, isDone);
}

/* ----------------------------------------------------------------
 *      ExecEvalVecRelabelType
 *
 *      Evaluate a RelabelType node.
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecRelabelType(
    GenericExprState* exprstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    ScalarVector* relabelVector = NULL;
    Assert(econtext->align_rows != 0);
    Assert(pSelection != NULL);
    relabelVector = VectorExprEngine(exprstate->arg, econtext, pSelection, pVector, isDone);
    if (pVector != relabelVector) {
        pVector->m_rows = econtext->align_rows;
        if (econtext->m_fUseSelection)
            pVector->copy(relabelVector, pSelection);
        else
            pVector->copy(relabelVector);
    }
    pVector->m_desc.typeId = ((RelabelType*)exprstate->xprstate.expr)->resulttype;
    pVector->m_desc.encoded = COL_IS_ENCODE(pVector->m_desc.typeId);
    return pVector;
}

/* ----------------------------------------------------------------
 *		ExecEvalCoerceViaIO
 *
 *		Evaluate a CoerceViaIO node.
 * ----------------------------------------------------------------
 */
template <GenericArgExtract outFunc, Oid retType>
static ScalarVector* ExecEvalVecCoerceViaIO(
    CoerceViaIOState* iostate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    Datum resultVal = (Datum)0;
    ScalarVector* outputVec = NULL;
    char* string = NULL;
    int i = 0;
    ScalarValue* pResultVal = NULL;
    ScalarValue* pOutputVal = NULL;
    Assert(econtext->align_rows != 0);

    Assert(pSelection != NULL);
    pResultVal = pVector->m_vals;
    outputVec = VectorExprEngine(iostate->arg, econtext, pSelection, pVector, isDone);
    pOutputVal = outputVec->m_vals;

    if (econtext->m_fUseSelection) {
        for (i = 0; i < outputVec->m_rows; i++) {
            if (pSelection[i]) {
                if (!IS_NULL(outputVec->m_flag[i])) {
                    SET_NOTNULL(pVector->m_flag[i]);
                    string = OutputFunctionCall(&iostate->outfunc, outFunc(&pOutputVal[i]));

                    // The input function cannot change the null/not-null status
                    //
                    resultVal = InputFunctionCall(&iostate->infunc, string, iostate->intypioparam, -1);
                    pResultVal[i] = ScalarVector::DatumToScalarT<retType>(resultVal, false);
                } else {
                    // output or input functions are not called on nulls
                    //
                    SET_NULL(pVector->m_flag[i]);
                }
            }
        }
    } else {
        for (i = 0; i < outputVec->m_rows; i++) {
            if (!IS_NULL(outputVec->m_flag[i])) {
                SET_NOTNULL(pVector->m_flag[i]);
                string = OutputFunctionCall(&iostate->outfunc, outFunc(&pOutputVal[i]));

                // The input function cannot change the null/not-null status
                //
                resultVal = InputFunctionCall(&iostate->infunc, string, iostate->intypioparam, -1);

                pResultVal[i] = ScalarVector::DatumToScalarT<retType>(resultVal, false);
            } else {
                // output or input functions are not called on nulls
                //
                SET_NULL(pVector->m_flag[i]);
            }
        }
    }

    pVector->m_rows = econtext->align_rows;
    pVector->m_desc.typeId = iostate->infunc.fn_rettype;
    pVector->m_desc.encoded = COL_IS_ENCODE(retType);

    return pVector;
}

/* ----------------------------------------------------------------
 *		ExecEvalVecWindowFunc
 *
 *		Returns a Datum whose value is the value of the precomputed
 *		window function found in the given expression context.
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecWindowFunc(
    WindowFuncExprState* wfunc, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    ScalarVector* resVector = wfunc->m_resultVector;

    ScalarDesc desc = pVector->m_desc;
    pVector = resVector;
    pVector->m_desc = desc;
    return pVector;
}

/* ----------------------------------------------------------------
 *      ExecEvalVecFunc
 * ----------------------------------------------------------------
 */
static ScalarVector* ExecEvalVecFunc(
    FuncExprState* fcache, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    /* This is called only the first time through */
    FuncExpr* func = (FuncExpr*)fcache->xprstate.expr;
    DispatchVectorFunction(func->funcid, func->inputcollid, fcache, econtext);

    if (fcache->func.fn_retset || expression_returns_set((Node*)func->args)) {
        fcache->xprstate.vecExprFun = (VectorExprFun)ExecMakeVecFunctionResultWithSets;
        return ExecMakeVecFunctionResultWithSets(fcache, econtext, pSelection, pVector, isDone);
    } else {
        fcache->xprstate.vecExprFun = (VectorExprFun)ExecMakeVecFunctionResult;
        return ExecMakeVecFunctionResult(fcache, econtext, pSelection, pVector, isDone);
    }
}

template <Oid retType>
VecCoerceIOFunc DispatchCoerceIOFunc(GenericArgExtract func)
{
    if (func == ExtractCstringType)
        return ExecEvalVecCoerceViaIO<ExtractCstringType, retType>;
    else if (func == ExtractFixedType)
        return ExecEvalVecCoerceViaIO<ExtractFixedType, retType>;
    else if (func == ExtractVarType)
        return ExecEvalVecCoerceViaIO<ExtractVarType, retType>;
    else if (func == ExtractAddrType)
        return ExecEvalVecCoerceViaIO<ExtractAddrType, retType>;
    else
        return NULL;
}

static Datum ExecEvalVecRow(
    RowExprState* rstate, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    ListCell* arg = NULL;
    int i = 0;
    ScalarVector* tmp = NULL;

    rstate->rowBatch->Reset(true);
    /* Evaluate field values */
    foreach (arg, rstate->args) {
        ExprState* e = (ExprState*)lfirst(arg);

        tmp = VectorExprEngine(e, econtext, pSelection, pVector, isDone);
        rstate->rowBatch->m_arr[i].copy(tmp);
        i++;
    }

    rstate->rowBatch->m_rows = rstate->rowBatch->m_arr[0].m_rows;
    return (Datum)rstate->rowBatch;
}

ExprState* ExecInitVecExpr(Expr* node, PlanState* parent)
{
    ExprState* state = NULL;
    ScalarDesc desc;

    if (node == NULL)
        return NULL;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(node)) {
        case T_Var:
            state = (ExprState*)makeNode(ExprState);
            state->vecExprFun = ExecEvalVecVar;
            break;
        case T_Const:
            state = (ExprState*)makeNode(ExprState);
            state->tmpVector.m_rows = 0;
            state->vecExprFun = ExecEvalVecConst;
            break;
        case T_Param:
            state = (ExprState*)makeNode(ExprState);
            switch (((Param*)node)->paramkind) {
                case PARAM_EXEC:
                    state->vecExprFun = ExecEvalVecParamExec;
                    break;
                case PARAM_EXTERN:
                    state->vecExprFun = ExecEvalVecParamExtern;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_VEC_EXECUTOR),
                            errmsg("unrecognized paramtype: %d", (int)((Param*)node)->paramkind)));
            }
            break;
        case T_CaseTestExpr:
            state = (ExprState*)makeNode(ExprState);
            state->vecExprFun = ExecEvalVecCaseTestExpr;
            break;
        case T_Aggref: {
            Aggref* aggref = (Aggref*)node;
            AggrefExprState* astate = makeNode(AggrefExprState);

            astate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecAggref;
            if (parent && (IsA(parent, AggState) || IsA(parent, VecAggState))) {
                AggState* aggstate = (AggState*)parent;
                int naggs;

                aggstate->aggs = lcons(astate, aggstate->aggs);
                naggs = ++aggstate->numaggs;

                astate->args = (List*)ExecInitVecExpr((Expr*)aggref->args, parent);

                /*
                 * Complain if the aggregate's arguments contain any
                 * aggregates; nested agg functions are semantically
                 * nonsensical.  (This should have been caught earlier,
                 * but we defend against it here anyway.)
                 */
                if (naggs != aggstate->numaggs)
                    ereport(ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR),
                            errmodule(MOD_VEC_EXECUTOR),
                            errmsg("aggregate function calls cannot be nested")));
            } else {
                /* planner messed up */
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Aggref found in non-Agg plan node")));
            }
            state = (ExprState*)astate;
        } break;
        case T_GroupingFunc: {
            GroupingFunc* grp_node = (GroupingFunc*)node;
            GroupingFuncExprState* grp_state = makeNode(GroupingFuncExprState);
            VecAgg* agg = NULL;

            if (parent == NULL || !IsA(parent, VecAggState) || !IsA(parent->plan, VecAgg)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmodule(MOD_VEC_EXECUTOR),
                        errmsg("parent of GROUPING is not VecAgg node")));
            }

            grp_state->aggstate = (AggState*)parent;

            agg = (VecAgg*)(parent->plan);

            if (agg->groupingSets)
                grp_state->clauses = grp_node->cols;
            else
                grp_state->clauses = NIL;

            state = (ExprState*)grp_state;
            state->vecExprFun = (VectorExprFun)ExecEvalVecGroupingFuncExpr;
        } break;
        case T_GroupingId: {
            GroupingIdExprState* grp_id_state = makeNode(GroupingIdExprState);
            if (parent == NULL || !IsA(parent, VecAggState) || !IsA(parent->plan, VecAgg)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmodule(MOD_VEC_EXECUTOR),
                        errmsg("parent of GROUPINGID is not VecAgg node")));
            }
            grp_id_state->aggstate = (AggState*)parent;
            state = (ExprState*)grp_id_state;
            state->vecExprFun = (VectorExprFun)ExecEvalVecGroupingIdExpr;
        } break;
        case T_WindowFunc: {
            WindowFunc* wfunc = (WindowFunc*)node;
            WindowFuncExprState* wfstate = makeNode(WindowFuncExprState);

            wfstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecWindowFunc;
            if (parent && IsA(parent, VecWindowAggState)) {
                VecWindowAggState* winstate = (VecWindowAggState*)parent;
                int nfuncs;

                winstate->funcs = lcons(wfstate, winstate->funcs);
                nfuncs = ++winstate->numfuncs;
                if (wfunc->winagg)
                    winstate->numaggs++;

                wfstate->args = (List*)ExecInitVecExpr((Expr*)wfunc->args, parent);

                /*
                 * Complain if the windowfunc's arguments contain any
                 * windowfuncs; nested window functions are semantically
                 * nonsensical.  (This should have been caught earlier,
                 * but we defend against it here anyway.)
                 */
                if (nfuncs != winstate->numfuncs)
                    ereport(ERROR,
                        (errcode(ERRCODE_WINDOWING_ERROR),
                            errmodule(MOD_VEC_EXECUTOR),
                            errmsg("window function calls cannot be nested")));
            } else {
                /* planner messed up */
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmodule(MOD_VEC_EXECUTOR),
                        errmsg("WindowFunc found in non-WindowAgg plan node")));
            }
            state = (ExprState*)wfstate;

            // To handle window function, we have to use selection vector
            //      Optimize: some functions actually don't need it, like rank().
            //
            parent->ps_ExprContext->m_fUseSelection = true;
        } break;
        case T_ArrayRef: {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported array reference expression in vector engine")));
        }
        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)node;
            FuncExprState* fstate = makeNode(FuncExprState);

            fstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecFunc;
            fstate->args = (List*)ExecInitVecExpr((Expr*)funcexpr->args, parent);
            fstate->func.fn_oid = InvalidOid; /* not initialized */
            state = (ExprState*)fstate;

            if (true == funcexpr->funcretset) {
                parent->ps_ExprContext->have_vec_set_fun = true;
            }
        } break;
        case T_OpExpr: {
            OpExpr* opexpr = (OpExpr*)node;
            FuncExprState* fstate = makeNode(FuncExprState);

            fstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecOper;
            fstate->args = (List*)ExecInitVecExpr((Expr*)opexpr->args, parent);
            fstate->func.fn_oid = InvalidOid; /* not initialized */
            state = (ExprState*)fstate;
        } break;
        case T_DistinctExpr: {
            DistinctExpr* distexpr = (DistinctExpr*)node;
            FuncExprState* fstate = makeNode(FuncExprState);

            fstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecDistinct;
            fstate->args = (List*)ExecInitVecExpr((Expr*)distexpr->args, parent);
            fstate->func.fn_oid = InvalidOid; /* not initialized */
            state = (ExprState*)fstate;
        } break;
        case T_NullIfExpr: {
            NullIfExpr* nullifexpr = (NullIfExpr*)node;
            FuncExprState* fstate = makeNode(FuncExprState);

            fstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecNullIf;
            fstate->args = (List*)ExecInitVecExpr((Expr*)nullifexpr->args, parent);
            fstate->func.fn_oid = InvalidOid; /* not initialized */
            fstate->tmpVec = New(CurrentMemoryContext) ScalarVector;
            fstate->tmpVec->init(CurrentMemoryContext, desc);
            state = (ExprState*)fstate;
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
            ScalarArrayOpExprState* sstate = makeNode(ScalarArrayOpExprState);

            sstate->fxprstate.xprstate.vecExprFun = (VectorExprFun)ExecEvalVecScalarArrayOp;
            sstate->fxprstate.args = (List*)ExecInitVecExpr((Expr*)opexpr->args, parent);
            sstate->fxprstate.func.fn_oid = InvalidOid; /* not initialized */
            sstate->element_type = InvalidOid;          /* ditto */
            sstate->pSel = (bool*)palloc(sizeof(bool) * BatchMaxSize);
            sstate->tmpVecLeft = New(CurrentMemoryContext) ScalarVector;
            sstate->tmpVecLeft->init(CurrentMemoryContext, desc);
            sstate->tmpVecRight = New(CurrentMemoryContext) ScalarVector;
            sstate->tmpVecRight->init(CurrentMemoryContext, desc);
            sstate->tmpVec = New(CurrentMemoryContext) ScalarVector;
            sstate->tmpVec->init(CurrentMemoryContext, desc);
            state = (ExprState*)sstate;
        } break;
        case T_BoolExpr: {
            BoolExpr* boolexpr = (BoolExpr*)node;
            BoolExprState* bstate = makeNode(BoolExprState);

            ScalarVector* (*ptr)(BoolExprState* boolExpr,
                ExprContext* econtext,
                bool* pSelection,
                ScalarVector* pVector,
                ExprDoneCond* isDone);

            switch (boolexpr->boolop) {
                case AND_EXPR:

                    // false means if we encounter a false, we can stop.
                    ptr = ExecEvalProcessAndOrLogic<false>;
                    bstate->xprstate.vecExprFun = (VectorExprFun)ptr;
                    bstate->xprstate.tmpVector.init(CurrentMemoryContext, desc);
                    break;
                case OR_EXPR:
                    // true means if we encounter a true, we can stop.
                    ptr = ExecEvalProcessAndOrLogic<true>;
                    bstate->xprstate.vecExprFun = (VectorExprFun)ptr;
                    bstate->xprstate.tmpVector.init(CurrentMemoryContext, desc);
                    break;
                case NOT_EXPR:
                    bstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecNot;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_VEC_EXECUTOR),
                            errmsg("unrecognized boolop: %d", (int)boolexpr->boolop)));
            }
            bstate->args = (List*)ExecInitVecExpr((Expr*)boolexpr->args, parent);
            state = (ExprState*)bstate;
        } break;
        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            SubPlanState* sstate = NULL;

            if (parent == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                        errmodule(MOD_VEC_EXECUTOR),
                        errmsg("SubPlan found with no parent plan")));

            sstate = ExecInitVecSubPlan(subplan, parent);

            /* Add SubPlanState nodes to parent->subPlan */
            parent->subPlan = lappend(parent->subPlan, sstate);

            state = (ExprState*)sstate;
        } break;
        case T_AlternativeSubPlan: {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported alternative subPlan expression in vector engine")));
        } break;
        case T_FieldSelect:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported field expression in vector engine")));
            break;
        case T_FieldStore:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported field store expression in vector engine")));
        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            GenericExprState* gstate = makeNode(GenericExprState);

            gstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecRelabelType;
            gstate->arg = ExecInitVecExpr(relabel->arg, parent);
            state = (ExprState*)gstate;
        } break;
        case T_CoerceViaIO: {
            CoerceViaIO* iocoerce = (CoerceViaIO*)node;
            CoerceViaIOState* iostate = makeNode(CoerceViaIOState);
            Oid iofunc;
            Oid inputType;
            TupleDesc tupdesc;
            bool typisvarlena = false;

            ScalarVector* (*ptr)(CoerceViaIOState* caseExpr,
                ExprContext* econtext,
                bool* pSelection,
                ScalarVector* pVector,
                ExprDoneCond* isDone);

            iostate->arg = ExecInitVecExpr(iocoerce->arg, parent);
            /* lookup the result type's input function */
            getTypeInputInfo(iocoerce->resulttype, &iofunc, &iostate->intypioparam);
            fmgr_info(iofunc, &iostate->infunc);
            /* lookup the input type's output function */
            getTypeOutputInfo(exprType((Node*)iocoerce->arg), &iofunc, &typisvarlena);
            fmgr_info(iofunc, &iostate->outfunc);

            get_expr_result_type((Node*)iocoerce->arg, &inputType, &tupdesc);

            GenericArgExtract outputFunc = ChooseExtractFun(inputType);

            if (COL_IS_ENCODE(iostate->infunc.fn_rettype)) {
                switch (iostate->infunc.fn_rettype) {
                    case MACADDROID:
                        ptr = DispatchCoerceIOFunc<MACADDROID>(outputFunc);
                        break;

                    case TIMETZOID:
                    case TINTERVALOID:
                        ptr = DispatchCoerceIOFunc<TIMETZOID>(outputFunc);
                        break;

                    case INTERVALOID:
                    case UUIDOID:
                        ptr = DispatchCoerceIOFunc<INTERVALOID>(outputFunc);
                        break;

                    case UNKNOWNOID:
                    case CSTRINGOID:
                        ptr = DispatchCoerceIOFunc<CSTRINGOID>(outputFunc);
                        break;

                    case NAMEOID:
                        ptr = DispatchCoerceIOFunc<NAMEOID>(outputFunc);
                        break;
                    
                    default:
                        ptr = DispatchCoerceIOFunc<VARCHAROID>(outputFunc);
                        break;
                }
            } else {
                ptr = DispatchCoerceIOFunc<INT4OID>(outputFunc);
            }

            iostate->xprstate.vecExprFun = (VectorExprFun)ptr;
            state = (ExprState*)iostate;
        } break;
        case T_ConvertRowtypeExpr:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported convert row type expression in vector engine")));
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            CaseExprState* cstate = makeNode(CaseExprState);
            List* outlist = NIL;
            ListCell* l = NULL;
            ScalarVector* (*ptr)(CaseExprState* caseExpr,
                ExprContext* econtext,
                const bool* pSelection,
                ScalarVector* pVector,
                ExprDoneCond* isDone);
            if (caseexpr->arg) {
                ptr = ExecEvalVecCase<true>;
                cstate->arg = ExecInitVecExpr(caseexpr->arg, parent);
            } else
                ptr = ExecEvalVecCase<false>;

            cstate->xprstate.vecExprFun = (VectorExprFun)ptr;
            foreach (l, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(l);
                CaseWhenState* wstate = makeNode(CaseWhenState);

                Assert(IsA(when, CaseWhen));
                wstate->xprstate.vecExprFun = NULL; /* not used */
                wstate->xprstate.expr = (Expr*)when;
                wstate->expr = ExecInitVecExpr(when->expr, parent);
                wstate->result = ExecInitVecExpr(when->result, parent);
                outlist = lappend(outlist, wstate);
            }

            cstate->save_vector = New(CurrentMemoryContext) ScalarVector;
            cstate->save_vector->init(CurrentMemoryContext, desc);
            cstate->xprstate.tmpVector.init(CurrentMemoryContext, desc);
            cstate->args = outlist;
            cstate->defresult = ExecInitVecExpr(caseexpr->defresult, parent);
            cstate->matchedResult = (bool*)palloc(sizeof(bool) * BatchMaxSize);
            cstate->localSel = (bool*)palloc(sizeof(bool) * BatchMaxSize);
            state = (ExprState*)cstate;
        } break;
        case T_ArrayExpr:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported array expression in vector engine")));
        case T_CoalesceExpr: {
            CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;
            CoalesceExprState* cstate = makeNode(CoalesceExprState);
            List* outlist = NIL;
            ListCell* l = NULL;

            cstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecCoalesce;
            foreach (l, coalesceexpr->args) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;

                estate = ExecInitVecExpr(e, parent);
                outlist = lappend(outlist, estate);
            }
            cstate->args = outlist;
            cstate->pSel = (bool*)palloc(sizeof(bool) * BatchMaxSize);
            state = (ExprState*)cstate;
        } break;
        case T_MinMaxExpr: {
            MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;
            MinMaxExprState* mstate = makeNode(MinMaxExprState);
            List* outlist = NIL;
            ListCell* l = NULL;
            TypeCacheEntry* typentry = NULL;

            mstate->argvec = New(CurrentMemoryContext) ScalarVector;
            mstate->argvec->init(CurrentMemoryContext, desc);
            mstate->cmpresult = New(CurrentMemoryContext) ScalarVector;
            mstate->cmpresult->init(CurrentMemoryContext, desc);

            mstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecMinMax;

            foreach (l, minmaxexpr->args) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;

                estate = ExecInitVecExpr(e, parent);
                outlist = lappend(outlist, estate);
            }
            mstate->args = outlist;
            /* Look up the btree comparison function for the datatype */
            typentry = lookup_type_cache(minmaxexpr->minmaxtype, TYPECACHE_CMP_PROC);
            if (!OidIsValid(typentry->cmp_proc))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("could not identify a comparison function for type %s",
                            format_type_be(minmaxexpr->minmaxtype))));

            {
                HeapTuple tp;

                tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(typentry->cmp_proc));
                if (HeapTupleIsValid(tp)) {
                    Form_pg_proc functup = (Form_pg_proc)GETSTRUCT(tp);
                    int nargs;
                    oidvector* proargs = ProcedureGetArgTypes(tp);
                    nargs = functup->pronargs;
                    mstate->cfunc.genericRuntime = (GenericFunRuntime*)palloc0(sizeof(GenericFunRuntime));
                    InitGenericFunRuntimeInfo(*(mstate->cfunc.genericRuntime), nargs);
                    for (int j = 0; j < nargs; j++) {
                        mstate->cfunc.genericRuntime->args[j].argType = proargs->values[j];
                    }

                    ReleaseSysCache(tp);
                }
            }

            /*
             * If we enforced permissions checks on index support
             * functions, we'd need to make a check here.  But the index
             * support machinery doesn't do that, and neither does this
             * code.
             */
            fmgr_info(typentry->cmp_proc, &(mstate->cfunc));

            InitFunctionCallInfoData(mstate->cinfo, &mstate->cfunc, 2, minmaxexpr->inputcollid, NULL, NULL);

            InitVectorFunction(&mstate->cinfo, CurrentMemoryContext);
            state = (ExprState*)mstate;
        } break;
        case T_XmlExpr:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported xml expression in vector engine")));
        case T_NullTest: {
            NullTest* ntest = (NullTest*)node;
            NullTestState* nstate = makeNode(NullTestState);

            nstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecNullTest;
            nstate->arg = ExecInitVecExpr(ntest->arg, parent);
            nstate->argdesc = NULL;
            state = (ExprState*)nstate;
        } break;
        case T_HashFilter: {
            HashFilter* htest = (HashFilter*)node;
            HashFilterState* hstate = makeNode(HashFilterState);
            List* outlist = NIL;
            ListCell* l = NULL;
            int idx = 0;

            hstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecHashFilter;
            foreach (l, htest->arg) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;

                estate = ExecInitVecExpr(e, parent);
                outlist = lappend(outlist, estate);
            }

            hstate->arg = outlist;
            hstate->bucketMap = get_bucketmap_by_execnode(parent->plan->exec_nodes, 
                                                          parent->state->es_plannedstmt, 
                                                          &hstate->bucketCnt);
            hstate->nodelist = (uint2*)palloc(list_length(htest->nodeList) * sizeof(uint2));
            foreach (l, htest->nodeList)
                hstate->nodelist[idx++] = lfirst_int(l);
            state = (ExprState*)hstate;
        } break;
        case T_BooleanTest: {
            BooleanTest* btest = (BooleanTest*)node;
            GenericExprState* gstate = makeNode(GenericExprState);
            ScalarVector* (*ptr)(GenericExprState* caseExpr,
                ExprContext* econtext,
                bool* pSelection,
                ScalarVector* pVector,
                ExprDoneCond* isDone) = NULL;
            switch (btest->booltesttype) {
                case IS_TRUE:
                    ptr = &ExecEvalVecBooleanTest<IS_TRUE>;
                    break;
                case IS_NOT_TRUE:
                    ptr = &ExecEvalVecBooleanTest<IS_NOT_TRUE>;
                    break;
                case IS_FALSE:
                    ptr = &ExecEvalVecBooleanTest<IS_FALSE>;
                    break;
                case IS_NOT_FALSE:
                    ptr = &ExecEvalVecBooleanTest<IS_NOT_FALSE>;
                    break;
                case IS_UNKNOWN:
                    ptr = &ExecEvalVecBooleanTest<IS_UNKNOWN>;
                    break;
                case IS_NOT_UNKNOWN:
                    ptr = &ExecEvalVecBooleanTest<IS_NOT_UNKNOWN>;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_VEC_EXECUTOR),
                            errmsg("unrecognized booltesttype: %d", (int)btest->booltesttype)));
            }

            gstate->xprstate.vecExprFun = (VectorExprFun)ptr;
            gstate->arg = ExecInitVecExpr(btest->arg, parent);
            state = (ExprState*)gstate;
        } break;
        case T_CoerceToDomain:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported coerce to domain expression in vector engine")));
        case T_CoerceToDomainValue:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported coerce to domain value expression in vector engine")));
        case T_CurrentOfExpr:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported current of expression in vector engine")));
        case T_TargetEntry: {
            TargetEntry* tle = (TargetEntry*)node;
            GenericExprState* gstate = makeNode(GenericExprState);

            gstate->xprstate.vecExprFun = NULL; /* not used */
            gstate->arg = ExecInitVecExpr(tle->expr, parent);
            state = (ExprState*)gstate;
        } break;
        case T_List: {
            List* outlist = NIL;
            ListCell* l = NULL;

            foreach (l, (List*)node) {
                outlist = lappend(outlist, ExecInitVecExpr((Expr*)lfirst(l), parent));
            }
            /* Don't fall through to the "common" code below */
            return (ExprState*)outlist;
        } break;
        case T_RowExpr: {
            RowExpr* rowexpr = (RowExpr*)node;
            RowExprState* rstate = makeNode(RowExprState);
            FormData_pg_attribute* attrs = NULL;
            List* outlist = NIL;
            ListCell* l = NULL;
            int i;

            // RowExpr is for the expansion of the function of redistribution,
            // in the normal use(u_sess->attr.attr_sql.enable_cluster_resize = false), RowExpr need to be forbidden
            if (u_sess->attr.attr_sql.enable_cluster_resize == false)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupported rowexpr expression in vector engine")));

            rstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecRow;
            /* Build tupdesc to describe result tuples */
            if (rowexpr->row_typeid == RECORDOID) {
                /* generic record, use runtime type assignment */
                rstate->tupdesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
                BlessTupleDesc(rstate->tupdesc);
                /* we won't need to redo this at runtime */
            } else {
                /* it's been cast to a named type, use that */
                rstate->tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
            }

            /* Set up evaluation, skipping any deleted columns */
            Assert(list_length(rowexpr->args) <= rstate->tupdesc->natts);
            /* RowBatch save all need columns */
            rstate->rowBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, rstate->tupdesc);

            attrs = rstate->tupdesc->attrs;
            i = 0;
            foreach (l, rowexpr->args) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;

                if (!attrs[i].attisdropped) {
                    /*
                     * Guard against ALTER COLUMN TYPE on rowtype since
                     * the RowExpr was created.  XXX should we check
                     * typmod too?	Not sure we can be sure it'll be the
                     * same.
                     */
                    if (exprType((Node*)e) != attrs[i].atttypid)
                        ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("ROW() column has type %s instead of type %s",
                                    format_type_be(exprType((Node*)e)),
                                    format_type_be(attrs[i].atttypid))));
                } else {
                    /*
                     * Ignore original expression and insert a NULL. We
                     * don't really care what type of NULL it is, so
                     * always make an int4 NULL.
                     */
                    e = (Expr*)makeNullConst(INT4OID, -1, InvalidOid);
                }
                estate = ExecInitVecExpr(e, parent);
                outlist = lappend(outlist, estate);
                i++;
            }
            rstate->args = outlist;
            state = (ExprState*)rstate;
        } break;
        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            RowCompareExprState* rstate = makeNode(RowCompareExprState);
            int nopers = list_length(rcexpr->opnos);
            List* outlist = NIL;
            ListCell* l = NULL;
            ListCell* l2 = NULL;
            ListCell* l3 = NULL;
            int i;

            rstate->left_argvec = New(CurrentMemoryContext) ScalarVector;
            rstate->left_argvec->init(CurrentMemoryContext, desc);
            rstate->right_argvec = New(CurrentMemoryContext) ScalarVector;
            rstate->right_argvec->init(CurrentMemoryContext, desc);
            rstate->cmpresult = New(CurrentMemoryContext) ScalarVector;
            rstate->cmpresult->init(CurrentMemoryContext, desc);
            rstate->pSel = (bool*)palloc0(sizeof(bool) * BatchMaxSize);

            rstate->xprstate.vecExprFun = (VectorExprFun)ExecEvalVecRowCompare;
            Assert(list_length(rcexpr->largs) == nopers);
            outlist = NIL;
            foreach (l, rcexpr->largs) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;

                estate = ExecInitVecExpr(e, parent);
                outlist = lappend(outlist, estate);
            }
            rstate->largs = outlist;
            Assert(list_length(rcexpr->rargs) == nopers);
            outlist = NIL;
            foreach (l, rcexpr->rargs) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;

                estate = ExecInitVecExpr(e, parent);
                outlist = lappend(outlist, estate);
            }
            rstate->rargs = outlist;
            Assert(list_length(rcexpr->opfamilies) == nopers);
            rstate->funcs = (FmgrInfo*)palloc(nopers * sizeof(FmgrInfo));
            rstate->cinfo = (FunctionCallInfoData*)palloc(nopers * sizeof(FunctionCallInfoData));
            rstate->collations = (Oid*)palloc(nopers * sizeof(Oid));
            i = 0;
            forthree(l, rcexpr->opnos, l2, rcexpr->opfamilies, l3, rcexpr->inputcollids)
            {
                Oid opno = lfirst_oid(l);
                Oid opfamily = lfirst_oid(l2);
                Oid inputcollid = lfirst_oid(l3);
                int strategy;
                Oid lefttype;
                Oid righttype;
                Oid proc;

                get_op_opfamily_properties(opno, opfamily, false, &strategy, &lefttype, &righttype);
                proc = get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);
                {
                    HeapTuple tp;

                    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc));
                    if (HeapTupleIsValid(tp)) {
                        Form_pg_proc functup = (Form_pg_proc)GETSTRUCT(tp);
                        int nargs;
                        oidvector* proargs = ProcedureGetArgTypes(tp);
                        nargs = functup->pronargs;
                        rstate->funcs[i].genericRuntime = (GenericFunRuntime*)palloc0(sizeof(GenericFunRuntime));
                        InitGenericFunRuntimeInfo(*(rstate->funcs[i].genericRuntime), nargs);
                        for (int j = 0; j < nargs; j++) {
                            rstate->funcs[i].genericRuntime->args[j].argType = proargs->values[j];
                        }

                        ReleaseSysCache(tp);
                    }
                }

                /*
                 * If we enforced permissions checks on index support
                 * functions, we'd need to make a check here.  But the
                 * index support machinery doesn't do that, and neither
                 * does this code.
                 */
                fmgr_info(proc, &(rstate->funcs[i]));
                InitFunctionCallInfoData(rstate->cinfo[i], &rstate->funcs[i], 2, inputcollid, NULL, NULL);

                InitVectorFunction(&rstate->cinfo[i], CurrentMemoryContext);
                rstate->collations[i] = inputcollid;
                i++;
            }
            state = (ExprState*)rstate;
        } break;
        case T_ArrayCoerceExpr:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unsupported array coerce expression in vector engine")));
        case T_Rownum: {
            RownumState* rnstate = (RownumState*)makeNode(RownumState);
            rnstate->ps = parent;
            state = (ExprState*)rnstate;
            state->vecExprFun = (VectorExprFun)ExecEvalVecRownum;
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("unrecognized node type: %d", (int)nodeTag(node))));
    }

    /* Common code for all state-node types */
    state->expr = node;
    if (nodeTag(node) != T_TargetEntry)
        state->resultType = exprType((Node*)node);

    return state;
}

ScalarVector* ExecVectorExprEngineSwitchContext(
    ExprState* expr, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    ScalarVector* pResVector = NULL;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    pResVector = VectorExprEngine(expr, econtext, pSelection, pVector, isDone);
    (void)MemoryContextSwitchTo(oldContext);

    return pResVector;
}

/*
 * We save the bool value in the selection vector
 * do not use the return vector to fetch the qual result, only can use NULL as no value match
 *
 */
ScalarVector* ExecVecQual(List* qual, ExprContext* econtext, bool resultForNull, bool isReset)
{
    ListCell* l = NULL;
    int i;
    bool* pSel = NULL;
    ScalarValue* pVal = NULL;
    uint8* pFlag = NULL;
    bool res = false;
    ScalarVector* qual_result = NULL;
    int rows = 0;
    ScalarVector* pVector = econtext->boolVector;

    bool savedUseSelection = econtext->m_fUseSelection;

    /*
     * debugging stuff
     */
    EV_printf("ExecQual: qual is ");
    EV_nodeDisplay(qual);
    EV_printf("\n");

    Assert(econtext->ecxt_scanbatch != NULL);

    // Reset selection vector to be fully loaded
    //
    if (isReset)
        econtext->ecxt_scanbatch->ResetSelection(true);

    // update the aligned_rows of econtext
    econtext->align_rows = econtext->ecxt_scanbatch->m_rows;

    // When doing multi qualification, unqualified results should not be eval
    // When isReset is false,  the last  pSelection should be used in this qual,
    // so econtext->m_fUseSelection must be true
    if (list_length(qual) > 1 || isReset == false)
        econtext->m_fUseSelection = true;

    AutoContextSwitch contexS(econtext->ecxt_per_tuple_memory);
    Assert(econtext->align_rows != 0);

    pSel = econtext->ecxt_scanbatch->m_sel;
    // Evaluate the qual conditions one at a time.
    //
    foreach (l, qual) {
        ExprState* clause = (ExprState*)lfirst(l);
        res = false;

        // Reset null flag to avoid influence from each qual.
        // no need to reset m_vals because it will be overwritten.
        errno_t rc = memset_s(pVector->m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * econtext->align_rows);
        securec_check(rc, "\0", "\0");
        pVector->m_rows = 0;

        if (!PointerIsValid(clause))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid clause in qual")));

        qual_result = VectorExprEngine(clause, econtext, econtext->ecxt_scanbatch->m_sel, pVector, NULL);

        rows = qual_result->m_rows;
        pVal = qual_result->m_vals;
        pFlag = qual_result->m_flag;
        // use pSel to control if a record should go into next qual.
        for (i = 0; i < rows; i++) {
            if (NOT_NULL(pFlag[i]))
                pSel[i] = pSel[i] && pVal[i];
            else
                pSel[i] = pSel[i] && resultForNull;

            res = res || pSel[i];
        }

        if (!res)
            return NULL;
    }

    if (!res)
        return NULL;

    for (i = rows; i < econtext->align_rows; i++)
        pSel[i] = false;

    econtext->m_fUseSelection = savedUseSelection;
    // qual result do not save any result.
    return econtext->qual_results;
}

/*
 * ExecVecTargetList
 *      Evaluates a targetlist with respect to the given
 *      expression context.  Returns TRUE if we were able to create
 *      a result, FALSE if we have exhausted a set-valued expression.
 *
 */
static bool ExecVecTargetList(List* targetlist, ExprContext* econtext, VectorBatch* pBatch)
{
    MemoryContext oldContext;
    ListCell* tl = NULL;
    ScalarVector* pVector = NULL;
    int rowCount = BatchMaxSize;
    bool* pSelection = NULL;

    // Run in short-lived per-tuple context while computing expressions.
    //

    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    pSelection = pBatch->m_sel;

    foreach (tl, targetlist) {
        GenericExprState* gstate = (GenericExprState*)lfirst(tl);
        TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
        AttrNumber resind = tle->resno - 1;

        // maybe we shall hornour m_fUseSelection or some related flags?
        // This is important for performance as if we know there is no check expression
        // in the targetlist, we can just set selection vector to NULL and expression
        // engine can ignore selection vector.
        //
        DBG_ASSERT(resind >= 0 && resind < pBatch->m_cols);

        ELOG_FIELD_NAME_START(tle->resname);

        pVector = VectorExprEngine(gstate->arg, econtext, pSelection, &pBatch->m_arr[resind], NULL);

        ELOG_FIELD_NAME_END;

        if (pVector != &pBatch->m_arr[resind]) {
            ShallowCopyVector(pBatch->m_arr[resind], *pVector);
        }

        rowCount = Min(rowCount, pBatch->m_arr[resind].m_rows);
    }

    // Fix the row count and return
    //
    pBatch->m_rows = rowCount;
    (void)MemoryContextSwitchTo(oldContext);
    return true;
}

/*
 * ExecVecTargetListSetFunc
 *      Evaluates a targetlist with respect to the given
 *      expression context.  Returns TRUE if we were able to create
 *      a result, FALSE if we have exhausted a set-valued expression.
 *
 */
static bool ExecVecTargetListSetFunc(List* targetlist, ExprContext* econtext, VectorBatch* pBatch,
    VectorBatch* resultBatch, ExprDoneCond* itemIsDone, ExprDoneCond* isDone)
{
    MemoryContext oldContext = NULL;
    ListCell* tl = NULL;
    ScalarVector* pVector = NULL;
    bool* pSelection = NULL;

    // Run in short-lived per-tuple context while computing expressions.
    //

    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    pSelection = pBatch->m_sel;

    /* backup */
    for (int i = 0; i < econtext->align_rows; i++) {
        econtext->vec_fun_sel[i] = pSelection[i];
        pSelection[i] = false;
    }
    econtext->m_fUseSelection = true;

    resultBatch->Reset();
    resultBatch->ResetSelection(true);

    int target_row = 0;
    while (target_row < BatchMaxSize) {
        if (econtext->current_row >= econtext->align_rows) {
            break;
        }

        int current_row = econtext->current_row;

        if (econtext->vec_fun_sel[current_row] == false) {
            econtext->current_row++;
            continue;
        }

        pSelection[current_row] = true;
        if (current_row > 0) {
            pSelection[current_row - 1] = false;
        }

        bool haveDoneSets = false; /* any exhausted set exprs in tlist? */

        *isDone = ExprSingleResult;

        foreach (tl, targetlist) {
            GenericExprState* gstate = (GenericExprState*)lfirst(tl);
            TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
            AttrNumber resind = tle->resno - 1;

            // maybe we shall hornour m_fUseSelection or some related flags?
            // This is important for performance as if we know there is no check expression
            // in the targetlist, we can just set selection vector to NULL and expression
            // engine can ignore selection vector.
            //
            DBG_ASSERT(resind >= 0 && resind < pBatch->m_cols);

            ELOG_FIELD_NAME_START(tle->resname);

            pVector = VectorExprEngine(gstate->arg, econtext, pSelection, &pBatch->m_arr[resind], &itemIsDone[resind]);

            ELOG_FIELD_NAME_END;

            if (pVector != NULL && pVector != &pBatch->m_arr[resind]) {
                ShallowCopyVector(pBatch->m_arr[resind], *pVector);
            }

            if (itemIsDone[resind] != ExprSingleResult) {
                /* We have a set-valued expression in the tlist */
                if (itemIsDone[resind] == ExprMultipleResult) {
                    /* we have undone sets in the tlist, set flag */
                    *isDone = ExprMultipleResult;
                } else {
                    /* we have done sets in the tlist, set flag for that */
                    haveDoneSets = true;
                }
            }
        }

        if (haveDoneSets) {
            /*
             * note: can't get here unless we verified isDone != NULL
             */
            if (*isDone == ExprSingleResult) {
                /*
                 * all sets are done, so report that tlist expansion is complete.
                 */
                *isDone = ExprEndResult;
                econtext->current_row++;
                continue;
            } else {
                /*
                 * We have some done and some undone sets.	Restart the done ones
                 * so that we can deliver a tuple (if possible).
                 */
                foreach (tl, targetlist) {
                    GenericExprState* gstate = (GenericExprState*)lfirst(tl);
                    TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
                    AttrNumber resind = tle->resno - 1;

                    if (itemIsDone[resind] == ExprEndResult) {
                        pVector = VectorExprEngine(
                            gstate->arg, econtext, pSelection, &pBatch->m_arr[resind], &itemIsDone[resind]);

                        if (itemIsDone[resind] == ExprEndResult) {
                            /*
                             * Oh dear, this item is returning an empty set. Guess
                             * we can't make a tuple after all.
                             */
                            *isDone = ExprEndResult;
                            break;
                        }
                    }
                }

                /*
                 * If we cannot make a tuple because some sets are empty, we still
                 * have to cycle the nonempty sets to completion, else resources
                 * will not be released from subplans etc.
                 *
                 * XXX is that still necessary?
                 */
                if (*isDone == ExprEndResult) {
                    foreach (tl, targetlist) {
                        GenericExprState* gstate = (GenericExprState*)lfirst(tl);
                        TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
                        AttrNumber resind = tle->resno - 1;

                        while (itemIsDone[resind] == ExprMultipleResult) {
                            pVector = VectorExprEngine(
                                gstate->arg, econtext, pSelection, &pBatch->m_arr[resind], &itemIsDone[resind]);
                        }
                    }

                    econtext->current_row++;
                    continue;
                }
            }
        }

        for (int i = 0; i < pBatch->m_cols; i++) {
            resultBatch->m_arr[i].m_vals[target_row] = pBatch->m_arr[i].m_vals[current_row];
            resultBatch->m_arr[i].m_flag[target_row] = pBatch->m_arr[i].m_flag[current_row];
        }
        target_row++;
    }

    resultBatch->FixRowCount(target_row);

    /* restore */
    for (int j = 0; j < econtext->align_rows; j++) {
        pSelection[j] = econtext->vec_fun_sel[j];
    }

    (void)MemoryContextSwitchTo(oldContext);
    return true;
}

/*
 * @Description: set align rows for expression context
 * @IN  econtext: the expression context for expression evaluation
 * @IN  batch: vector batch data
 * @Return: void
 */
static inline void SetAlignRowsForProject(ExprContext* econtext, VectorBatch* batch)
{
    if (batch == NULL)
        return;

    Assert(batch->m_rows != 0);

    if (econtext->align_rows == 0)
        econtext->align_rows = batch->m_rows;
    else {
        Assert(econtext->align_rows == batch->m_rows);
        if (econtext->align_rows != batch->m_rows)
            ereport(ERROR,
                (errcode(ERRCODE_CHECK_VIOLATION),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unaligned rows for batches need to be expression evaluation")));
    }
}

/*
 * ExecVecProject
 *
 * @Description: projects a VectorBatch based on projection info and stores
 *      it in the previously specified VectorBatch.
 * @in projInfo: ProjectionInfo node information
 * @in selReSet: Sign projInfo->pi_batch's m_sel if need reset.
 * @in batchReset: True if pProjBatch is used for multi-entry.
 * @return: Return project result.
 */
VectorBatch* ExecVecProject(ProjectionInfo* projInfo, bool selReSet, ExprDoneCond* isDone)
{
    Assert(projInfo != NULL);

    VectorBatch* pProjBatch = projInfo->pi_batch;
    VectorBatch* srcBatch = NULL;
    ExprContext* econtext = projInfo->pi_exprContext;
    int numSimpleVars;

    /* Assume single result row until proven otherwise */
    if (isDone != NULL)
        *isDone = ExprSingleResult;

    /* Clear any former contents of the result pProjBatch */
    pProjBatch->Reset();

    if (selReSet) {
        pProjBatch->ResetSelection(true);
    }

    /* align rows */
    econtext->align_rows = 0;
    SetAlignRowsForProject(econtext, econtext->ecxt_outerbatch);
    SetAlignRowsForProject(econtext, econtext->ecxt_innerbatch);
    SetAlignRowsForProject(econtext, econtext->ecxt_aggbatch);
    SetAlignRowsForProject(econtext, econtext->ecxt_scanbatch);
    Assert(econtext->align_rows != 0);
    /*
     * Assign simple Vars to result by direct extraction of fields from source
     * slots ... a mite ugly, but fast ...
     */
    numSimpleVars = projInfo->pi_numSimpleVars;
    if (numSimpleVars > 0) {
        ScalarVector* values = pProjBatch->m_arr;
        int* varSlotOffSet = projInfo->pi_varSlotOffsets;
        int* varNumbers = projInfo->pi_varNumbers;
        int i;

        if (projInfo->pi_directMap) {
            /* especially simple case where vars go to output in order */
            for (i = 0; i < numSimpleVars; i++) {
                char* ptr = ((char*)econtext) + varSlotOffSet[i];
                srcBatch = *((VectorBatch**)ptr);
                int varNumber = varNumbers[i] - 1;

                Assert(varNumber >= 0 && varNumber < srcBatch->m_cols);
                values[i] = srcBatch->m_arr[varNumber];
            }
        } else {
            /* we have to pay attention to varOutputCols[] */
            int* varOutputCols = projInfo->pi_varOutputCols;

            for (i = 0; i < numSimpleVars; i++) {
                char* ptr = ((char*)econtext) + varSlotOffSet[i];
                srcBatch = *((VectorBatch**)ptr);
                int varNumber = varNumbers[i] - 1;
                int varOutputCol = varOutputCols[i] - 1;

                Assert(varNumber >= 0 && varNumber < srcBatch->m_cols);
                Assert(varOutputCol >= 0 && varOutputCol < pProjBatch->m_cols);
                values[varOutputCol] = srcBatch->m_arr[varNumber];
            }
        }

        /* Set the number of rows on which batch is used */
        pProjBatch->m_rows = srcBatch->m_rows;
        for (i = 0; i < pProjBatch->m_cols; i++) {
            pProjBatch->m_arr[i].m_rows = srcBatch->m_rows;
        }
    }

    /* If there are any generic expressions, evaluate them. */
    if (projInfo->pi_targetlist) {
        if (projInfo->jitted_vectarget) {
            projInfo->jitted_vectarget(econtext, pProjBatch);
        } else {
            if (econtext->have_vec_set_fun) {
                if (isDone == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_EXECUTOR),
                            errmsg("set-return function not supported in vector eninge")));
                }

                ExecVecTargetListSetFunc(projInfo->pi_targetlist,
                    econtext,
                    pProjBatch,
                    projInfo->pi_setFuncBatch,
                    projInfo->pi_vec_itemIsDone,
                    isDone);
            } else {
                ExecVecTargetList(projInfo->pi_targetlist, econtext, pProjBatch);
            }
        }
    }

    /*
     * Kludge: this is to fix some cases only const evaluation in target list, thus
     * we may get a over sized batch. Adjust it back here.
     */
    if (srcBatch != NULL)
        pProjBatch->m_rows = Min(pProjBatch->m_rows, srcBatch->m_rows);

    return (econtext->have_vec_set_fun) ? projInfo->pi_setFuncBatch : pProjBatch;
}

static ScalarVector* ExecEvalVecGroupingFuncExpr(GroupingFuncExprState* gstate, ExprContext* econtext,
    const bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    uint32 result = 0;
    int attnum = 0;
    Bitmapset* grouped_cols = gstate->aggstate->grouped_cols;
    ScalarValue* pDest = NULL;
    ListCell* lc = NULL;
    pDest = pVector->m_vals;
    Assert(econtext->align_rows != 0);

    foreach (lc, (gstate->clauses)) {
        attnum = lfirst_int(lc);
        result = result << 1;
        if (!bms_is_member(attnum, grouped_cols))
            result = result | 1;
    }
    if (econtext->m_fUseSelection) {
        for (int i = 0; i < econtext->align_rows; i++) {
            if (pSelection[i]) {
                SET_NOTNULL(pVector->m_flag[i]);
                pDest[i] = result;
            }
        }
    } else {
        for (int i = 0; i < econtext->align_rows; i++) {
            /* the value of pDest[i] need not */
            SET_NOTNULL(pVector->m_flag[i]);
            pDest[i] = result;
        }
    }
    pVector->m_rows = econtext->align_rows;
    return pVector;
}

/* ExecEvalVecGroupingIdExpr
 * Compute groiping id, mark the row is from which group
 */
static ScalarVector* ExecEvalVecGroupingIdExpr(GroupingIdExprState* gstate, ExprContext* econtext,
    const bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    int groupingId = 0;
    ScalarValue* pDest = NULL;
    pDest = pVector->m_vals;
    for (int i = 0; i < gstate->aggstate->current_phase; i++) {
        groupingId += gstate->aggstate->phases[i].numsets;
    }

    groupingId += gstate->aggstate->projected_set + 1;
    Assert(econtext->align_rows != 0);

    if (econtext->m_fUseSelection) {
        for (int i = 0; i < econtext->align_rows; i++) {
            if (pSelection[i]) {
                SET_NOTNULL(pVector->m_flag[i]);
                pDest[i] = groupingId;
            }
        }
    } else {
        for (int i = 0; i < econtext->align_rows; i++) {
            SET_NOTNULL(pVector->m_flag[i]);
            pDest[i] = groupingId;
        }
    }

    pVector->m_rows = econtext->align_rows;
    return pVector;
}
