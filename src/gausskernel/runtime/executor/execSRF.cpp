/*-------------------------------------------------------------------------
 *
 * execSRF.c
 *	  Routines implementing the API for set-returning functions
 *
 * This file serves nodeFunctionscan.c and nodeProjectSet.c, providing
 * common code for calling set-returning functions according to the
 * ReturnSetInfo API.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execSRF.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "executor/exec/execdebug.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "access/tableam.h"

/* static function decls */
template <bool has_refcursor>
static ExprDoneCond ExecEvalFuncArgs(
    FunctionCallInfo fcinfo, List* argList, ExprContext* econtext, int* plpgsql_var_dno = NULL);

extern bool func_has_refcursor_args(Oid Funcid, FunctionCallInfoData* fcinfo);
extern void check_huge_clob_paramter(FunctionCallInfoData* fcinfo, bool is_have_huge_clob);

/*
 * Prepare function call in FROM (ROWS FROM) for execution.
 *
 * This is used by nodeFunctionscan.c.
 */
FuncExprState *
ExecInitTableFunctionResult(Expr *expr,
							ExprContext *econtext, PlanState *parent)
{
	FuncExprState *state = makeNode(FuncExprState);

	state->funcReturnsSet = false;
	state->xprstate.expr = expr;
    state->xprstate.is_flt_frame = true;
	state->func.fn_oid = InvalidOid;

	/*
	 * Normally the passed expression tree will be a FuncExpr, since the
	 * grammar only allows a function call at the top level of a table
	 * function reference.  However, if the function doesn't return set then
	 * the planner might have replaced the function call via constant-folding
	 * or inlining.  So if we see any other kind of expression node, execute
	 * it via the general ExecEvalExpr() code.  That code path will not
	 * support set-returning functions buried in the expression, though.
	 */
	
	
	FuncExpr   *func = (FuncExpr *) expr;

	state->funcReturnsSet = func->funcretset;
	state->args = ExecInitExprList(func->args, parent);

	init_fcache<false>(func->funcid, func->inputcollid, state,
				   econtext->ecxt_per_query_memory, func->funcretset, false);


	return state;
}

/*
 * Evaluate arguments for a function.
 */
template <bool has_refcursor>
static ExprDoneCond ExecEvalFuncArgs(
    FunctionCallInfo fcinfo, List* argList, ExprContext* econtext, int* plpgsql_var_dno)
{
    ExprDoneCond argIsDone;
    int i;
    ListCell *arg;

    argIsDone = ExprSingleResult; /* default assumption */

    i = 0;
    econtext->is_cursor = false;
    u_sess->plsql_cxt.func_tableof_index = NIL;
    bool is_have_huge_clob = false;

    foreach (arg, argList) {
        ExprState *argstate = (ExprState *)lfirst(arg);

        if (has_refcursor && argstate->resultType == REFCURSOROID)
            econtext->is_cursor = true;

        fcinfo->arg[i] = ExecEvalExpr(argstate, econtext, &fcinfo->argnull[i]);
        ExecTableOfIndexInfo execTableOfIndexInfo;
        initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
        ExecEvalParamExternTableOfIndex((Node*)argstate->expr, &execTableOfIndexInfo);
        if (execTableOfIndexInfo.tableOfIndex != NULL) {
            MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
            PLpgSQL_func_tableof_index* func_tableof =
                (PLpgSQL_func_tableof_index*)palloc0(sizeof(PLpgSQL_func_tableof_index));
            func_tableof->varno = i;
            func_tableof->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
            func_tableof->tableOfIndex = copyTableOfIndex(execTableOfIndexInfo.tableOfIndex);
            u_sess->plsql_cxt.func_tableof_index = lappend(u_sess->plsql_cxt.func_tableof_index, func_tableof);
            MemoryContextSwitchTo(oldCxt);
        }

        if (has_refcursor && econtext->is_cursor && plpgsql_var_dno != NULL) {
            plpgsql_var_dno[i] = econtext->dno;
            CopyCursorInfoData(&fcinfo->refcursor_data.argCursor[i], &econtext->cursor_data);
        }
        fcinfo->argTypes[i] = argstate->resultType;
        econtext->is_cursor = false;
        if (is_huge_clob(fcinfo->argTypes[i], fcinfo->argnull[i], fcinfo->arg[i])) {
            is_have_huge_clob = true;
        }
        i++;
    }
    check_huge_clob_paramter(fcinfo, is_have_huge_clob);

    Assert(i == fcinfo->nargs);

    return argIsDone;
}

/*
 * Prepare targetlist SRF function call for execution.
 *
 * This is used by nodeProjectSet.c.
 */
FuncExprState *ExecInitFunctionResultSet(Expr *expr, ExprContext *econtext, PlanState *parent)
{
    FuncExprState *state = makeNode(FuncExprState);

    state->funcReturnsSet = true;
    state->xprstate.expr = expr;
    state->xprstate.is_flt_frame = true;
    state->func.fn_oid = InvalidOid;

    /*
     * Initialize metadata.  The expression node could be either a FuncExpr or
     * an OpExpr.
     */
    if (IsA(expr, FuncExpr)) {
        FuncExpr *func = (FuncExpr *)expr;

        state->args = ExecInitExprList(func->args, parent);
        init_fcache<false>(func->funcid, func->inputcollid, state, econtext->ecxt_per_query_memory, true, true);
    } else if (IsA(expr, OpExpr)) {
        OpExpr *op = (OpExpr *)expr;

        state->args = ExecInitExprList(op->args, parent);
        init_fcache<false>(op->opfuncid, op->inputcollid, state, econtext->ecxt_per_query_memory, true, true);
    } else
        elog(ERROR, "unrecognized node type: %d", (int)nodeTag(expr));

    /* shouldn't get here unless the selected function returns set */
    Assert(state->func.fn_retset);

    state->has_refcursor = func_has_refcursor_args(state->func.fn_oid, &state->fcinfo_data);
    
    return state;
}

/*
 *		ExecMakeFunctionResultSet
 *
 * Evaluate the arguments to a set-returning function and then call the
 * function itself.  The argument expressions may not contain set-returning
 * functions (the planner is supposed to have separated evaluation for those).
 *
 * This should be called in a short-lived (per-tuple) context, argContext
 * needs to live until all rows have been returned (i.e. *isDone set to
 * ExprEndResult or ExprSingleResult).
 *
 * This is used by nodeProjectSet.c.
 */
Datum ExecMakeFunctionResultSet(FuncExprState *fcache, ExprContext *econtext, MemoryContext argContext,
                                bool *isNull, ExprDoneCond *isDone)
{
    List	   *arguments;
    Datum		result;
    FunctionCallInfo fcinfo;
    PgStat_FunctionCallUsage fcusage;
    ReturnSetInfo rsinfo;
    bool		callit;
    int			i;
    int* var_dno = NULL;
    bool has_refcursor = fcache->has_refcursor;
    int has_cursor_return = fcache->fcinfo_data.refcursor_data.return_number;

    econtext->plpgsql_estate = plpgsql_estate;
    plpgsql_estate = NULL;

restart:

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /*
	 * If a previous call of the function returned a set result in the form of
	 * a tuplestore, continue reading rows from the tuplestore until it's
	 * empty.
     */
    if (fcache->funcResultStore)
    {
        TupleTableSlot *slot = fcache->funcResultSlot;
        MemoryContext oldContext;
        bool foundTup;
        econtext->hasSetResultStore = true;
        /*
         * Have to make sure tuple in slot lives long enough, otherwise
         * clearing the slot could end up trying to free something already
         * freed.
         */
        oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
        foundTup = tuplestore_gettupleslot(fcache->funcResultStore, true, false, fcache->funcResultSlot);
        MemoryContextSwitchTo(oldContext);
        if (foundTup) {
            *isDone = ExprMultipleResult;
            if (fcache->funcReturnsTuple) {
                /* We must return the whole tuple as a Datum. */
                *isNull = false;
                return ExecFetchSlotTupleDatum(fcache->funcResultSlot);
            } else {
                /* Extract the first column and return it as a scalar. */
                Assert(fcache->funcResultSlot != NULL);
                /* Get the Table Accessor Method*/
                return tableam_tslot_getattr(fcache->funcResultSlot, 1, isNull);
            }
        }

        /* Exhausted the tuplestore, so clean up */
        tuplestore_end(fcache->funcResultStore);
        fcache->funcResultStore = NULL;
        *isDone = ExprEndResult;
        *isNull = true;
        return (Datum) 0;
    }

    /*
	 * arguments is a list of expressions to evaluate before passing to the
	 * function manager.  We skip the evaluation if it was already done in the
	 * previous call (ie, we are continuing the evaluation of a set-valued
	 * function).  Otherwise, collect the current argument values into fcinfo.
     */
    fcinfo = &fcache->fcinfo_data;

    if (has_cursor_return) {
        /* init returnCursor to store out-args cursor info on ExprContext*/
        fcinfo->refcursor_data.returnCursor =
            (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->refcursor_data.return_number);
    } else {
        fcinfo->refcursor_data.returnCursor = NULL;
    }

    if (has_refcursor) {
        /* init argCursor to store in-args cursor info on ExprContext*/
        fcinfo->refcursor_data.argCursor = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->nargs);
        var_dno = (int*)palloc0(sizeof(int) * fcinfo->nargs);
        for (i = 0; i < fcinfo->nargs; i++) {
            var_dno[i] = -1;
        }
    }

    arguments = fcache->args;
    if (!fcache->setArgsValid) {
        MemoryContext oldContext = MemoryContextSwitchTo(argContext);
        if (has_refcursor)
            ExecEvalFuncArgs<true>(fcinfo, arguments, econtext, var_dno);
        else
            ExecEvalFuncArgs<false>(fcinfo, arguments, econtext);
        MemoryContextSwitchTo(oldContext);
    } else {
        /* Reset flag (we may set it again below) */
        fcache->setArgsValid = false;
    }

    /*
	 * Now call the function, passing the evaluated parameter values.
     */

    /* Prepare a resultinfo node for communication. */
    fcinfo->resultinfo = (Node *) &rsinfo;
    rsinfo.type = T_ReturnSetInfo;
    rsinfo.econtext = econtext;
    rsinfo.expectedDesc = fcache->funcResultDesc;
    rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
    /* note we do not set SFRM_Materialize_Random or _Preferred */
    rsinfo.returnMode = SFRM_ValuePerCall;
    /* isDone is filled below */
    rsinfo.setResult = NULL;
    rsinfo.setDesc = NULL;

    /*
	 * If function is strict, and there are any NULL arguments, skip calling
	 * the function.
     */
    callit = true;
    if (fcache->func.fn_strict) {
        for (i = 0; i < fcinfo->nargs; i++) {
            if (fcinfo->argnull[i]) {
                callit = false;
                break;
            }
        }
    }

    if (callit)
    {
        pgstat_init_function_usage(fcinfo, &fcusage);

        fcinfo->isnull = false;
        rsinfo.isDone = ExprSingleResult;
        result = FunctionCallInvoke(fcinfo);
        *isNull = fcinfo->isnull;
        *isDone = rsinfo.isDone;

        pgstat_end_function_usage(&fcusage, rsinfo.isDone != ExprMultipleResult);
    } else {
        /* for a strict SRF, result for NULL is an empty set */
        result = (Datum) 0;
        *isNull = true;
        *isDone = ExprEndResult;
    }

    if (has_refcursor && econtext->plpgsql_estate != NULL) {
        PLpgSQL_execstate* estate = econtext->plpgsql_estate;
        /* copy in-args cursor option info */
        for (i = 0; i < fcinfo->nargs; i++) {
            if (var_dno[i] >= 0) {
                int dno = var_dno[i];
                Cursor_Data* cursor_data = &fcinfo->refcursor_data.argCursor[i];
#ifdef USE_ASSERT_CHECKING
                PLpgSQL_datum* datum = estate->datums[dno];
#endif
                Assert(datum->dtype == PLPGSQL_DTYPE_VAR);
                Assert(((PLpgSQL_var*)datum)->datatype->typoid == REFCURSOROID);

                ExecCopyDataToDatum(estate->datums, dno, cursor_data);
            }
        }

        if (fcinfo->refcursor_data.return_number > 0) {
            /* copy function returns cursor option info.
             * for simple expr in exec_eval_expr, we can not get the result type,
             * so cursor_return_data mallocs here.
             */
            if (estate->cursor_return_data == NULL && estate->tuple_store_cxt != NULL) {
                MemoryContext oldcontext = MemoryContextSwitchTo(estate->tuple_store_cxt);
                estate->cursor_return_data =
                    (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->refcursor_data.return_number);
                estate->cursor_return_numbers = fcinfo->refcursor_data.return_number;
                (void)MemoryContextSwitchTo(oldcontext);
            }

            if (estate->cursor_return_data != NULL) {
                for (i = 0; i < fcinfo->refcursor_data.return_number; i++) {
                    int rc = memcpy_s(&estate->cursor_return_data[i], sizeof(Cursor_Data),
                                      &fcinfo->refcursor_data.returnCursor[i], sizeof(Cursor_Data));
                    securec_check(rc, "\0", "\0");
                }
            }
        }
    }

    /* Which protocol does function want to use? */
    if (rsinfo.returnMode == SFRM_ValuePerCall)
    {
        if (*isDone != ExprEndResult)
        {
            /*
			 * Save the current argument values to re-use on the next call.
             */
            if (*isDone == ExprMultipleResult)
            {
                fcache->setArgsValid = true;
                /* Register cleanup callback if we didn't already */
                if (!fcache->shutdown_reg)
                {
                    RegisterExprContextCallback(econtext,
                                                ShutdownFuncExpr,
                                                PointerGetDatum(fcache));
                    fcache->shutdown_reg = true;
                }
            }
        }
    }
    else if (rsinfo.returnMode == SFRM_Materialize)
    {
        /* check we're on the same page as the function author */
        if (rsinfo.isDone != ExprSingleResult)
            ereport(ERROR,
                    (errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
                     errmsg("table-function protocol for materialize mode was not followed")));
        if (rsinfo.setResult != NULL)
        {
            /* prepare to return values from the tuplestore */
            ExecPrepareTuplestoreResult(fcache, econtext,
                                        rsinfo.setResult,
                                        rsinfo.setDesc);
            /* loop back to top to start returning from tuplestore */
            goto restart;
        }
        /* if setResult was left null, treat it as empty set */
        *isDone = ExprEndResult;
        *isNull = true;
        result = (Datum) 0;
    }
    else
        ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
                 errmsg("unrecognized table-function returnMode: %d",
                        (int) rsinfo.returnMode)));

    if (has_refcursor) {
        pfree_ext(fcinfo->refcursor_data.argCursor);
        pfree_ext(var_dno);
    }

    if (fcache->is_plpgsql_func_with_outparam) {
        set_result_for_plpgsql_language_function_with_outparam_by_flatten(&result, isNull);
    }

    return result;
}