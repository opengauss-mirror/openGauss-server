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
static void init_sexpr(Oid foid, Oid input_collation, FuncExprState *sexpr, MemoryContext sexprCxt, bool allowSRF,
                       bool needDescForSRF);
static void ShutdownSetExpr(Datum arg);
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
 * Find the real function return type based on the actual func args' types.
 * @inPara arg_num: the number of func's args.
 * @inPara actual_arg_types: the type array of actual func args'.
 * @inPara fcache: the FuncExprState of this functin.
 * @return Oid: the real func return type.
 */
static Oid getRealFuncRetype(int arg_num, Oid* actual_arg_types, FuncExprState* fcache)
{
    Oid funcid = fcache->func.fn_oid;
    Oid rettype = fcache->func.fn_rettype;

    /* Find the declared arg types in PROCOID by funcid. */
    HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_EXECUTOR),
                errmsg("cache lookup failed for function %u", funcid)));

    oidvector* proargs = ProcedureGetArgTypes(proctup);
    Oid* declared_arg_types = proargs->values;

    /* Find the real return type based on the declared arg types and actual arg types.*/
    rettype = enforce_generic_type_consistency(actual_arg_types, declared_arg_types, arg_num, rettype, false);

    ReleaseSysCache(proctup);
    return rettype;
}

/*
 * Check whether the function is a set function supported by the vector engine.
 */
static bool isVectorEngineSupportSetFunc(Oid funcid)
{
    switch (funcid) {
        case OID_REGEXP_SPLIT_TO_TABLE:                // regexp_split_to_table
        case OID_REGEXP_SPLIT_TO_TABLE_NO_FLAG:        // regexp_split_to_table
        case OID_ARRAY_UNNEST:                         // unnest
            return true;
            break;
        default:
            return false;
            break;
    }
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
 * init_sexpr - initialize a FuncExprState node during first use
 */
static void init_sexpr(Oid foid, Oid input_collation, FuncExprState *sexpr, MemoryContext sexprCxt, bool allowSRF,
                       bool needDescForSRF)
{
    AclResult aclresult;

    /* Check permission to call function */
    aclresult = pg_proc_aclcheck(foid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(foid));
    InvokeFunctionExecuteHook(foid);

    /*
     * Safety check on nargs.  Under normal circumstances this should never
     * fail, as parser should check sooner.  But possibly it might fail if
     * server has been compiled with FUNC_MAX_ARGS smaller than some functions
     * declared in pg_proc?
     */
    if (list_length(sexpr->args) > FUNC_MAX_ARGS)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg_plural("cannot pass more than %d argument to a function",
                               "cannot pass more than %d arguments to a function", FUNC_MAX_ARGS, FUNC_MAX_ARGS)));

    /* Set up the primary fmgr lookup information */
    fmgr_info_cxt(foid, &(sexpr->func), sexprCxt);
    fmgr_info_set_expr((Node *)sexpr->xprstate.expr, &(sexpr->func));

    /* Initialize the function call parameter struct as well */
    InitFunctionCallInfoData(sexpr->fcinfo_data, &(sexpr->func), list_length(sexpr->args), input_collation, NULL, NULL);

    /* If function returns set, check if that's allowed by caller */
    if (sexpr->func.fn_retset && !allowSRF)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));

    /* Otherwise, caller should have marked the sexpr correctly */
    Assert(sexpr->func.fn_retset == sexpr->funcReturnsSet);

    /* If function returns set, prepare expected tuple descriptor */
    if (sexpr->func.fn_retset && needDescForSRF) {
        TypeFuncClass functypclass;
        Oid funcrettype;
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        functypclass = get_expr_result_type(sexpr->func.fn_expr, &funcrettype, &tupdesc);

        /* Must save tupdesc in sexpr's context */
        oldcontext = MemoryContextSwitchTo(sexprCxt);

        if (functypclass == TYPEFUNC_COMPOSITE) {
            /* Composite data type, e.g. a table's row type */
            Assert(tupdesc);
            /* Must copy it out of typcache for safety */
            sexpr->funcResultDesc = CreateTupleDescCopy(tupdesc);
            sexpr->funcReturnsTuple = true;
        } else if (functypclass == TYPEFUNC_SCALAR) {
            /* Base data type, i.e. scalar */
            tupdesc = CreateTemplateTupleDesc(1, false);
            TupleDescInitEntry(tupdesc, (AttrNumber)1, NULL, funcrettype, -1, 0);
            sexpr->funcResultDesc = tupdesc;
            sexpr->funcReturnsTuple = false;
        } else if (functypclass == TYPEFUNC_RECORD) {
            /* This will work if function doesn't need an expectedDesc */
            sexpr->funcResultDesc = NULL;
            sexpr->funcReturnsTuple = true;
        } else {
            /* Else, we will fail if function needs an expectedDesc */
            sexpr->funcResultDesc = NULL;
        }

        MemoryContextSwitchTo(oldcontext);
    } else
        sexpr->funcResultDesc = NULL;

    /* Initialize additional state */
    sexpr->funcResultStore = NULL;
    sexpr->funcResultSlot = NULL;
    sexpr->shutdown_reg = false;
}

/*
 * callback function in case a FuncExprState needs to be shut down before it
 * has been run to completion
 */
static void ShutdownSetExpr(Datum arg)
{
    FuncExprState *sexpr = castNode(FuncExprState, DatumGetPointer(arg));

    /* If we have a slot, make sure it's let go of any tuplestore pointer */
    if (sexpr->funcResultSlot)
        ExecClearTuple(sexpr->funcResultSlot);

    /* Release any open tuplestore */
    if (sexpr->funcResultStore)
        tuplestore_end(sexpr->funcResultStore);
    sexpr->funcResultStore = NULL;

    /* Clear any active set-argument state */
    sexpr->setArgsValid = false;

    /* execUtils will deregister the callback... */
    sexpr->shutdown_reg = false;
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