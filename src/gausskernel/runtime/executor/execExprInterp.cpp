/*-------------------------------------------------------------------------
 *
 * execExprInterp.c
 *	  Interpreted evaluation of an expression step list.
 *
 * This file provides either a "direct threaded" (for gcc, clang and
 * compatible) or a "switch threaded" (for all compilers) implementation of
 * expression evaluation.  The former is amongst the fastest known methods
 * of interpreting programs without resorting to assembly level work, or
 * just-in-time compilation, but it requires support for computed gotos.
 * The latter is amongst the fastest approaches doable in standard C.
 *
 * In either case we use ExprEvalStep->opcode to dispatch to the code block
 * within ExecInterpExpr() that implements the specific opcode type.
 *
 * Switch-threading uses a plain switch() statement to perform the
 * dispatch.  This has the advantages of being plain C and allowing the
 * compiler to warn if implementation of a specific opcode has been forgotten.
 * The disadvantage is that dispatches will, as commonly implemented by
 * compilers, happen from a single location, requiring more jumps and causing
 * bad branch prediction.
 *
 * In direct threading, we use gcc's label-as-values extension - also adopted
 * by some other compilers - to replace ExprEvalStep->opcode with the address
 * of the block implementing the instruction. Dispatch to the next instruction
 * is done by a "computed goto".  This allows for better branch prediction
 * (as the jumps are happening from different locations) and fewer jumps
 * (as no preparatory jump to a common dispatch location is needed).
 *
 * When using direct threading, ExecReadyInterpretedExpr will replace
 * each step's opcode field with the address of the relevant code block and
 * ExprState->flags will contain EEO_FLAG_DIRECT_THREADED to remember that
 * that's been done.
 *
 * For very simple instructions the overhead of the full interpreter
 * "startup", as minimal as it is, is noticeable.  Therefore
 * ExecReadyInterpretedExpr will choose to implement certain simple
 * opcode patterns using special fast-path routines (ExecJust*).
 *
 * Complex or uncommon instructions are not implemented in-line in
 * ExecInterpExpr(), rather we call out to a helper function appearing later
 * in this file.  For one reason, there'd not be a noticeable performance
 * benefit, but more importantly those complex routines are intended to be
 * shared between different expression evaluation approaches.  For instance
 * a JIT compiler would generate calls to them.  (This is why they are
 * exported rather than being "static" in this file.)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execExprInterp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "commands/sequence.h"
#include "nodes/execExpr.h"
#include "executor/node/nodeSubplan.h"
#include "funcapi.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "utils/elog.h"
#include "utils/expandeddatum.h"
#include "access/tableam.h"
#include "access/tupconvert.h"
#include "executor/node/nodeCtescan.h"
#include "rewrite/rewriteHandler.h"
#ifdef USE_SPQ
#include "access/sysattr.h"
#endif
/*
 * Use computed-goto-based opcode dispatch when computed gotos are available.
 * But use a separate symbol so that it's easy to adjust locally in this file
 * for development and testing.
 */
#ifdef HAVE_COMPUTED_GOTO
#define EEO_USE_COMPUTED_GOTO
#endif

/*
 * Macros for opcode dispatch.
 *
 * EEO_SWITCH - just hides the switch if not in use.
 * EEO_CASE - labels the implementation of named expression step type.
 * EEO_DISPATCH - jump to the implementation of the step type for 'op'.
 * EEO_OPCODE - compute opcode required by used expression evaluation method.
 * EEO_NEXT - increment 'op' and jump to correct next step type.
 * EEO_JUMP - jump to the specified step number within the current expression.
 */
#if defined(EEO_USE_COMPUTED_GOTO)

/* struct for jump target -> opcode lookup table */
typedef struct ExprEvalOpLookup
{
	const void *opcode;
	ExprEvalOp	op;
} ExprEvalOpLookup;

/* to make dispatch_table accessible outside ExecInterpExpr() */
static const void **dispatch_table = NULL;

/* jump target -> opcode lookup table */
static ExprEvalOpLookup reverse_dispatch_table[EEOP_LAST];

#define EEO_SWITCH()
#define EEO_CASE(name)		CASE_##name:
#define EEO_DISPATCH()		goto *((void *) op->opcode)
#define EEO_OPCODE(opcode)	((intptr_t) dispatch_table[opcode])

#else							/* !EEO_USE_COMPUTED_GOTO */

#define EEO_SWITCH()		starteval: switch ((ExprEvalOp) op->opcode)
#define EEO_CASE(name)		case name:
#define EEO_DISPATCH()		goto starteval
#define EEO_OPCODE(opcode)	(opcode)

#endif							/* EEO_USE_COMPUTED_GOTO */

#define EEO_NEXT() \
	do { \
		op++; \
		EEO_DISPATCH(); \
	} while (0)

#define EEO_JUMP(stepno) \
	do { \
		op = &state->steps[stepno]; \
		EEO_DISPATCH(); \
	} while (0)

static Datum ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static void ExecInitInterpreter(void);

/* support functions */
static TupleDesc get_cached_rowtype(Oid type_id, int32 typmod,
				   TupleDesc *cache_field, ExprContext *econtext);
static void ShutdownTupleDescRef(Datum arg);
static void ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op,
				   ExprContext *econtext, bool checkisnull);

/* fast-path evaluation functions */
static Datum ExecJustInnerVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static Datum ExecJustOuterVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static Datum ExecJustScanVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static Datum ExecJustConst(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static Datum ExecJustAssignInnerVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static Datum ExecJustAssignOuterVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);
static Datum ExecJustAssignScanVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone);

extern bool func_has_refcursor_args(Oid Funcid, FunctionCallInfoData* fcinfo);
extern void check_huge_clob_paramter(FunctionCallInfoData* fcinfo, bool is_have_huge_clob);
extern Datum fetch_lob_value_from_tuple(varatt_lob_pointer* lob_pointer, Oid update_oid, bool* is_null);

static FORCE_INLINE void ExecAggPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno);
static FORCE_INLINE void ExecAggCollectPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno);
static FORCE_INLINE void ExecAggPlainTransByRef(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno);
static FORCE_INLINE void ExecAggCollectPlainTransByRef(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno);

static void CheckVarSlotCompatibility(TupleTableSlot *slot,  int attnum, Oid vartype);
/*
 * Prepare ExprState for interpreted execution.
 */
void
ExecReadyInterpretedExpr(ExprState *state)
{
    /* Ensure one-time interpreter setup has been done */
	ExecInitInterpreter();


	/* Simple validity checks on expression */
	Assert(state->steps_len >= 1);
	Assert(state->steps[state->steps_len - 1].opcode == EEOP_DONE);

	/*
	 * Don't perform redundant initialization. This is unreachable in current
	 * cases, but might be hit if there's additional expression evaluation
	 * methods that rely on interpreted execution to work.
	 */
	if (state->flags & EEO_FLAG_INTERPRETER_INITIALIZED)
		return;

	/* DIRECT_THREADED should not already be set */
	Assert((state->flags & EEO_FLAG_DIRECT_THREADED) == 0);

	/*
	 * There shouldn't be any errors before the expression is fully
	 * initialized, and even if so, it'd lead to the expression being
	 * abandoned.  So we can set the flag now and save some code.
	 */
	state->flags |= EEO_FLAG_INTERPRETER_INITIALIZED;

	state->evalfunc = ExecInterpExprStillValid;
	/*
	 * Select fast-path evalfuncs for very simple expressions.  "Starting up"
	 * the full interpreter is a measurable overhead for these, and these
	 * patterns occur often enough to be worth optimizing.
	 */
	if (state->steps_len == 3)
	{
		ExprEvalOp	step0 = (ExprEvalOp)state->steps[0].opcode;
		ExprEvalOp	step1 = (ExprEvalOp) state->steps[1].opcode;

		if (step0 == EEOP_INNER_FETCHSOME &&
			step1 == EEOP_INNER_VAR)
		{
			state->evalfunc = ExecJustInnerVar;
			return;
		}
		else if (step0 == EEOP_OUTER_FETCHSOME &&
				 step1 == EEOP_OUTER_VAR)
		{
			state->evalfunc = ExecJustOuterVar;
			return;
		}
		else if (step0 == EEOP_SCAN_FETCHSOME &&
				 step1 == EEOP_SCAN_VAR)
		{
			state->evalfunc = ExecJustScanVar;
			return;
		}
		else if (step0 == EEOP_INNER_FETCHSOME &&
				 step1 == EEOP_ASSIGN_INNER_VAR)
		{
			state->evalfunc = ExecJustAssignInnerVar;
			return;
		}
		else if (step0 == EEOP_OUTER_FETCHSOME &&
				 step1 == EEOP_ASSIGN_OUTER_VAR)
		{
			state->evalfunc = ExecJustAssignOuterVar;
			return;
		}
		else if (step0 == EEOP_SCAN_FETCHSOME &&
				 step1 == EEOP_ASSIGN_SCAN_VAR)
		{
			state->evalfunc = ExecJustAssignScanVar;
			return;
		}
	}
	else if (state->steps_len == 2 &&
			 state->steps[0].opcode == EEOP_CONST)
	{
		state->evalfunc = ExecJustConst;
		return;
	}

#if defined(EEO_USE_COMPUTED_GOTO)

	/*
	 * In the direct-threaded implementation, replace each opcode with the
	 * address to jump to.  (Use ExecEvalStepOp() to get back the opcode.)
	 */
	{
		int			off;

		for (off = 0; off < state->steps_len; off++)
		{
			ExprEvalStep *op = &state->steps[off];

			op->opcode = EEO_OPCODE(op->opcode);
		}

		state->flags |= EEO_FLAG_DIRECT_THREADED;
	}
#endif							/* EEO_USE_COMPUTED_GOTO */

	state->evalfunc = ExecInterpExpr;
}

bool IsTableOfFunc(Oid funcOid)
{
    const Oid array_function_start_oid = 7881;
    const Oid array_function_end_oid = 7892;
    const Oid array_indexby_delete_oid = 7896;
    return (funcOid >= array_function_start_oid && funcOid <= array_function_end_oid) ||
        funcOid == array_indexby_delete_oid;
}

static Datum 
ExecMakeFunctionResultNoSets(ExprState *state, ExprEvalStep *op,ExprContext *econtext)
{
	FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    PgStat_FunctionCallUsage fcusage;
	int i;
	ListCell* lc = NULL;
	
    bool savedIsSTP = u_sess->SPI_cxt.is_stp;
    bool savedProConfigIsSet = u_sess->SPI_cxt.is_proconfig_set;
    bool is_have_huge_clob = false;
	bool needResetErrMsg = op->d.func.needResetErrMsg;
    int func_encoding = PG_INVALID_ENCODING;
    int db_encoding = PG_INVALID_ENCODING;

	if(!u_sess->SPI_cxt.is_allow_commit_rollback){
		if(fcinfo->context){
			((FunctionScanState *)(fcinfo->context))->atomic =true;
		}
	}

    econtext->plpgsql_estate = plpgsql_estate;
    plpgsql_estate = NULL;

	if (econtext) {
        fcinfo->can_ignore = econtext->can_ignore;
    }

    /*
     * Incause of connet_by_root() and sys_connect_by_path() we need get the
     * current scan tuple slot so attach the econtext here
     *
     * NOTE: Have to revisit!! so I don't have better solution to handle the case
     *       where scantuple is available in built in funct
     */
    if (fcinfo->flinfo->fn_oid == CONNECT_BY_ROOT_FUNCOID ||
        fcinfo->flinfo->fn_oid == SYS_CONNECT_BY_PATH_FUNCOID) {
        fcinfo->swinfo.sw_econtext = (Node *)econtext;
        fcinfo->swinfo.sw_exprstate = (Node *)linitial(op->d.func.args);
        fcinfo->swinfo.sw_is_flt_frame = true;
    }

    if (DB_IS_CMPT(B_FORMAT)) {
        func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
        db_encoding = GetDatabaseEncoding();
    }

    i = 0;
    econtext->is_cursor = false;
    u_sess->plsql_cxt.func_tableof_index = NIL;

    foreach (lc, op->d.func.args) {
		Expr *arg = (Expr *)lfirst(lc);

        if ((op->d.func.flag & FUNC_EXPR_FLAG_HAS_REFCURSOR) && 
			fcinfo->argTypes[i] == REFCURSOROID) {
            econtext->is_cursor = true;
		}

        if (is_huge_clob(fcinfo->argTypes[i], fcinfo->argnull[i], fcinfo->arg[i])) {
            is_have_huge_clob = true;
        }

        ExecTableOfIndexInfo execTableOfIndexInfo;
        initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
        ExecEvalParamExternTableOfIndex((Node*)arg, &execTableOfIndexInfo);
        if (execTableOfIndexInfo.tableOfIndex != NULL) {
            if (!IsTableOfFunc(fcinfo->flinfo->fn_oid)) {
                MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
                PLpgSQL_func_tableof_index* func_tableof =
                    (PLpgSQL_func_tableof_index*)palloc0(sizeof(PLpgSQL_func_tableof_index));
                func_tableof->varno = i;
                func_tableof->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
                func_tableof->tableOfIndex = copyTableOfIndex(execTableOfIndexInfo.tableOfIndex);
                u_sess->plsql_cxt.func_tableof_index = lappend(u_sess->plsql_cxt.func_tableof_index, func_tableof);
                MemoryContextSwitchTo(oldCxt);
            }

            u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
            u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = execTableOfIndexInfo.tableOfIndex;
            u_sess->SPI_cxt.cur_tableof_index->tableOfNestLayer = execTableOfIndexInfo.tableOfLayers;
            /* for nest table of output, save layer of this var tableOfGetNestLayer in ExecEvalArrayRef,
            or set to zero for get whole nest table. */
            u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer = -1;
        }

        if ((op->d.func.flag & FUNC_EXPR_FLAG_HAS_REFCURSOR) && 
			econtext->is_cursor) {
            op->d.func.var_dno[i] = econtext->dno;
            CopyCursorInfoData(&fcinfo->refcursor_data.argCursor[i], &econtext->cursor_data);
        }
        econtext->is_cursor = false;
        i++;
    }

    /*
     * If function is strict, and there are any NULL arguments, skip calling
     * the function and return NULL.
     */
    if (op->d.func.flag & FUNC_EXPR_FLAG_STRICT) {
        while (--i >= 0) {
            if (fcinfo->argnull[i]) {
				*op->resnull = true;
                u_sess->SPI_cxt.is_stp = savedIsSTP;
                u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
                if (needResetErrMsg) {
                    stp_reset_commit_rolback_err_msg();
                }
				*op->resvalue = 0;
                return *op->resvalue;
            }
        }
    }

	if (op->d.func.flag & FUNC_EXPR_FLAG_FUSAGE)
    	pgstat_init_function_usage(fcinfo, &fcusage);

    fcinfo->isnull = false;
    check_huge_clob_paramter(fcinfo, is_have_huge_clob);
    if (u_sess->instr_cxt.global_instr != NULL && fcinfo->flinfo->fn_addr == plpgsql_call_handler) {
        StreamInstrumentation* save_global_instr = u_sess->instr_cxt.global_instr;
        u_sess->instr_cxt.global_instr = NULL;
        if (func_encoding != db_encoding) {
            DB_ENCODING_SWITCH_TO(func_encoding);
            *op->resvalue = op->d.func.fn_addr(fcinfo);
            DB_ENCODING_SWITCH_BACK(db_encoding);
        } else {
            *op->resvalue = op->d.func.fn_addr(fcinfo);
        }
        u_sess->instr_cxt.global_instr = save_global_instr;
    } else {
        if (fcinfo->argTypes[0] == CLOBOID && fcinfo->argTypes[1] == CLOBOID && fcinfo->flinfo->fn_addr == textcat) {
            bool is_null = false;
            if (fcinfo->arg[0] != 0 && VARATT_IS_EXTERNAL_LOB(fcinfo->arg[0])) {
                struct varatt_lob_pointer* lob_pointer = (varatt_lob_pointer*)(VARDATA_EXTERNAL(fcinfo->arg[0]));
                fcinfo->arg[0] = fetch_lob_value_from_tuple(lob_pointer, InvalidOid, &is_null);
            }
            if (fcinfo->arg[1] != 0 && VARATT_IS_EXTERNAL_LOB(fcinfo->arg[1])) {
                struct varatt_lob_pointer* lob_pointer = (varatt_lob_pointer*)(VARDATA_EXTERNAL(fcinfo->arg[1]));
                fcinfo->arg[1] = fetch_lob_value_from_tuple(lob_pointer, InvalidOid, &is_null);
            }
        }
        if (func_encoding != db_encoding) {
            DB_ENCODING_SWITCH_TO(func_encoding);
            *op->resvalue = op->d.func.fn_addr(fcinfo);
            DB_ENCODING_SWITCH_BACK(db_encoding);
        } else {
            *op->resvalue = op->d.func.fn_addr(fcinfo);
        }
    }
	*op->resnull = fcinfo->isnull;

    if ((op->d.func.flag & FUNC_EXPR_FLAG_HAS_REFCURSOR) && 
		econtext->plpgsql_estate != NULL) {
        PLpgSQL_execstate* estate = econtext->plpgsql_estate;
        for (i = 0; i < fcinfo->nargs; i++) {
            /* copy in-args cursor option info */
            if (op->d.func.var_dno[i] >= 0) {
                int dno = op->d.func.var_dno[i];
                Cursor_Data* cursor_data = &fcinfo->refcursor_data.argCursor[i];
#ifdef USE_ASSERT_CHECKING
                PLpgSQL_datum* datum = estate->datums[dno];
#endif
                Assert(datum->dtype == PLPGSQL_DTYPE_VAR);
                Assert(((PLpgSQL_var*)datum)->datatype->typoid == REFCURSOROID);

                ExecCopyDataToDatum(estate->datums, dno, cursor_data);
            }
        }

        if (fcinfo->flinfo->fn_rettype == REFCURSOROID) {
            /* copy function returns cursor option info.
             * for simple expr in exec_eval_expr, we can not get the result type,
             * so cursor_return_data mallocs here.
             */
            if (estate->cursor_return_data == NULL) {
                estate->cursor_return_data = (Cursor_Data*)palloc0(sizeof(Cursor_Data));
                estate->cursor_return_numbers = 1;
            }
            int rc = memcpy_s(estate->cursor_return_data,
                sizeof(Cursor_Data),
                fcinfo->refcursor_data.returnCursor,
                sizeof(Cursor_Data));
            securec_check(rc, "\0", "\0");
        }
    }

	if (op->d.func.flag & FUNC_EXPR_FLAG_FUSAGE)
    	pgstat_end_function_usage(&fcusage, true);

    if (op->d.func.flag & FUNC_EXPR_FLAG_HAS_REFCURSOR) {
        if (fcinfo->refcursor_data.argCursor != NULL)
            pfree_ext(fcinfo->refcursor_data.argCursor);
        if (op->d.func.var_dno != NULL)
            pfree_ext(op->d.func.var_dno);
    }

    u_sess->SPI_cxt.is_stp = savedIsSTP;
    u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
    if (needResetErrMsg) {
        stp_reset_commit_rolback_err_msg();
    }

	if (op->d.func.is_plpgsql_func_with_outparam) {
    	set_result_for_plpgsql_language_function_with_outparam_by_flatten(op->resvalue, op->resnull);
	}

    return *op->resvalue;
}

/*
 * Evaluate expression identified by "state" in the execution context
 * given by "econtext".  *isnull is set to the is-null flag for the result,
 * and the Datum value is the function result.
 *
 * As a special case, return the dispatch table's address if state is NULL.
 * This is used by ExecInitInterpreter to set up the dispatch_table global.
 * (Only applies when EEO_USE_COMPUTED_GOTO is defined.)
 */
static Datum
ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op;
	TupleTableSlot *resultslot;
	TupleTableSlot *innerslot;
	TupleTableSlot *outerslot;
	TupleTableSlot *scanslot;

	/*
	 * This array has to be in the same order as enum ExprEvalOp.
	 */
#if defined(EEO_USE_COMPUTED_GOTO)
	static const void *const dispatch_table[] = {
		&&CASE_EEOP_DONE,
		&&CASE_EEOP_INNER_FETCHSOME,
		&&CASE_EEOP_OUTER_FETCHSOME,
		&&CASE_EEOP_SCAN_FETCHSOME,
		&&CASE_EEOP_INNER_VAR,
		&&CASE_EEOP_OUTER_VAR,
		&&CASE_EEOP_SCAN_VAR,
		&&CASE_EEOP_INNER_SYSVAR,
		&&CASE_EEOP_OUTER_SYSVAR,
		&&CASE_EEOP_SCAN_SYSVAR,
		&&CASE_EEOP_WHOLEROW,
		&&CASE_EEOP_ASSIGN_INNER_VAR,
		&&CASE_EEOP_ASSIGN_OUTER_VAR,
		&&CASE_EEOP_ASSIGN_SCAN_VAR,
		&&CASE_EEOP_ASSIGN_TMP,
		&&CASE_EEOP_ASSIGN_TMP_MAKE_RO,
		&&CASE_EEOP_CONST,
		&&CASE_EEOP_FUNCEXPR,
        &&CASE_EEOP_FUNCEXPR_STRICT,
        &&CASE_EEOP_FUNCEXPR_FUSAGE,
        &&CASE_EEOP_FUNCEXPR_STRICT_FUSAGE,
        &&CASE_EEOP_FUNCEXPR_MAKE_FUNCTION_RESULT,
		&&CASE_EEOP_BOOL_AND_STEP_FIRST,
		&&CASE_EEOP_BOOL_AND_STEP,
		&&CASE_EEOP_BOOL_AND_STEP_LAST,
		&&CASE_EEOP_BOOL_OR_STEP_FIRST,
		&&CASE_EEOP_BOOL_OR_STEP,
		&&CASE_EEOP_BOOL_OR_STEP_LAST,
		&&CASE_EEOP_BOOL_NOT_STEP,
		&&CASE_EEOP_QUAL,
		&&CASE_EEOP_JUMP,
		&&CASE_EEOP_JUMP_IF_NULL,
		&&CASE_EEOP_JUMP_IF_NOT_NULL,
		&&CASE_EEOP_JUMP_IF_NOT_TRUE,
		&&CASE_EEOP_NULLTEST_ISNULL,
		&&CASE_EEOP_NULLTEST_ISNOTNULL,
		&&CASE_EEOP_NULLTEST_ROWISNULL,
		&&CASE_EEOP_NULLTEST_ROWISNOTNULL,
		&&CASE_EEOP_BOOLTEST_IS_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_FALSE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_FALSE,
		&&CASE_EEOP_PARAM_EXEC,
		&&CASE_EEOP_PARAM_EXTERN,
		&&CASE_EEOP_CASE_TESTVAL,
		&&CASE_EEOP_MAKE_READONLY,
		&&CASE_EEOP_IOCOERCE,
		&&CASE_EEOP_DISTINCT,
		&&CASE_EEOP_NULLIF,
		&&CASE_EEOP_CURRENTOFEXPR,
		&&CASE_EEOP_ARRAYEXPR,
		&&CASE_EEOP_ARRAYCOERCE,
		&&CASE_EEOP_ROW,
		&&CASE_EEOP_ROWCOMPARE_STEP,
		&&CASE_EEOP_ROWCOMPARE_FINAL,
		&&CASE_EEOP_MINMAX,
		&&CASE_EEOP_FIELDSELECT,
		&&CASE_EEOP_FIELDSTORE_DEFORM,
		&&CASE_EEOP_FIELDSTORE_FORM,
		&&CASE_EEOP_ARRAYREF_SUBSCRIPT,
		&&CASE_EEOP_ARRAYREF_OLD,
		&&CASE_EEOP_ARRAYREF_ASSIGN,
		&&CASE_EEOP_ARRAYREF_FETCH,
		&&CASE_EEOP_DOMAIN_TESTVAL,
		&&CASE_EEOP_DOMAIN_NOTNULL,
		&&CASE_EEOP_DOMAIN_CHECK,
		&&CASE_EEOP_CONVERT_ROWTYPE,
		&&CASE_EEOP_SCALARARRAYOP,
		&&CASE_EEOP_XMLEXPR,
		&&CASE_EEOP_AGGREF,
		&&CASE_EEOP_GROUPING_FUNC,
		&&CASE_EEOP_WINDOW_FUNC,
		&&CASE_EEOP_SUBPLAN,
		&&CASE_EEOP_ALTERNATIVE_SUBPLAN,
		&&CASE_EEOP_ROWNUM,
		&&CASE_EEOP_GROUPING_ID,
		&&CASE_EEOP_HASH_FILTER,
		&&CASE_EEOP_USERVAR_OR_SETVARIABLE,
		&&CASE_EEOP_USERSET_ELEM,
        &&CASE_EEOP_PREFIX_BTYEA,
        &&CASE_EEOP_PREFIX_TEXT,
        &&CASE_EEOP_AGG_STRICT_DESERIALIZE,
        &&CASE_EEOP_AGG_DESERIALIZE,
        &&CASE_EEOP_AGG_STRICT_INPUT_CHECK,
        &&CASE_EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYVAL,
        &&CASE_EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYVAL,
        &&CASE_EEOP_AGG_PLAIN_TRANS_BYVAL,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_BYVAL,
        &&CASE_EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYREF,
        &&CASE_EEOP_AGG_PLAIN_TRANS_STRICT_BYREF,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYREF,
        &&CASE_EEOP_AGG_PLAIN_TRANS_BYREF,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_BYREF,
        &&CASE_EEOP_AGG_ORDERED_TRANS_DATUM,
        &&CASE_EEOP_AGG_ORDERED_TRANS_TUPLE,
		&&CASE_EEOP_LAST
	};

	StaticAssertStmt(EEOP_LAST + 1 == lengthof(dispatch_table),
					 "dispatch_table out of whack with ExprEvalOp");

	if (unlikely(state == NULL))
		return PointerGetDatum(dispatch_table);
#else
	Assert(state != NULL);
#endif							/* EEO_USE_COMPUTED_GOTO */

	/* setup state */
	op = state->steps;
	resultslot = state->resultslot;
	innerslot = econtext->ecxt_innertuple;
	outerslot = econtext->ecxt_outertuple;
	scanslot = econtext->ecxt_scantuple;

	if (isDone != NULL)
        *isDone = ExprSingleResult;

#if defined(EEO_USE_COMPUTED_GOTO)
	EEO_DISPATCH();
#endif

	EEO_SWITCH()
	{
		EEO_CASE(EEOP_DONE)
		{
			goto out;
		}

		EEO_CASE(EEOP_INNER_FETCHSOME)
		{
			tableam_tslot_getsomeattrs(innerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_FETCHSOME)
		{
			tableam_tslot_getsomeattrs(outerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_FETCHSOME)
		{
			tableam_tslot_getsomeattrs(scanslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_VAR)
		{
			int			attnum = op->d.var.attnum;

			/*
			 * Since we already extracted all referenced columns from the
			 * tuple with a FETCHSOME step, we can just grab the value
			 * directly out of the slot's decomposed-data arrays.  But let's
			 * have an Assert to check that that did happen.
			 */
//			Assert((innerslot->tts_tupslotTableAm == TAM_HEAP) ? (attnum >= 0 && attnum < innerslot->tts_nvalid) : (attnum >= 0));
			*op->resvalue = innerslot->tts_values[attnum];
			*op->resnull = innerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_VAR)
		{
			int			attnum = op->d.var.attnum;

			/* See EEOP_INNER_VAR comments */

//			Assert((outerslot->tts_tupslotTableAm == TAM_HEAP) ? (attnum >= 0 && attnum < outerslot->tts_nvalid) : (attnum >= 0));
			*op->resvalue = outerslot->tts_values[attnum];
			*op->resnull = outerslot->tts_isnull[attnum];

			EEO_NEXT();
		}
		
		EEO_CASE(EEOP_SCAN_VAR)
		{
			int			attnum = op->d.var.attnum;

			/* See EEOP_INNER_VAR comments */
			
//			Assert((scanslot->tts_tupslotTableAm == TAM_HEAP) ? (attnum >= 0 && attnum < scanslot->tts_nvalid) : (attnum >= 0));
			*op->resvalue = scanslot->tts_values[attnum];
			*op->resnull = scanslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_SYSVAR)
		{
			int			attnum = op->d.var.attnum;
			Datum		d;

			/* these asserts must match defenses in slot_getattr */
			Assert(innerslot->tts_tuple != NULL);
			Assert(innerslot->tts_tuple != &(innerslot->tts_minhdr));

#ifdef USE_SPQ
            if (attnum == RootSelfItemPointerAttributeNumber) {
                Assert(innerslot->tts_tuple);
                *op->resnull = false;
                d = spq_get_root_ctid((HeapTuple)innerslot->tts_tuple, innerslot->tts_buffer, econtext);
                *op->resvalue = d;
            } else
#endif
            {
			/* heap_getsysattr has sufficient defenses against bad attnums */
			d = tableam_tslot_getattr(innerslot, attnum, op->resnull);
			*op->resvalue = d;
            }

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_SYSVAR)
		{
			int			attnum = op->d.var.attnum;
			Datum		d;

			/* these asserts must match defenses in slot_getattr */
			Assert(outerslot->tts_tuple != NULL);
			Assert(outerslot->tts_tuple != &(outerslot->tts_minhdr));
#ifdef USE_SPQ
            if (attnum == RootSelfItemPointerAttributeNumber) {
                Assert(outerslot->tts_tuple);
                *op->resnull = false;
                d = spq_get_root_ctid((HeapTuple)outerslot->tts_tuple, outerslot->tts_buffer, econtext);
                *op->resvalue = d;
            } else
#endif
            {
            /* heap_getsysattr has sufficient defenses against bad attnums */
            d = tableam_tslot_getattr(outerslot, attnum, op->resnull);
            *op->resvalue = d;
            }

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_SYSVAR)
		{
			int			attnum = op->d.var.attnum;
			Datum		d;

			/* these asserts must match defenses in slot_getattr */
			Assert(scanslot->tts_tuple != NULL);
			Assert(scanslot->tts_tuple != &(scanslot->tts_minhdr));
#ifdef USE_SPQ
            if (attnum == RootSelfItemPointerAttributeNumber) {
                Assert(scanslot->tts_tuple);
                *op->resnull = false;
                d = spq_get_root_ctid((HeapTuple)scanslot->tts_tuple, scanslot->tts_buffer, econtext);
                *op->resvalue = d;
            } else
#endif
            {
            /* heap_getsysattr has sufficient defenses against bad attnums */
            d = tableam_tslot_getattr(scanslot, attnum, op->resnull);
            *op->resvalue = d;
            }

			EEO_NEXT();

		}

		EEO_CASE(EEOP_WHOLEROW)
		{
			/* too complex for an inline implementation */
			ExecEvalWholeRowVar(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_INNER_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
//			Assert((innerslot->tts_tupslotTableAm == TAM_HEAP) ? (attnum >= 0 && attnum < innerslot->tts_nvalid) : (attnum >= 0));
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = innerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = innerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_OUTER_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
//			Assert((outerslot->tts_tupslotTableAm == TAM_HEAP) ? (attnum >= 0 && attnum < outerslot->tts_nvalid) : (attnum >= 0));
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = outerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = outerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_SCAN_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
//			Assert((scanslot->tts_tupslotTableAm == TAM_HEAP) ? (attnum >= 0 && attnum < scanslot->tts_nvalid) : (attnum >= 0));
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = scanslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = scanslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP)
		{
			int			resultnum = op->d.assign_tmp.resultnum;

			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = state->resvalue;
			resultslot->tts_isnull[resultnum] = state->resnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP_MAKE_RO)
		{
			int			resultnum = op->d.assign_tmp.resultnum;

			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_isnull[resultnum] = state->resnull;
			if (!resultslot->tts_isnull[resultnum])
				resultslot->tts_values[resultnum] =
					MakeExpandedObjectReadOnlyInternal(state->resvalue);
			else
				resultslot->tts_values[resultnum] = state->resvalue;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONST)
		{
			*op->resnull = op->d.constval.isnull;
			*op->resvalue = op->d.constval.value;

			/* if a const cursor, copy cursor option data to econtext */
    		if ((econtext->is_cursor || op->d.constval.is_cursor) && op->d.constval.con && 
				op->d.constval.con->consttype == REFCURSOROID) {
    		    CopyCursorInfoData(&econtext->cursor_data, &op->d.constval.con->cursor_data);
    		    econtext->dno = op->d.constval.con->cursor_data.cur_dno;
    		}

			EEO_NEXT();
		}

		/*
		 * Function-call implementations. Arguments have previously been
		 * evaluated directly into fcinfo->args.
		 *
		 * As both STRICT checks and function-usage are noticeable performance
		 * wise, and function calls are a very hot-path (they also back
		 * operators!), it's worth having so many separate opcodes.
		 *
		 * Note: the reason for using a temporary variable "d", here and in
		 * other places, is that some compilers think "*op->resvalue = f();"
		 * requires them to evaluate op->resvalue into a register before
		 * calling f(), just in case f() is able to modify op->resvalue
		 * somehow.  The extra line of code can save a useless register spill
		 * and reload across the function call.
		 */
        EEO_CASE(EEOP_FUNCEXPR)
        {
            // ExecMakeFunctionResultNoSets(state, op, econtext);
            FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
            Datum d;

            int func_encoding = PG_INVALID_ENCODING;
            int db_encoding = PG_INVALID_ENCODING;

            if (DB_IS_CMPT(B_FORMAT)) {
                func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
                db_encoding = GetDatabaseEncoding();
            }

            if (econtext) {
                fcinfo->can_ignore = econtext->can_ignore;
            }

            fcinfo->isnull = false;
            if (func_encoding != db_encoding) {
                DB_ENCODING_SWITCH_TO(func_encoding);
                d = op->d.func.fn_addr(fcinfo);
                DB_ENCODING_SWITCH_BACK(db_encoding);
            } else {
                d = op->d.func.fn_addr(fcinfo);
            }
            *op->resvalue = d;
            *op->resnull = fcinfo->isnull;

            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_STRICT)
        {
            FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
            int nargs = op->d.func.nargs;
            Datum d;

            int func_encoding = PG_INVALID_ENCODING;
            int db_encoding = PG_INVALID_ENCODING;

            if (DB_IS_CMPT(B_FORMAT)) {
                func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
                db_encoding = GetDatabaseEncoding();
            }

            if (econtext) {
                fcinfo->can_ignore = econtext->can_ignore;
            }

            /* strict function, so check for NULL args */
            for (int argno = 0; argno < nargs; argno++) {
                if (fcinfo->argnull[argno]) {
                    *op->resnull = true;
                    goto strictfail;
                }
            }
            fcinfo->isnull = false;
            if (func_encoding != db_encoding) {
                DB_ENCODING_SWITCH_TO(func_encoding);
                d = op->d.func.fn_addr(fcinfo);
                DB_ENCODING_SWITCH_BACK(db_encoding);
            } else {
                d = op->d.func.fn_addr(fcinfo);
            }
            *op->resvalue = d;
            *op->resnull = fcinfo->isnull;

        strictfail:
            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_FUSAGE)
        {
            /* not common enough to inline */
            ExecEvalFuncExprFusage(op, econtext);

            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_STRICT_FUSAGE)
        {
            /* not common enough to inline */
            ExecEvalFuncExprStrictFusage(op, econtext);

            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_MAKE_FUNCTION_RESULT)
        {
            ExecMakeFunctionResultNoSets(state, op, econtext);
            EEO_NEXT();
        }

		/*
		 * If any of its clauses is FALSE, an AND's result is FALSE regardless
		 * of the states of the rest of the clauses, so we can stop evaluating
		 * and return FALSE immediately.  If none are FALSE and one or more is
		 * NULL, we return NULL; otherwise we return TRUE.  This makes sense
		 * when you interpret NULL as "don't know": perhaps one of the "don't
		 * knows" would have been FALSE if we'd known its value.  Only when
		 * all the inputs are known to be TRUE can we state confidently that
		 * the AND's result is TRUE.
		 */
		EEO_CASE(EEOP_BOOL_AND_STEP_FIRST)
		{
			*op->d.boolexpr.anynull = false;

			/*
			 * EEOP_BOOL_AND_STEP_FIRST resets anynull, otherwise it's the
			 * same as EEOP_BOOL_AND_STEP - so fall through to that.
			 */

			/* FALL THROUGH */
		}

		EEO_CASE(EEOP_BOOL_AND_STEP)
		{
			if (*op->resnull)
			{
				*op->d.boolexpr.anynull = true;
			}
			else if (!DatumGetBool(*op->resvalue))
			{
				/* result is already set to FALSE, need not change it */
				/* bail out early */
				EEO_JUMP(op->d.boolexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_AND_STEP_LAST)
		{
			if (*op->resnull)
			{
				/* result is already set to NULL, need not change it */
			}
			else if (!DatumGetBool(*op->resvalue))
			{
				/* result is already set to FALSE, need not change it */

				/*
				 * No point jumping early to jumpdone - would be same target
				 * (as this is the last argument to the AND expression),
				 * except more expensive.
				 */
			}
			else if (*op->d.boolexpr.anynull)
			{
				*op->resvalue = (Datum) 0;
				*op->resnull = true;
			}
			else
			{
				/* result is already set to TRUE, need not change it */
			}

			EEO_NEXT();
		}

		/*
		 * If any of its clauses is TRUE, an OR's result is TRUE regardless of
		 * the states of the rest of the clauses, so we can stop evaluating
		 * and return TRUE immediately.  If none are TRUE and one or more is
		 * NULL, we return NULL; otherwise we return FALSE.  This makes sense
		 * when you interpret NULL as "don't know": perhaps one of the "don't
		 * knows" would have been TRUE if we'd known its value.  Only when all
		 * the inputs are known to be FALSE can we state confidently that the
		 * OR's result is FALSE.
		 */
		EEO_CASE(EEOP_BOOL_OR_STEP_FIRST)
		{
			*op->d.boolexpr.anynull = false;

			/*
			 * EEOP_BOOL_OR_STEP_FIRST resets anynull, otherwise it's the same
			 * as EEOP_BOOL_OR_STEP - so fall through to that.
			 */

			/* FALL THROUGH */
		}

		EEO_CASE(EEOP_BOOL_OR_STEP)
		{
			if (*op->resnull)
			{
				*op->d.boolexpr.anynull = true;
			}
			else if (DatumGetBool(*op->resvalue))
			{
				/* result is already set to TRUE, need not change it */
				/* bail out early */
				EEO_JUMP(op->d.boolexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_OR_STEP_LAST)
		{
			if (*op->resnull)
			{
				/* result is already set to NULL, need not change it */
			}
			else if (DatumGetBool(*op->resvalue))
			{
				/* result is already set to TRUE, need not change it */

				/*
				 * No point jumping to jumpdone - would be same target (as
				 * this is the last argument to the AND expression), except
				 * more expensive.
				 */
			}
			else if (*op->d.boolexpr.anynull)
			{
				*op->resvalue = (Datum) 0;
				*op->resnull = true;
			}
			else
			{
				/* result is already set to FALSE, need not change it */
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_NOT_STEP)
		{
			/*
			 * Evaluation of 'not' is simple... if expr is false, then return
			 * 'true' and vice versa.  It's safe to do this even on a
			 * nominally null value, so we ignore resnull; that means that
			 * NULL in produces NULL out, which is what we want.
			 */
			*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));

			EEO_NEXT();
		}

		EEO_CASE(EEOP_QUAL)
		{
			/* simplified version of BOOL_AND_STEP for use by ExecQual() */

			/* If argument (also result) is false or null ... */
			if (*op->resnull ||
				!DatumGetBool(*op->resvalue))
			{
				/* ... bail out early, returning FALSE */
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);
				EEO_JUMP(op->d.qualexpr.jumpdone);
			}

			/*
			 * Otherwise, leave the TRUE value in place, in case this is the
			 * last qual.  Then, TRUE is the correct answer.
			 */

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP)
		{
			/* Unconditionally jump to target step */
			EEO_JUMP(op->d.jump.jumpdone);
		}

		EEO_CASE(EEOP_JUMP_IF_NULL)
		{
			/* Transfer control if current result is null */
			if (*op->resnull)
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP_IF_NOT_NULL)
		{
			/* Transfer control if current result is non-null */
			if (!*op->resnull)
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP_IF_NOT_TRUE)
		{
			/* Transfer control if current result is null or false */
			if (*op->resnull || !DatumGetBool(*op->resvalue))
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNULL)
		{
			*op->resvalue = BoolGetDatum(*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNOTNULL)
		{
			*op->resvalue = BoolGetDatum(!*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNULL)
		{
			/* out of line implementation: too large */
			ExecEvalRowNull(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNOTNULL)
		{
			/* out of line implementation: too large */
			ExecEvalRowNotNull(state, op, econtext);

			EEO_NEXT();
		}

		/* BooleanTest implementations for all booltesttypes */

		EEO_CASE(EEOP_BOOLTEST_IS_TRUE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(false);
			else
				*op->resvalue = *op->resvalue;
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_TRUE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(true);
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_FALSE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(false);
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_FALSE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(true);
			else
				*op->resvalue = *op->resvalue;
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXEC)
		{
			/* out of line implementation: too large */
			ExecEvalParamExec(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXTERN)
		{
			/* out of line implementation: too large */
			ExecEvalParamExtern(state, op, econtext);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_CASE_TESTVAL)
		{
			/*
			 * Normally upper parts of the expression tree have setup the
			 * values to be returned here, but some parts of the system
			 * currently misuse {caseValue,domainValue}_{datum,isNull} to set
			 * run-time data.  So if no values have been set-up, use
			 * ExprContext's.  This isn't pretty, but also not *that* ugly,
			 * and this is unlikely to be performance sensitive enough to
			 * worry about an extra branch.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->caseValue_datum;
				*op->resnull = econtext->caseValue_isNull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_TESTVAL)
		{
			/*
			 * See EEOP_CASE_TESTVAL comment.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->domainValue_datum;
				*op->resnull = econtext->domainValue_isNull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_MAKE_READONLY)
		{
			/*
			 * Force a varlena value that might be read multiple times to R/O
			 */
			if (!*op->d.make_readonly.isnull)
				*op->resvalue =
					MakeExpandedObjectReadOnlyInternal(*op->d.make_readonly.value);
			*op->resnull = *op->d.make_readonly.isnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_IOCOERCE)
		{
			/*
			 * Evaluate a CoerceViaIO node.  This can be quite a hot path, so
			 * inline as much work as possible.  The source value is in our
			 * result variable.
			 */
			char	   *str;

			/* call output function (similar to OutputFunctionCall) */
			if (*op->resnull)
			{
				/* output functions are not called on nulls */
				str = NULL;
			}
			else
			{
				FunctionCallInfo fcinfo_out;

				fcinfo_out = op->d.iocoerce.fcinfo_data_out;
				fcinfo_out->arg[0] = *op->resvalue;
				fcinfo_out->argnull[0] = false;

				fcinfo_out->isnull = false;
				str = DatumGetCString(FunctionCallInvoke(fcinfo_out));

				/* OutputFunctionCall assumes result isn't null */
				Assert(!fcinfo_out->isnull);
			}

			/* call input function (similar to InputFunctionCall) */
			if (!op->d.iocoerce.finfo_in->fn_strict || str != NULL)
			{
				FunctionCallInfo fcinfo_in;

				fcinfo_in = op->d.iocoerce.fcinfo_data_in;
				fcinfo_in->arg[0] = PointerGetDatum(str);
				fcinfo_in->argnull[0] = *op->resnull;
				/* second and third arguments are already set up */

				fcinfo_in->isnull = false;
				*op->resvalue = FunctionCallInvoke(fcinfo_in);

				/* Should get null result if and only if str is NULL */
				if (str == NULL)
				{
					Assert(*op->resnull);
					Assert(fcinfo_in->isnull);
				}
				else
				{
					Assert(!*op->resnull);
					Assert(!fcinfo_in->isnull);
				}
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DISTINCT)
		{
			/*
			 * IS DISTINCT FROM must evaluate arguments (already done into
			 * fcinfo->args) to determine whether they are NULL; if either is
			 * NULL then the result is determined.  If neither is NULL, then
			 * proceed to evaluate the comparison function, which is just the
			 * type's standard equality operator.  We need not care whether
			 * that function is strict.  Because the handling of nulls is
			 * different, we can't just reuse EEOP_FUNCEXPR.
			 */
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* check function arguments for NULLness */
			if (fcinfo->argnull[0] && fcinfo->argnull[1])
			{
				/* Both NULL? Then is not distinct... */
				*op->resvalue = BoolGetDatum(false);
				*op->resnull = false;
			}
			else if (fcinfo->argnull[0] || fcinfo->argnull[1])
			{
				/* Only one is NULL? Then is distinct... */
				*op->resvalue = BoolGetDatum(true);
				*op->resnull = false;
			}
			else
			{
				/* Neither null, so apply the equality function */
				Datum		eqresult;

				fcinfo->isnull = false;
				eqresult = op->d.func.fn_addr(fcinfo);
				/* Must invert result of "="; safe to do even if null */
				*op->resvalue = BoolGetDatum(!DatumGetBool(eqresult));
				*op->resnull = fcinfo->isnull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLIF)
		{
			/*
			 * The arguments are already evaluated into fcinfo->args.
			 */
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* if either argument is NULL they can't be equal */
			if (!fcinfo->argnull[0] && !fcinfo->argnull[1])
			{
				Datum		result;

				fcinfo->isnull = false;
				result = op->d.func.fn_addr(fcinfo);

				/* if the arguments are equal return null */
				if (!fcinfo->isnull && DatumGetBool(result))
				{
					*op->resvalue = (Datum) 0;
					*op->resnull = true;

					EEO_NEXT();
				}
			}

			/* Arguments aren't equal, so return the first one */
			*op->resvalue = fcinfo->arg[0];
			*op->resnull = fcinfo->argnull[0];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CURRENTOFEXPR)
		{
			/* error invocation uses space, and shouldn't ever occur */
			ExecEvalCurrentOfExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYCOERCE)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayCoerce(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROW)
		{
			/* too complex for an inline implementation */
			ExecEvalRow(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROWCOMPARE_STEP)
		{
			FunctionCallInfo fcinfo = op->d.rowcompare_step.fcinfo_data;

			/* force NULL result if strict fn and NULL input */
			if (op->d.rowcompare_step.finfo->fn_strict &&
				(fcinfo->argnull[0] || fcinfo->argnull[1]))
			{
				*op->resnull = true;
				EEO_JUMP(op->d.rowcompare_step.jumpnull);
			}

			/* Apply comparison function */
			fcinfo->isnull = false;
			*op->resvalue = (op->d.rowcompare_step.fn_addr) (fcinfo);

			/* force NULL result if NULL function result */
			if (fcinfo->isnull)
			{
				*op->resnull = true;
				EEO_JUMP(op->d.rowcompare_step.jumpnull);
			}
			*op->resnull = false;

			/* If unequal, no need to compare remaining columns */
			if (DatumGetInt32(*op->resvalue) != 0)
			{
				EEO_JUMP(op->d.rowcompare_step.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROWCOMPARE_FINAL)
		{
			int32		cmpresult = DatumGetInt32(*op->resvalue);
			RowCompareType rctype = op->d.rowcompare_final.rctype;

			*op->resnull = false;
			switch (rctype)
			{
					/* EQ and NE cases aren't allowed here */
				case ROWCOMPARE_LT:
					*op->resvalue = BoolGetDatum(cmpresult < 0);
					break;
				case ROWCOMPARE_LE:
					*op->resvalue = BoolGetDatum(cmpresult <= 0);
					break;
				case ROWCOMPARE_GE:
					*op->resvalue = BoolGetDatum(cmpresult >= 0);
					break;
				case ROWCOMPARE_GT:
					*op->resvalue = BoolGetDatum(cmpresult > 0);
					break;
				default:
					Assert(false);
					break;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_MINMAX)
		{
			/* too complex for an inline implementation */
			ExecEvalMinMax(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSELECT)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldSelect(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSTORE_DEFORM)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldStoreDeForm(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSTORE_FORM)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldStoreForm(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYREF_SUBSCRIPT)
		{
			/* Process an array subscript */

			/* too complex for an inline implementation */
			if (ExecEvalArrayRefSubscript(state, op, econtext))
			{
				EEO_NEXT();
			}
			else
			{
				/* Subscript is null, short-circuit ArrayRef to NULL */
				EEO_JUMP(op->d.arrayref_subscript.jumpdone);
			}
		}

		EEO_CASE(EEOP_ARRAYREF_OLD)
		{
			/*
			 * Fetch the old value in an arrayref assignment, in case it's
			 * referenced (via a CaseTestExpr) inside the assignment
			 * expression.
			 */

			/* too complex for an inline implementation */
			ExecEvalArrayRefOld(state, op);

			EEO_NEXT();
		}

		/*
		 * Perform ArrayRef assignment
		 */
		EEO_CASE(EEOP_ARRAYREF_ASSIGN)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefAssign(state, op);

			EEO_NEXT();
		}

		/*
		 * Fetch subset of an array.
		 */
		EEO_CASE(EEOP_ARRAYREF_FETCH)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefFetch(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONVERT_ROWTYPE)
		{
			/* too complex for an inline implementation */
			ExecEvalConvertRowtype(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCALARARRAYOP)
		{
			/* too complex for an inline implementation */
			ExecEvalScalarArrayOp(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_NOTNULL)
		{
			/* too complex for an inline implementation */
			ExecEvalConstraintNotNull(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_CHECK)
		{
			/* too complex for an inline implementation */
			ExecEvalConstraintCheck(state, op);

			EEO_NEXT();
		}

        /* evaluate a strict aggregate deserialization function */
        EEO_CASE(EEOP_AGG_STRICT_DESERIALIZE)
        {
            bool *argnull = op->d.agg_deserialize.fcinfo_data->argnull;

            /* Don't call a strict deserialization function with NULL input */
            if (argnull[0])
                EEO_JUMP(op->d.agg_deserialize.jumpnull);

            /* fallthrough */
        }

        /* evaluate aggregate deserialization function (non-strict portion) */
        EEO_CASE(EEOP_AGG_DESERIALIZE)
        {
            FunctionCallInfo fcinfo = op->d.agg_deserialize.fcinfo_data;
            AggState *aggstate = op->d.agg_deserialize.aggstate;
            MemoryContext oldContext;

            /*
             * We run the deserialization functions in per-input-tuple memory
             * context.
             */
            oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);
            fcinfo->isnull = false;
            *op->resvalue = FunctionCallInvoke(fcinfo);
            *op->resnull = fcinfo->isnull;
            MemoryContextSwitchTo(oldContext);

            EEO_NEXT();
        }

        /*
         * Check that a strict aggregate transition / combination function's
         * input is not NULL.
         */
        EEO_CASE(EEOP_AGG_STRICT_INPUT_CHECK)
        {
            int argno;
            bool *nulls = op->d.agg_strict_input_check.nulls;
            int nargs = op->d.agg_strict_input_check.nargs;

            for (argno = 0; argno < nargs; argno++) {
                if (nulls[argno])
                    EEO_JUMP(op->d.agg_strict_input_check.jumpnull);
            }
            EEO_NEXT();
        }

        EEO_CASE(EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

            if (pergroup->noTransValue) {
                /* If transValue has not yet been initialized, do so now. */
                ExecAggInitGroup(aggstate, pertrans, pergroup, op->d.agg_trans.aggcontext);
                /* copied trans value from input, done this round */
            } else if (likely(!pergroup->transValueIsNull)) {
                /* invoke transition function, unless prevented by strictness */
                ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

            if (pergroup->noCollectValue) {
                /* If transValue has not yet been initialized, do so now. */
                ExecAggInitCollectGroup(aggstate, pertrans, pergroup,
                                 op->d.agg_trans.aggcontext);
                /* copied trans value from input, done this round */
            } else if (likely(!pergroup->collectValueIsNull)) {
                /* invoke transition function, unless prevented by strictness */
                ExecAggCollectPlainTransByVal(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

            if (likely(!pergroup->transValueIsNull)) {
                ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(pertrans->transtypeByVal);

            if (likely(!pergroup->collectValueIsNull)) {
                ExecAggCollectPlainTransByVal(aggstate, pertrans, pergroup,
                                              op->d.agg_trans.aggcontext,
                                              op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(pertrans->transtypeByVal);

            ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(pertrans->transtypeByVal);

            ExecAggCollectPlainTransByVal(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(!pertrans->transtypeByVal);

            if (pergroup->noTransValue) {
                ExecAggInitGroup(aggstate, pertrans, pergroup, op->d.agg_trans.aggcontext);
            } else if (likely(!pergroup->transValueIsNull)) {
                ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(!pertrans->transtypeByVal);

            if (pergroup->noCollectValue) {
                ExecAggInitCollectGroup(aggstate, pertrans, pergroup, op->d.agg_trans.aggcontext);
            } else if (likely(!pergroup->collectValueIsNull)) {
                ExecAggCollectPlainTransByRef(aggstate, pertrans, pergroup,
                                              op->d.agg_trans.aggcontext,
                                              op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }
        

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            if (likely(!pergroup->transValueIsNull))
                ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                    op->d.agg_trans.aggcontext,
                                    op->d.agg_trans.setno);
            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            if (likely(!pergroup->collectValueIsNull))
                ExecAggCollectPlainTransByRef(aggstate, pertrans, pergroup,
                                    op->d.agg_trans.aggcontext,
                                    op->d.agg_trans.setno);
            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            ExecAggCollectPlainTransByRef(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* process single-column ordered aggregate datum */
        EEO_CASE(EEOP_AGG_ORDERED_TRANS_DATUM)
        {
            /* too complex for an inline implementation */
            ExecEvalAggOrderedTransDatum(state, op, econtext);

            EEO_NEXT();
        }

        /* process multi-column ordered aggregate tuple */
        EEO_CASE(EEOP_AGG_ORDERED_TRANS_TUPLE)
        {
            /* too complex for an inline implementation */
            ExecEvalAggOrderedTransTuple(state, op, econtext);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_XMLEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalXmlExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_AGGREF)
		{
			/*
			 * Returns a Datum whose value is the precomputed aggregate value
			 * found in the given expression context.
			 */
			AggrefExprState *aggref = op->d.aggref.astate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resvalue = econtext->ecxt_aggvalues[aggref->aggno];
			*op->resnull = econtext->ecxt_aggnulls[aggref->aggno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_GROUPING_FUNC)
		{
			/* too complex/uncommon for an inline implementation */
			ExecEvalGroupingFunc(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_WINDOW_FUNC)
		{
			/*
			 * Like Aggref, just return a precomputed value from the econtext.
			 */
			WindowFuncExprState *wfunc = op->d.window_func.wfstate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resvalue = econtext->ecxt_aggvalues[wfunc->wfuncno];
			*op->resnull = econtext->ecxt_aggnulls[wfunc->wfuncno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SUBPLAN)
		{
			/* too complex for an inline implementation */
			ExecEvalSubPlan(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ALTERNATIVE_SUBPLAN)
		{
			/* too complex for an inline implementation */
			ExecEvalAlternativeSubPlan(state, op, econtext);

			EEO_NEXT();
		}

		/* rownum */
		EEO_CASE(EEOP_ROWNUM)
		{
			if (op->d.rownum.typeCompat) {
        		*op->resvalue = DirectFunctionCall1(int8_numeric, Int64GetDatum(op->d.rownum.RownumState->ps_rownum + 1));
    		} else {
        		*op->resvalue = Int64GetDatum(op->d.rownum.RownumState->ps_rownum + 1);
    		}
			*op->resnull = false;

			EEO_NEXT();
		}

		/* grouping_id */
		EEO_CASE(EEOP_GROUPING_ID)
		{
			int groupingId = 0;

    		for (int i = 0; i < op->d.grouping_id.GroupingIdState->current_phase; i++) {
    		    groupingId += op->d.grouping_id.GroupingIdState->phases[i].numsets;
    		}
    		groupingId += op->d.grouping_id.GroupingIdState->projected_set + 1;

    		*op->resvalue = (Datum)groupingId;
			*op->resnull = false;

			EEO_NEXT();
		}

		/* hash_filter */
		EEO_CASE(EEOP_HASH_FILTER)
		{
			ExecEvalHashFilter(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_USERVAR_OR_SETVARIABLE)
		{
			Const* con = op->d.uservar.con;
			*op->resnull = con->constisnull;

			if (econtext->is_cursor && con->consttype == REFCURSOROID) {
				CopyCursorInfoData(&econtext->cursor_data, &con->cursor_data);
				econtext->dno = con->cursor_data.cur_dno;
			}
			*op->resvalue = con->constvalue;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_USERSET_ELEM)
		{
            UserSetElem *elem = op->d.userset.useexpr;
            Node *node = NULL;
            UserSetElem elemcopy;
            elemcopy.xpr = elem->xpr;
            elemcopy.name = elem->name;

            node = eval_const_expression_value(NULL, (Node *)elem->val, NULL);
            if (nodeTag(node) == T_Const) {
                elemcopy.val = (Expr *)const_expression_to_const(node);
            } else {
                elemcopy.val = (Expr *)const_expression_to_const(QueryRewriteNonConstant(node));
            }

            check_set_user_message(&elemcopy);

            *op->resvalue = ((Const *)elemcopy.val)->constvalue;

			*op->resnull = false;

            EEO_NEXT();
		}

        EEO_CASE(EEOP_PREFIX_BTYEA)
        {
            if (*op->resnull) {
                *op->resvalue = (Datum)0;
                *op->resnull = true;
            } else {
                *op->resvalue = PointerGetDatum(bytea_substring(*op->resvalue, 1, op->d.prefix_key.pkey->length, false));
                *op->resnull = false;
            }

            EEO_NEXT();
        }

        EEO_CASE(EEOP_PREFIX_TEXT)
        {
            if (*op->resnull) {
                *op->resvalue = (Datum)0;
                *op->resnull = true;
            } else {
                *op->resvalue = PointerGetDatum(text_substring_with_encoding(
                    *op->resvalue, 1, op->d.prefix_key.pkey->length, false, op->d.prefix_key.encoding));
                *op->resnull = false;
            }

            EEO_NEXT();
        }

		EEO_CASE(EEOP_LAST)
		{
			/* unreachable */
			Assert(false);
			goto out;
		}
	}

out:
	*isnull = state->resnull;
	return state->resvalue;
}

/*
 * Expression evaluation callback that performs extra checks before executing
 * the expression. Declared extern so other methods of execution can use it
 * too.
 */
Datum ExecInterpExprStillValid(ExprState *state, ExprContext *econtext, bool *isNull, ExprDoneCond* isDone)
{
    /*
     * First time through, check whether attribute matches Var.  Might not be
     * ok anymore, due to schema changes.
     */
    CheckExprStillValid(state, econtext);

    /* skip the check during further executions */
    state->evalfunc = (ExprStateEvalFunc)state->evalfunc_private;

    /* and actually execute */
    return state->evalfunc(state, econtext, isNull, isDone);
}

/*
 * Check that an expression is still valid in the face of potential schema
 * changes since the plan has been created.
 */
void CheckExprStillValid(ExprState *state, ExprContext *econtext)
{
    TupleTableSlot *innerslot;
    TupleTableSlot *outerslot;
    TupleTableSlot *scanslot;

    innerslot = econtext->ecxt_innertuple;
    outerslot = econtext->ecxt_outertuple;
    scanslot = econtext->ecxt_scantuple;

    for (int i = 0; i < state->steps_len; i++) {
        ExprEvalStep *op = &state->steps[i];

        switch (ExecEvalStepOp(state, op)) {
            case EEOP_INNER_VAR: {
                int attnum = op->d.var.attnum;

                CheckVarSlotCompatibility(innerslot, attnum + 1, op->d.var.vartype);
                break;
            }

            case EEOP_OUTER_VAR: {
                int attnum = op->d.var.attnum;

                CheckVarSlotCompatibility(outerslot, attnum + 1, op->d.var.vartype);
                break;
            }

            case EEOP_SCAN_VAR: {
                int attnum = op->d.var.attnum;

                CheckVarSlotCompatibility(scanslot, attnum + 1, op->d.var.vartype);
                break;
            }
            default:
                break;
        }
    }
}

/*
 * get_cached_rowtype: utility function to lookup a rowtype tupdesc
 *
 * type_id, typmod: identity of the rowtype
 * cache_field: where to cache the TupleDesc pointer in expression state node
 *		(field must be initialized to NULL)
 * econtext: expression context we are executing in
 *
 * NOTE: because the shutdown callback will be called during plan rescan,
 * must be prepared to re-do this during any node execution; cannot call
 * just once during expression initialization.
 */
static TupleDesc
get_cached_rowtype(Oid type_id, int32 typmod,
				   TupleDesc *cache_field, ExprContext *econtext)
{
	TupleDesc	tupDesc = *cache_field;

	/* Do lookup if no cached value or if requested type changed */
	if (tupDesc == NULL ||
		type_id != tupDesc->tdtypeid ||
		typmod != tupDesc->tdtypmod)
	{
		tupDesc = lookup_rowtype_tupdesc(type_id, typmod);

		if (*cache_field)
		{
			/* Release old tupdesc; but callback is already registered */
			ReleaseTupleDesc(*cache_field);
		}
		else
		{
			/* Need to register shutdown callback to release tupdesc */
			RegisterExprContextCallback(econtext,
										ShutdownTupleDescRef,
										PointerGetDatum(cache_field));
		}
		*cache_field = tupDesc;
	}
	return tupDesc;
}

/*
 * Callback function to release a tupdesc refcount at econtext shutdown
 */
static void
ShutdownTupleDescRef(Datum arg)
{
	TupleDesc  *cache_field = (TupleDesc *) DatumGetPointer(arg);

	if (*cache_field)
		ReleaseTupleDesc(*cache_field);
	*cache_field = NULL;
}

/*
 * Fast-path functions, for very simple expressions
 */

/* Simple reference to inner Var */
static Datum
ExecJustInnerVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.var.attnum + 1;
	TupleTableSlot *slot = econtext->ecxt_innertuple;

    if (isDone != NULL) {
        *isDone = ExprSingleResult;
	}

	/* See comments in ExecJustInnerVar */
	return tableam_tslot_getattr(slot, attnum, isnull);
}

/* Simple reference to outer Var */
static Datum
ExecJustOuterVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.var.attnum + 1;
	TupleTableSlot *slot = econtext->ecxt_outertuple;

    if (isDone != NULL) {
        *isDone = ExprSingleResult;
	}
	/* See comments in ExecJustOuterVar */
	return tableam_tslot_getattr(slot, attnum, isnull);
}

/* Simple reference to scan Var */
static Datum
ExecJustScanVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.var.attnum + 1;
	TupleTableSlot *slot = econtext->ecxt_scantuple;

    if (isDone != NULL) {
        *isDone = ExprSingleResult;
	}
	/* See comments in ExecJustScanVar */
	return tableam_tslot_getattr(slot, attnum, isnull);
}

/* implementation of ExecJustAssign(Inner|Outer|Scan)Var */
static inline Datum
ExecJustAssignVarImpl(ExprState *state, TupleTableSlot *inslot, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op = &state->steps[1];
	int			attnum = op->d.assign_var.attnum + 1;
	int			resultnum = op->d.assign_var.resultnum;
	TupleTableSlot *outslot = state->resultslot;

	if (isDone != NULL)
        *isDone = ExprSingleResult;

	/*
	 * We do not need CheckVarSlotCompatibility here; that was taken care of
	 * at compilation time.
	 *
	 * Since we use slot_getattr(), we don't need to implement the FETCHSOME
	 * step explicitly, and we also needn't Assert that the attnum is in range
	 * --- slot_getattr() will take care of any problems.  Nonetheless, check
	 * that resultnum is in range.
	 */
	Assert(resultnum >= 0 && resultnum < outslot->tts_tupleDescriptor->natts);
	outslot->tts_values[resultnum] =
		tableam_tslot_getattr(inslot, attnum, &outslot->tts_isnull[resultnum]);
	return 0;
}

/* Simple Const expression */
static Datum
ExecJustConst(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op = &state->steps[0];

	if (isDone != NULL)
        *isDone = ExprSingleResult;

	*isnull = op->d.constval.isnull;

	/* if a const cursor, copy cursor option data to econtext */
    if ((econtext->is_cursor || op->d.constval.is_cursor) && op->d.constval.con && 
		op->d.constval.con->consttype == REFCURSOROID) {
        CopyCursorInfoData(&econtext->cursor_data, &op->d.constval.con->cursor_data);
        econtext->dno = op->d.constval.con->cursor_data.cur_dno;
    }

	return op->d.constval.value;
}

/* Evaluate inner Var and assign to appropriate column of result tuple */
static Datum
ExecJustAssignInnerVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	return ExecJustAssignVarImpl(state, econtext->ecxt_innertuple, isnull, isDone);
}

/* Evaluate outer Var and assign to appropriate column of result tuple */
static Datum
ExecJustAssignOuterVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	return ExecJustAssignVarImpl(state, econtext->ecxt_outertuple, isnull, isDone);
}

/* Evaluate scan Var and assign to appropriate column of result tuple */
static Datum
ExecJustAssignScanVar(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	return ExecJustAssignVarImpl(state, econtext->ecxt_scantuple, isnull, isDone);
}

#if defined(EEO_USE_COMPUTED_GOTO)
/*
 * Comparator used when building address->opcode lookup table for
 * ExecEvalStepOp() in the threaded dispatch case.
 */
static int
dispatch_compare_ptr(const void *a, const void *b)
{
	const ExprEvalOpLookup *la = (const ExprEvalOpLookup *) a;
	const ExprEvalOpLookup *lb = (const ExprEvalOpLookup *) b;

	if (la->opcode < lb->opcode)
		return -1;
	else if (la->opcode > lb->opcode)
		return 1;
	return 0;
}
#endif

/*
 * Do one-time initialization of interpretation machinery.
 */
static void
ExecInitInterpreter(void)
{
#if defined(EEO_USE_COMPUTED_GOTO)
	/* Set up externally-visible pointer to dispatch table */
	if (dispatch_table == NULL)
	{
		int			i;

		dispatch_table = (const void **)
			DatumGetPointer(ExecInterpExpr(NULL, NULL, NULL, NULL));

		/* build reverse lookup table */
		for (i = 0; i < EEOP_LAST; i++)
		{
			reverse_dispatch_table[i].opcode = dispatch_table[i];
			reverse_dispatch_table[i].op = (ExprEvalOp) i;
		}

		/* make it bsearch()able */
		qsort(reverse_dispatch_table,
			  EEOP_LAST /* nmembers */ ,
			  sizeof(ExprEvalOpLookup),
			  dispatch_compare_ptr);
	}
#endif
}

/*
 * Function to return the opcode of an expression step.
 *
 * When direct-threading is in use, ExprState->opcode isn't easily
 * decipherable. This function returns the appropriate enum member.
 */
ExprEvalOp
ExecEvalStepOp(ExprState *state, ExprEvalStep *op)
{
#if defined(EEO_USE_COMPUTED_GOTO)
	if (state->flags & EEO_FLAG_DIRECT_THREADED)
	{
		ExprEvalOpLookup key;
		ExprEvalOpLookup *res;

		key.opcode = (void *) op->opcode;
		res = (ExprEvalOpLookup*)bsearch(&key,
					  reverse_dispatch_table,
					  EEOP_LAST /* nmembers */ ,
					  sizeof(ExprEvalOpLookup),
					  dispatch_compare_ptr);
		Assert(res);			/* unknown ops shouldn't get looked up */
		return res->op;
	}
#endif
	return (ExprEvalOp) op->opcode;
}

/*
 * Evaluate EEOP_FUNCEXPR_FUSAGE
 */
void ExecEvalFuncExprFusage(ExprEvalStep *op, ExprContext *econtext)
{
    FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    PgStat_FunctionCallUsage fcusage;
    Datum d;

    if (econtext) {
        fcinfo->can_ignore = econtext->can_ignore;
    }

    pgstat_init_function_usage(fcinfo, &fcusage);

    fcinfo->isnull = false;
    d = op->d.func.fn_addr(fcinfo);
    *op->resvalue = d;
    *op->resnull = fcinfo->isnull;

    pgstat_end_function_usage(&fcusage, true);
}

/*
 * Evaluate EEOP_FUNCEXPR_STRICT_FUSAGE
 */
void ExecEvalFuncExprStrictFusage(ExprEvalStep *op, ExprContext *econtext)
{

    FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    PgStat_FunctionCallUsage fcusage;
    int nargs = op->d.func.nargs;
    Datum d;

    if (econtext) {
        fcinfo->can_ignore = econtext->can_ignore;
    }

    /* strict function, so check for NULL args */
    for (int argno = 0; argno < nargs; argno++) {
        if (fcinfo->argnull[argno]) {
            *op->resnull = true;
            return;
        }
    }

    pgstat_init_function_usage(fcinfo, &fcusage);

    fcinfo->isnull = false;
    d = op->d.func.fn_addr(fcinfo);
    *op->resvalue = d;
    *op->resnull = fcinfo->isnull;

    pgstat_end_function_usage(&fcusage, true);
}

/*
 * Out-of-line helper functions for complex instructions.
 */

/*
 * Evaluate a PARAM_EXEC parameter.
 *
 * PARAM_EXEC params (internal executor parameters) are stored in the
 * ecxt_param_exec_vals array, and can be accessed by array index.
 */
void
ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ParamExecData *prm;

	prm = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);
	if (unlikely(prm->execPlan != NULL))
	{
		/* Parameter not evaluated yet, so go do it */
		ExecSetParamPlan((SubPlanState*)prm->execPlan, econtext);
		/* ExecSetParamPlan should have processed this param... */
		Assert(prm->execPlan == NULL);
	}
	*op->resvalue = prm->value;
	*op->resnull = prm->isnull;
}

/*
 * Evaluate a PARAM_EXTERN parameter.
 *
 * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
 */
void
ExecEvalParamExtern(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ParamListInfo paramInfo = econtext->ecxt_param_list_info;
	int			paramId = op->d.param.paramid;

	if (likely(paramInfo &&
			   paramId > 0 && paramId <= paramInfo->numParams))
	{
		ParamExternData *prm = &paramInfo->params[paramId - 1];

		/* give hook a chance in case parameter is dynamic */
		if (paramInfo->paramFetch != NULL)
			(*paramInfo->paramFetch) (paramInfo, paramId);

		if (likely(OidIsValid(prm->ptype)))
		{
			/* safety check in case hook did something unexpected */
			if (unlikely(prm->ptype != op->d.param.paramtype))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
								paramId,
								format_type_be(prm->ptype),
								format_type_be(op->d.param.paramtype))));
			*op->resvalue = prm->value;
			*op->resnull = prm->isnull;
			
			/* copy cursor option from param to econtext */
            if ((econtext->is_cursor || op->d.param.is_cursor) && prm->ptype == REFCURSOROID) {
                CopyCursorInfoData(&econtext->cursor_data, &prm->cursor_data);
                econtext->dno = paramId - 1;
            }

			return;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("no value found for parameter %d", paramId)));
}

/*
 * Raise error if a CURRENT OF expression is evaluated.
 *
 * The planner should convert CURRENT OF into a TidScan qualification, or some
 * other special handling in a ForeignScan node.  So we have to be able to do
 * ExecInitExpr on a CurrentOfExpr, but we shouldn't ever actually execute it.
 * If we get here, we suppose we must be dealing with CURRENT OF on a foreign
 * table whose FDW doesn't handle it, and complain accordingly.
 */
void
ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op)
{
	ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_EXECUTOR), errmsg("CURRENT OF cannot be executed")));
    return; /* keep compiler quiet */
}

/*
 * Evaluate NullTest / IS NULL for rows.
 */
void
ExecEvalRowNull(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ExecEvalRowNullInt(state, op, econtext, true);
}

/*
 * Evaluate NullTest / IS NOT NULL for rows.
 */
void
ExecEvalRowNotNull(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ExecEvalRowNullInt(state, op, econtext, false);
}

static Datum CheckRowTypeIsNull(TupleDesc tupDesc, HeapTupleData tmptup, bool checkisnull)
{
    int att;

    for (att = 1; att <= tupDesc->natts; att++) {
        /* ignore dropped columns */
        if (tupDesc->attrs[att - 1].attisdropped)
            continue;
        if (tableam_tops_tuple_attisnull(&tmptup, att, tupDesc)) {
            /* null field disproves IS NOT NULL */
            if (!checkisnull)
                return BoolGetDatum(false);
        } else {
            /* non-null field disproves IS NULL */
            if (checkisnull)
                return BoolGetDatum(false);
        }
    }

    return BoolGetDatum(true);
}

static Datum CheckRowTypeIsNullForAFormat(TupleDesc tupDesc, HeapTupleData tmptup, bool checkisnull)
{
    int att;

    for (att = 1; att <= tupDesc->natts; att++) {
        /* ignore dropped columns */
        if (tupDesc->attrs[att - 1].attisdropped)
            continue;
        if (!tableam_tops_tuple_attisnull(&tmptup, att, tupDesc)) {
            /* non-null field disproves IS NULL */
            if (checkisnull) {
                return BoolGetDatum(false);
            } else {
                return BoolGetDatum(true);
            }
        }
    }

    /* non-null field disproves IS NULL */
    if (checkisnull) {
        return BoolGetDatum(true);
    } else {
        return BoolGetDatum(false);
    }
}

/* Common code for IS [NOT] NULL on a row value */
static void
ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op,
				   ExprContext *econtext, bool checkisnull)
{
	Datum		value = *op->resvalue;
	bool		isnull = *op->resnull;
	HeapTupleHeader tuple;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	HeapTupleData tmptup;

	*op->resnull = false;

	/* NULL row variables are treated just as NULL scalar columns */
	if (isnull)
	{
		*op->resvalue = BoolGetDatum(checkisnull);
		return;
	}

	/*
	 * The SQL standard defines IS [NOT] NULL for a non-null rowtype argument
	 * as:
	 *
	 * "R IS NULL" is true if every field is the null value.
	 *
	 * "R IS NOT NULL" is true if no field is the null value.
	 *
	 * This definition is (apparently intentionally) not recursive; so our
	 * tests on the fields are primitive attisnull tests, not recursive checks
	 * to see if they are all-nulls or no-nulls rowtypes.
	 *
	 * The standard does not consider the possibility of zero-field rows, but
	 * here we consider them to vacuously satisfy both predicates.
	 */

	tuple = DatumGetHeapTupleHeader(value);

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);

	/* Lookup tupdesc if first time through or if type changes */
	tupDesc = get_cached_rowtype(tupType, tupTypmod,
								 &op->d.nulltest_row.argdesc,
								 econtext);

	/*
	 * heap_attisnull needs a HeapTuple not a bare HeapTupleHeader.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	if (AFORMAT_NULL_TEST_MODE)
		*op->resvalue =  CheckRowTypeIsNullForAFormat(tupDesc, tmptup, checkisnull);
	else
		*op->resvalue =  CheckRowTypeIsNull(tupDesc, tmptup, checkisnull);
}

/*
 * Evaluate an ARRAY[] expression.
 *
 * The individual array elements (or subarrays) have already been evaluated
 * into op->d.arrayexpr.elemvalues[]/elemnulls[].
 */
void
ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op)
{
	ArrayType  *result;
	Oid			element_type = op->d.arrayexpr.elemtype;
	int			nelems = op->d.arrayexpr.nelems;
	int			ndims = 0;
	int			dims[MAXDIM];
	int			lbs[MAXDIM];

	/* Set non-null as default */
	*op->resnull = false;

	if (!op->d.arrayexpr.multidims)
	{
		/* Elements are presumably of scalar type */
		Datum	   *dvalues = op->d.arrayexpr.elemvalues;
		bool	   *dnulls = op->d.arrayexpr.elemnulls;

        /* Shouldn't happen here, but if length is 0, return empty array */
        if (nelems == 0) {
			*op->resvalue = PointerGetDatum(construct_empty_array(element_type));
			return;
		}

		/* setup for 1-D array of the given length */
		ndims = 1;
		dims[0] = nelems;
		lbs[0] = 1;

		result = construct_md_array(dvalues, dnulls, ndims, dims, lbs,
									element_type,
									op->d.arrayexpr.elemlength,
									op->d.arrayexpr.elembyval,
									op->d.arrayexpr.elemalign);
	}
	else
	{
		/* Must be nested array expressions */
		int			nbytes = 0;
		int			nitems = 0;
		int			outer_nelems = 0;
		int			elem_ndims = 0;
		int		   *elem_dims = NULL;
		int		   *elem_lbs = NULL;
		bool		firstone = true;
		bool		havenulls = false;
		bool		haveempty = false;
		char	  **subdata;
		bits8	  **subbitmaps;
		int		   *subbytes;
		int		   *subnitems;
		int32		dataoffset;
		char	   *dat;
		int			iitem;
		int			elemoff;
		int			i;

		subdata = (char **) palloc(nelems * sizeof(char *));
		subbitmaps = (bits8 **) palloc(nelems * sizeof(bits8 *));
		subbytes = (int *) palloc(nelems * sizeof(int));
		subnitems = (int *) palloc(nelems * sizeof(int));

		/* loop through and get data area from each element */
		for (elemoff = 0; elemoff < nelems; elemoff++)
		{
			Datum		arraydatum;
			bool		eisnull;
			ArrayType  *array;
			int			this_ndims;

			arraydatum = op->d.arrayexpr.elemvalues[elemoff];
			eisnull = op->d.arrayexpr.elemnulls[elemoff];

			/* temporarily ignore null subarrays */
			if (eisnull)
			{
				haveempty = true;
				continue;
			}

			array = DatumGetArrayTypeP(arraydatum);

			/* run-time double-check on element type */
			if (element_type != ARR_ELEMTYPE(array))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("cannot merge incompatible arrays"),
						 errdetail("Array with element type %s cannot be "
								   "included in ARRAY construct with element type %s.",
								   format_type_be(ARR_ELEMTYPE(array)),
								   format_type_be(element_type))));

			this_ndims = ARR_NDIM(array);
			/* temporarily ignore zero-dimensional subarrays */
			if (this_ndims <= 0)
			{
				haveempty = true;
				continue;
			}

			if (firstone)
			{
				/* Get sub-array details from first member */
				elem_ndims = this_ndims;
				ndims = elem_ndims + 1;
				if (ndims <= 0 || ndims > MAXDIM)
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("number of array dimensions (%d) exceeds " \
									"the maximum allowed (%d)", ndims, MAXDIM)));

				elem_dims = (int *) palloc(elem_ndims * sizeof(int));
				memcpy(elem_dims, ARR_DIMS(array), elem_ndims * sizeof(int));
				elem_lbs = (int *) palloc(elem_ndims * sizeof(int));
				memcpy(elem_lbs, ARR_LBOUND(array), elem_ndims * sizeof(int));

				firstone = false;
			}
			else
			{
				/* Check other sub-arrays are compatible */
				if (elem_ndims != this_ndims ||
					memcmp(elem_dims, ARR_DIMS(array),
						   elem_ndims * sizeof(int)) != 0 ||
					memcmp(elem_lbs, ARR_LBOUND(array),
						   elem_ndims * sizeof(int)) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
							 errmsg("multidimensional arrays must have array "
									"expressions with matching dimensions")));
			}

			subdata[outer_nelems] = ARR_DATA_PTR(array);
			subbitmaps[outer_nelems] = ARR_NULLBITMAP(array);
			subbytes[outer_nelems] = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
			nbytes += subbytes[outer_nelems];
			subnitems[outer_nelems] = ArrayGetNItems(this_ndims,
													 ARR_DIMS(array));
			nitems += subnitems[outer_nelems];
			havenulls |= ARR_HASNULL(array);
			outer_nelems++;
		}

		/*
		 * If all items were null or empty arrays, return an empty array;
		 * otherwise, if some were and some weren't, raise error.  (Note: we
		 * must special-case this somehow to avoid trying to generate a 1-D
		 * array formed from empty arrays.  It's not ideal...)
		 */
		if (haveempty)
		{
			if (ndims == 0)		/* didn't find any nonempty array */
			{
				*op->resvalue = PointerGetDatum(construct_empty_array(element_type));
				return;
			}
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("multidimensional arrays must have array "
							"expressions with matching dimensions")));
		}

		/* setup for multi-D array */
		dims[0] = outer_nelems;
		lbs[0] = 1;
		for (i = 1; i < ndims; i++)
		{
			dims[i] = elem_dims[i - 1];
			lbs[i] = elem_lbs[i - 1];
		}

		if (havenulls)
		{
			dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
			nbytes += dataoffset;
		}
		else
		{
			dataoffset = 0;		/* marker for no null bitmap */
			nbytes += ARR_OVERHEAD_NONULLS(ndims);
		}

		result = (ArrayType *) palloc(nbytes);
		SET_VARSIZE(result, nbytes);
		result->ndim = ndims;
		result->dataoffset = dataoffset;
		result->elemtype = element_type;
		memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
		memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

		dat = ARR_DATA_PTR(result);
		iitem = 0;
		for (i = 0; i < outer_nelems; i++)
		{
			memcpy(dat, subdata[i], subbytes[i]);
			dat += subbytes[i];
			if (havenulls)
				array_bitmap_copy(ARR_NULLBITMAP(result), iitem,
								  subbitmaps[i], 0,
								  subnitems[i]);
			iitem += subnitems[i];
		}
	}

	*op->resvalue = PointerGetDatum(result);
}

/*
 * Evaluate an ArrayCoerceExpr expression.
 *
 * Source array is in step's result variable.
 */
void
ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op)
{
	ArrayCoerceExpr *acoerce = op->d.arraycoerce.coerceexpr;
	Datum result;
	ArrayType* array = NULL;
	FunctionCallInfoData locfcinfo;

	/* NULL array -> NULL result */
	if (*op->resnull)
		return;

	result = *op->resvalue;

	/*
	 * If it's binary-compatible, modify the element type in the array header,
	 * but otherwise leave the array as we received it.
	 */
	if (!OidIsValid(acoerce->elemfuncid))
	{
		/* Detoast input array if necessary, and copy in any case */
		array = DatumGetArrayTypePCopy(result);
		ARR_ELEMTYPE(array) = op->d.arraycoerce.resultelemtype;
		*op->resvalue = PointerGetDatum(array);
		return;
	}

	/* Detoast input array if necessary, but don't make a useless copy */
    array = DatumGetArrayTypeP(result);

	/*
	 * Use array_map to apply the function to each array element.
	 *
	 * We pass on the desttypmod and isExplicit flags whether or not the
	 * function wants them.
	 *
	 * Note: coercion functions are assumed to not use collation.
	 */
	InitFunctionCallInfoData(locfcinfo, op->d.arraycoerce.elemfunc, 3,
							 InvalidOid, NULL, NULL);
	locfcinfo.arg[0] = PointerGetDatum(array);;
	locfcinfo.arg[1] = Int32GetDatum(acoerce->resulttypmod);
	locfcinfo.arg[2] = BoolGetDatum(acoerce->isExplicit);
	locfcinfo.argnull[0] = false;
	locfcinfo.argnull[1] = false;
	locfcinfo.argnull[2] = false;

	*op->resvalue = array_map(&locfcinfo, ARR_ELEMTYPE(array), op->d.arraycoerce.resultelemtype,
							  op->d.arraycoerce.amstate);
}

/*
 * Evaluate a ROW() expression.
 *
 * The individual columns have already been evaluated into
 * op->d.row.elemvalues[]/elemnulls[].
 */
void
ExecEvalRow(ExprState *state, ExprEvalStep *op)
{
	HeapTuple	tuple;

	/* build tuple from evaluated field values */
	tuple = (HeapTuple)tableam_tops_form_tuple(op->d.row.tupdesc,
							op->d.row.elemvalues,
							op->d.row.elemnulls);

	*op->resvalue = HeapTupleGetDatum(tuple);
	*op->resnull = false;
}

/*
 * Evaluate GREATEST() or LEAST() expression (note this is *not* MIN()/MAX()).
 *
 * All of the to-be-compared expressions have already been evaluated into
 * op->d.minmax.values[]/nulls[].
 */
void ExecEvalMinMax(ExprState *state, ExprEvalStep *op)
{
    Datum *values = op->d.minmax.values;
    bool *nulls = op->d.minmax.nulls;
    FunctionCallInfo fcinfo = op->d.minmax.fcinfo_data;
    MinMaxOp op_operator = op->d.minmax.op;
    int off;

    /* set at initialization */
    Assert(fcinfo->argnull[0] == false);
    Assert(fcinfo->argnull[1] == false);

    /* default to null result */
    *op->resnull = true;

    for (off = 0; off < op->d.minmax.nelems; off++) {
        /* ignore NULL inputs */
        if (nulls[off]) {
            continue;
        }

        if (*op->resnull) {
            /* first nonnull input, adopt value */
            *op->resvalue = values[off];
            *op->resnull = false;
        } else {
            int cmpresult;

            /* apply comparison function */
            fcinfo->arg[0] = *op->resvalue;
            fcinfo->arg[1] = values[off];

            fcinfo->isnull = false;
            cmpresult = DatumGetInt32(FunctionCallInvoke(fcinfo));
            if (fcinfo->isnull) /* probably should not happen */
                continue;

            if (cmpresult > 0 && op_operator == IS_LEAST)
                *op->resvalue = values[off];
            else if (cmpresult < 0 && op_operator == IS_GREATEST)
                *op->resvalue = values[off];
        }
    }
}

/*
 * Evaluate a FieldSelect node.
 *
 * Source record is in step's result variable.
 */
void
ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	AttrNumber	fieldnum = op->d.fieldselect.fieldnum;
	Datum		tupDatum;
	HeapTupleHeader tuple;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;
	Form_pg_attribute attr;
	HeapTupleData tmptup;

	/* NULL record -> NULL result */
	if (*op->resnull)
		return;

	/* Get the composite datum and extract its type fields */
	tupDatum = *op->resvalue;
	tuple = DatumGetHeapTupleHeader(tupDatum);

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);

	/* Lookup tupdesc if first time through or if type changes */
	tupDesc = get_cached_rowtype(tupType, tupTypmod,
								 &op->d.fieldselect.argdesc,
								 econtext);

    /*
     * Find field's attr record.  Note we don't support system columns here: a
     * datum tuple doesn't have valid values for most of the interesting
     * system columns anyway.
     */
    if (fieldnum <= 0) /* should never happen */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmodule(MOD_EXECUTOR),
                errmsg("unsupported reference to system column %d in FieldSelect", fieldnum)));
    if (fieldnum > tupDesc->natts) /* should never happen */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ATTRIBUTE),
                errmodule(MOD_EXECUTOR),
                errmsg("attribute number %d exceeds number of columns %d", fieldnum, tupDesc->natts)));
    attr = &tupDesc->attrs[fieldnum - 1];

	/* Check for dropped column, and force a NULL result if so */
	if (attr->attisdropped)
	{
		*op->resnull = true;
		return;
	}

	/* Check for type mismatch --- possible after ALTER COLUMN TYPE? */
	/* As in CheckVarSlotCompatibility, we should but can't check typmod */
	if (op->d.fieldselect.resulttype != attr->atttypid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("attribute %d has wrong type", fieldnum),
				 errdetail("Table has type %s, but query expects %s.",
						   format_type_be(attr->atttypid),
						   format_type_be(op->d.fieldselect.resulttype))));

	/* heap_getattr needs a HeapTuple not a bare HeapTupleHeader */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	/* extract the field */
	*op->resvalue = tableam_tops_tuple_getattr(&tmptup, fieldnum, tupDesc, op->resnull);
}

/*
 * Deform source tuple, filling in the step's values/nulls arrays, before
 * evaluating individual new values as part of a FieldStore expression.
 * Subsequent steps will overwrite individual elements of the values/nulls
 * arrays with the new field values, and then FIELDSTORE_FORM will build the
 * new tuple value.
 *
 * Source record is in step's result variable.
 */
void
ExecEvalFieldStoreDeForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	TupleDesc	tupDesc;
	errno_t rc = EOK;

	/* Lookup tupdesc if first time through or after rescan */
	tupDesc = get_cached_rowtype(op->d.fieldstore.fstore->resulttype, -1,
								 op->d.fieldstore.argdesc, econtext);

	/* Check that current tupdesc doesn't have more fields than we allocated */
	if (unlikely(tupDesc->natts > op->d.fieldstore.ncolumns))
		elog(ERROR, "too many columns in composite type %u",
			 op->d.fieldstore.fstore->resulttype);

	if (!*op->resnull) {
        /*
         * heap_deform_tuple needs a HeapTuple not a bare HeapTupleHeader. We
         * set all the fields in the struct just in case.
         */
		Datum		tupDatum = *op->resvalue;
        HeapTupleHeader tuphdr;
        HeapTupleData tmptup;

        tuphdr = DatumGetHeapTupleHeader(tupDatum);
        tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
        ItemPointerSetInvalid(&(tmptup.t_self));
        tmptup.t_tableOid = InvalidOid;
        tmptup.t_bucketId = InvalidBktId;
#ifdef PGXC
        tmptup.t_xc_node_id = 0;
#endif
        HeapTupleSetZeroBase(&tmptup);
        tmptup.t_data = tuphdr;

        tableam_tops_deform_tuple(&tmptup, tupDesc, op->d.fieldstore.values, op->d.fieldstore.nulls);
    } else {
        /* Convert null input tuple into an all-nulls row */
        rc = memset_s(op->d.fieldstore.nulls, op->d.fieldstore.ncolumns * sizeof(bool), 
			true, op->d.fieldstore.ncolumns * sizeof(bool));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * Compute the new composite datum after each individual field value of a
 * FieldStore expression has been evaluated.
 */
void
ExecEvalFieldStoreForm(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	HeapTuple	tuple;

	/* argdesc should already be valid from the DeForm step */
	tuple = (HeapTuple)tableam_tops_form_tuple(*op->d.fieldstore.argdesc, 
		op->d.fieldstore.values, op->d.fieldstore.nulls);
	
	*op->resvalue = HeapTupleGetDatum(tuple);
	*op->resnull = false;
}

/*
 * Process a subscript in an ArrayRef expression.
 *
 * If subscript is NULL, throw error in assignment case, or in fetch case
 * set result to NULL and return false (instructing caller to skip the rest
 * of the ArrayRef sequence).
 *
 * Subscript expression result is in subscriptvalue/subscriptnull.
 * On success, integer subscript value has been saved in upperindex[] or
 * lowerindex[] for use later.
 */
bool
ExecEvalArrayRefSubscript(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ArrayRefState *arefstate = op->d.arrayref_subscript.state;
	int			off = op->d.arrayref_subscript.off;

	ExecTableOfIndexInfo execTableOfIndexInfo;
    initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
    ExecEvalParamExternTableOfIndex((Node*)arefstate->refexpr, &execTableOfIndexInfo);
    if (u_sess->SPI_cxt.cur_tableof_index != NULL) {
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = execTableOfIndexInfo.tableOfIndex;
        u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer = arefstate->refupperindexpr_count;
    }

	/* Convert datum to int, save in appropriate place */
	if (op->d.arrayref_subscript.isupper) {
		if (OidIsValid(execTableOfIndexInfo.tableOfIndexType) || execTableOfIndexInfo.isnestedtable) {
            bool isTran = false;
            PLpgSQL_execstate* old_estate = plpgsql_estate;
            Datum exprValue = arefstate->subscriptvalue;

            plpgsql_estate = old_estate;
            if (execTableOfIndexInfo.tableOfIndexType == VARCHAROID && !arefstate->subscriptnull && VARATT_IS_1B(exprValue)) {
                exprValue = transVaratt1BTo4B(exprValue);
                isTran = true;
            }
            TableOfIndexKey key;
            PLpgSQL_var* node = NULL;
            key.exprtypeid = execTableOfIndexInfo.tableOfIndexType;
            key.exprdatum = exprValue;
            int index = getTableOfIndexByDatumValue(key, execTableOfIndexInfo.tableOfIndex, &node);
            if (isTran) {
                pfree(DatumGetPointer(exprValue));
            }
            if (execTableOfIndexInfo.isnestedtable) {
                /* for nested table, we should take inner table's array and skip current indx */
                if (node == NULL || index == -1) {
                    arefstate->subscriptnull = true;
                } else {
                    PLpgSQL_var* var = node;
                    execTableOfIndexInfo.isnestedtable  = (var->nest_table != NULL);
					*op->resvalue = var->value;
                    execTableOfIndexInfo.tableOfIndexType = var->datatype->tableOfIndexType;
                    execTableOfIndexInfo.tableOfIndex = var->tableOfIndex;
                    arefstate->subscriptnull = var->isnull;
                    if (plpgsql_estate)
                        plpgsql_estate->curr_nested_table_type = var->datatype->typoid;
                    return true;
                }
            }
            if (index == -1) {
                arefstate->subscriptnull = true;
            } else {
				arefstate->upperindex[off] = index;
            }
        }
		else {
			arefstate->upperindex[off] = DatumGetInt32(arefstate->subscriptvalue);
		}

		arefstate->plpgsql_index++;

		/* If any index expr yields NULL, result is NULL or error */
        if (arefstate->subscriptnull) {
            if (arefstate->isassignment)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("array subscript in assignment must not be null")));
            *op->resnull = true;
            return (Datum)NULL;
        }
	}
	else {
		if (execTableOfIndexInfo.tableOfIndexType == VARCHAROID || execTableOfIndexInfo.isnestedtable) {
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("index by varchar or nested table don't support two subscripts")));
        } else {
			arefstate->lowerindex[off] = DatumGetInt32(arefstate->subscriptvalue);
        }
		
		/* If any index expr yields NULL, result is NULL or error */
		if (arefstate->subscriptnull)
		{
			if (arefstate->isassignment)
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("array subscript in assignment must not be null")));
			*op->resnull = true;
			return false;
		}
	}

	return true;
}

/*
 * Evaluate ArrayRef fetch.
 *
 * Source array is in step's result variable.
 */
void
ExecEvalArrayRefFetch(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref.state;
	ArrayType* array_source = (ArrayType*)DatumGetPointer(*op->resvalue);
	/* Should not get here if source array (or any subscript) is null */
	Assert(!(*op->resnull));
	
	/* for nested table, if get inner table's elem, need cover elem type */
    if (arefstate->refupperindexpr_count > arefstate->plpgsql_index && 
		arefstate->plpgsql_index > 0 && plpgsql_estate) {
        if (plpgsql_estate->curr_nested_table_type != arefstate->typOid) {
            plpgsql_estate->curr_nested_table_type = ARR_ELEMTYPE(array_source);
            get_typlenbyvalalign(plpgsql_estate->curr_nested_table_type,
                                &arefstate->refelemlength,
                                &arefstate->refelembyval,
                                &arefstate->refelemalign);
        }
    }

	if (arefstate->numlower == 0)
	{
		/* Scalar case */
		*op->resvalue = (Datum)array_ref(array_source,
										  arefstate->numupper,
										  arefstate->upperindex,
										  arefstate->refattrlength,
										  arefstate->refelemlength,
										  arefstate->refelembyval,
										  arefstate->refelemalign,
										  op->resnull);
	}
	else
	{
		/* Slice case */
		*op->resvalue = (Datum)array_get_slice(array_source,
										arefstate->numupper,
										arefstate->upperindex,
										arefstate->lowerindex,
										arefstate->refattrlength,
										arefstate->refelemlength,
										arefstate->refelembyval,
										arefstate->refelemalign);
	}
}

/*
 * Compute old array element/slice value for an ArrayRef assignment
 * expression.  Will only be generated if the new-value subexpression
 * contains ArrayRef or FieldStore.  The value is stored into the
 * ArrayRefState's prevvalue/prevnull fields.
 */
void
ExecEvalArrayRefOld(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref.state;
	ArrayType* array_source = (ArrayType*)DatumGetPointer(*op->resvalue);

	if (*op->resnull)
	{
		/* whole array is null, so any element or slice is too */
		arefstate->prevvalue = (Datum) 0;
		arefstate->prevnull = true;
	}
	else if (arefstate->numlower == 0)
	{
		/* Scalar case */
		arefstate->prevvalue = (Datum)array_ref(array_source,
												 arefstate->numupper,
												 arefstate->upperindex,
												 arefstate->refattrlength,
												 arefstate->refelemlength,
												 arefstate->refelembyval,
												 arefstate->refelemalign,
												 &arefstate->prevnull);
	}
	else
	{
		/* Slice case */
		/* this is currently unreachable */
		arefstate->prevvalue = (Datum)array_get_slice(array_source,
											   	arefstate->numupper,
											   arefstate->upperindex,
											   arefstate->lowerindex,
											   arefstate->refattrlength,
											   arefstate->refelemlength,
											   arefstate->refelembyval,
											   arefstate->refelemalign);
		arefstate->prevnull = false;
	}
}

/*
 * Evaluate ArrayRef assignment.
 *
 * Input array (possibly null) is in result area, replacement value is in
 * ArrayRefState's replacevalue/replacenull.
 */
void
ExecEvalArrayRefAssign(ExprState *state, ExprEvalStep *op)
{
	ArrayRefState *arefstate = op->d.arrayref.state;
	ArrayType* array_source = (ArrayType*)DatumGetPointer(*op->resvalue);

	/*
	 * For an assignment to a fixed-length array type, both the original array
	 * and the value to be assigned into it must be non-NULL, else we punt and
	 * return the original array.
	 */
	if (arefstate->refattrlength > 0)	/* fixed-length array? */
	{
		if (*op->resnull || arefstate->replacenull)
			return;
	}

	/*
	 * For assignment to varlena arrays, we handle a NULL original array by
	 * substituting an empty (zero-dimensional) array; insertion of the new
	 * element will result in a singleton array value.  It does not matter
	 * whether the new element is NULL.
	 */
	if (*op->resnull)
	{
		array_source = construct_empty_array(arefstate->refelemtype);
		*op->resnull = false;
	}

	if (arefstate->numlower == 0)
	{
		/* Scalar case */
		*op->resvalue = (Datum)array_set(array_source,
										  arefstate->numupper,
										  arefstate->upperindex,
										  arefstate->replacevalue,
										  arefstate->replacenull,
										  arefstate->refattrlength,
										  arefstate->refelemlength,
										  arefstate->refelembyval,
										  arefstate->refelemalign);
	}
	else
	{
		/* Slice case */
		*op->resvalue = (Datum)array_set_slice(array_source,
										arefstate->numupper,
										arefstate->upperindex,
										arefstate->lowerindex,
										(ArrayType*)DatumGetPointer(arefstate->replacevalue),
										arefstate->replacenull,
										arefstate->refattrlength,
										arefstate->refelemlength,
										arefstate->refelembyval,
										arefstate->refelemalign);
	}
}

/*
 * Evaluate a rowtype coercion operation.
 * This may require rearranging field positions.
 *
 * Source record is in step's result variable.
 */
void
ExecEvalConvertRowtype(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ConvertRowtypeExpr *convert = op->d.convert_rowtype.convert;
	HeapTuple	result;
	Datum		tupDatum;
	HeapTupleHeader tuple;
	HeapTupleData tmptup;
	TupleDesc	indesc,
				outdesc;

	/* NULL in -> NULL out */
	if (*op->resnull)
		return;

	tupDatum = *op->resvalue;
	tuple = DatumGetHeapTupleHeader(tupDatum);

	/* Lookup tupdescs if first time through or after rescan */
	if (op->d.convert_rowtype.indesc == NULL)
	{
		get_cached_rowtype(exprType((Node *) convert->arg), -1,
						   &op->d.convert_rowtype.indesc,
						   econtext);
		op->d.convert_rowtype.initialized = false;
	}
	if (op->d.convert_rowtype.outdesc == NULL)
	{
		get_cached_rowtype(convert->resulttype, -1,
						   &op->d.convert_rowtype.outdesc,
						   econtext);
		op->d.convert_rowtype.initialized = false;
	}

	indesc = op->d.convert_rowtype.indesc;
	outdesc = op->d.convert_rowtype.outdesc;

	/*
	 * We used to be able to assert that incoming tuples are marked with
	 * exactly the rowtype of indesc.  However, now that ExecEvalWholeRowVar
	 * might change the tuples' marking to plain RECORD due to inserting
	 * aliases, we can only make this weak test:
	 */
	Assert(HeapTupleHeaderGetTypeId(tuple) == indesc->tdtypeid ||
		   HeapTupleHeaderGetTypeId(tuple) == RECORDOID);

	/* if first time through, initialize conversion map */
	if (!op->d.convert_rowtype.initialized)
	{
		MemoryContext old_cxt;

		/* allocate map in long-lived memory context */
		old_cxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

		/* prepare map from old to new attribute numbers */
		op->d.convert_rowtype.map =
			convert_tuples_by_name(indesc, outdesc,
								 gettext_noop("could not convert row type"));
		op->d.convert_rowtype.initialized = true;

		MemoryContextSwitchTo(old_cxt);
	}

	/*
	 * No-op if no conversion needed (not clear this can happen here).
	 */
	if (op->d.convert_rowtype.map == NULL)
		return;

	/*
	 * do_convert_tuple needs a HeapTuple not a bare HeapTupleHeader.
	 */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
	tmptup.t_data = tuple;

	result = do_convert_tuple(&tmptup, op->d.convert_rowtype.map);

	*op->resvalue = HeapTupleGetDatum(result);
}

/*
 * Evaluate "scalar op ANY/ALL (array)".
 *
 * Source array is in our result area, scalar arg is already evaluated into
 * fcinfo->arg[0]/argnull[0].
 *
 * The operator always yields boolean, and we combine the results across all
 * array elements using OR and AND (for ANY and ALL respectively).  Of course
 * we short-circuit as soon as the result is known.
 */
void
ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)
{
	FunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo_data;
	bool		useOr = op->d.scalararrayop.useOr;
	bool		strictfunc = op->d.scalararrayop.finfo->fn_strict;
	ArrayType  *arr;
	int			nitems;
	Datum		result;
	bool		resultnull;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char	   *s;
	bits8	   *bitmap;
	int			bitmask;

	/*
	 * If the array is NULL then we return NULL --- it's not very meaningful
	 * to do anything else, even if the operator isn't strict.
	 */
	if (*op->resnull)
		return;

	/* Else okay to fetch and detoast the array */
	arr = DatumGetArrayTypeP(*op->resvalue);

	/*
	 * If the array is empty, we return either FALSE or TRUE per the useOr
	 * flag.  This is correct even if the scalar is NULL; since we would
	 * evaluate the operator zero times, it matters not whether it would want
	 * to return NULL.
	 */
	nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	if (nitems <= 0)
	{
		*op->resvalue = BoolGetDatum(!useOr);
		*op->resnull = false;
		return;
	}

	/*
	 * If the scalar is NULL, and the function is strict, return NULL; no
	 * point in iterating the loop.
	 */
	if (fcinfo->argnull[0] && strictfunc)
	{
		*op->resnull = true;
		return;
	}

	/*
	 * We arrange to look up info about the element type only once per series
	 * of calls, assuming the element type doesn't change underneath us.
	 */
	if (op->d.scalararrayop.element_type != ARR_ELEMTYPE(arr))
	{
		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
							 &op->d.scalararrayop.typlen,
							 &op->d.scalararrayop.typbyval,
							 &op->d.scalararrayop.typalign);
		op->d.scalararrayop.element_type = ARR_ELEMTYPE(arr);
	}

	typlen = op->d.scalararrayop.typlen;
	typbyval = op->d.scalararrayop.typbyval;
	typalign = op->d.scalararrayop.typalign;

	/* Initialize result appropriately depending on useOr */
	result = BoolGetDatum(!useOr);
	resultnull = false;

	/* Loop over the array elements */
	s = (char *) ARR_DATA_PTR(arr);
	bitmap = ARR_NULLBITMAP(arr);
	bitmask = 1;

	for (int i = 0; i < nitems; i++)
	{
		Datum		elt;
		Datum		thisresult;

		/* Get array element, checking for NULL */
		if (bitmap && (*bitmap & bitmask) == 0)
		{
			fcinfo->arg[1] = (Datum) 0;
			fcinfo->argnull[1] = true;
		}
		else
		{
			elt = fetch_att(s, typbyval, typlen);
			s = att_addlength_pointer(s, typlen, s);
			s = (char *) att_align_nominal(s, typalign);
			fcinfo->arg[1] = elt;
			fcinfo->argnull[1] = false;
		}

		/* Call comparison function */
		if (fcinfo->argnull[1] && strictfunc)
		{
			fcinfo->isnull = true;
			thisresult = (Datum) 0;
		}
		else
		{
			fcinfo->isnull = false;
			thisresult = op->d.scalararrayop.fn_addr(fcinfo);
		}

		/* Combine results per OR or AND semantics */
		if (fcinfo->isnull)
			resultnull = true;
		else if (useOr)
		{
			if (DatumGetBool(thisresult))
			{
				result = BoolGetDatum(true);
				resultnull = false;
				break;			/* needn't look at any more elements */
			}
		}
		else
		{
			if (!DatumGetBool(thisresult))
			{
				result = BoolGetDatum(false);
				resultnull = false;
				break;			/* needn't look at any more elements */
			}
		}

		/* advance bitmap pointer if any */
		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	*op->resvalue = result;
	*op->resnull = resultnull;
}

/*
 * Evaluate a NOT NULL domain constraint.
 */
void
ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)
{
	if (*op->resnull)
        ereport(ERROR,
                (errcode(ERRCODE_NOT_NULL_VIOLATION),
                    errmsg("domain %s does not allow null values", format_type_be(op->d.domaincheck.resulttype))));
}

/*
 * Evaluate a CHECK domain constraint.
 */
void
ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op)
{
	if (!*op->d.domaincheck.checknull &&
		!DatumGetBool(*op->d.domaincheck.checkvalue))
		ereport(ERROR,
            	(errcode(ERRCODE_CHECK_VIOLATION),
                	errmsg("value for domain %s violates check constraint \"%s\"",
                    format_type_be(op->d.domaincheck.resulttype),
                    op->d.domaincheck.constraintname)));
}

/*
 * Evaluate the various forms of XmlExpr.
 *
 * Arguments have been evaluated into named_argvalue/named_argnull
 * and/or argvalue/argnull arrays.
 */
void
ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)
{
	XmlExpr    *xexpr = op->d.xmlexpr.xexpr;
	Datum		value;
	int			i;

	*op->resnull = true;		/* until we get a result */
	*op->resvalue = (Datum) 0;

	switch (xexpr->op)
	{
		case IS_XMLCONCAT:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;
				List	   *values = NIL;

				for (i = 0; i < list_length(xexpr->args); i++)
				{
					if (!argnull[i])
						values = lappend(values, DatumGetPointer(argvalue[i]));
				}

				if (values != NIL)
				{
					*op->resvalue = PointerGetDatum(xmlconcat(values));
					*op->resnull = false;
				}
			}
			break;

		case IS_XMLFOREST:
			{
				Datum	   *argvalue = op->d.xmlexpr.named_argvalue;
				bool	   *argnull = op->d.xmlexpr.named_argnull;
				StringInfoData buf;
				ListCell   *lc;
				ListCell   *lc2;

				initStringInfo(&buf);

				i = 0;
				forboth(lc, xexpr->named_args, lc2, xexpr->arg_names)
				{
					Expr	   *e = (Expr *) lfirst(lc);
					char	   *argname = strVal(lfirst(lc2));

					if (!argnull[i])
					{
						value = argvalue[i];
						appendStringInfo(&buf, "<%s>%s</%s>",
										 argname,
										 map_sql_value_to_xml_value(value,
												 exprType((Node *) e), true),
										 argname);
						*op->resnull = false;
					}
					i++;
				}

				if (!*op->resnull)
				{
					text	   *result;

					result = cstring_to_text_with_len(buf.data, buf.len);
					*op->resvalue = PointerGetDatum(result);
				}

				pfree(buf.data);
			}
			break;

		case IS_XMLELEMENT:
			*op->resvalue = PointerGetDatum(xmlelementByFlatten(xexpr,
												op->d.xmlexpr.named_argvalue,
												 op->d.xmlexpr.named_argnull,
													   op->d.xmlexpr.argvalue,
													 op->d.xmlexpr.argnull));
			*op->resnull = false;
			break;

		case IS_XMLPARSE:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;
				text	   *data;
				bool		preserve_whitespace;

				/* arguments are known to be text, bool */
				Assert(list_length(xexpr->args) == 2);

				if (argnull[0])
					return;
				value = argvalue[0];
				data = DatumGetTextPP(value);

				if (argnull[1]) /* probably can't happen */
					return;
				value = argvalue[1];
				preserve_whitespace = DatumGetBool(value);

				*op->resvalue = PointerGetDatum(xmlparse(data,
														 xexpr->xmloption,
													   preserve_whitespace));
				*op->resnull = false;
			}
			break;

		case IS_XMLPI:
			{
				text	   *arg;
				bool		isnull;

				/* optional argument is known to be text */
				Assert(list_length(xexpr->args) <= 1);

				if (xexpr->args)
				{
					isnull = op->d.xmlexpr.argnull[0];
					if (isnull)
						arg = NULL;
					else
						arg = DatumGetTextPP(op->d.xmlexpr.argvalue[0]);
				}
				else
				{
					arg = NULL;
					isnull = false;
				}

				*op->resvalue = PointerGetDatum(xmlpi(xexpr->name,
													  arg,
													  isnull,
													  op->resnull));
			}
			break;

		case IS_XMLROOT:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;
				xmltype    *data;
				text	   *version;
				int			standalone;

				/* arguments are known to be xml, text, int */
				Assert(list_length(xexpr->args) == 3);

				if (argnull[0])
					return;
				data = DatumGetXmlP(argvalue[0]);

				if (argnull[1])
					version = NULL;
				else
					version = DatumGetTextPP(argvalue[1]);

				Assert(!argnull[2]);	/* always present */
				standalone = DatumGetInt32(argvalue[2]);

				*op->resvalue = PointerGetDatum(xmlroot(data,
														version,
														standalone));
				*op->resnull = false;
			}
			break;

		case IS_XMLSERIALIZE:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;

				/* argument type is known to be xml */
				Assert(list_length(xexpr->args) == 1);

				if (argnull[0])
					return;
				value = argvalue[0];

				*op->resvalue = PointerGetDatum(
								xmltotext_with_xmloption(DatumGetXmlP(value),
														 xexpr->xmloption));
				*op->resnull = false;
			}
			break;

		case IS_DOCUMENT:
			{
				Datum	   *argvalue = op->d.xmlexpr.argvalue;
				bool	   *argnull = op->d.xmlexpr.argnull;

				/* optional argument is known to be xml */
				Assert(list_length(xexpr->args) == 1);

				if (argnull[0])
					return;
				value = argvalue[0];

				*op->resvalue =
					BoolGetDatum(xml_is_document(DatumGetXmlP(value)));
				*op->resnull = false;
			}
			break;

		default:
			elog(ERROR, "unrecognized XML operation");
			break;
	}
}

/*
 * ExecEvalGroupingFunc
 *
 * Computes a bitmask with a bit for each (unevaluated) argument expression
 * (rightmost arg is least significant bit).
 *
 * A bit is set if the corresponding expression is NOT part of the set of
 * grouping expressions in the current grouping set.
 */
void
ExecEvalGroupingFunc(ExprState *state, ExprEvalStep *op)
{
	AggState   *aggstate = castNode(AggState, state->parent);
	int			result = 0;
	Bitmapset  *grouped_cols = aggstate->grouped_cols;
	ListCell   *lc;

	foreach(lc, op->d.grouping_func.clauses)
	{
		int			attnum = lfirst_int(lc);

		result <<= 1;

		if (!bms_is_member(attnum, grouped_cols))
			result |= 1;
	}

	*op->resvalue = Int32GetDatum(result);
	*op->resnull = false;
}

/*
 * Hand off evaluation of a subplan to nodeSubplan.c
 */
void
ExecEvalSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	SubPlanState *sstate = op->d.subplan.sstate;

	/* could potentially be nested, so make sure there's enough stack */
	check_stack_depth();

	*op->resvalue = ExecSubPlan(sstate, econtext, op->resnull);
}

/*
 * Hand off evaluation of an alternative subplan to nodeSubplan.c
 */
void
ExecEvalAlternativeSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	AlternativeSubPlanState *asstate = op->d.alternative_subplan.asstate;

	/* could potentially be nested, so make sure there's enough stack */
	check_stack_depth();

	*op->resvalue = ExecAlternativeSubPlan(asstate, econtext, op->resnull);
}

void
ExecEvalHashFilter(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	Datum result = 0;
    Datum value = 0;
    uint64 hashValue = 0;
    int modulo = 0;
    int nodeIndex = 0;
    ListCell *vartypes = NULL;
    bool isFirst = true;
    bool hasNonNullValue = false;
	int offset = 0;

	*op->resnull = true;

	/* Get every distribute key in arg and compute hash value */
    foreach(vartypes, op->d.hash_filter.typeOids)
    {
        Oid vartype = (Oid)lfirst_oid(vartypes);
			
		value = op->d.hash_filter.argvalue[offset];

        int null_value_dn_index = (op->d.hash_filter.nodelist != NULL) ? op->d.hash_filter.nodelist[0]
                                                             : /* fetch first dn in group's dn list */
                                      0;                       /* fetch first dn index */

        if (op->d.hash_filter.argnull[offset]) {
            if (null_value_dn_index == u_sess->pgxc_cxt.PGXCNodeId) {
                *op->resnull = false;
                result = BoolGetDatum(true);
            } else
                result = BoolGetDatum(false);
        } else {
            if (isFirst) {
                hashValue = compute_hash(vartype, value, LOCATOR_TYPE_HASH);
                isFirst = false;
            } else {
                hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
                hashValue ^= compute_hash(vartype, value, LOCATOR_TYPE_HASH);
            }

            hasNonNullValue = true;
        }

		offset++;
    }

	/* If has non null value, it should get nodeId and deside if need filter the value or not. */
    if (hasNonNullValue) {
        modulo = op->d.hash_filter.bucketMap[abs((int)hashValue) & (op->d.hash_filter.bucketCnt - 1)];
        nodeIndex = op->d.hash_filter.nodelist[modulo];

        /* If there are null value and non null value, and the last value in distkey is null,
            we should set isNull is false. */
        *op->resnull = false;
        /* Look into the handles and return correct position in array */
        if (nodeIndex == u_sess->pgxc_cxt.PGXCNodeId)
			*op->resvalue = BoolGetDatum(true);
        else
			*op->resvalue = BoolGetDatum(false);
    } else /* If all the value is null, return result. */
        *op->resvalue = result;
}

/*
 * Evaluate a wholerow Var expression.
 *
 * Returns a Datum whose value is the value of a whole-row range variable
 * with respect to given expression context.
 */
void
ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	Var *variable = op->d.wholerow.var;
	TupleTableSlot *slot;
	TupleDesc output_tupdesc;
	MemoryContext oldcontext;
	HeapTupleHeader dtuple;
	HeapTuple tuple;
	errno_t rc = EOK;
	
	/* This was checked by ExecInitExpr */
	Assert(variable->varattno == InvalidAttrNumber);

	/* Get the input slot we want */
	switch (variable->varno)
	{
		case INNER_VAR:
			/* get the tuple from the inner node */
			slot = econtext->ecxt_innertuple;
			break;

		case OUTER_VAR:
			/* get the tuple from the outer node */
			slot = econtext->ecxt_outertuple;
			break;

			/* INDEX_VAR is handled by default case */

		default:
			/* get the tuple from the relation being scanned */
			slot = econtext->ecxt_scantuple;
			break;
	}

	/* Apply the junkfilter if any */
	if (op->d.wholerow.junkFilter != NULL)
		slot = ExecFilterJunk(op->d.wholerow.junkFilter, slot);

	/*
	 * If first time through, obtain tuple descriptor and check compatibility.
	 *
	 * XXX: It'd be great if this could be moved to the expression
	 * initialization phase, but due to using slots that's currently not
	 * feasible.
	 */
	if (op->d.wholerow.first)
	{
		/* optimistically assume we don't need slow path */
		op->d.wholerow.slow = false;

		/*
    	 * If it's a RECORD Var, we'll use the slot's type ID info.  It's likely
    	 * that the slot's type is also RECORD; if so, make sure it's been
    	 * "blessed", so that the Datum can be interpreted later.
    	 *
    	 * If the Var identifies a named composite type, we must check that the
    	 * actual tuple type is compatible with it.
    	 */
    	if (variable->vartype != RECORDOID) {
    	    TupleDesc var_tupdesc;
			TupleDesc slot_tupdesc;
    	    int i;

    	    /*
    	     * We really only care about numbers of attributes and data types.
    	     * Also, we can ignore type mismatch on columns that are dropped in
    	     * the destination type, so long as (1) the physical storage matches
    	     * or (2) the actual column value is NULL.	Case (1) is helpful in
    	     * some cases involving out-of-date cached plans, while case (2) is
    	     * expected behavior in situations such as an INSERT into a table with
    	     * dropped columns (the planner typically generates an INT4 NULL
    	     * regardless of the dropped column type).	If we find a dropped
    	     * column and cannot verify that case (1) holds, we have to use
    	     * ExecEvalWholeRowSlow to check (2) for each row.
    	     */
    	    var_tupdesc = lookup_rowtype_tupdesc(variable->vartype, -1);

			slot_tupdesc = slot->tts_tupleDescriptor;

    	    if (var_tupdesc->natts != slot_tupdesc->natts)
    	        ereport(ERROR,
    	            (errcode(ERRCODE_DATATYPE_MISMATCH),
    	                errmsg("table row type and query-specified row type do not match"),
    	                errdetail_plural("Table row contains %d attribute, but query expects %d.",
    	                    "Table row contains %d attributes, but query expects %d.",
    	                    slot_tupdesc->natts,
    	                    slot_tupdesc->natts,
    	                    var_tupdesc->natts)));

    	    for (i = 0; i < var_tupdesc->natts; i++) {
    	        Form_pg_attribute vattr = &var_tupdesc->attrs[i];
    	        Form_pg_attribute sattr = &slot_tupdesc->attrs[i];

    	        if (vattr->atttypid == sattr->atttypid)
    	            continue; /* no worries */
    	        if (!vattr->attisdropped)
    	            ereport(ERROR,
    	                (errcode(ERRCODE_DATATYPE_MISMATCH),
    	                    errmsg("table row type and query-specified row type do not match"),
    	                    errdetail("Table has type %s at ordinal position %d, but query expects %s.",
    	                        format_type_be(sattr->atttypid),
    	                        i + 1,
    	                        format_type_be(vattr->atttypid))));

    	        if (vattr->attlen != sattr->attlen || vattr->attalign != sattr->attalign)
    	            op->d.wholerow.slow = true; /* need to check for nulls */
    	    }

			/*
			 * Use the variable's declared rowtype as the descriptor for the
			 * output values, modulo possibly assigning new column names
			 * below. In particular, we *must* absorb any attisdropped
			 * markings.
			 */
			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			output_tupdesc = CreateTupleDescCopy(var_tupdesc);
			MemoryContextSwitchTo(oldcontext);

    	    ReleaseTupleDesc(var_tupdesc);
    	}
		else {
			/*
			 * In the RECORD case, we use the input slot's rowtype as the
			 * descriptor for the output values, modulo possibly assigning new
			 * column names below.
			 */
			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
			output_tupdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
			MemoryContextSwitchTo(oldcontext);
		}

		/* Bless the tupdesc if needed, and save it in the execution state */
		op->d.wholerow.tupdesc = BlessTupleDesc(output_tupdesc);

		op->d.wholerow.first = false;
	}

	/*
	 * Make sure all columns of the slot are accessible in the slot's
	 * Datum/isnull arrays.
	 */
	tableam_tslot_getallattrs(slot);

	if (op->d.wholerow.slow)
	{
		/* Check to see if any dropped attributes are non-null */
		TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
		TupleDesc	var_tupdesc = op->d.wholerow.tupdesc;
		int			i;

		Assert(var_tupdesc->natts == tupleDesc->natts);

		for (i = 0; i < var_tupdesc->natts; i++)
		{
			Form_pg_attribute vattr = &var_tupdesc->attrs[i];
			Form_pg_attribute sattr = &tupleDesc->attrs[i];

			if (!vattr->attisdropped)
				continue;		/* already checked non-dropped cols */
			if (slot->tts_isnull[i])
				continue;		/* null is always okay */
			if (vattr->attlen != sattr->attlen ||
				vattr->attalign != sattr->attalign)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.",
								   i + 1)));
		}
	}
	
	/*
	 * Copy the slot tuple and make sure any toasted fields get detoasted.
	 *
	 * (The intermediate copy is a tad annoying here, but we currently have no
	 * primitive that will do the right thing.  Note it is critical that we
	 * not change the slot's state, so we can't use ExecFetchSlotTupleDatum.)
	 */
    tuple = ExecCopySlotTuple(slot);

    /*
     * We have to make a copy of the tuple so we can safely insert the Datum
     * overhead fields, which are not set in on-disk tuples.
     */
    dtuple = (HeapTupleHeader)palloc(tuple->t_len);
    rc = memcpy_s((char*)dtuple, tuple->t_len, (char*)tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");

    HeapTupleHeaderSetDatumLength(dtuple, tuple->t_len);
	HeapTupleHeaderSetTypeId(dtuple, slot->tts_tupleDescriptor->tdtypeid);
	HeapTupleHeaderSetTypMod(dtuple, slot->tts_tupleDescriptor->tdtypmod);

	heap_freetuple(tuple);

	/*
	 * Label the datum with the composite type info we identified before.
	 */
    HeapTupleHeaderSetTypeId(dtuple, op->d.wholerow.tupdesc->tdtypeid);
    HeapTupleHeaderSetTypMod(dtuple, op->d.wholerow.tupdesc->tdtypmod);

	*op->resnull = false;
	*op->resvalue = PointerGetDatum(dtuple);
}

void ExecAggInitGroup(AggState *aggstate, AggStatePerTrans pertrans, AggStatePerGroup pergroup,
                      MemoryContext aggcontext)
{
    FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
    MemoryContext oldContext;

    /*
     * We must copy the datum into aggcontext if it is pass-by-ref. We do not
     * need to pfree the old transValue, since it's NULL.  (We already checked
     * that the agg's input type is binary-compatible with its transtype, so
     * straight copy here is OK.)
     */
    oldContext = MemoryContextSwitchTo(aggcontext);
    pergroup->transValue = datumCopy(fcinfo->arg[1], pertrans->transtypeByVal, pertrans->transtypeLen);
    pergroup->transValueIsNull = false;
    pergroup->noTransValue = false;
    MemoryContextSwitchTo(oldContext);
}

void ExecAggInitCollectGroup(AggState *aggstate, AggStatePerTrans pertrans, AggStatePerGroup pergroup,
                             MemoryContext aggcontext)
{
    FunctionCallInfo fcinfo = &pertrans->collectfn_fcinfo;
    MemoryContext oldContext;

    /*
     * We must copy the datum into aggcontext if it is pass-by-ref. We do not
     * need to pfree the old transValue, since it's NULL.  (We already checked
     * that the agg's input type is binary-compatible with its transtype, so
     * straight copy here is OK.)
     */
    oldContext = MemoryContextSwitchTo(aggcontext);
    pergroup->collectValue = datumCopy(fcinfo->arg[1], pertrans->transtypeByVal, pertrans->transtypeLen);
    pergroup->collectValueIsNull = false;
    pergroup->noCollectValue = false;
    MemoryContextSwitchTo(oldContext);
}

/*
 * Ensure that the current transition value is a child of the aggcontext,
 * rather than the per-tuple context.
 *
 * NB: This can change the current memory context.
 */
Datum ExecAggTransReparent(AggState *aggstate, AggStatePerTrans pertrans, Datum newValue, bool newValueIsNull,
                           Datum oldValue, bool oldValueIsNull)
{
    Assert(newValue != oldValue);

    if (!newValueIsNull) {
        MemoryContextSwitchTo(aggstate->curaggcontext);
        newValue = datumCopy(newValue, pertrans->transtypeByVal, pertrans->transtypeLen);
    } else {
        /*
         * Ensure that AggStatePerGroup->transValue ends up being 0, so
         * callers can safely compare newValue/oldValue without having to
         * check their respective nullness.
         */
        newValue = (Datum)0;
    }

    if (!oldValueIsNull) {
        pfree(DatumGetPointer(oldValue));
    }

    return newValue;
}

/*
 * Invoke ordered transition function, with a datum argument.
 */
void ExecEvalAggOrderedTransDatum(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
    int setno = op->d.agg_trans.setno;

    tuplesort_putdatum(pertrans->sortstates[setno], *op->resvalue, *op->resnull);
}

/*
 * Invoke ordered transition function, with a tuple argument.
 */
void ExecEvalAggOrderedTransTuple(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
    int setno = op->d.agg_trans.setno;

    ExecClearTuple(pertrans->sortslot);
    pertrans->sortslot->tts_nvalid = pertrans->numInputs;
    ExecStoreVirtualTuple(pertrans->sortslot);
    tuplesort_puttupleslot(pertrans->sortstates[setno], pertrans->sortslot);
}

static FORCE_INLINE void ExecAggPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
    MemoryContext oldContext;
    Datum        newVal;

    aggstate->curaggcontext = aggcontext;
    aggstate->current_set = setno;
    aggstate->curpertrans = pertrans;

    oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

    fcinfo->arg[0] = pergroup->transValue;
    fcinfo->argnull[0] = pergroup->transValueIsNull;
    fcinfo->isnull = false;

    newVal = FunctionCallInvoke(fcinfo);

    pergroup->transValue = newVal;
    pergroup->transValueIsNull = fcinfo->isnull;

    MemoryContextSwitchTo(oldContext);
}

static FORCE_INLINE void ExecAggCollectPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    FunctionCallInfo fcinfo = &pertrans->collectfn_fcinfo;
    MemoryContext oldContext;
    Datum        newVal;

    aggstate->curaggcontext = aggcontext;
    aggstate->current_set = setno;

    aggstate->curpertrans = pertrans;

    oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

    fcinfo->arg[0] = pergroup->collectValue;
    fcinfo->argnull[0] = pergroup->collectValueIsNull;
    fcinfo->isnull = false;

    newVal = FunctionCallInvoke(fcinfo);

    pergroup->collectValue = newVal;
    pergroup->collectValueIsNull = fcinfo->isnull;

    MemoryContextSwitchTo(oldContext);
}

static FORCE_INLINE void ExecAggPlainTransByRef(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    FunctionCallInfo fcinfo = &pertrans->transfn_fcinfo;
    MemoryContext oldContext;
    Datum        newVal;

    aggstate->curaggcontext = aggcontext;
    aggstate->current_set = setno;
    aggstate->curpertrans = pertrans;

    oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

    fcinfo->arg[0] = pergroup->transValue;
    fcinfo->argnull[0] = pergroup->transValueIsNull;
    fcinfo->isnull = false;

    newVal = FunctionCallInvoke(fcinfo);

    if (DatumGetPointer(newVal) != DatumGetPointer(pergroup->transValue)) {
        newVal = ExecAggTransReparent(aggstate, pertrans, newVal, fcinfo->isnull, pergroup->transValue,
            pergroup->transValueIsNull);
    }

    pergroup->transValue = newVal;
    pergroup->transValueIsNull = fcinfo->isnull;

    MemoryContextSwitchTo(oldContext);
}

static FORCE_INLINE void ExecAggCollectPlainTransByRef(AggState *aggstate, AggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    FunctionCallInfo fcinfo = &pertrans->collectfn_fcinfo;
    MemoryContext oldContext;
    Datum        newVal;

    aggstate->curaggcontext = aggcontext;
    aggstate->current_set = setno;
    aggstate->curpertrans = pertrans;

    oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

    fcinfo->arg[0] = pergroup->collectValue;
    fcinfo->argnull[0] = pergroup->collectValueIsNull;
    fcinfo->isnull = false;

    newVal = FunctionCallInvoke(fcinfo);

    if (DatumGetPointer(newVal) != DatumGetPointer(pergroup->collectValue)) {
        newVal = ExecAggTransReparent(aggstate, pertrans, newVal, fcinfo->isnull, pergroup->collectValue,
            pergroup->collectValueIsNull);
    }

    pergroup->collectValue = newVal;
    pergroup->collectValueIsNull = fcinfo->isnull;

    MemoryContextSwitchTo(oldContext);
}

static void
CheckVarSlotCompatibility(TupleTableSlot *slot, int attnum, Oid vartype)
{
	/*
	 * What we have to check for here is the possibility of an attribute
	 * having been dropped or changed in type since the plan tree was created.
	 * Ideally the plan will get invalidated and not re-used, but just in
	 * case, we keep these defenses.  Fortunately it's sufficient to check
	 * once on the first time through.
	 *
	 * Note: ideally we'd check typmod as well as typid, but that seems
	 * impractical at the moment: in many cases the tupdesc will have been
	 * generated by ExecTypeFromTL(), and that can't guarantee to generate an
	 * accurate typmod in all cases, because some expression node types don't
	 * carry typmod.  Fortunately, for precisely that reason, there should be
	 * no places with a critical dependency on the typmod of a value.
	 *
	 * System attributes don't require checking since their types never
	 * change.
	 */
    if (attnum > 0) {
        TupleDesc slot_tupdesc = slot->tts_tupleDescriptor;
        Form_pg_attribute attr;

        if (attnum > slot_tupdesc->natts)	/* should never happen */
            elog(ERROR, "attribute number %d exceeds number of columns %d",
                attnum, slot_tupdesc->natts);

        attr = TupleDescAttr(slot_tupdesc, attnum - 1);

        if (attr->attisdropped)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("attribute %d of type %s has been dropped",
                        attnum, format_type_be(slot_tupdesc->tdtypeid))));

        if (vartype != attr->atttypid)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("attribute %d of type %s has wrong type",
                        attnum, format_type_be(slot_tupdesc->tdtypeid)),
                    errdetail("Table has type %s, but query expects %s.",
                        format_type_be(attr->atttypid),
                            format_type_be(vartype))));
    }
}
