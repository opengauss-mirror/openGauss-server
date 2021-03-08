/* -------------------------------------------------------------------------
 *
 * pl_exec.cpp		- Executor for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_exec.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "plpgsql.h"

#include <ctype.h>

#include "access/transam.h"
#include "access/tupconvert.h"
#include "auditfuncs.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "funcapi.h"
#include "gssignal/gs_signal.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/scansup.h"
#include "pgaudit.h"
#include "pgstat.h"
#include "optimizer/clauses.h"
#include "storage/proc.h"
#ifndef ENABLE_MULTIPLE_NODES
#include "tcop/autonomoustransaction.h"
#endif
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "parser/parse_coerce.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "instruments/instr_unique_sql.h"

extern bool checkRecompileCondition(CachedPlanSource* plansource);
static const char* const raise_skip_msg = "RAISE";
typedef struct {
    int nargs;       /* number of arguments */
    Oid* types;      /* types of arguments */
    Datum* values;   /* evaluated argument values */
    char* nulls;     /* null markers (' '/'n' style) */
    bool* freevals;  /* which arguments are pfree-able */
    bool* isouttype; /* flag of out type */
    Cursor_Data* cursor_data;
} PreparedParamsData;

/*
 * All plpgsql function executions within a single transaction share the same
 * executor EState for evaluating "simple" expressions.  Each function call
 * creates its own "eval_econtext" ExprContext within this estate for
 * per-evaluation workspace.  eval_econtext is freed at normal function exit,
 * and the EState is freed at transaction end (in case of error, we assume
 * that the abort mechanisms clean it all up).	Furthermore, any exception
 * block within a function has to have its own eval_econtext separate from
 * the containing function's, so that we can clean up ExprContext callbacks
 * properly at subtransaction exit.  We maintain a stack that tracks the
 * individual econtexts so that we can clean up correctly at subxact exit.
 *
 * This arrangement is a bit tedious to maintain, but it's worth the trouble
 * so that we don't have to re-prepare simple expressions on each trip through
 * a function.	(We assume the case to optimize is many repetitions of a
 * function within a transaction.)
 */
typedef struct SimpleEcontextStackEntry {
    ExprContext* stack_econtext;           /* a stacked econtext */
    SubTransactionId xact_subxid;          /* ID for current subxact */
    struct SimpleEcontextStackEntry* next; /* next stack entry up */
} SimpleEcontextStackEntry;

/************************************************************
 * Local function forward declarations
 ************************************************************/
static void plpgsql_exec_error_callback(void* arg);
static PLpgSQL_datum* copy_plpgsql_datum(PLpgSQL_datum* datum);

static int exec_stmt_block(PLpgSQL_execstate* estate, PLpgSQL_stmt_block* block);
static int exec_stmts(PLpgSQL_execstate* estate, List* stmts);
static int exec_stmt(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt);
static int exec_stmt_assign(PLpgSQL_execstate* estate, PLpgSQL_stmt_assign* stmt);
static int exec_stmt_perform(PLpgSQL_execstate* estate, PLpgSQL_stmt_perform* stmt);
static int exec_stmt_getdiag(PLpgSQL_execstate* estate, PLpgSQL_stmt_getdiag* stmt);
static int exec_stmt_if(PLpgSQL_execstate* estate, PLpgSQL_stmt_if* stmt);
static int exec_stmt_goto(PLpgSQL_execstate* estate, PLpgSQL_stmt_goto* stmt);
static PLpgSQL_stmt* search_goto_target_global(PLpgSQL_execstate* estate, const PLpgSQL_stmt_goto* goto_stmt);
static PLpgSQL_stmt* search_goto_target_current_block(
    PLpgSQL_execstate* estate, const List* stmts, PLpgSQL_stmt* target_stmt, int* stepno);
static int exec_stmt_case(PLpgSQL_execstate* estate, PLpgSQL_stmt_case* stmt);
static int exec_stmt_loop(PLpgSQL_execstate* estate, PLpgSQL_stmt_loop* stmt);
static int exec_stmt_while(PLpgSQL_execstate* estate, PLpgSQL_stmt_while* stmt);
static int exec_stmt_fori(PLpgSQL_execstate* estate, PLpgSQL_stmt_fori* stmt);
static int exec_stmt_fors(PLpgSQL_execstate* estate, PLpgSQL_stmt_fors* stmt);
static int exec_stmt_forc(PLpgSQL_execstate* estate, PLpgSQL_stmt_forc* stmt);
static int exec_stmt_foreach_a(PLpgSQL_execstate* estate, PLpgSQL_stmt_foreach_a* stmt);
static int exec_stmt_open(PLpgSQL_execstate* estate, PLpgSQL_stmt_open* stmt);
static int exec_stmt_fetch(PLpgSQL_execstate* estate, PLpgSQL_stmt_fetch* stmt);
static int exec_stmt_close(PLpgSQL_execstate* estate, PLpgSQL_stmt_close* stmt);
static int exec_stmt_null(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt);
static int exec_stmt_exit(PLpgSQL_execstate* estate, PLpgSQL_stmt_exit* stmt);
static int exec_stmt_return(PLpgSQL_execstate* estate, PLpgSQL_stmt_return* stmt);
static int exec_stmt_return_next(PLpgSQL_execstate* estate, PLpgSQL_stmt_return_next* stmt);
static int exec_stmt_return_query(PLpgSQL_execstate* estate, PLpgSQL_stmt_return_query* stmt);
static int exec_stmt_raise(PLpgSQL_execstate* estate, PLpgSQL_stmt_raise* stmt);
static int exec_stmt_execsql(PLpgSQL_execstate* estate, PLpgSQL_stmt_execsql* stmt);
static int exec_stmt_dynexecute(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* stmt);
static int exec_stmt_transaction(PLpgSQL_execstate *estate, PLpgSQL_stmt* stmt);

static int exchange_parameters(
    PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* dynstmt, List* stmts, int* ppdindex, int* datumindex);
static bool is_anonymous_block(const char* query);
static int exec_stmt_dynfors(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynfors* stmt);
static void plpgsql_estate_setup(PLpgSQL_execstate* estate, PLpgSQL_function* func, ReturnSetInfo* rsi);
void exec_eval_cleanup(PLpgSQL_execstate* estate);

static void exec_prepare_plan(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, int cursorOptions);
static bool exec_simple_check_node(Node* node);
static void exec_simple_check_plan(PLpgSQL_expr* expr);
static void exec_simple_recheck_plan(PLpgSQL_expr* expr, CachedPlan* cplan);
static bool exec_eval_simple_expr(
    PLpgSQL_execstate* estate, PLpgSQL_expr* expr, Datum* result, bool* isNull, Oid* rettype);

static void exec_assign_expr(PLpgSQL_execstate* estate, PLpgSQL_datum* target, PLpgSQL_expr* expr);
static void exec_assign_c_string(PLpgSQL_execstate* estate, PLpgSQL_datum* target, const char* str);
void exec_assign_value(PLpgSQL_execstate* estate, PLpgSQL_datum* target, Datum value, Oid valtype, bool* isNull);
static void exec_eval_datum(
    PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typeId, int32* typetypmod, Datum* value, bool* isnull);
static int exec_eval_integer(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull);
static bool exec_eval_boolean(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull);
static Datum exec_eval_expr(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull, Oid* rettype);
static int exec_run_select(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, long maxtuples, Portal* portalP);
static int exec_for_query(PLpgSQL_execstate* estate, PLpgSQL_stmt_forq* stmt, Portal portal, bool prefetch_ok, int dno);
static ParamListInfo setup_param_list(PLpgSQL_execstate* estate, PLpgSQL_expr* expr);
static void plpgsql_param_fetch(ParamListInfo params, int paramid);
static void exec_move_row(
    PLpgSQL_execstate* estate, PLpgSQL_rec* rec, PLpgSQL_row* row, HeapTuple tup, TupleDesc tupdesc);
static HeapTuple make_tuple_from_row(PLpgSQL_execstate* estate, PLpgSQL_row* row, TupleDesc tupdesc);
static char* convert_value_to_string(PLpgSQL_execstate* estate, Datum value, Oid valtype);
static Datum pl_coerce_type_typmod(Datum value, Oid targetTypeId, int32 targetTypMod);
static Datum exec_cast_value(PLpgSQL_execstate* estate, Datum value, Oid valtype, Oid reqtype, FmgrInfo* reqinput,
    Oid reqtypioparam, int32 reqtypmod, bool isnull);
static Datum exec_simple_cast_value(
    PLpgSQL_execstate* estate, Datum value, Oid valtype, Oid reqtype, int32 reqtypmod, bool isnull);
static void exec_init_tuple_store(PLpgSQL_execstate* estate);
static void exec_set_found(PLpgSQL_execstate* estate, bool state);

static void exec_set_cursor_found(PLpgSQL_execstate* estate, PLpgSQL_state state, int dno);
static void exec_set_notfound(PLpgSQL_execstate* estate, PLpgSQL_state state, int dno);
static void exec_set_isopen(PLpgSQL_execstate* estate, bool state, int varno);
static void exec_set_rowcount(PLpgSQL_execstate* estate, int rowcount, bool reset, int dno);
static void exec_set_sql_cursor_found(PLpgSQL_execstate* estate, PLpgSQL_state state);
static void exec_set_sql_notfound(PLpgSQL_execstate* estate, PLpgSQL_state state);
static void exec_set_sql_isopen(PLpgSQL_execstate* estate, bool state);
static void exec_set_sql_rowcount(PLpgSQL_execstate* estate, int rowcount);
static void plpgsql_create_econtext(PLpgSQL_execstate *estate);
static void plpgsql_destroy_econtext(PLpgSQL_execstate *estate);
static void free_var(PLpgSQL_var* var);
static void assign_text_var(PLpgSQL_var* var, const char* str);
static PreparedParamsData* exec_eval_using_params(PLpgSQL_execstate* estate, List* params);
static void free_params_data(PreparedParamsData* ppd);
static Portal exec_dynquery_with_params(
    PLpgSQL_execstate* estate, PLpgSQL_expr* dynquery, List* params, const char* portalname, int cursorOptions);
static void exec_set_sqlcode(PLpgSQL_execstate* estate, int sqlcode);

static int search_for_valid_line(PLpgSQL_stmt* stmt, int linenum, int);
static int check_line_validity(List* stmts, int linenum, int);
static int check_line_validity_in_block(PLpgSQL_stmt_block* block, int linenum, int);
static int check_line_validity_in_if(PLpgSQL_stmt_if* stmt, int linenum, int);
static int check_line_validity_in_case(PLpgSQL_stmt_case* stmt, int linenum, int);
static int check_line_validity_in_loop(PLpgSQL_stmt_loop* stmt, int linenum, int);
static int check_line_validity_in_while(PLpgSQL_stmt_while* stmt, int linenum, int);
static int check_line_validity_in_fori(PLpgSQL_stmt_fori* stmt, int, int);
static int check_line_validity_in_foreach_a(PLpgSQL_stmt_foreach_a* stmt, int, int);
static int check_line_validity_in_for_query(PLpgSQL_stmt_forq* stmt, int, int);

static void BindCursorWithPortal(Portal portal, PLpgSQL_execstate *estate, int varno);
static char* transformAnonymousBlock(char* query);
static bool needRecompilePlan(SPIPlanPtr plan);

#ifdef ENABLE_MULTIPLE_NODES
static void rebuild_exception_subtransaction_chain(PLpgSQL_execstate* estate);
#endif

static void stp_check_transaction_and_set_resource_owner(ResourceOwner oldResourceOwner,TransactionId oldTransactionId);
static void stp_check_transaction_and_create_econtext(PLpgSQL_execstate* estate,TransactionId oldTransactionId);

bool plpgsql_get_current_value_stp_with_exception();
void plpgsql_restore_current_value_stp_with_exception(bool saved_current_stp_with_exception);
/* ----------
 * plpgsql_check_line_validity	Called by the debugger plugin for
 * validating a given linenumber
 * ----------
 */
int plpgsql_check_line_validity(PLpgSQL_stmt_block* block, int linenum)
{
    return check_line_validity_in_block(block, linenum, -1);
}

/* ----------
 * check_line_validity: checks if a given number is valid.
 * ----------
 */
static int check_line_validity(List* stmts, int linenum, int prevValidLine)
{
    ListCell* s = NULL;
    int valid_line;
    PLpgSQL_stmt* min_bndry = NULL;
    PLpgSQL_stmt* max_bndry = NULL;

    if (stmts == NIL) {
        /*
         * Ensure we do a CHECK_FOR_INTERRUPTS() even though there is no
         * statement.  This prevents hangup in a tight loop if, for instance,
         * there is a LOOP construct with an empty body.
         */
        CHECK_FOR_INTERRUPTS();
        return -1;
    }

    foreach (s, stmts) {
		/*
		* if there is a next stmt to the current then
		* a.determine the  position of the wanted line w.r.t to the first stmt
		* and nxt stsmt. It is possible that required line could be
		*	 i)equals firt stmt line
		*	 ii)falls b/n first and second stmt
		* b. if the next stmt does not exist,then check within first stmt only
		*/
        PLpgSQL_stmt* firststmt = (PLpgSQL_stmt*)lfirst(s);

        /*
        the line number is set to zero, when explicit return is added by the
        parser/compiler.
        */
        if (firststmt->lineno == 0) {
            if (min_bndry == NULL) {
                return -1;
            }

            break;
        }

        if (linenum > firststmt->lineno) {
            min_bndry = firststmt;
        } else if (linenum == firststmt->lineno) {
            if (firststmt->cmd_type == PLPGSQL_STMT_BLOCK) {
                min_bndry = firststmt;
            } else {
                return linenum;
            }
        } else {
            if (min_bndry == NULL) {
                return -1;
            }

            max_bndry = firststmt;
            break;
        }
    }

    valid_line = search_for_valid_line(min_bndry, linenum, prevValidLine);
    if (valid_line == -1) {
        if (max_bndry == NULL) {
            return -1;
        } else {
            if (max_bndry->cmd_type != PLPGSQL_STMT_BLOCK) {
                return max_bndry->lineno;
            }
            /* if is a block, step in to find a line */
            valid_line = search_for_valid_line(max_bndry, linenum, prevValidLine);
        }
    }

    return valid_line;
}

/* ----------
 * plpgsql_exec_function	Called by the call handler for
 *				function execution.
 * ----------
 */
Datum plpgsql_exec_function(PLpgSQL_function* func, FunctionCallInfo fcinfo, bool dynexec_anonymous_block)
{
    PLpgSQL_execstate estate;
    ErrorContextCallback plerrcontext;
    int i;
    int rc;
    bool saved_current_stp_with_exception;
    /*
     * Setup the execution state
     */
    plpgsql_estate_setup(&estate, func, (ReturnSetInfo*)fcinfo->resultinfo);
    saved_current_stp_with_exception = plpgsql_get_current_value_stp_with_exception();
    /*
     * Setup error traceback support for ereport()
     */
    plerrcontext.callback = plpgsql_exec_error_callback;
    plerrcontext.arg = &estate;
    plerrcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &plerrcontext;

    /*
     * Make local execution copies of all the datums
     */
    estate.err_text = gettext_noop("during initialization of execution state");
    for (i = 0; i < estate.ndatums; i++) {
        estate.datums[i] = copy_plpgsql_datum(func->datums[i]);
    }

    /*
     * Store the actual call argument values into the appropriate variables
     */
    estate.err_text = gettext_noop("while storing call arguments into local variables");
    for (i = 0; i < func->fn_nargs; i++) {
        int n = func->fn_argvarnos[i];

        switch (estate.datums[n]->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)estate.datums[n];

                var->value = fcinfo->arg[i];
                var->isnull = fcinfo->argnull[i];
                var->freeval = false;
            } break;

            case PLPGSQL_DTYPE_ROW: {
                PLpgSQL_row* row = (PLpgSQL_row*)estate.datums[n];

                if (!fcinfo->argnull[i]) {
                    HeapTupleHeader td;
                    Oid tupType;
                    int32 tupTypmod;
                    TupleDesc tupdesc;
                    HeapTupleData tmptup;

                    td = DatumGetHeapTupleHeader(fcinfo->arg[i]);
                    /* Extract rowtype info and find a tupdesc */
                    tupType = HeapTupleHeaderGetTypeId(td);
                    tupTypmod = HeapTupleHeaderGetTypMod(td);
                    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
                    /* Build a temporary HeapTuple control structure */
                    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
                    ItemPointerSetInvalid(&(tmptup.t_self));
                    tmptup.t_tableOid = InvalidOid;
                    tmptup.t_bucketId = InvalidBktId;
#ifdef PGXC
                    tmptup.t_xc_node_id = 0;
#endif
                    tmptup.t_data = td;
                    exec_move_row(&estate, NULL, row, &tmptup, tupdesc);
                    ReleaseTupleDesc(tupdesc);
                } else {
                    /* If arg is null, treat it as an empty row */
                    exec_move_row(&estate, NULL, row, NULL, NULL);
                }
                /* clean up after exec_move_row() */
                exec_eval_cleanup(&estate);
            } break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized argument data type: %d for PLSQL function.", func->datums[i]->dtype)));
                break;
        }
    }

    estate.err_text = gettext_noop("during function entry");

    estate.cursor_return_data = fcinfo->refcursor_data.returnCursor;

    /*
     * Set the magic variable FOUND to false
     */
    exec_set_found(&estate, false);
    /* Set the magic implicit cursor attribute variable FOUND to false */
    exec_set_sql_cursor_found(&estate, PLPGSQL_NULL);

    /* Set the magic implicit cursor attribute variable NOTFOUND to true */
    exec_set_sql_notfound(&estate, PLPGSQL_NULL);

    /* Set the magic implicit cursor attribute variable ISOPEN to false */
    exec_set_sql_isopen(&estate, false);

    /* Set the magic implicit cursor attribute variable ROWCOUNT to 0 */
    exec_set_sql_rowcount(&estate, -1);

    exec_set_sqlcode(&estate, 0);

    /*
     * Let the instrumentation plugin peek at this function
     */
    if (*u_sess->plsql_cxt.plugin_ptr && (*u_sess->plsql_cxt.plugin_ptr)->func_beg) {
        ((*u_sess->plsql_cxt.plugin_ptr)->func_beg)(&estate, func);
    }

    // Check for interrupts sent by pl debugger and other plugins.
    //
    CHECK_FOR_INTERRUPTS();

    /*
     * Now call the toplevel block of statements
     */
    estate.err_text = NULL;
    estate.err_stmt = (PLpgSQL_stmt*)(func->action);
    rc = exec_stmt_block(&estate, func->action);
    if (rc != PLPGSQL_RC_RETURN) {
        estate.err_stmt = NULL;
        estate.err_text = NULL;

        /*
         * Provide a more helpful message if a CONTINUE or RAISE has been used
         * outside the context it can work in.
         */
        if (rc == PLPGSQL_RC_CONTINUE) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("CONTINUE cannot be used outside a loop")));
        } else if (rc == PLPGSQL_RC_GOTO_UNRESOLVED) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg(
                        "illegal GOTO statement; this GOTO cannot branch to label \"%s\"", estate.goto_target_label)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
                    errmodule(MOD_PLSQL),
                    errmsg("control reached end of function without RETURN")));
        }
    }

    /*
     * We got a return value - process it
     */
    estate.err_stmt = NULL;
    estate.err_text = gettext_noop("while casting return value to function's return type");

    fcinfo->isnull = estate.retisnull;

    if (estate.retisset) {
        ReturnSetInfo* rsi = estate.rsi;

        /* Check caller can handle a set result */
        if ((rsi == NULL) || !IsA(rsi, ReturnSetInfo) || (rsi->allowedModes & SFRM_Materialize) == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_PLSQL),
                    errmsg("set-valued function called in context that cannot accept a set for PLSQL function.")));
        }
        rsi->returnMode = SFRM_Materialize;

        /* If we produced any tuples, send back the result */
        if (estate.tuple_store != NULL) {
            rsi->setResult = estate.tuple_store;
            if (estate.rettupdesc) {
                MemoryContext oldcxt;

                oldcxt = MemoryContextSwitchTo(estate.tuple_store_cxt);
                rsi->setDesc = CreateTupleDescCopy(estate.rettupdesc);
                MemoryContextSwitchTo(oldcxt);
            }
        }
        estate.retval = (Datum)0;
        fcinfo->isnull = true;
    } else if (!estate.retisnull) {
        if (estate.retistuple) {
            /*
             * We have to check that the returned tuple actually matches the
             * expected result type.  XXX would be better to cache the tupdesc
             * instead of repeating get_call_result_type()
             */
            HeapTuple rettup = (HeapTuple)DatumGetPointer(estate.retval);
            TupleDesc tupdesc;
            TupleConversionMap* tupmap = NULL;

            switch (get_call_result_type(fcinfo, NULL, &tupdesc)) {
                case TYPEFUNC_COMPOSITE:
                    /* got the expected result rowtype, now check it */
                    if (estate.rettupdesc == NULL && estate.func->out_param_varno >= 0) {
                        PLpgSQL_datum* out_param_datum = estate.datums[estate.func->out_param_varno];
                        if (out_param_datum->dtype == PLPGSQL_DTYPE_VAR &&
                            ((PLpgSQL_var*)out_param_datum)->datatype != NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                                    errmodule(MOD_PLSQL),
                                    errmsg("function result type must be %s because of OUT parameters",
                                        ((PLpgSQL_var*)out_param_datum)->datatype->typname)));
                        }
                    }
                    tupmap = convert_tuples_by_position(estate.rettupdesc,
                        tupdesc,
                        gettext_noop("returned record type does not match expected record type"));
                    /* it might need conversion */
                    if (tupmap != NULL) {
                        rettup = do_convert_tuple(rettup, tupmap);
	                }
                    /* no need to free map, we're about to return anyway */
                    break;
                case TYPEFUNC_RECORD:

                    /*
                     * Failed to determine actual type of RECORD.  We could
                     * raise an error here, but what this means in practice is
                     * that the caller is expecting any old generic rowtype,
                     * so we don't really need to be restrictive. Pass back
                     * the generated result type, instead.
                     */
                    tupdesc = estate.rettupdesc;
                    if (tupdesc == NULL) { /* shouldn't happen */
                        ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmodule(MOD_PLSQL),
                                errmsg("return type must be a row type for PLSQL function.")));
                    }
                    break;
                default:
                    /* shouldn't get here if retistuple is true ... */
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_PLSQL),
                            errmsg("unrecognized return type for PLSQL function.")));
                    break;
            }

            /*
             * Copy tuple to upper executor memory, as a tuple Datum. Make
             * sure it is labeled with the caller-supplied tuple type.
             */
            estate.retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));
        } else {
            if (estate.rettype == InvalidOid && estate.func->out_param_varno >= 0) {
                PLpgSQL_datum* out_param_datum = estate.datums[estate.func->out_param_varno];
                if (out_param_datum->dtype == PLPGSQL_DTYPE_ROW && ((PLpgSQL_row*)out_param_datum)->nfields > 1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmodule(MOD_PLSQL),
                            errmsg("function result type must be record because of OUT parameters")));
                }
            }
            /* Cast value to proper type */
            estate.retval = exec_cast_value(&estate,
                estate.retval,
                estate.rettype,
                func->fn_rettype,
                &(func->fn_retinput),
                func->fn_rettypioparam,
                -1,
                fcinfo->isnull);

            /*
             * If the function's return type isn't by value, copy the value
             * into upper executor memory context.
             */
            if (!fcinfo->isnull && !func->fn_retbyval) {
                Size len;
                void* tmp = NULL;
                errno_t rc = EOK;

                len = datumGetSize(estate.retval, false, func->fn_rettyplen);
                tmp = SPI_palloc(len);
                rc = memcpy_s(tmp, len, DatumGetPointer(estate.retval), len);
                securec_check(rc, "\0", "\0");
                estate.retval = PointerGetDatum(tmp);
            }
        }
    }

    estate.err_text = gettext_noop("during function exit");

    /*
     * Let the instrumentation plugin peek at this function
     */
    if (*u_sess->plsql_cxt.plugin_ptr && (*u_sess->plsql_cxt.plugin_ptr)->func_end) {
        ((*u_sess->plsql_cxt.plugin_ptr)->func_end)(&estate, func);
    }

    // Check for interrupts sent by pl debugger and other plugins.
    //
    CHECK_FOR_INTERRUPTS();

    for (i = 0; i < estate.ndatums; i++) {
        /* copy cursor var */
        errno_t errorno = EOK;

        if (estate.datums[i]->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* newm = (PLpgSQL_var*)estate.datums[i];
            if (newm->is_cursor_var) {
                errorno = memcpy_s(func->datums[i], sizeof(PLpgSQL_var), estate.datums[i], sizeof(PLpgSQL_var));
                securec_check(errorno, "\0", "\0");
            }
        }
    }

    estate.cursor_return_data = NULL;
    /* Clean up any leftover temporary memory */
    plpgsql_destroy_econtext(&estate);
    exec_eval_cleanup(&estate);

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = plerrcontext.previous;
    plpgsql_restore_current_value_stp_with_exception(saved_current_stp_with_exception);
    /*
     * Return the function's result
     */
    return estate.retval;
}

/* ----------
 * plpgsql_exec_trigger		Called by the call handler for
 *				trigger execution.
 * ----------
 */
HeapTuple plpgsql_exec_trigger(PLpgSQL_function* func, TriggerData* trigdata)
{
    PLpgSQL_execstate estate;
    ErrorContextCallback plerrcontext;
    int i;
    int rc;
    PLpgSQL_var* var = NULL;
    PLpgSQL_rec *rec_new = NULL;
    PLpgSQL_rec *rec_old = NULL;
    HeapTuple rettup;

    /*
     * Setup the execution state
     */
    plpgsql_estate_setup(&estate, func, NULL);

    /*
     * Setup error traceback support for ereport()
     */
    plerrcontext.callback = plpgsql_exec_error_callback;
    plerrcontext.arg = &estate;
    plerrcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &plerrcontext;

    /*
     * Make local execution copies of all the datums
     */
    estate.err_text = gettext_noop("during initialization of execution state");
    for (i = 0; i < estate.ndatums; i++) {
        estate.datums[i] = copy_plpgsql_datum(func->datums[i]);
    }

    /*
     * Put the OLD and NEW tuples into record variables
     *
     * We make the tupdescs available in both records even though only one may
     * have a value.  This allows parsing of record references to succeed in
     * functions that are used for multiple trigger types.	For example, we
     * might have a test like "if (TG_OP = 'INSERT' and NEW.foo = 'xyz')",
     * which should parse regardless of the current trigger type.
     */
    rec_new = (PLpgSQL_rec*)(estate.datums[func->new_varno]);
    rec_new->freetup = false;
    rec_new->tupdesc = trigdata->tg_relation->rd_att;
    rec_new->freetupdesc = false;
    rec_old = (PLpgSQL_rec*)(estate.datums[func->old_varno]);
    rec_old->freetup = false;
    rec_old->tupdesc = trigdata->tg_relation->rd_att;
    rec_old->freetupdesc = false;

    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)) {
        /*
         * Per-statement triggers don't use OLD/NEW variables
         */
        rec_new->tup = NULL;
        rec_old->tup = NULL;
    } else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
        rec_new->tup = trigdata->tg_trigtuple;
        rec_old->tup = NULL;
    } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
        rec_new->tup = trigdata->tg_newtuple;
        rec_old->tup = trigdata->tg_trigtuple;
    } else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
        rec_new->tup = NULL;
        rec_old->tup = trigdata->tg_trigtuple;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("unrecognized trigger action %u: not INSERT, DELETE, or UPDATE", trigdata->tg_event)));
    }

    /*
     * Assign the special tg_ variables
     */
    var = (PLpgSQL_var*)(estate.datums[func->tg_op_varno]);
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("INSERT");
    } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("UPDATE");
    } else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("DELETE");
    } else if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("TRUNCATE");
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("unrecognized trigger action %u: not INSERT, DELETE, UPDATE, or TRUNCATE", trigdata->tg_event)));
    }
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_name_varno]);
    var->value = DirectFunctionCall1(namein, CStringGetDatum(trigdata->tg_trigger->tgname));
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_when_varno]);
    if (TRIGGER_FIRED_BEFORE(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("BEFORE");
    } else if (TRIGGER_FIRED_AFTER(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("AFTER");
    } else if (TRIGGER_FIRED_INSTEAD(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("INSTEAD OF");
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmodule(MOD_PLSQL),
                errmsg(
                    "unrecognized trigger execution time %u: not BEFORE, AFTER, or INSTEAD OF", trigdata->tg_event)));
    }
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_level_varno]);
    if (TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("ROW");
    } else if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event)) {
        var->value = CStringGetTextDatum("STATEMENT");
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("unrecognized trigger event type %u: not ROW or STATEMENT", trigdata->tg_event)));
    }
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_relid_varno]);
    var->value = ObjectIdGetDatum(trigdata->tg_relation->rd_id);
    var->isnull = false;
    var->freeval = false;

    var = (PLpgSQL_var*)(estate.datums[func->tg_relname_varno]);
    var->value = DirectFunctionCall1(namein, CStringGetDatum(RelationGetRelationName(trigdata->tg_relation)));
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_table_name_varno]);
    var->value = DirectFunctionCall1(namein, CStringGetDatum(RelationGetRelationName(trigdata->tg_relation)));
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_table_schema_varno]);
    var->value =
        DirectFunctionCall1(namein, CStringGetDatum(get_namespace_name(RelationGetNamespace(trigdata->tg_relation))));
    var->isnull = false;
    var->freeval = true;

    var = (PLpgSQL_var*)(estate.datums[func->tg_nargs_varno]);
    var->value = Int16GetDatum(trigdata->tg_trigger->tgnargs);
    var->isnull = false;
    var->freeval = false;

    var = (PLpgSQL_var*)(estate.datums[func->tg_argv_varno]);
    if (trigdata->tg_trigger->tgnargs > 0) {
        /*
         * For historical reasons, tg_argv[] subscripts start at zero not one.
         * So we can't use construct_array().
         */
        int nelems = trigdata->tg_trigger->tgnargs;
        Datum* elems = NULL;
        int dims[1];
        int lbs[1];

        elems = (Datum*)palloc(sizeof(Datum) * nelems);
        for (i = 0; i < nelems; i++) {
            elems[i] = CStringGetTextDatum(trigdata->tg_trigger->tgargs[i]);
        }
        dims[0] = nelems;
        lbs[0] = 0;

        var->value = PointerGetDatum(construct_md_array(elems, NULL, 1, dims, lbs, TEXTOID, -1, false, 'i'));
        var->isnull = false;
        var->freeval = true;
    } else {
        var->value = (Datum)0;
        var->isnull = true;
        var->freeval = false;
    }

    estate.err_text = gettext_noop("during function entry");

    /*
     * Set the magic variable FOUND to false
     */
    exec_set_found(&estate, false);

    /* Set the magic implicit cursor attribute variable FOUND to false */
    exec_set_sql_cursor_found(&estate, PLPGSQL_NULL);

    /* Set the magic implicit cursor attribute variable NOTFOUND to true */
    exec_set_sql_notfound(&estate, PLPGSQL_NULL);

    /* Set the magic implicit cursor attribute variable ISOPEN to false */
    exec_set_sql_isopen(&estate, false);

    /* Set the magic implicit cursor attribute variable ROWCOUNT to 0 */
    exec_set_sql_rowcount(&estate, -1);

    exec_set_sqlcode(&estate, 0);

    /*
     * Let the instrumentation plugin peek at this function
     */
    if (*u_sess->plsql_cxt.plugin_ptr && (*u_sess->plsql_cxt.plugin_ptr)->func_beg) {
        ((*u_sess->plsql_cxt.plugin_ptr)->func_beg)(&estate, func);
    }

    // Check for interrupts sent by pl debugger and other plugins.
    //
    CHECK_FOR_INTERRUPTS();

    /*
     * Now call the toplevel block of statements
     */
    estate.err_text = NULL;
    estate.err_stmt = (PLpgSQL_stmt*)(func->action);

    // Forbid trigger to call procedure with commit/rollback.
    bool savedIsSTP = u_sess->SPI_cxt.is_stp;
    u_sess->SPI_cxt.is_stp = false;
    rc = exec_stmt_block(&estate, func->action);
    u_sess->SPI_cxt.is_stp = savedIsSTP;

    if (rc != PLPGSQL_RC_RETURN) {
        estate.err_stmt = NULL;
        estate.err_text = NULL;

        /*
         * Provide a more helpful message if a CONTINUE or RAISE has been used
         * outside the context it can work in.
         */
        if (rc == PLPGSQL_RC_CONTINUE) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("CONTINUE cannot be used outside a loop for trigger execution.")));
        } else if (rc == PLPGSQL_RC_GOTO_UNRESOLVED) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg(
                        "illegal GOTO statement; this GOTO cannot branch to label \"%s\"", estate.goto_target_label)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
                    errmodule(MOD_PLSQL),
                    errmsg("control reached end of trigger procedure without RETURN")));
        }
    }

    estate.err_stmt = NULL;
    estate.err_text = gettext_noop("during function exit");

    if (estate.retisset) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("trigger procedure cannot return a set")));
    }

    /*
     * Check that the returned tuple structure has the same attributes, the
     * relation that fired the trigger has. A per-statement trigger always
     * needs to return NULL, so we ignore any return value the function itself
     * produces (XXX: is this a good idea?)
     *
     * XXX This way it is possible, that the trigger returns a tuple where
     * attributes don't have the correct atttypmod's length. It's up to the
     * trigger's programmer to ensure that this doesn't happen. Jan
     */
    if (estate.retisnull || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)) {
        rettup = NULL;
    } else {
        TupleConversionMap* tupmap = NULL;

        rettup = (HeapTuple)DatumGetPointer(estate.retval);
        /* check rowtype compatibility */
        tupmap = convert_tuples_by_position(estate.rettupdesc,
            trigdata->tg_relation->rd_att,
            gettext_noop("returned row structure does not match the structure of the triggering table"));
        /* it might need conversion */
        if (tupmap != NULL) {
			/* no need to free map, we're about to return anyway */
            rettup = do_convert_tuple(rettup, tupmap);
        }

        /* Copy tuple to upper executor memory */
        rettup = SPI_copytuple(rettup);
    }

    /*
     * Let the instrumentation plugin peek at this function
     */
    if (*u_sess->plsql_cxt.plugin_ptr && (*u_sess->plsql_cxt.plugin_ptr)->func_end) {
        ((*u_sess->plsql_cxt.plugin_ptr)->func_end)(&estate, func);
    }

    // Check for interrupts sent by pl debugger and other plugins.
    //
    CHECK_FOR_INTERRUPTS();

    /* Clean up any leftover temporary memory */
    plpgsql_destroy_econtext(&estate);
    exec_eval_cleanup(&estate);

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = plerrcontext.previous;

    /*
     * Return the trigger's result
     */
    return rettup;
}

/*
 * error context callback to let us supply a call-stack traceback
 */
static void plpgsql_exec_error_callback(void* arg)
{
    PLpgSQL_execstate* estate = (PLpgSQL_execstate*)arg;

    /* if we are doing RAISE, don't report its location */
    if (estate->err_text == raise_skip_msg) {
        return;
    }

    if (AUDIT_EXEC_ENABLED) {
        int elevel;
        int sqlState;

        getElevelAndSqlstate(&elevel, &sqlState);
        if (elevel >= ERROR && estate->func->fn_oid >= FirstNormalObjectId) {
            errno_t ret = EOK;
            char details[PGAUDIT_MAXLENGTH] = {0};

            ret = snprintf_s(details,
                sizeof(details),
                sizeof(details) - 1,
                "Execute PL/pgSQL function(%s). ",
                estate->func->fn_signature);
            securec_check_ss(ret, "", "");
            audit_report(AUDIT_FUNCTION_EXEC, AUDIT_FAILED, estate->func->fn_signature, details);
        }
    }

    if (estate->err_text != NULL) {
        /*
         * We don't expend the cycles to run gettext() on err_text unless we
         * actually need it.  Therefore, places that set up err_text should
         * use gettext_noop() to ensure the strings get recorded in the
         * message dictionary.
         *
         * If both err_text and err_stmt are set, use the err_text as
         * description, but report the err_stmt's line number.  When err_stmt
         * is not set, we're in function entry/exit, or some such place not
         * attached to a specific line number.
         */
        if (estate->err_stmt != NULL) {
            /*
             * translator: last %s is a phrase such as "during statement block
             * local variable initialization"
             */
            errcontext("PL/pgSQL function %s line %d %s",
                estate->func->fn_signature,
                estate->err_stmt->lineno,
                _(estate->err_text));
        } else {
            /*
             * translator: last %s is a phrase such as "while storing call
             * arguments into local variables"
             */
            errcontext("PL/pgSQL function %s %s", estate->func->fn_signature, _(estate->err_text));
        }
    } else if (estate->err_stmt != NULL) {
        /* translator: last %s is a plpgsql statement type name */
        errcontext("PL/pgSQL function %s line %d at %s",
            estate->func->fn_signature,
            estate->err_stmt->lineno,
            plpgsql_stmt_typename(estate->err_stmt));
    } else {
        errcontext("PL/pgSQL function %s", estate->func->fn_signature);
    }
}

/* ----------
 * search_for_valid_line
 * ----------
 */
static int search_for_valid_line(PLpgSQL_stmt* stmt, int linenum, int prevValidLine)
{
    int rc;

    if (stmt == NULL) {
        return -1;
    }

    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            rc = check_line_validity_in_block((PLpgSQL_stmt_block*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_IF:
            rc = check_line_validity_in_if((PLpgSQL_stmt_if*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_CASE:
            rc = check_line_validity_in_case((PLpgSQL_stmt_case*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_LOOP:
            rc = check_line_validity_in_loop((PLpgSQL_stmt_loop*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_WHILE:
            rc = check_line_validity_in_while((PLpgSQL_stmt_while*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_FORI:
            rc = check_line_validity_in_fori((PLpgSQL_stmt_fori*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_FOREACH_A:
            rc = check_line_validity_in_foreach_a((PLpgSQL_stmt_foreach_a*)stmt, linenum, prevValidLine);
            break;

        case PLPGSQL_STMT_FORS:
        case PLPGSQL_STMT_FORC:
        case PLPGSQL_STMT_DYNFORS:
            rc = check_line_validity_in_for_query((PLpgSQL_stmt_forq*)stmt, linenum, prevValidLine);
            break;

        default:
            return (stmt->lineno == linenum) ? linenum : -1;
    }

    return rc;
}

/* ----------
 * check_line_validity_in_block
 * ----------
 */
static int check_line_validity_in_block(PLpgSQL_stmt_block* block, int linenum, int prevValidLine)
{
    PLpgSQL_exception* exception = NULL;
    PLpgSQL_stmt* firststmt = NULL;
    ListCell* templc = NULL;
    ListCell* e = NULL;
    int rc;

    /* block's body may be NIL */
    if (block->body != NIL) {
        templc = list_head(block->body);
        firststmt = (PLpgSQL_stmt*)lfirst(templc);
        if (linenum < firststmt->lineno) {
            /*
             * if the first stmt is a block, step in. if not, just set the
             * breakpoint on it.
             */
            if (firststmt->cmd_type != PLPGSQL_STMT_BLOCK) {
                return firststmt->lineno;
            }

            linenum = firststmt->lineno;
            block = (PLpgSQL_stmt_block*)firststmt;
            rc = check_line_validity_in_block(block, linenum, prevValidLine);
        } else {
            rc = check_line_validity(block->body, linenum, prevValidLine);
        }
    } else {
        rc = -1;
    }

    if (rc == -1) {
        if (block->exceptions != NULL) {
            foreach (e, block->exceptions->exc_list) {
                exception = (PLpgSQL_exception*)lfirst(e);
                /* If the action is null, go next */
                if (exception->action == NIL) {
                    continue;
                }
                templc = list_head(exception->action);
                firststmt = (PLpgSQL_stmt*)lfirst(templc);

                if (linenum < firststmt->lineno) {
                    return firststmt->lineno;
                }

                rc = check_line_validity(exception->action, linenum, prevValidLine);
                if (rc != -1) {
                    break;
                }
            }
        }
    }

    return rc;
}

/* ----------
 * check_line_validity_in_if
 * ----------
 */
static int check_line_validity_in_if(PLpgSQL_stmt_if* stmt, int linenum, int prevValidLine)
{
    PLpgSQL_stmt* firststmt = NULL;
    ListCell* lc = NULL;
    ListCell* templc = NULL;
    int rc;

    if (linenum > stmt->lineno) {
        templc = list_head(stmt->then_body);
        if (templc != NULL) {
            firststmt = (PLpgSQL_stmt*)lfirst(templc);
            if (linenum < firststmt->lineno) {
                return firststmt->lineno;
            }
        }
    }

    rc = check_line_validity(stmt->then_body, linenum, prevValidLine);
    if (rc == -1) {
        foreach (lc, stmt->elsif_list) {
            PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(lc);

            templc = list_head(elif->stmts);
            if (templc != NULL) {
                firststmt = (PLpgSQL_stmt*)lfirst(templc);
                if (linenum < firststmt->lineno) {
                    return firststmt->lineno;
                }
            }

            rc = check_line_validity(elif->stmts, linenum, prevValidLine);
            if (rc != -1) {
                return rc;
            }
        }

        if (stmt->else_body != NIL) {
            templc = list_head(stmt->else_body);
            if (templc != NULL) {
                firststmt = (PLpgSQL_stmt*)lfirst(templc);
                if (linenum < firststmt->lineno) {
                    return firststmt->lineno;
                }
            }
            return check_line_validity(stmt->else_body, linenum, prevValidLine);
        }
    }

    return rc;
}

/* ----------
 * check_line_validity_in_loop
 * ----------
 */
static int check_line_validity_in_loop(PLpgSQL_stmt_loop* stmt, int linenum, int prevValidLine)
{
    PLpgSQL_stmt* firststmt = NULL;
    ListCell* templc = NULL;

    templc = list_head(stmt->body);
    if (templc != NULL) {
        firststmt = (PLpgSQL_stmt*)lfirst(templc);
        if (linenum < firststmt->lineno) {
            return firststmt->lineno;
        }
    }
    return check_line_validity(stmt->body, linenum, prevValidLine);
}

/* ----------
 * check_line_validity_in_case
 * ----------
 */
static int check_line_validity_in_case(PLpgSQL_stmt_case* stmt, int linenum, int prevValidLine)
{
    ListCell* templc = NULL;
    PLpgSQL_stmt* firststmt = NULL;
    ListCell* l = NULL;
    int rc;

    if (linenum == stmt->lineno) {
        return linenum;
    }

    /* Now search for a successful WHEN clause */
    foreach (l, stmt->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(l);

        templc = list_head(cwt->stmts);
        if (templc != NULL) {
            firststmt = (PLpgSQL_stmt*)lfirst(templc);
            if (linenum < firststmt->lineno) {
                return firststmt->lineno;
            }
        }
        rc = check_line_validity(cwt->stmts, linenum, prevValidLine);
        if (rc != -1) {
            return rc;
        }
    }

    if (stmt->have_else) {
        templc = list_head(stmt->else_stmts);
        if (templc != NULL) {
            firststmt = (PLpgSQL_stmt*)lfirst(templc);
            if (linenum < firststmt->lineno) {
                return firststmt->lineno;
            }
        }
        return check_line_validity(stmt->else_stmts, linenum, prevValidLine);
    }

    return -1;
}

/* ----------
 * check_line_validity_in_while
 * ----------
 */
static int check_line_validity_in_while(PLpgSQL_stmt_while* stmt, int linenum, int prevValidLine)
{
    if (linenum == stmt->lineno) {
        return linenum;
    }

    return check_line_validity(stmt->body, linenum, prevValidLine);
}

/* ----------
 * check_line_validity_in_fori
 * ----------
 */
static int check_line_validity_in_fori(PLpgSQL_stmt_fori* stmt, int linenum, int prevValidLine)
{
    if (linenum == stmt->lineno) {
        return linenum;
    }

    return check_line_validity(stmt->body, linenum, prevValidLine);
}

/* ----------
 * check_line_validity_in_foreach_a
 * ----------
 */
static int check_line_validity_in_foreach_a(PLpgSQL_stmt_foreach_a* stmt, int linenum, int prevValidLine)
{
    if (linenum == stmt->lineno) {
        return linenum;
    }

    return check_line_validity(stmt->body, linenum, prevValidLine);
}

/* ----------
 * check_line_validity_in_for_query
 * ----------
 */
static int check_line_validity_in_for_query(PLpgSQL_stmt_forq* stmt, int linenum, int prevValidLine)
{
    if (linenum == stmt->lineno) {
        return linenum;
    }

    return check_line_validity(stmt->body, linenum, prevValidLine);
}

/* ----------
 * Support function for initializing local execution variables
 * ----------
 */
static PLpgSQL_datum* copy_plpgsql_datum(PLpgSQL_datum* datum)
{
    PLpgSQL_datum* result = NULL;
    errno_t errorno = EOK;

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* newm = (PLpgSQL_var*)palloc(sizeof(PLpgSQL_var));

            errorno = memcpy_s(newm, sizeof(PLpgSQL_var), datum, sizeof(PLpgSQL_var));
            securec_check(errorno, "\0", "\0");
            /* Ensure the value is null (possibly not needed?) */
            if (newm->is_cursor_open) {
                /* for isopen option, cur%isopen always be true or false, so its initial value should be not null */
                PLpgSQL_var* cursorvar = (PLpgSQL_var*)datum;
                newm->isnull = false;
                newm->value = cursorvar->value;
            } else {
                newm->value = 0;
                newm->isnull = true;
            }
            newm->freeval = false;

            result = (PLpgSQL_datum*)newm;
        } break;

        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* newm = (PLpgSQL_rec*)palloc(sizeof(PLpgSQL_rec));

            errorno = memcpy_s(newm, sizeof(PLpgSQL_rec), datum, sizeof(PLpgSQL_rec));
            securec_check(errorno, "\0", "\0");
            /* Ensure the value is null (possibly not needed?) */
            newm->tup = NULL;
            newm->tupdesc = NULL;
            newm->freetup = false;
            newm->freetupdesc = false;

            result = (PLpgSQL_datum*)newm;
        } break;
        case PLPGSQL_DTYPE_EXPR:
        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_RECORD:
        case PLPGSQL_DTYPE_RECFIELD:
        case PLPGSQL_DTYPE_ARRAYELEM:

            /*
             * These datum records are read-only at runtime, so no need to
             * copy them (well, ARRAYELEM contains some cached type data, but
             * we'd just as soon centralize the caching anyway)
             */
            result = datum;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized dtype: %d when copy plsql datum.", datum->dtype)));
            result = NULL; /* keep compiler quiet */
            break;
    }

    return result;
}

static bool exception_matches_conditions(ErrorData* edata, PLpgSQL_condition* cond)
{
    for (; cond != NULL; cond = cond->next) {
        int sqlerrstate = cond->sqlerrstate;

        /*
         * OTHERS matches everything *except* query-canceled; if you're
         * foolish enough, you can match that explicitly.
         */
        if (sqlerrstate == 0) {
            if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED) {
                return true;
            }
        } else if (edata->sqlerrcode == sqlerrstate) {
            /* Exact match? */
            return true;
        } else if (ERRCODE_IS_CATEGORY(sqlerrstate) && (ERRCODE_TO_CATEGORY(edata->sqlerrcode) == sqlerrstate) &&
                 ((edata->sqlerrcode & CUSTOM_ERRCODE_P1) != CUSTOM_ERRCODE_P1)) {
	        /*
	         * If the sqlerrstate match an item in the category
	         * and not a custom error code.
	         */
            return true;
        }
    }
    return false;
}

/* 
 * Get last transaction's ResourceOwner.  
 *
 * 1. TOP ResourceOwner, one sub transaction ResourceOwner, portal ResourceOwner.
 * TOP  <->  sub1 << current transaction 
 *   ^  f,p   |n     
 *    \       v          
 *     \---- Portal
 *     P 
 * 
 * 2. TOP ResourceOwner, N * sub transaction ResourceOwner, portal ResourceOwner.
 * TOP  <->  sub1  <->  sub2
 *  ^   f,p   |n   f,p   ^
 *   \        v          ^
 *    \----- Portal   current transaction
 *     P 
 *
 * 3. TOP ResourceOwner, portal ResourceOwner.
 * TOP <-> Portal << current transaction 
 *     f,p
 *
 * f: first child, p:parent n:next.
 */
static ResourceOwner get_last_transaction_resourceowner() 
{
    ResourceOwner oldowner = NULL;
    const char *Toptransaction = "TopTransaction";
    if (strcmp(Toptransaction, ResourceOwnerGetName(t_thrd.utils_cxt.CurrentResourceOwner)) == 0 ||
        strcmp(Toptransaction, ResourceOwnerGetName(
            ResourceOwnerGetParent(t_thrd.utils_cxt.CurrentResourceOwner))) ==0) {
        oldowner = t_thrd.utils_cxt.STPSavedResourceOwner;
    } else {
        oldowner = ResourceOwnerGetParent(t_thrd.utils_cxt.CurrentResourceOwner);
    }
    return oldowner;
}

/* ----------
 * exec_stmt_block			Execute a block of statements
 * ----------
 */
static int exec_stmt_block(PLpgSQL_execstate* estate, PLpgSQL_stmt_block* block)
{
    volatile int rc = -1;
    int i;
    int n;
    SubTransactionId subXid = InvalidSubTransactionId;

    /*
     * First initialize all variables declared in this block
     */
    bool savedisAllowCommitRollback = u_sess->SPI_cxt.is_allow_commit_rollback;
    bool savedIsSTP = u_sess->SPI_cxt.is_stp;
    bool savedProConfigIsSet = u_sess->SPI_cxt.is_proconfig_set;
    estate->err_text = gettext_noop("during statement block local variable initialization");

    for (i = 0; i < block->n_initvars; i++) {
        n = block->initvarnos[i];

        switch (estate->datums[n]->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[n]);

                /* free any old value, in case re-entering block */
                free_var(var);

                /* Initially it contains a NULL */
                var->value = (Datum)0;

                if (var->is_cursor_open) {
                    var->isnull = false;
                } else {
                    var->isnull = true;
                }

                if (var->default_val == NULL) {
                    /*
                     * If needed, give the datatype a chance to reject
                     * NULLs, by assigning a NULL to the variable. We
                     * claim the value is of type UNKNOWN, not the var's
                     * datatype, else coercion will be skipped. (Do this
                     * before the notnull check to be consistent with
                     * exec_assign_value.)
                     */
                    if (!var->datatype->typinput.fn_strict) {
                        bool valIsNull = true;

                        exec_assign_value(estate, (PLpgSQL_datum*)var, (Datum)0, UNKNOWNOID, &valIsNull);
                    }
                    if (var->notnull) {
                        ereport(ERROR,
                            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                errmodule(MOD_PLSQL),
                                errmsg("variable \"%s\" declared NOT NULL cannot default to NULL", var->refname)));
                    }
                } else {
                    exec_assign_expr(estate, (PLpgSQL_datum*)var, var->default_val);
                }
            } break;

            case PLPGSQL_DTYPE_REC: {
                PLpgSQL_rec* rec = (PLpgSQL_rec*)(estate->datums[n]);

                if (rec->freetup) {
                    heap_freetuple_ext(rec->tup);
                    rec->freetup = false;
                }
                if (rec->freetupdesc) {
                    FreeTupleDesc(rec->tupdesc);
                    rec->freetupdesc = false;
                }
                rec->tup = NULL;
                rec->tupdesc = NULL;
            } break;

            case PLPGSQL_DTYPE_RECFIELD:
            case PLPGSQL_DTYPE_ARRAYELEM:
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized declare data type: %d for PLSQL function.", estate->datums[n]->dtype)));
                break;
        }
    }

    if (block->exceptions != NULL) {
        u_sess->SPI_cxt.portal_stp_exception_counter++;
        plpgsql_set_current_value_stp_with_exception();
        /*
         * Execute the statements in the block's body inside a sub-transaction
         */
        MemoryContext oldcontext = CurrentMemoryContext;
        ResourceOwner oldOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        ErrorData* save_cur_error = estate->cur_error;
        TransactionId oldTransactionId = SPI_get_top_transaction_id();

        estate->err_text = gettext_noop("during statement block entry");
#ifdef ENABLE_MULTIPLE_NODES
        /* CN should send savepoint command to remote nodes to begin sub transaction remotely. */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            /* if do savepoint, always treat myself as local write node */
            RegisterTransactionLocalNode(true);
            RecordSavepoint("Savepoint s1", "s1", false, SUB_STMT_SAVEPOINT);
            pgxc_node_remote_savepoint("Savepoint s1", EXEC_ON_DATANODES, true, true);
        }
#endif
        PG_TRY();
        {
            BeginInternalSubTransaction(NULL);
        }
        PG_CATCH();
        {
            stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);

            OverrideStackEntry *entry = NULL;

            Assert (u_sess->catalog_cxt.overrideStack != NULL);
            u_sess->SPI_cxt.portal_stp_exception_counter--;

            entry = (OverrideStackEntry *) linitial(u_sess->catalog_cxt.overrideStack);
            if (entry->nestLevel < GetCurrentTransactionNestLevel()) {
                AbortSubTransaction();
                CleanupSubTransaction();
            }
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                HandleReleaseOrRollbackSavepoint("rollback to s1", "s1", SUB_STMT_ROLLBACK_TO);
                /* CN send rollback savepoint to remote nodes to abort sub transaction remotely */
                pgxc_node_remote_savepoint("rollback to s1", EXEC_ON_DATANODES, false, false);

                HandleReleaseOrRollbackSavepoint("release s1", "s1", SUB_STMT_RELEASE);
                /* CN should send release savepoint command to remote nodes for savepoint name reuse */
                pgxc_node_remote_savepoint("release s1", EXEC_ON_DATANODES, false, false);
            }    
#endif
            // Get last transaction's ResourceOwner.
            stp_check_transaction_and_set_resource_owner(oldOwner,oldTransactionId);

            estate->eval_econtext = u_sess->plsql_cxt.simple_econtext_stack->stack_econtext;
            MemoryContextSwitchTo(oldcontext);

            /*
             * If AtEOSubXact_SPI() popped any SPI context of the subxact, it
             * will have left us in a disconnected state.  We need this hack
             * to return to connected state.
             */
            SPI_restore_connection();

            /* Must clean up the econtext too */
            exec_eval_cleanup(estate);
            PG_RE_THROW();
        }
        PG_END_TRY();

        subXid = GetCurrentSubTransactionId();
        /* Want to run statements inside function's memory context */
        MemoryContextSwitchTo(oldcontext);
        Cursor_Data* saved_cursor_data = estate->cursor_return_data;
        PG_TRY();
        {
            /*
             * We need to run the block's statements with a new eval_econtext
             * that belongs to the current subtransaction; if we try to use
             * the outer econtext then ExprContext shutdown callbacks will be
             * called at the wrong times.
             */
            plpgsql_create_econtext(estate);

            estate->err_text = NULL;
#ifndef ENABLE_MULTIPLE_NODES
            if (IsAutonomousTransaction(estate, block)) {
                AttachToAutonomousSession(estate, block);
            }
#endif
            /* Run the block's statements */
            rc = exec_stmts(estate, block->body);

#ifndef ENABLE_MULTIPLE_NODES
            if (IsAutonomousTransaction(estate, block)) {
                DetachToAutonomousSession(estate);
            }
#endif
            estate->err_text = gettext_noop("during statement block exit");

            /*
             * If the block ended with RETURN, we may need to copy the return
             * value out of the subtransaction eval_context.  This is
             * currently only needed for scalar result types --- rowtype
             * values will always exist in the function's own memory context.
             */
            if (rc == PLPGSQL_RC_RETURN && !estate->retisset && !estate->retisnull && estate->rettupdesc == NULL) {
                int16 resTypLen;
                bool resTypByVal = false;

                get_typlenbyval(estate->rettype, &resTypLen, &resTypByVal);
                estate->retval = datumCopy(estate->retval, resTypByVal, resTypLen);
            }

            /* Commit the inner transaction, return to outer xact context */
            ReleaseCurrentSubTransaction();
#ifdef ENABLE_MULTIPLE_NODES
            /* CN should send release savepoint command to remote nodes for savepoint name reuse */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                HandleReleaseOrRollbackSavepoint("release s1", "s1", SUB_STMT_RELEASE);
                pgxc_node_remote_savepoint("release s1", EXEC_ON_DATANODES, false, false);
            }
#endif
            MemoryContextSwitchTo(oldcontext);

            // Get last transaction's ResourceOwner.
            stp_check_transaction_and_set_resource_owner(oldOwner,oldTransactionId); 
            u_sess->SPI_cxt.portal_stp_exception_counter--;

            /*
             * Revert to outer eval_econtext.  (The inner one was
             * automatically cleaned up during subxact exit.)
             */
            estate->eval_econtext = u_sess->plsql_cxt.simple_econtext_stack->stack_econtext;

            /*
             * AtEOSubXact_SPI() should not have popped any SPI context, but
             * just in case it did, make sure we remain connected.
             */
            SPI_restore_connection();

            stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
        }
        PG_CATCH();
        {
            ErrorData* edata = NULL;
            ListCell* e = NULL;

            stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
            u_sess->SPI_cxt.is_stp = savedIsSTP;
            u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;

            estate->cursor_return_data = saved_cursor_data;

            /* gs_signal_handle maybe block sigusr2 when accept SIGINT */
            gs_signal_unblock_sigusr2();

            estate->err_text = gettext_noop("during exception cleanup");

            /* Save error info */
            MemoryContextSwitchTo(oldcontext);
            /* Set bInAbortTransaction to avoid core dump caused by recursive memory alloc exception */
            t_thrd.xact_cxt.bInAbortTransaction = true;
            edata = CopyErrorData();
            t_thrd.xact_cxt.bInAbortTransaction = false;
            FlushErrorState();

#ifndef ENABLE_MULTIPLE_NODES
            if (IsAutonomousTransaction(estate, block)) {
                DetachToAutonomousSession(estate);
            }
#endif
            if (edata->sqlerrcode == ERRCODE_OPERATOR_INTERVENTION ||
                edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
                edata->sqlerrcode == ERRCODE_QUERY_INTERNAL_CANCEL ||
                edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN ||
                edata->sqlerrcode == ERRCODE_CRASH_SHUTDOWN ||
                edata->sqlerrcode == ERRCODE_CANNOT_CONNECT_NOW ||
                edata->sqlerrcode == ERRCODE_DATABASE_DROPPED ||
                edata->sqlerrcode == ERRCODE_RU_STOP_QUERY) {
                    SetInstrNull();
                }
            u_sess->SPI_cxt.portal_stp_exception_counter--;

            /* Abort the inner transaction */
            if (GetCurrentSubTransactionId() == subXid) {
                ResetPortalCursor(subXid, InvalidOid, 0);
                RollbackAndReleaseCurrentSubTransaction();
            } else {
                ereport(FATAL,
                    (errmsg("exception happens after current savepoint released error message is: %s",
                        edata->message ? edata->message : " ")));
            }

            if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
                ereport(ERROR,
                    (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                        errmodule(MOD_PLSQL),
                        errmsg("Transaction aborted as connection handles were destroyed due to clean up stream "
                               "failed. error message is: %s",
                            edata->message ? edata->message : " ")));
            }
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                HandleReleaseOrRollbackSavepoint("rollback to s1", "s1", SUB_STMT_ROLLBACK_TO);
                /* CN send rollback savepoint to remote nodes to abort sub transaction remotely */
                pgxc_node_remote_savepoint("rollback to s1", EXEC_ON_DATANODES, false, false);

                HandleReleaseOrRollbackSavepoint("release s1", "s1", SUB_STMT_RELEASE);
                /* CN should send release savepoint command to remote nodes for savepoint name reuse */
                pgxc_node_remote_savepoint("release s1", EXEC_ON_DATANODES, false, false);
            }
#endif

            // Get last transaction's ResourceOwner.
            stp_check_transaction_and_set_resource_owner(oldOwner,oldTransactionId);

            estate->eval_econtext = u_sess->plsql_cxt.simple_econtext_stack->stack_econtext;
            MemoryContextSwitchTo(oldcontext);

            /*
             * If AtEOSubXact_SPI() popped any SPI context of the subxact, it
             * will have left us in a disconnected state.  We need this hack
             * to return to connected state.
             */
            SPI_restore_connection();

            /* Must clean up the econtext too */
            exec_eval_cleanup(estate);

            /* Look for a matching exception handler */
            foreach (e, block->exceptions->exc_list) {
                PLpgSQL_exception* exception = (PLpgSQL_exception*)lfirst(e);

                if (exception_matches_conditions(edata, exception->conditions)) {
                    /*
                     * Initialize the magic SQLSTATE and SQLERRM variables for
                     * the exception block. We needn't do this until we have
                     * found a matching exception.
                     */
                    PLpgSQL_var* state_var = NULL;
                    PLpgSQL_var* errm_var = NULL;

                    state_var = (PLpgSQL_var*)estate->datums[block->exceptions->sqlstate_varno];
                    errm_var = (PLpgSQL_var*)estate->datums[block->exceptions->sqlerrm_varno];

                    assign_text_var(state_var, unpack_sql_state(edata->sqlerrcode));
                    assign_text_var(errm_var, edata->message);

                    /*
                     * Also set up cur_error so the error data is accessible
                     * inside the handler.
                     */
                    estate->cur_error = edata;

                    estate->err_text = NULL;

                    exec_set_sqlcode(estate, edata->sqlerrcode);

                    rc = exec_stmts(estate, exception->action);

                    stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
                    u_sess->SPI_cxt.is_stp = savedIsSTP;
                    u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;

                    free_var(state_var);
                    state_var->value = (Datum)0;
                    state_var->isnull = true;
                    free_var(errm_var);
                    errm_var->value = (Datum)0;
                    errm_var->isnull = true;

                    break;
                }
            }

            /*
             * Restore previous state of cur_error, whether or not we executed
             * a handler.  This is needed in case an error got thrown from
             * some inner block's exception handler.
             */
            estate->cur_error = save_cur_error;

            /* If no match found, re-throw the error */
            if (e == NULL) {
                ReThrowError(edata);
            } else {
                FreeErrorData(edata);
            }
        }
        PG_END_TRY();

        AssertEreport(save_cur_error == estate->cur_error,
            MOD_PLSQL,
            "save current error should be same error  as estate current error.");
    } else {
        /*
         * Just execute the statements in the block's body
         */
        estate->err_text = NULL;
#ifdef ENABLE_MULTIPLE_NODES
        rc = exec_stmts(estate, block->body);
#else
        if (IsAutonomousTransaction(estate, block)) {
            AttachToAutonomousSession(estate, block);

            MemoryContext oldcontext = CurrentMemoryContext;
            PG_TRY();
            {
                rc = exec_stmts(estate, block->body);

                DetachToAutonomousSession(estate);
            }
            PG_CATCH();
            {
                /* here to make sure closing session properly when error happens in exec_stmts, then rethrow error */
                MemoryContextSwitchTo(oldcontext);
                ErrorData* edata = CopyErrorData();
                FlushErrorState();

                DetachToAutonomousSession(estate);

                ReThrowError(edata);
            }
            PG_END_TRY();
        } else {
            rc = exec_stmts(estate, block->body);
        }
#endif

        stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
        u_sess->SPI_cxt.is_stp = savedIsSTP;
        u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
    }

    estate->err_text = NULL;

    /*
     * Handle the return code.
     */
    switch (rc) {
        case PLPGSQL_RC_OK:
        case PLPGSQL_RC_RETURN:
        case PLPGSQL_RC_CONTINUE:
        case PLPGSQL_RC_GOTO_UNRESOLVED:
            return rc;

        case PLPGSQL_RC_EXIT:

            /*
             * This is intentionally different from the handling of RC_EXIT
             * for loops: to match a block, we require a match by label.
             */
            if (estate->exitlabel == NULL) {
                return PLPGSQL_RC_EXIT;
            }
            if (block->label == NULL) {
                return PLPGSQL_RC_EXIT;
            }
            if (strcmp(block->label, estate->exitlabel) != 0) {
                return PLPGSQL_RC_EXIT;
            }
            estate->exitlabel = NULL;
            return PLPGSQL_RC_OK;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized return code: %d for PLSQL function.", rc)));
            break;
    }
    return PLPGSQL_RC_OK;
}

/* ----------
 * search_goto_target_global			search goto target statement in global
 *				return the searched PLpgSQL statement.
 * ----------
 */
static PLpgSQL_stmt* search_goto_target_global(PLpgSQL_execstate* estate, const PLpgSQL_stmt_goto* goto_stmt)
{
    PLpgSQL_stmt* result_stmt = NULL;
    ListCell* lc = NULL;
    int muti_labels = 0;

    foreach (lc, estate->goto_labels) {
        PLpgSQL_gotoLabel* gl = (PLpgSQL_gotoLabel*)lfirst(lc);
        if (gl->label != NULL && strcmp(goto_stmt->label, gl->label) == 0) {
            result_stmt = gl->stmt;
            muti_labels++;
        }
    }

    if (muti_labels == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("GOTO target statement \"%s\" is not found in the whole procedure execution context",
                    goto_stmt->label)));
    }
    if (muti_labels > 1) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("at most one GOTO target statement declaration for \"%s\" is permitted", goto_stmt->label)));
    }
    estate->goto_target_stmt = result_stmt;

    return result_stmt;
}

/* ----------
 * exec_stmt			search goto target statement in current block
 *				return the searched goto statement.
 * ----------
 */
static PLpgSQL_stmt* search_goto_target_current_block(
    PLpgSQL_execstate* estate, const List* stmts, PLpgSQL_stmt* target_stmt, int* stepno)
{
    ListCell* lc = NULL;
    Assert(stepno != NULL);
    *stepno = 0;
    foreach (lc, stmts) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(lc);

        if (stmt == target_stmt) {
            return stmt;
        }

        (*stepno)++;
    }

    /*
     * We don't find the target stmt in current statemnet level, we need set current
     * stmtid to the tail to let the upper caller exec_stmts() exit current stmt-block
     */
    (*stepno) = list_length(stmts);
    return (PLpgSQL_stmt*)NULL;
}

/* ----------
 * exec_stmts			Iterate over a list of statements
 *				as long as their return code is OK
 * ----------
 */
static int exec_stmts(PLpgSQL_execstate* estate, List* stmts)
{
    if (stmts == NIL) {
        /*
         * Ensure we do a CHECK_FOR_INTERRUPTS() even though there is no
         * statement.  This prevents hangup in a tight loop if, for instance,
         * there is a LOOP construct with an empty body.
         */
        CHECK_FOR_INTERRUPTS();
        return PLPGSQL_RC_OK;
    }

    /* Increase block_level */
    estate->block_level++;
    int num_stmts = list_length(stmts);
    int stmtid = 0;

    for (;;) {
        if (stmtid >= num_stmts) {
            /* Reach the end of the stmt-block, just return normally */
            goto normal_exit;
        }

        /* Fetch next stmt to execute */
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)list_nth(stmts, stmtid);

        /* If current statement is GOTO, process it here, */
        if (stmt->cmd_type == PLPGSQL_STMT_GOTO) {
            int new_index = -1;
            PLpgSQL_stmt_goto* goto_stmt = (PLpgSQL_stmt_goto*)stmt;
            PLpgSQL_stmt* target_stmt = NULL;
            estate->goto_target_label = goto_stmt->label;

            /* Reset statement execution index and continue to run */
            target_stmt = search_goto_target_global(estate, goto_stmt);

            /* Try to find target stmt in current statement-block */
            stmt = search_goto_target_current_block(estate, stmts, target_stmt, &new_index);

            elog(DEBUG1, "For goto reset execution from %d to id:%d", stmtid, new_index);

            /*
             * We didn't find target statement in current stmt-block level, exit the
             * current stmt block layer
             */
            if (stmt == NULL) {
                estate->block_level--;
                exec_eval_cleanup(estate);
                return PLPGSQL_RC_GOTO_UNRESOLVED;
            }

            /* Reset index in current block-level to continue execute */
            stmtid = new_index;
            continue;
        }

        int rc = exec_stmt(estate, stmt);
        stmtid++;

        if (rc == PLPGSQL_RC_GOTO_UNRESOLVED) {
            /*
             * If the under layer returns an unresolved GOTO(not found), we need
             * try to resolve it in current layer.
             */
            int new_index = -1;
            PLpgSQL_stmt* target_stmt = estate->goto_target_stmt;
            stmt = search_goto_target_current_block(estate, stmts, target_stmt, &new_index);

            if (stmt == NULL) {
                estate->block_level--;
                return PLPGSQL_RC_GOTO_UNRESOLVED;
            }

            stmtid = new_index;
            continue;
        } else if (rc != PLPGSQL_RC_OK) {
            estate->block_level--;
            return rc;
        }
    }

normal_exit:
    estate->block_level--;
    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt			Distribute one statement to the statements
 *				type specific execution function.
 * ----------
 */
static int exec_stmt(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt)
{
    PLpgSQL_stmt* save_estmt = NULL;
    int rc = -1;

    save_estmt = estate->err_stmt;
    estate->err_stmt = stmt;

    /* Let the plugin know that we are about to execute this statement */
    if (*u_sess->plsql_cxt.plugin_ptr && (*u_sess->plsql_cxt.plugin_ptr)->stmt_beg) {
        ((*u_sess->plsql_cxt.plugin_ptr)->stmt_beg)(estate, stmt);
    }

    CHECK_FOR_INTERRUPTS();

    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            rc = exec_stmt_block(estate, (PLpgSQL_stmt_block*)stmt);
            break;

        case PLPGSQL_STMT_ASSIGN:
            rc = exec_stmt_assign(estate, (PLpgSQL_stmt_assign*)stmt);
            break;

        case PLPGSQL_STMT_PERFORM:
            rc = exec_stmt_perform(estate, (PLpgSQL_stmt_perform*)stmt);
            break;

        case PLPGSQL_STMT_GETDIAG:
            rc = exec_stmt_getdiag(estate, (PLpgSQL_stmt_getdiag*)stmt);
            break;

        case PLPGSQL_STMT_IF:
            rc = exec_stmt_if(estate, (PLpgSQL_stmt_if*)stmt);
            break;

        case PLPGSQL_STMT_GOTO:
            rc = exec_stmt_goto(estate, (PLpgSQL_stmt_goto*)stmt);
            break;

        case PLPGSQL_STMT_CASE:
            rc = exec_stmt_case(estate, (PLpgSQL_stmt_case*)stmt);
            break;

        case PLPGSQL_STMT_LOOP:
            rc = exec_stmt_loop(estate, (PLpgSQL_stmt_loop*)stmt);
            break;

        case PLPGSQL_STMT_WHILE:
            rc = exec_stmt_while(estate, (PLpgSQL_stmt_while*)stmt);
            break;

        case PLPGSQL_STMT_FORI:
            rc = exec_stmt_fori(estate, (PLpgSQL_stmt_fori*)stmt);
            break;

        case PLPGSQL_STMT_FORS:
            rc = exec_stmt_fors(estate, (PLpgSQL_stmt_fors*)stmt);
            break;

        case PLPGSQL_STMT_FORC:
            rc = exec_stmt_forc(estate, (PLpgSQL_stmt_forc*)stmt);
            break;

        case PLPGSQL_STMT_FOREACH_A:
            rc = exec_stmt_foreach_a(estate, (PLpgSQL_stmt_foreach_a*)stmt);
            break;

        case PLPGSQL_STMT_EXIT:
            rc = exec_stmt_exit(estate, (PLpgSQL_stmt_exit*)stmt);
            break;

        case PLPGSQL_STMT_RETURN:
            rc = exec_stmt_return(estate, (PLpgSQL_stmt_return*)stmt);
            break;

        case PLPGSQL_STMT_RETURN_NEXT:
            rc = exec_stmt_return_next(estate, (PLpgSQL_stmt_return_next*)stmt);
            break;

        case PLPGSQL_STMT_RETURN_QUERY:
            rc = exec_stmt_return_query(estate, (PLpgSQL_stmt_return_query*)stmt);
            break;

        case PLPGSQL_STMT_RAISE:
            rc = exec_stmt_raise(estate, (PLpgSQL_stmt_raise*)stmt);
            break;

        case PLPGSQL_STMT_EXECSQL:
            rc = exec_stmt_execsql(estate, (PLpgSQL_stmt_execsql*)stmt);
            break;

        case PLPGSQL_STMT_DYNEXECUTE:
            rc = exec_stmt_dynexecute(estate, (PLpgSQL_stmt_dynexecute*)stmt);
            break;

        case PLPGSQL_STMT_DYNFORS:
            rc = exec_stmt_dynfors(estate, (PLpgSQL_stmt_dynfors*)stmt);
            break;

        case PLPGSQL_STMT_OPEN: {
            /* UniqueSQL: handle open cursor in stored procedure, 
             * need to generate separate unique SQL id for cursor stmt */
            INIT_UNIQUE_SQL_CXT();
            BACKUP_UNIQUE_SQL_CXT();
            PG_TRY();
            {
                rc = exec_stmt_open(estate, (PLpgSQL_stmt_open*)stmt);
            }
            PG_CATCH();
            {
                RESTORE_UNIQUE_SQL_CXT();
                PG_RE_THROW();
            }
            PG_END_TRY();
            RESTORE_UNIQUE_SQL_CXT();
            break;
        }

        case PLPGSQL_STMT_FETCH:
            rc = exec_stmt_fetch(estate, (PLpgSQL_stmt_fetch*)stmt);
            break;

        case PLPGSQL_STMT_CLOSE:
            rc = exec_stmt_close(estate, (PLpgSQL_stmt_close*)stmt);
            break;

        case PLPGSQL_STMT_COMMIT:
        case PLPGSQL_STMT_ROLLBACK:
#ifndef ENABLE_MULTIPLE_NODES
            if (estate->autonomous_session) {
                const char* query = ((enum PLpgSQL_stmt_types)stmt->cmd_type == PLPGSQL_STMT_COMMIT) ?
                                    "commit" : "rollback";
                estate->autonomous_session->ExecSimpleQuery(query);
                return PLPGSQL_RC_OK;
            }
#endif
            rc = exec_stmt_transaction(estate, stmt);
            break;

        case PLPGSQL_STMT_NULL:
            rc = exec_stmt_null(estate, (PLpgSQL_stmt*)stmt);
            break;

        default:
            estate->err_stmt = save_estmt;
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized statement type: %d for PLSQL function.", stmt->cmd_type)));
            break;
    }

    /* Let the plugin know that we have finished executing this statement */
    if (*u_sess->plsql_cxt.plugin_ptr && (*u_sess->plsql_cxt.plugin_ptr)->stmt_end) {
        ((*u_sess->plsql_cxt.plugin_ptr)->stmt_end)(estate, stmt);
    }

    // Check for interrupts sent by pl debugger and other plugins.
    //
    CHECK_FOR_INTERRUPTS();

    estate->err_stmt = save_estmt;

    return rc;
}

/* ----------
 * exec_stmt_assign			Evaluate an expression and
 *					put the result into a variable.
 * ----------
 */
static int exec_stmt_assign(PLpgSQL_execstate* estate, PLpgSQL_stmt_assign* stmt)
{
    if (unlikely(stmt->varno < 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_PLSQL), errmsg("It should be valid var number.")));
    }

    exec_assign_expr(estate, estate->datums[stmt->varno], stmt->expr);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_perform		Evaluate query and discard result (but set
 *							FOUND depending on whether at least one row
 *							was returned).
 * ----------
 */
static int exec_stmt_perform(PLpgSQL_execstate* estate, PLpgSQL_stmt_perform* stmt)
{
    TransactionId oldTransactionId = SPI_get_top_transaction_id();
    PLpgSQL_expr* expr = stmt->expr;
    int rc;

    rc = exec_run_select(estate, expr, 0, NULL);
    if (rc != SPI_OK_SELECT) {
        ereport(DEBUG1,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmodule(MOD_PLSQL), errmsg("exec_run_select returns %d", rc)));
    }

    /*
     * This is used for nested STP. If the transaction Id changed,
     * then need to create new econtext for the TopTransaction.
     */
    stp_check_transaction_and_create_econtext(estate,oldTransactionId);

    exec_set_found(estate, (estate->eval_processed != 0));
    exec_eval_cleanup(estate);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_getdiag					Put internal PG information into
 *										specified variables.
 * ----------
 */
static int exec_stmt_getdiag(PLpgSQL_execstate* estate, PLpgSQL_stmt_getdiag* stmt)
{
    ListCell* lc = NULL;

    /*
     * GET STACKED DIAGNOSTICS is only valid inside an exception handler.
     *
     * Note: we trust the grammar to have disallowed the relevant item kinds
     * if not is_stacked, otherwise we'd dump core below.
     */
    if (stmt->is_stacked && estate->cur_error == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
                errmodule(MOD_PLSQL),
                errmsg("GET STACKED DIAGNOSTICS cannot be used outside an exception handler")));
    }

    foreach (lc, stmt->diag_items) {
        PLpgSQL_diag_item* diag_item = (PLpgSQL_diag_item*)lfirst(lc);
        PLpgSQL_datum* var = estate->datums[diag_item->target];
        bool isnull = false;

        switch (diag_item->kind) {
            case PLPGSQL_GETDIAG_ROW_COUNT:
                exec_assign_value(estate, var, UInt32GetDatum(estate->eval_processed), INT4OID, &isnull);
                break;

            case PLPGSQL_GETDIAG_RESULT_OID:
                exec_assign_value(estate, var, ObjectIdGetDatum(estate->eval_lastoid), OIDOID, &isnull);
                break;

            case PLPGSQL_GETDIAG_ERROR_CONTEXT:
                exec_assign_c_string(estate, var, estate->cur_error->context);
                break;

            case PLPGSQL_GETDIAG_ERROR_DETAIL:
                exec_assign_c_string(estate, var, estate->cur_error->detail);
                break;

            case PLPGSQL_GETDIAG_ERROR_HINT:
                exec_assign_c_string(estate, var, estate->cur_error->hint);
                break;

            case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
                exec_assign_c_string(estate, var, unpack_sql_state(estate->cur_error->sqlerrcode));
                break;

            case PLPGSQL_GETDIAG_MESSAGE_TEXT:
                exec_assign_c_string(estate, var, estate->cur_error->message);
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized diagnostic item kind: %d", diag_item->kind)));
                break;
        }
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_if				Evaluate a bool expression and
 *					execute the true or false body
 *					conditionally.
 * ----------
 */
static int exec_stmt_if(PLpgSQL_execstate* estate, PLpgSQL_stmt_if* stmt)
{
    bool value = false;
    bool isnull = false;
    ListCell* lc = NULL;

    value = exec_eval_boolean(estate, stmt->cond, &isnull);
    exec_eval_cleanup(estate);
    if (!isnull && value) {
        return exec_stmts(estate, stmt->then_body);
    }

    foreach (lc, stmt->elsif_list) {
        PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(lc);

        value = exec_eval_boolean(estate, elif->cond, &isnull);
        exec_eval_cleanup(estate);
        if (!isnull && value) {
            return exec_stmts(estate, elif->stmts);
        }
    }

    return exec_stmts(estate, stmt->else_body);
}

/* -----------
 * exec_stmt_goto
 * -----------
 */
static int exec_stmt_goto(PLpgSQL_execstate* estate, PLpgSQL_stmt_goto* stmt)
{
    /*
     * For current GOTO implementation we rewind the execution tree from the upper
     * layer in exec_stmts(), so base on current context we can't reach here.
     */
    Assert(false);
    return PLPGSQL_RC_EXIT;
}

/* -----------
 * exec_stmt_case
 * -----------
 */
static int exec_stmt_case(PLpgSQL_execstate* estate, PLpgSQL_stmt_case* stmt)
{
    PLpgSQL_var* t_var = NULL;
    bool isnull = false;
    ListCell* l = NULL;

    if (stmt->t_expr != NULL) {
        /* simple case */
        Datum t_val;
        Oid t_oid;

        t_val = exec_eval_expr(estate, stmt->t_expr, &isnull, &t_oid);

        t_var = (PLpgSQL_var*)estate->datums[stmt->t_varno];

        /*
         * When expected datatype is different from real, change it. Note that
         * what we're modifying here is an execution copy of the datum, so
         * this doesn't affect the originally stored function parse tree.
         */
        if (t_var->datatype->typoid != t_oid) {
            t_var->datatype = plpgsql_build_datatype(t_oid, -1, estate->func->fn_input_collation);
        }

        /* now we can assign to the variable */
        exec_assign_value(estate, (PLpgSQL_datum*)t_var, t_val, t_oid, &isnull);

        exec_eval_cleanup(estate);
    }

    /* Now search for a successful WHEN clause */
    foreach (l, stmt->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(l);
        bool value = false;

        value = exec_eval_boolean(estate, cwt->expr, &isnull);
        exec_eval_cleanup(estate);
        if (!isnull && value) {
            /* Found it */

            /* We can now discard any value we had for the temp variable */
            if (t_var != NULL) {
                free_var(t_var);
                t_var->value = (Datum)0;
                t_var->isnull = true;
            }

            /* Evaluate the statement(s), and we're done */
            return exec_stmts(estate, cwt->stmts);
        }
    }

    /* We can now discard any value we had for the temp variable */
    if (t_var != NULL) {
        free_var(t_var);
        t_var->value = (Datum)0;
        t_var->isnull = true;
    }

    /* SQL2003 mandates this error if there was no ELSE clause */
    if (!stmt->have_else) {
        ereport(ERROR,
            (errcode(ERRCODE_CASE_NOT_FOUND),
                errmodule(MOD_PLSQL),
                errmsg("case not found"),
                errhint("CASE statement is missing ELSE part.")));
    }

    /* Evaluate the ELSE statements, and we're done */
    return exec_stmts(estate, stmt->else_stmts);
}

/* ----------
 * exec_stmt_loop			Loop over statements until
 *					an exit occurs.
 * ----------
 */
static int exec_stmt_loop(PLpgSQL_execstate* estate, PLpgSQL_stmt_loop* stmt)
{
    for (;;) {
        int rc = exec_stmts(estate, stmt->body);

        switch (rc) {
            case PLPGSQL_RC_OK:
                break;

            case PLPGSQL_RC_EXIT:
                if (estate->exitlabel == NULL) {
                    return PLPGSQL_RC_OK;
                }
                if (stmt->label == NULL) {
                    return PLPGSQL_RC_EXIT;
                }
                if (strcmp(stmt->label, estate->exitlabel) != 0) {
                    return PLPGSQL_RC_EXIT;
                }
                estate->exitlabel = NULL;
                return PLPGSQL_RC_OK;

            case PLPGSQL_RC_CONTINUE:
                if (estate->exitlabel == NULL) {
                    /* anonymous continue, so re-run the loop */
                    break;
                } else if (stmt->label != NULL && 
                		   strcmp(stmt->label, estate->exitlabel) == 0) {
                    /* label matches named continue, so re-run loop */
                    estate->exitlabel = NULL;
                } else {
                    /* label doesn't match named continue, so propagate upward */
                    return PLPGSQL_RC_CONTINUE;
                }
                break;

            case PLPGSQL_RC_RETURN:
            /*
             * GOTO target is not in loop_body just return "PLPGSQL_RC_GOTO_UNRESOLVED",
             * to let the caller to goto-resolve
             */
            case PLPGSQL_RC_GOTO_UNRESOLVED:
                return rc;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized return code: %d in PLSQL LOOP statement.", rc)));
                break;
        }
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_while			Loop over statements as long
 *					as an expression evaluates to
 *					true or an exit occurs.
 * ----------
 */
static int exec_stmt_while(PLpgSQL_execstate* estate, PLpgSQL_stmt_while* stmt)
{
    for (;;) {
        int rc;
        bool value = false;
        bool isnull = false;

        value = exec_eval_boolean(estate, stmt->cond, &isnull);
        exec_eval_cleanup(estate);

        if (isnull || !value) {
            break;
        }

        rc = exec_stmts(estate, stmt->body);

        switch (rc) {
            case PLPGSQL_RC_OK:
                break;

            case PLPGSQL_RC_EXIT:
                if (estate->exitlabel == NULL) {
                    return PLPGSQL_RC_OK;
                }
                if (stmt->label == NULL) {
                    return PLPGSQL_RC_EXIT;
                }
                if (strcmp(stmt->label, estate->exitlabel) != 0) {
                    return PLPGSQL_RC_EXIT;
                }
                estate->exitlabel = NULL;
                return PLPGSQL_RC_OK;

            case PLPGSQL_RC_CONTINUE:
                if (estate->exitlabel == NULL) {
                    /* anonymous continue, so re-run loop */
                    break;
                } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                    /* label matches named continue, so re-run loop */
                    estate->exitlabel = NULL;
                } else {
                    /* label doesn't match named continue, propagate upward */
                    return PLPGSQL_RC_CONTINUE;
                }
                break;

            case PLPGSQL_RC_RETURN:
            /*
             * GOTO target is not in loop_body just return "PLPGSQL_RC_GOTO_UNRESOLVED",
             * to let the caller to goto-resolve
             */
            case PLPGSQL_RC_GOTO_UNRESOLVED:
                return rc;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized return code: %d in PLSQL WHILE statement.", rc)));
                break;
        }
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_fori			Iterate an integer variable
 *					from a lower to an upper value
 *					incrementing or decrementing by the BY value
 * ----------
 */
static int exec_stmt_fori(PLpgSQL_execstate* estate, PLpgSQL_stmt_fori* stmt)
{
    PLpgSQL_var* var = NULL;
    Datum value;
    bool isnull = false;
    Oid valtype;
    int32 loop_value;
    int32 end_value;
    int32 step_value;
    bool found = false;
    int rc = PLPGSQL_RC_OK;

    var = (PLpgSQL_var*)(estate->datums[stmt->var->dno]);

    /*
     * Get the value of the lower bound
     */
    value = exec_eval_expr(estate, stmt->lower, &isnull, &valtype);
    value = exec_cast_value(estate,
        value,
        valtype,
        var->datatype->typoid,
        &(var->datatype->typinput),
        var->datatype->typioparam,
        var->datatype->atttypmod,
        isnull);
    if (isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("lower bound of FOR loop cannot be null")));
    }
    loop_value = DatumGetInt32(value);
    exec_eval_cleanup(estate);

    /*
     * Get the value of the upper bound
     */
    value = exec_eval_expr(estate, stmt->upper, &isnull, &valtype);
    value = exec_cast_value(estate,
        value,
        valtype,
        var->datatype->typoid,
        &(var->datatype->typinput),
        var->datatype->typioparam,
        var->datatype->atttypmod,
        isnull);
    if (isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("upper bound of FOR loop cannot be null")));
    }
    end_value = DatumGetInt32(value);
    exec_eval_cleanup(estate);

    /*
     * Get the step value
     */
    if (stmt->step != NULL) {
        value = exec_eval_expr(estate, stmt->step, &isnull, &valtype);
        value = exec_cast_value(estate,
            value,
            valtype,
            var->datatype->typoid,
            &(var->datatype->typinput),
            var->datatype->typioparam,
            var->datatype->atttypmod,
            isnull);
        if (isnull) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("BY value of FOR loop cannot be null")));
        }
        step_value = DatumGetInt32(value);
        exec_eval_cleanup(estate);
        if (step_value <= 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmodule(MOD_PLSQL),
                    errmsg("BY value of FOR loop must be greater than zero")));
        }
    } else {
        step_value = 1;
    }

    /*
     * Now do the loop
     */
    for (;;) {
        /*
         * Check against upper bound
         */
        if (stmt->reverse) {
            if (loop_value < end_value) {
                break;
            }
        } else {
            if (loop_value > end_value) {
                break;
            }
        }

        found = true; /* looped at least once */

        /*
         * Assign current value to loop var
         */
        var->value = Int32GetDatum(loop_value);
        var->isnull = false;

        /*
         * Execute the statements
         */
        rc = exec_stmts(estate, stmt->body);

        if (rc == PLPGSQL_RC_RETURN) {
            break; /* break out of the loop */
        } else if (rc == PLPGSQL_RC_EXIT) {
            if (estate->exitlabel == NULL) {
                /* unlabelled exit, finish the current loop */
                rc = PLPGSQL_RC_OK;
            } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                /* labelled exit, matches the current stmt's label */
                estate->exitlabel = NULL;
                rc = PLPGSQL_RC_OK;
            }

            /*
             * otherwise, this is a labelled exit that does not match the
             * current statement's label, if any: return RC_EXIT so that the
             * EXIT continues to propagate up the stack.
             */
            break;
        } else if (rc == PLPGSQL_RC_CONTINUE) {
            if (estate->exitlabel == NULL) {
                /* unlabelled continue, so re-run the current loop */
                rc = PLPGSQL_RC_OK;
            } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                /* label matches named continue, so re-run loop */
                estate->exitlabel = NULL;
                rc = PLPGSQL_RC_OK;
            } else {
                /*
                 * otherwise, this is a named continue that does not match the
                 * current statement's label, if any: return RC_CONTINUE so
                 * that the CONTINUE will propagate up the stack.
                 */
                break;
            }
        } else if (rc == PLPGSQL_RC_GOTO_UNRESOLVED) {
            break;
        }

        /*
         * Increase/decrease loop value, unless it would overflow, in which
         * case exit the loop.
         */
        if (stmt->reverse) {
            if ((int32)(loop_value - step_value) > loop_value) {
                break;
            }
            loop_value -= step_value;
        } else {
            if ((int32)(loop_value + step_value) < loop_value) {
                break;
            }
            loop_value += step_value;
        }
    }

    /*
     * Set the FOUND variable to indicate the result of executing the loop
     * (namely, whether we looped one or more times). This must be set here so
     * that it does not interfere with the value of the FOUND variable inside
     * the loop processing itself.
     */
    exec_set_found(estate, found);

    return rc;
}

/* ----------
 * exec_stmt_fors			Execute a query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int exec_stmt_fors(PLpgSQL_execstate* estate, PLpgSQL_stmt_fors* stmt)
{
    Portal portal = NULL;
    int rc;

    /*
     * Open the implicit cursor for the statement using exec_run_select
     */
    rc = exec_run_select(estate, stmt->query, 0, &portal);
    if (rc != SPI_OK_SELECT) {
        ereport(DEBUG1,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmodule(MOD_PLSQL), errmsg("exec_run_select returns %d", rc)));
    }

    /*
     * Execute the loop
     */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq*)stmt, portal, true, -1);

    /*
     * Close the implicit cursor
     */
    SPI_cursor_close(portal);

    return rc;
}

/* ----------
 * exec_stmt_forc			Execute a loop for each row from a cursor.
 * ----------
 */
static int exec_stmt_forc(PLpgSQL_execstate* estate, PLpgSQL_stmt_forc* stmt)
{
    PLpgSQL_var* curvar = NULL;
    char* curname = NULL;
    PLpgSQL_expr* query = NULL;
    ParamListInfo paramLI;
    Portal portal;
    int rc;
    /* Declare variables for save display cursor status */
    PLpgSQL_var isopen;
    PLpgSQL_var cursor_found;
    PLpgSQL_var notfound;
    PLpgSQL_var rowcount;
    /* Declare variables for save Implicit cursor status */
    PLpgSQL_var sql_isopen;
    PLpgSQL_var sql_cursor_found;
    PLpgSQL_var sql_notfound;
    PLpgSQL_var sql_rowcount;

    /* ----------
     * Get the cursor variable and if it has an assigned name, check
     * that it's not in use currently.
     * ----------
     */
    curvar = (PLpgSQL_var*)(estate->datums[stmt->curvar]);
    if (!curvar->isnull) {
        curname = TextDatumGetCString(curvar->value);
        if (SPI_cursor_find(curname) != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_CURSOR),
                    errmodule(MOD_PLSQL),
                    errmsg("cursor \"%s\" already in use in loop for row.", curname)));
        }
    }

    /* ----------
     * Open the cursor just like an OPEN command
     *
     * Note: parser should already have checked that statement supplies
     * args iff cursor needs them, but we check again to be safe.
     * ----------
     */
    if (stmt->argquery != NULL) {
        /* ----------
         * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
         * statement to evaluate the args and put 'em into the
         * internal row.
         * ----------
         */
        PLpgSQL_stmt_execsql set_args;
        errno_t rc = EOK;

        if (curvar->cursor_explicit_argrow < 0) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("arguments given for cursor without arguments in loop for row.")));
        }

        rc = memset_s(&set_args, sizeof(set_args), 0, sizeof(set_args));
        securec_check(rc, "\0", "\0");
        set_args.cmd_type = PLPGSQL_STMT_EXECSQL;
        set_args.lineno = stmt->lineno;
        set_args.sqlstmt = stmt->argquery;
        set_args.into = true;
        /* XXX historically this has not been STRICT */
        set_args.row = (PLpgSQL_row*)(estate->datums[curvar->cursor_explicit_argrow]);

        if (exec_stmt_execsql(estate, &set_args) != PLPGSQL_RC_OK) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("open cursor failed during argument processing in loop for row.")));
	    }
    } else {
        if (curvar->cursor_explicit_argrow >= 0) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("arguments required for cursor in loop for row.")));
        }
    }

    query = curvar->cursor_explicit_expr;
    AssertEreport(query, MOD_PLSQL, "query should not be null.");
    if (query->plan == NULL) {
        exec_prepare_plan(estate, query, curvar->cursor_options);
    }
    if (ENABLE_CN_GPC && g_instance.plan_cache->CheckRecreateSPICachePlan(query->plan)) {
        g_instance.plan_cache->RecreateSPICachePlan(query->plan);
    }

    /*
     * Set up ParamListInfo (hook function and possibly data values)
     */
    paramLI = setup_param_list(estate, query);

    /*
     * Open the cursor (the paramlist will get copied into the portal)
     */
    portal = SPI_cursor_open_with_paramlist(curname, query->plan, paramLI, estate->readonly_func);
    if (portal == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_CURSOR),
                errmodule(MOD_PLSQL),
                errmsg("could not open cursor: %s", SPI_result_code_string(SPI_result))));
    }

    /* don't need paramlist any more */
    if (paramLI) {
        pfree_ext(paramLI);
    }

    /*
     * If cursor variable was NULL, store the generated portal name in it
     */
    if (curname == NULL) {
        assign_text_var(curvar, portal->name);
    }

    /* save the status of display cursor before Modify attributes of cursor */
    isopen.value = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ISOPEN]))->value;
    isopen.isnull = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ISOPEN]))->isnull;
    cursor_found.value = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_FOUND]))->value;
    cursor_found.isnull = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_FOUND]))->isnull;
    notfound.value = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_NOTFOUND]))->value;
    notfound.isnull = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_NOTFOUND]))->isnull;
    rowcount.value = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ROWCOUNT]))->value;
    rowcount.isnull = ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ROWCOUNT]))->isnull;

    /* save the status of Implicit cursor before Modify attributes of cursor */
    sql_isopen.value = ((PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]))->value;
    sql_isopen.isnull = ((PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]))->isnull;
    sql_cursor_found.value = ((PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]))->value;
    sql_cursor_found.isnull = ((PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]))->isnull;
    sql_notfound.value = ((PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]))->value;
    sql_notfound.isnull = ((PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]))->isnull;
    sql_rowcount.value = ((PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]))->value;
    sql_rowcount.isnull = ((PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]))->isnull;

    /* init all cursors */
    exec_set_isopen(estate, false, curvar->dno + CURSOR_ISOPEN);
    exec_set_cursor_found(estate, PLPGSQL_FALSE, curvar->dno + CURSOR_FOUND);
    exec_set_notfound(estate, PLPGSQL_TRUE, curvar->dno + CURSOR_NOTFOUND);
    exec_set_rowcount(estate, 0, true, curvar->dno + CURSOR_ROWCOUNT);
    exec_set_sql_isopen(estate, false);
    exec_set_sql_cursor_found(estate, PLPGSQL_FALSE);
    exec_set_sql_notfound(estate, PLPGSQL_TRUE);
    exec_set_sql_rowcount(estate, 0);

    /*
     * Execute the loop.  We can't prefetch because the cursor is accessible
     * to the user, for instance via UPDATE WHERE CURRENT OF within the loop.
     */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq*)stmt, portal, false, curvar->dno);

    /* restore the status of display cursor after Modify attributes of cursor */
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ISOPEN]))->value = isopen.value;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ISOPEN]))->isnull = isopen.isnull;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_FOUND]))->value = cursor_found.value;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_FOUND]))->isnull = cursor_found.isnull;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_NOTFOUND]))->value = notfound.value;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_NOTFOUND]))->isnull = notfound.isnull;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ROWCOUNT]))->value = rowcount.value;
    ((PLpgSQL_var*)(estate->datums[curvar->dno + CURSOR_ROWCOUNT]))->isnull = rowcount.isnull;

    /* restore the status of Implicit cursor after Modify attributes of cursor */
    ((PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]))->value = sql_isopen.value;
    ((PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]))->isnull = sql_isopen.isnull;
    ((PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]))->value = sql_cursor_found.value;
    ((PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]))->isnull = sql_cursor_found.isnull;
    ((PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]))->value = sql_notfound.value;
    ((PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]))->isnull = sql_notfound.isnull;
    ((PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]))->value = sql_rowcount.value;
    ((PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]))->isnull = sql_rowcount.isnull;

    /* ----------
     * Close portal, and restore cursor variable if it was initially NULL.
     * ----------
     */
    SPI_cursor_close(portal);

    if (curname == NULL) {
        free_var(curvar);
        curvar->value = (Datum)0;
        curvar->isnull = true;
    }

    if (curname != NULL) {
        pfree_ext(curname);
    }

    return rc;
}

/* ----------
 * exec_stmt_foreach_a			Loop over elements or slices of an array
 *
 * When looping over elements, the loop variable is the same type that the
 * array stores (eg: integer), when looping through slices, the loop variable
 * is an array of size and dimensions to match the size of the slice.
 * ----------
 */
static int exec_stmt_foreach_a(PLpgSQL_execstate* estate, PLpgSQL_stmt_foreach_a* stmt)
{
    ArrayType* arr = NULL;
    Oid arrtype;
    PLpgSQL_datum* loop_var = NULL;
    Oid loop_var_elem_type;
    bool found = false;
    int rc = PLPGSQL_RC_OK;
    ArrayIterator array_iterator;
    Oid iterator_result_type;
    Datum value;
    bool isnull = false;

    /* get the value of the array expression */
    value = exec_eval_expr(estate, stmt->expr, &isnull, &arrtype);
    if (isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("FOREACH expression must not be null")));
    }

    /* check the type of the expression - must be an array */
    if (!OidIsValid(get_element_type(arrtype))) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("FOREACH expression must yield an array, not type %s", format_type_be(arrtype))));
    }

    /*
     * We must copy the array, else it will disappear in exec_eval_cleanup.
     * This is annoying, but cleanup will certainly happen while running the
     * loop body, so we have little choice.
     */
    arr = DatumGetArrayTypePCopy(value);

    /* Clean up any leftover temporary memory */
    exec_eval_cleanup(estate);

    /* Slice dimension must be less than or equal to array dimension */
    if (stmt->slice < 0 || stmt->slice > ARR_NDIM(arr)) {
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("slice dimension (%d) is out of the valid range 0..%d", stmt->slice, ARR_NDIM(arr))));
    }

    /* Set up the loop variable and see if it is of an array type */
    loop_var = estate->datums[stmt->varno];
    if (loop_var->dtype == PLPGSQL_DTYPE_REC || loop_var->dtype == PLPGSQL_DTYPE_ROW) {
        /*
         * Record/row variable is certainly not of array type, and might not
         * be initialized at all yet, so don't try to get its type
         */
        loop_var_elem_type = InvalidOid;
    } else {
        loop_var_elem_type = get_element_type(exec_get_datum_type(estate, loop_var));
    }

    /*
     * Sanity-check the loop variable type.  We don't try very hard here, and
     * should not be too picky since it's possible that exec_assign_value can
     * coerce values of different types.  But it seems worthwhile to complain
     * if the array-ness of the loop variable is not right.
     */
    if (stmt->slice > 0 && loop_var_elem_type == InvalidOid) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("FOREACH ... SLICE loop variable must be of an array type")));
    }
    if (stmt->slice == 0 && loop_var_elem_type != InvalidOid) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("FOREACH loop variable must not be of an array type")));
    }

    /* Create an iterator to step through the array */
    array_iterator = array_create_iterator(arr, stmt->slice);

    /* Identify iterator result type */
    if (stmt->slice > 0) {
        /* When slicing, nominal type of result is same as array type */
        iterator_result_type = arrtype;
    } else {
        /* Without slicing, results are individual array elements */
        iterator_result_type = ARR_ELEMTYPE(arr);
    }

    /* Iterate over the array elements or slices */
    while (array_iterate(array_iterator, &value, &isnull)) {
        found = true; /* looped at least once */

        /* Assign current element/slice to the loop variable */
        exec_assign_value(estate, loop_var, value, iterator_result_type, &isnull);

        /* In slice case, value is temporary; must free it to avoid leakage */
        if (stmt->slice > 0) {
            pfree(DatumGetPointer(value));
        }

        /*
         * Execute the statements
         */
        rc = exec_stmts(estate, stmt->body);

        /* Handle the return code */
        if (rc == PLPGSQL_RC_RETURN) {
            break; /* break out of the loop */
        } else if (rc == PLPGSQL_RC_EXIT) {
            if (estate->exitlabel == NULL) {
                /* unlabelled exit, finish the current loop */
                rc = PLPGSQL_RC_OK;
            } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                /* labelled exit, matches the current stmt's label */
                estate->exitlabel = NULL;
                rc = PLPGSQL_RC_OK;
            }

            /*
             * otherwise, this is a labelled exit that does not match the
             * current statement's label, if any: return RC_EXIT so that the
             * EXIT continues to propagate up the stack.
             */
            break;
        } else if (rc == PLPGSQL_RC_CONTINUE) {
            if (estate->exitlabel == NULL) {
                /* unlabelled continue, so re-run the current loop */
                rc = PLPGSQL_RC_OK;
            } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                /* label matches named continue, so re-run loop */
                estate->exitlabel = NULL;
                rc = PLPGSQL_RC_OK;
            } else {
                /*
                 * otherwise, this is a named continue that does not match the
                 * current statement's label, if any: return RC_CONTINUE so
                 * that the CONTINUE will propagate up the stack.
                 */
                break;
            }
        } else if (rc == PLPGSQL_RC_GOTO_UNRESOLVED) {
            break;
        }
    }

    /* Release temporary memory, including the array value */
    array_free_iterator(array_iterator);
    pfree_ext(arr);

    /*
     * Set the FOUND variable to indicate the result of executing the loop
     * (namely, whether we looped one or more times). This must be set here so
     * that it does not interfere with the value of the FOUND variable inside
     * the loop processing itself.
     */
    exec_set_found(estate, found);

    return rc;
}

/* ----------
 * exec_stmt_exit			Implements EXIT and CONTINUE
 *
 * This begins the process of exiting / restarting a loop.
 * ----------
 */
static int exec_stmt_exit(PLpgSQL_execstate* estate, PLpgSQL_stmt_exit* stmt)
{
    /*
     * If the exit / continue has a condition, evaluate it
     */
    if (stmt->cond != NULL) {
        bool value = false;
        bool isnull = false;

        value = exec_eval_boolean(estate, stmt->cond, &isnull);
        exec_eval_cleanup(estate);
        if (isnull || value == false) {
            return PLPGSQL_RC_OK;
        }
    }

    estate->exitlabel = stmt->label;
    if (stmt->is_exit) {
        return PLPGSQL_RC_EXIT;
    } else {
        return PLPGSQL_RC_CONTINUE;
    }
}

/* ----------
 * exec_stmt_return			Evaluate an expression and start
 *					returning from the function.
 * ----------
 */
static int exec_stmt_return(PLpgSQL_execstate* estate, PLpgSQL_stmt_return* stmt)
{
    /*
     * If processing a set-returning PL/pgSQL function, the final RETURN
     * indicates that the function is finished producing tuples.  The rest of
     * the work will be done at the top level.
     */
    if (estate->retisset) {
        return PLPGSQL_RC_RETURN;
    }

    /* initialize for null result (possibly a tuple) */
    estate->retval = (Datum)0;
    estate->rettupdesc = NULL;
    estate->retisnull = true;

    if (stmt->retvarno >= 0) {
        PLpgSQL_datum* retvar = estate->datums[stmt->retvarno];

        switch (retvar->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)retvar;

                estate->retval = var->value;
                estate->retisnull = var->isnull;
                estate->rettype = var->datatype->typoid;
                if (estate->rettype == REFCURSOROID) {
                    ExecCopyDataFromDatum(estate->datums, var->dno, estate->cursor_return_data);
                }
            } break;

            case PLPGSQL_DTYPE_REC: {
                PLpgSQL_rec* rec = (PLpgSQL_rec*)retvar;

                if (HeapTupleIsValid(rec->tup)) {
                    estate->retval = PointerGetDatum(rec->tup);
                    estate->rettupdesc = rec->tupdesc;
                    estate->retisnull = false;
                }
            } break;

            case PLPGSQL_DTYPE_ROW: {
                PLpgSQL_row* row = (PLpgSQL_row*)retvar;

                AssertEreport(row->rowtupdesc != NULL, MOD_PLSQL, "row's tuple description is required.");
                estate->retval = PointerGetDatum(make_tuple_from_row(estate, row, row->rowtupdesc));
                if (DatumGetPointer(estate->retval) == NULL) { /* should not happen */
                    ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmodule(MOD_PLSQL),
                            errmsg("row not compatible with its own tupdesc in RETURN statement.")));
                }
                estate->rettupdesc = row->rowtupdesc;
                estate->retisnull = false;

                if (estate->cursor_return_data != NULL) {
                    for (int i = 0,j = 0; i < row->rowtupdesc->natts; i++) {
                        if (row->rowtupdesc->attrs[i]->atttypid == REFCURSOROID) {
                            int dno = row->varnos[i];
                            ExecCopyDataFromDatum(estate->datums, dno, &estate->cursor_return_data[j]);
                            j = j + 1;
                        }
                    }
                }
            } break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized return data type: %d in RETURN statement.", retvar->dtype)));
                break;
        }

        return PLPGSQL_RC_RETURN;
    }

    if (stmt->expr != NULL) {
        if (estate->retistuple) {
            exec_run_select(estate, stmt->expr, 1, NULL);
            if (estate->eval_processed > 0) {
                estate->retval = PointerGetDatum(estate->eval_tuptable->vals[0]);
                estate->rettupdesc = estate->eval_tuptable->tupdesc;
                estate->retisnull = false;
            }
        } else {
            /* Normal case for scalar results */
            estate->retval = exec_eval_expr(estate, stmt->expr, &(estate->retisnull), &(estate->rettype));
            if (estate->rettype == REFCURSOROID) {
                CopyCursorInfoData(estate->cursor_return_data, &estate->eval_econtext->cursor_data);
            }
        }

        return PLPGSQL_RC_RETURN;
    }

    /*
     * Special hack for function returning VOID: instead of NULL, return a
     * non-null VOID value.  This is of dubious importance but is kept for
     * backwards compatibility.  Note that the only other way to get here is
     * to have written "RETURN NULL" in a function returning tuple.
     */
    if (estate->fn_rettype == VOIDOID) {
        estate->retval = (Datum)0;
        estate->retisnull = false;
        estate->rettype = VOIDOID;
    }

    return PLPGSQL_RC_RETURN;
}

/* ----------
 * exec_stmt_return_next		Evaluate an expression and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int exec_stmt_return_next(PLpgSQL_execstate* estate, PLpgSQL_stmt_return_next* stmt)
{
    TupleDesc tupdesc;
    int natts;
    HeapTuple tuple = NULL;
    bool free_tuple = false;

    if (!estate->retisset) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("cannot use RETURN NEXT in a non-SETOF function")));
    }

    if (estate->tuple_store == NULL) {
        exec_init_tuple_store(estate);
    }

    /* rettupdesc will be filled by exec_init_tuple_store */
    tupdesc = estate->rettupdesc;
    natts = tupdesc->natts;

    if (stmt->retvarno >= 0) {
        PLpgSQL_datum* retvar = estate->datums[stmt->retvarno];

        switch (retvar->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)retvar;
                Datum retval = var->value;
                bool isNull = var->isnull;

                if (natts != 1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("wrong result type supplied in RETURN NEXT")));
                }

                /* coerce type if needed */
                retval = exec_simple_cast_value(estate,
                    retval,
                    var->datatype->typoid,
                    tupdesc->attrs[0]->atttypid,
                    tupdesc->attrs[0]->atttypmod,
                    isNull);

                tuplestore_putvalues(estate->tuple_store, tupdesc, &retval, &isNull);
            } break;

            case PLPGSQL_DTYPE_REC: {
                PLpgSQL_rec* rec = (PLpgSQL_rec*)retvar;
                TupleConversionMap* tupmap = NULL;

                if (!HeapTupleIsValid(rec->tup)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmodule(MOD_PLSQL),
                            errmsg("record \"%s\" is not assigned yet", rec->refname),
                            errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
                }

                tupmap = convert_tuples_by_position(
                    rec->tupdesc, tupdesc, gettext_noop("wrong record type supplied in RETURN NEXT"));
                tuple = rec->tup;
                /* it might need conversion */
                if (tupmap != NULL) {
                    tuple = do_convert_tuple(tuple, tupmap);
                    free_conversion_map(tupmap);
                    free_tuple = true;
                }
            } break;

            case PLPGSQL_DTYPE_ROW: {
                PLpgSQL_row* row = (PLpgSQL_row*)retvar;

                tuple = make_tuple_from_row(estate, row, tupdesc);
                if (tuple == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("wrong record type supplied in RETURN NEXT statement.")));
                }
                free_tuple = true;
            } break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized data type: %d in RETURN NEXT statement.", retvar->dtype)));
                break;
        }
    } else if (stmt->expr != NULL) {
        Datum retval;
        bool isNull = false;
        Oid rettype;

        if (natts != 1) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_PLSQL),
                    errmsg("wrong result number %d (expect one) supplied in RETURN NEXT statment.", natts)));
        }

        retval = exec_eval_expr(estate, stmt->expr, &isNull, &rettype);

        /* coerce type if needed */
        retval = exec_simple_cast_value(
            estate, retval, rettype, tupdesc->attrs[0]->atttypid, tupdesc->attrs[0]->atttypmod, isNull);

        tuplestore_putvalues(estate->tuple_store, tupdesc, &retval, &isNull);
    } else {
        ereport(
            ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmodule(MOD_PLSQL), errmsg("RETURN NEXT must have a parameter")));
    }

    if (HeapTupleIsValid(tuple)) {
        tuplestore_puttuple(estate->tuple_store, tuple);

        if (free_tuple) {
            heap_freetuple_ext(tuple);
        }
    }

    exec_eval_cleanup(estate);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_return_query		Evaluate a query and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int exec_stmt_return_query(PLpgSQL_execstate* estate, PLpgSQL_stmt_return_query* stmt)
{
    Portal portal = NULL;
    uint32 processed = 0;
    TupleConversionMap* tupmap = NULL;

    if (!estate->retisset) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("cannot use RETURN QUERY in a non-SETOF function")));
    }

    if (estate->tuple_store == NULL) {
        exec_init_tuple_store(estate);
    }

    if (stmt->query != NULL) {
        /* static query */
        exec_run_select(estate, stmt->query, 0, &portal);
    } else {
        /* RETURN QUERY EXECUTE */
        AssertEreport(stmt->dynquery != NULL, MOD_PLSQL, "stmt's dynamic query is required.");
        portal = exec_dynquery_with_params(estate, stmt->dynquery, stmt->params, NULL, 0);
    }

    tupmap = convert_tuples_by_position(
        portal->tupDesc, estate->rettupdesc, gettext_noop("structure of query does not match function result type"));

    for (;;) {
        int i;

        SPI_cursor_fetch(portal, true, 50);
        if (SPI_processed == 0) {
            break;
        }

        for (i = 0; (unsigned int)(i) < SPI_processed; i++) {
            HeapTuple tuple = SPI_tuptable->vals[i];

            if (tupmap != NULL) {
                tuple = do_convert_tuple(tuple, tupmap);
            }
            tuplestore_puttuple(estate->tuple_store, tuple);
            if (tupmap != NULL) {
                heap_freetuple_ext(tuple);
            }
            processed++;
        }

        SPI_freetuptable(SPI_tuptable);
    }

    if (tupmap != NULL) {
        free_conversion_map(tupmap);
    }

    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);

    estate->eval_processed = processed;
    exec_set_found(estate, processed != 0);

    return PLPGSQL_RC_OK;
}

static void exec_init_tuple_store(PLpgSQL_execstate* estate)
{
    ReturnSetInfo* rsi = estate->rsi;
    MemoryContext oldcxt;
    ResourceOwner oldowner;

    /*
     * Check caller can handle a set result in the way we want
     */
    if ((rsi == NULL) || !IsA(rsi, ReturnSetInfo) || (rsi->allowedModes & SFRM_Materialize) == 0 ||
        (rsi->expectedDesc == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_PLSQL),
                errmsg("set-valued function called in context that cannot accept a set when init tuplestore for RETURN "
                       "NEXT/RETURN QUERY.")));
	}

    /*
     * Switch to the right memory context and resource owner for storing the
     * tuplestore for return set. If we're within a subtransaction opened for
     * an exception-block, for example, we must still create the tuplestore in
     * the resource owner that was active when this function was entered, and
     * not in the subtransaction resource owner.
     */
    oldcxt = MemoryContextSwitchTo(estate->tuple_store_cxt);
    oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = estate->tuple_store_owner;

    estate->tuple_store =
        tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);

    t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
    MemoryContextSwitchTo(oldcxt);

    estate->rettupdesc = rsi->expectedDesc;
}

/* ----------
 * exec_stmt_raise			Build a message and throw it with elog()
 * ----------
 */
static int exec_stmt_raise(PLpgSQL_execstate* estate, PLpgSQL_stmt_raise* stmt)
{
    int err_code = 0;
    char* condname = NULL;
    char* err_message = NULL;
    char* err_detail = NULL;
    char* err_hint = NULL;
    ListCell* lc = NULL;

    /* RAISE with no parameters: re-throw current exception */
    if (stmt->condname == NULL && stmt->message == NULL && stmt->options == NIL) {
        if (estate->cur_error != NULL) {
            ReThrowError(estate->cur_error);
        }
        /* oops, we're not inside a handler */
        ereport(ERROR,
            (errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
                errmodule(MOD_PLSQL),
                errmsg("RAISE without parameters cannot be used outside an exception handler")));
    }

    if (stmt->condname != NULL) {
        err_code = plpgsql_recognize_err_condition(stmt->condname, true);
        condname = pstrdup(stmt->condname);
    }

    if (stmt->message != NULL) {
        StringInfoData ds;
        ListCell* current_param = NULL;
        char* cp = NULL;

        initStringInfo(&ds);
        current_param = list_head(stmt->params);

        for (cp = stmt->message; *cp; cp++) {
            /*
             * Occurrences of a single % are replaced by the next parameter's
             * external representation. Double %'s are converted to one %.
             */
            if (cp[0] == '%') {
                Oid paramtypeid;
                Datum paramvalue;
                bool paramisnull = false;
                char* extval = NULL;

                if (cp[1] == '%') {
                    appendStringInfoChar(&ds, '%');
                    cp++;
                    continue;
                }

                if (current_param == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("too few parameters specified for RAISE")));
                }

                paramvalue = exec_eval_expr(estate, (PLpgSQL_expr*)lfirst(current_param), &paramisnull, &paramtypeid);

                if (paramisnull) {
                    extval = "<NULL>";
                } else {
                    extval = convert_value_to_string(estate, paramvalue, paramtypeid);
                }
                appendStringInfoString(&ds, extval);
                current_param = lnext(current_param);
                exec_eval_cleanup(estate);
            } else {
                appendStringInfoChar(&ds, cp[0]);
            }
        }

        /*
         * If more parameters were specified than were required to process the
         * format string, throw an error
         */
        if (current_param != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("too many parameters specified for RAISE")));
        }

        err_message = ds.data;
        /* No pfree_ext(ds.data), the pfree_ext(err_message) does it */
    }

    foreach (lc, stmt->options) {
        PLpgSQL_raise_option* opt = (PLpgSQL_raise_option*)lfirst(lc);
        Datum optionvalue;
        bool optionisnull = false;
        Oid optiontypeid;
        char* extval = NULL;

        optionvalue = exec_eval_expr(estate, opt->expr, &optionisnull, &optiontypeid);
        if (optionisnull) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("RAISE statement option cannot be null")));
        }

        extval = convert_value_to_string(estate, optionvalue, optiontypeid);

        switch (opt->opt_type) {
            case PLPGSQL_RAISEOPTION_ERRCODE:
                if (err_code) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("RAISE option already specified: %s", "ERRCODE")));
                }
                err_code = plpgsql_recognize_err_condition(extval, true);

                if (condname != NULL) {
                    pfree_ext(condname);
                }

                condname = pstrdup(extval);
                break;
            case PLPGSQL_RAISEOPTION_MESSAGE:
                if (err_message != NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("RAISE option already specified: %s", "MESSAGE")));
                }
                err_message = pstrdup(extval);
                break;
            case PLPGSQL_RAISEOPTION_DETAIL:
                if (err_detail != NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("RAISE option already specified: %s", "DETAIL")));
                }
                err_detail = pstrdup(extval);
                break;
            case PLPGSQL_RAISEOPTION_HINT:
                if (err_hint != NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("RAISE option already specified: %s", "HINT")));
                }
                err_hint = pstrdup(extval);
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized raise option: %d in RAISE statement.", opt->opt_type)));
                break;
        }

        exec_eval_cleanup(estate);
    }

    /* Default code if nothing specified */
    if (err_code == 0 && stmt->elog_level >= ERROR) {
        err_code = ERRCODE_RAISE_EXCEPTION;
    }

    /* Default error message if nothing specified */
    if (err_message == NULL) {
        if (condname != NULL) {
            err_message = condname;
            condname = NULL;
        } else {
            err_message = pstrdup(unpack_sql_state(err_code));
        }
    }

    /*
     * Throw the error (may or may not come back)
     */
    estate->err_text = raise_skip_msg; /* suppress traceback of raise */

    ereport(stmt->elog_level,
        (err_code ? errcode(err_code) : 0,
            errmsg_internal("%s", err_message),
            (err_detail != NULL) ? errdetail_internal("%s", err_detail) : 0,
            (err_hint != NULL) ? errhint("%s", err_hint) : 0,
            handle_in_client(true)));

    estate->err_text = NULL; /* un-suppress... */

    if (condname != NULL) {
        pfree_ext(condname);
    }
    if (err_message != NULL) {
        pfree_ext(err_message);
    }
    if (err_detail != NULL) {
        pfree_ext(err_detail);
    }
    if (err_hint != NULL) {
        pfree_ext(err_hint);
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * Initialize a mostly empty execution state
 * ----------
 */
static void plpgsql_estate_setup(PLpgSQL_execstate* estate, PLpgSQL_function* func, ReturnSetInfo* rsi)
{
    /* this link will be restored at exit from plpgsql_call_handler */
    func->cur_estate = estate;

    estate->func = func;
    estate->retval = (Datum)0;
    estate->retisnull = true;
    estate->rettype = InvalidOid;

    estate->fn_rettype = func->fn_rettype;
    estate->retistuple = func->fn_retistuple;
    estate->retisset = func->fn_retset;

    estate->readonly_func = func->fn_readonly;

    estate->rettupdesc = NULL;
    estate->exitlabel = NULL;
    estate->cur_error = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    estate->autonomous_session = NULL;
#endif
    estate->tuple_store = NULL;
    estate->cursor_return_data = NULL;
    if (rsi != NULL) {
        estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
        estate->tuple_store_owner = t_thrd.utils_cxt.CurrentResourceOwner;
    } else {
        estate->tuple_store_cxt = NULL;
        estate->tuple_store_owner = NULL;
    }
    estate->rsi = rsi;

    estate->found_varno = func->found_varno;

    estate->sql_cursor_found_varno = func->sql_cursor_found_varno;
    estate->sql_notfound_varno = func->sql_notfound_varno;
    estate->sql_isopen_varno = func->sql_isopen_varno;
    estate->sql_rowcount_varno = func->sql_rowcount_varno;
    estate->sqlcode_varno = func->sqlcode_varno;

    estate->ndatums = func->ndatums;
    estate->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * estate->ndatums);
    /* caller is expected to fill the datums array */

    estate->eval_tuptable = NULL;
    estate->eval_processed = 0;
    estate->eval_lastoid = InvalidOid;
    estate->eval_econtext = NULL;
    estate->cur_expr = NULL;

    estate->err_stmt = NULL;
    estate->err_text = NULL;

    estate->plugin_info = NULL;

    /*
     * Create an EState and ExprContext for evaluation of simple expressions.
     */
    plpgsql_create_econtext(estate);

    /* Support GOTO */
    estate->goto_labels = func->goto_labels;
    estate->block_level = 0;
    estate->goto_target_label = NULL;
    estate->goto_target_stmt = NULL;

    /*
     * Let the plugin see this function before we initialize any local
     * PL/pgSQL variables - note that we also give the plugin a few function
     * pointers so it can call back into PL/pgSQL for doing things like
     * variable assignments and stack traces
     */
    if (*u_sess->plsql_cxt.plugin_ptr) {
        (*u_sess->plsql_cxt.plugin_ptr)->error_callback = plpgsql_exec_error_callback;
        (*u_sess->plsql_cxt.plugin_ptr)->assign_expr = exec_assign_expr;

        /* change for pl_debugger */
        (*u_sess->plsql_cxt.plugin_ptr)->eval_expr = exec_eval_expr;
        (*u_sess->plsql_cxt.plugin_ptr)->assign_value = exec_assign_value;
        (*u_sess->plsql_cxt.plugin_ptr)->eval_cleanup = exec_eval_cleanup;
        (*u_sess->plsql_cxt.plugin_ptr)->validate_line = plpgsql_check_line_validity;

        if ((*u_sess->plsql_cxt.plugin_ptr)->func_setup) {
            ((*u_sess->plsql_cxt.plugin_ptr)->func_setup)(estate, func);
        }

        // Check for interrupts sent by pl debugger and other plugins.
        //
        CHECK_FOR_INTERRUPTS();
    }
}

/* ----------
 * Release temporary memory used by expression/subselect evaluation
 *
 * NB: the result of the evaluation is no longer valid after this is done,
 * unless it is a pass-by-value datatype.
 *
 * NB: if you change this code, see also the hacks in exec_assign_value's
 * PLPGSQL_DTYPE_ARRAYELEM case.
 * ----------
 */
void exec_eval_cleanup(PLpgSQL_execstate* estate)
{
    /* Clear result of a full SPI_execute */
    if (estate->eval_tuptable != NULL) {
        SPI_freetuptable(estate->eval_tuptable);
    }
    estate->eval_tuptable = NULL;

    /* Clear result of exec_eval_simple_expr (but keep the econtext) */
    if (estate->eval_econtext != NULL) {
        ResetExprContext(estate->eval_econtext);
    }
}

/* ----------
 * Generate a prepared plan
 * ----------
 */
static void exec_prepare_plan(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, int cursorOptions)
{
    SPIPlanPtr plan;

    /*
     * The grammar can't conveniently set expr->func while building the parse
     * tree, so make sure it's set before parser hooks need it.
     */
    expr->func = estate->func;
    u_sess->SPI_cxt._current->visit_id = expr->idx;

    /*
     * Generate and save the plan
     */
    plan = SPI_prepare_params(expr->query, (ParserSetupHook)plpgsql_parser_setup, (void*)expr, cursorOptions);
    if (plan == NULL) {
        /* Some SPI errors deserve specific error messages */
        switch (SPI_result) {
            case SPI_ERROR_COPY:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_PLSQL),
                        errmsg("cannot COPY to/from client in PL/pgSQL")));
                break;
            case SPI_ERROR_TRANSACTION:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_PLSQL),
                        errmsg("only support commit/rollback transaction statements."),
                        errhint("Use a BEGIN block with an EXCEPTION clause instead of begin/end transaction.")));
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_PLSQL),
                        errmsg("SPI_prepare_params failed for \"%s\": %s",
                            expr->query,
                            SPI_result_code_string(SPI_result))));
                break;
        }
    }
    SPI_keepplan(plan);
    expr->plan = plan;
    u_sess->SPI_cxt._current->visit_id = (uint32)-1;

    /* Check to see if it's a simple expression */
    exec_simple_check_plan(expr);
}

/* ----------
 * exec_stmt_execsql			Execute an SQL statement (possibly with INTO).
 * ----------
 */
static int exec_stmt_execsql(PLpgSQL_execstate* estate, PLpgSQL_stmt_execsql* stmt)
{
    ParamListInfo paramLI;
    long tcount;
    int rc;
    PLpgSQL_expr* expr = stmt->sqlstmt;
    Cursor_Data* saved_cursor_data = NULL;
    bool has_alloc = false;

    TransactionId oldTransactionId = SPI_get_top_transaction_id();

    /*
     * On the first call for this statement generate the plan, and detect
     * whether the statement is INSERT/UPDATE/DELETE/MERGE
     * When expr->plan is not NULL, then we also need to check if we need
     * redo the prepare plan task.
     */
    if (expr->plan == NULL || needRecompilePlan(expr->plan)) {
        ListCell* l = NULL;
        free_expr(expr);
        exec_prepare_plan(estate, expr, 0);
        stmt->mod_stmt = false;
        foreach (l, SPI_plan_get_plan_sources(expr->plan)) {
            CachedPlanSource* plansource = (CachedPlanSource*)lfirst(l);
            ListCell* l2 = NULL;

            foreach (l2, plansource->query_list) {
                Query* q = (Query*)lfirst(l2);

                AssertEreport(IsA(q, Query), MOD_PLSQL, "Query is required.");
                if (q->canSetTag) {
                    if (q->commandType == CMD_INSERT || q->commandType == CMD_UPDATE || q->commandType == CMD_DELETE ||
                        q->commandType == CMD_MERGE) {
                        stmt->mod_stmt = true;
                    }
                }
            }
        }
    }
    if (ENABLE_CN_GPC && g_instance.plan_cache->CheckRecreateSPICachePlan(expr->plan)) {
            g_instance.plan_cache->RecreateSPICachePlan(expr->plan);
    }
#ifndef ENABLE_MULTIPLE_NODES
        /* autonomous transaction */
        if (estate->autonomous_session && IsValidAutonomousTransactionQuery(STMT_SQL, expr, stmt->into)) {
            ATResult atresult =  estate->autonomous_session->ExecSimpleQuery(expr->query);
            exec_set_found(estate, atresult.withtuple);
            return PLPGSQL_RC_OK;
        }
#endif

    /*
     * Set up ParamListInfo (hook function and possibly data values)
     */
    paramLI = setup_param_list(estate, expr);

    /*
     * If we have INTO, then we only need one row back ... but if we have INTO
     * STRICT, ask for two rows, so that we can verify the statement returns
     * only one.  INSERT/UPDATE/DELETE are always treated strictly. Without
     * INTO, just run the statement to completion (tcount = 0).
     *
     * We could just ask for two rows always when using INTO, but there are
     * some cases where demanding the extra row costs significant time, eg by
     * forcing completion of a sequential scan.  So don't do it unless we need
     * to enforce strictness.
     */
    if (stmt->into) {
        if (!stmt->mod_stmt) {
            stmt->strict = true;
        }

        if (stmt->strict || stmt->mod_stmt) {
            tcount = 2;
        } else {
            tcount = 1;
        }
    } else {
        tcount = 0;
    }

    saved_cursor_data = estate->cursor_return_data;
    if (stmt->row != NULL && stmt->row->nfields > 0) {
        estate->cursor_return_data = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * stmt->row->nfields);
        has_alloc = true;
    } else {
        estate->cursor_return_data = NULL;
    }

    plpgsql_estate = estate;

    /*
     * Execute the plan
     */
    rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI, estate->readonly_func, tcount);

    // This is used for nested STP. If the transaction Id changed,
    // then need to create new econtext for the TopTransaction.
    stp_check_transaction_and_create_econtext(estate,oldTransactionId);
    
    plpgsql_estate = NULL;

    /*
     * Check for error, and set FOUND if appropriate (for historical reasons
     * we set FOUND only for certain query types).	Also Assert that we
     * identified the statement type the same as SPI did.
     */
    switch (rc) {
        case SPI_OK_SELECT:
            AssertEreport(!stmt->mod_stmt, MOD_PLSQL, "It should not be mod stmt.");
            exec_set_found(estate, (SPI_processed != 0));
            exec_set_sql_cursor_found(estate, (SPI_processed != 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE);
            exec_set_sql_notfound(estate, (0 == SPI_processed) ? PLPGSQL_TRUE : PLPGSQL_FALSE);
            exec_set_sql_isopen(estate, false);
            exec_set_sql_rowcount(estate, SPI_processed);
            break;

        case SPI_OK_INSERT:
            pgstat_set_io_state(IOSTATE_WRITE); /* only set io state for insert */
            /* fall through */
            
        case SPI_OK_UPDATE:
        case SPI_OK_DELETE:
        case SPI_OK_INSERT_RETURNING:
        case SPI_OK_UPDATE_RETURNING:
        case SPI_OK_DELETE_RETURNING:
        case SPI_OK_MERGE:
            SPI_forbid_exec_push_down_with_exception();
            AssertEreport(stmt->mod_stmt, MOD_PLSQL, "mod stmt is required.");
            exec_set_found(estate, (SPI_processed != 0));
            exec_set_sql_cursor_found(estate, (SPI_processed != 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE);
            exec_set_sql_notfound(estate, (0 == SPI_processed) ? PLPGSQL_TRUE : PLPGSQL_FALSE);
            exec_set_sql_isopen(estate, false);
            exec_set_sql_rowcount(estate, SPI_processed);
            break;

        case SPI_OK_SELINTO:
        case SPI_OK_UTILITY:
            SPI_forbid_exec_push_down_with_exception();
            AssertEreport(!stmt->mod_stmt, MOD_PLSQL, "It should not be mod stmt.");
            break;

        case SPI_OK_REWRITTEN:
            AssertEreport(!stmt->mod_stmt, MOD_PLSQL, "It should not be mod stmt.");

            /*
             * The command was rewritten into another kind of command. It's
             * not clear what FOUND would mean in that case (and SPI doesn't
             * return the row count either), so just set it to false.
             */
            exec_set_found(estate, false);
            break;

            /* Some SPI errors deserve specific error messages */
        case SPI_ERROR_COPY:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_PLSQL),
                    errmsg("cannot COPY to/from client in PL/pgSQL")));
            break;
        case SPI_ERROR_TRANSACTION:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_PLSQL),
                    errmsg("only support commit/rollback transaction statements."),
                    errhint("Use a BEGIN block with an EXCEPTION clause instead of begin/end transaction.")));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_PLSQL),
                    errmsg("SPI_execute_plan_with_paramlist failed executing query \"%s\": %s",
                        expr->query,
                        SPI_result_code_string(rc))));
            break;
    }

    /* All variants should save result info for GET DIAGNOSTICS */
    estate->eval_processed = SPI_processed;
    estate->eval_lastoid = u_sess->SPI_cxt.lastoid;

    /* Process INTO if present */
    if (stmt->into) {
        SPITupleTable* tuptab = SPI_tuptable;
        uint32 n = SPI_processed;
        PLpgSQL_rec* rec = NULL;
        PLpgSQL_row* row = NULL;

        /* If the statement did not return a tuple table, complain */
        if (tuptab == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("INTO used with a command that cannot return data")));
        }

        /* Determine if we assign to a record or a row */
        if (stmt->rec != NULL) {
            rec = (PLpgSQL_rec*)(estate->datums[stmt->rec->dno]);
        } else if (stmt->row != NULL) {
            row = (PLpgSQL_row*)(estate->datums[stmt->row->dno]);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("unsupported target, use record and row instead.")));
        }

        /*
         * If SELECT ... INTO specified STRICT, and the query didn't find
         * exactly one row, throw an error.  If STRICT was not specified, then
         * allow the query to find any number of rows.
         */
        if (n == 0) {
            if (stmt->strict) {
                ereport(ERROR,
                    (errcode(ERRCODE_NO_DATA_FOUND),
                        errmodule(MOD_PLSQL),
                        errmsg("query returned no rows when process INTO")));
            }
            /* set the target to NULL(s) */
            exec_move_row(estate, rec, row, NULL, tuptab->tupdesc);
        } else {
            if (n > 1 && (stmt->strict || stmt->mod_stmt)) {
                ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_ROWS),
                        errmodule(MOD_PLSQL),
                        errmsg("query returned %u rows more than one row", n)));
            }
            /* Put the first result row into the target */
            exec_move_row(estate, rec, row, tuptab->vals[0], tuptab->tupdesc);
        }

        /* Clean up */
        exec_eval_cleanup(estate);
        SPI_freetuptable(SPI_tuptable);
    } else {
        /* If the statement returned a tuple table, complain */
        if (SPI_tuptable != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("query has no destination for result data"),
                    (rc == SPI_OK_SELECT)
                        ? errhint("If you want to discard the results of a SELECT, use PERFORM instead.")
                        : 0));
        }
    }

    if (paramLI) {
        pfree_ext(paramLI);
    }

    /* For function nesting calls, only free this lever data */
    pfree_ext(estate->cursor_return_data);

    estate->cursor_return_data = saved_cursor_data;
    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_into_dynexecute			Process INTO in a dynamic SQL query
 * stmt_execsql            Execute an SQL statement (possibly with INTO).
 * ----------
 * we forbid a shippable function has exception that other SQL or Function rely 
 * on the exception results,the shippable function may get different results ,
 * because the exception declare start a subtransaction in DN node,
 * and different DN node may get different result.
 * create or replace function test1 return int shippable
 * as
 * declare
 * a int;
 * begin
 * select col1 from test into a;
 * return a;
 * exception when others then
 * select col2 from test into a;
 * return a;
 * end;
 */
static void exec_into_dynexecute(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* stmt)
{
    SPITupleTable* tuptab = SPI_tuptable;
    uint32 n = SPI_processed;
    PLpgSQL_rec* rec = NULL;
    PLpgSQL_row* row = NULL;

    if (!stmt->into) {
        return;
    }

    /* If the statement did not return a tuple table, complain */
    if (tuptab == NULL) {
        if (!stmt->isinouttype) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("INTO used with a command that cannot return data")));
        }
    } else {
        if (stmt->rec != NULL) {
            rec = (PLpgSQL_rec*)(estate->datums[stmt->rec->dno]);
        } else if (stmt->row != NULL) {
        /*
         * normally row is equal to estate datum row,
         * for anonymous block, we could set a new row here, not included
         * in estate datum arrays.
         */
            row = stmt->row;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_PLSQL),
                    errmsg("unsupported target when process INTO in dynamic SQL query. use record or row instead.")));
        }

        /*
         * If SELECT ... INTO specified STRICT, and the query didn't find
         * exactly one row, throw an error.  If STRICT was not specified, then
         * allow the query to find any number of rows.
         */
        if (n == 0) {
            if (stmt->strict) {
                ereport(
                    ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmodule(MOD_PLSQL), errmsg("query returned no rows")));
            }
            /* set the target to NULL(s) */
            exec_move_row(estate, rec, row, NULL, tuptab->tupdesc);
        } else {
            if (n > 1 && stmt->strict) {
                ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_ROWS), errmodule(MOD_PLSQL), errmsg("query returned more than one row")));
            }
            /* Put the first result row into the target */
            exec_move_row(estate, rec, row, tuptab->vals[0], tuptab->tupdesc);
        }
        /* clean up after exec_move_row() */
        exec_eval_cleanup(estate);
    }
}

/* ----------
 * exec_set_attr_dynexecute		Set attribute for dynamic SQL query base on SPI execution result.
 * ----------
 */
static void exec_set_attr_dynexecute(PLpgSQL_execstate *estate, int exec_res, char *querystr)
{
    switch (exec_res) {
        case SPI_OK_SELECT:
        case SPI_OK_INSERT:
        case SPI_OK_UPDATE:
        case SPI_OK_DELETE:
        case SPI_OK_INSERT_RETURNING:
        case SPI_OK_UPDATE_RETURNING:
        case SPI_OK_DELETE_RETURNING:
        case SPI_OK_MERGE:
            exec_set_sql_cursor_found(estate, (SPI_processed != 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE);
            exec_set_sql_notfound(estate, (0 == SPI_processed) ? PLPGSQL_TRUE : PLPGSQL_FALSE);
            exec_set_sql_isopen(estate, false);
            exec_set_sql_rowcount(estate, SPI_processed);
            break;

        case SPI_OK_UTILITY:
        case SPI_OK_REWRITTEN:
            break;

        case 0:
            /*
             * Also allow a zero return, which implies the querystring
             * contained no commands.
             */
            break;

        case SPI_OK_SELINTO:
            /*
             * We want to disallow SELECT INTO for now, because its behavior
             * is not consistent with SELECT INTO in a normal plpgsql context.
             * (We need to reimplement EXECUTE to parse the string as a
             * plpgsql command, not just feed it to SPI_execute.)  This is not
             * a functional limitation because CREATE TABLE AS is allowed.
             */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                errmsg("EXECUTE of SELECT ... INTO is not implemented"),
                errhint("You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead.")));
            break;

        /* Some SPI errors deserve specific error messages */
        case SPI_ERROR_COPY:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_PLSQL), 
                errmsg("cannot COPY to/from client in PL/pgSQL")));
            break;

        case SPI_ERROR_TRANSACTION:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_PLSQL),
                errmsg("cannot call transaction statements in EXECUTE IMMEDIATE statement.")));
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_PLSQL),
                errmsg("SPI_execute failed executing query \"%s\": %s", querystr, SPI_result_code_string(exec_res)),
                errhint("You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead.")));
            break;
    }

    return;
}
/* ----------
 * exec_stmt_dynexecute			Execute a dynamic SQL query
 *					(possibly with INTO).
 * ----------
 */
static int exec_stmt_dynexecute(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* stmt)
{
    Datum query;
    bool isnull = false;
    Oid restype;
    char* querystr = NULL;
    int exec_res;
    long tcount;
    PLpgSQL_function* func = NULL;
    bool isblock = false;
    int res;
    PLpgSQL_stmt stmtblock;
    stmtblock.cmd_type = stmt->cmd_type;
    stmtblock.lineno = stmt->lineno;
    TransactionId oldTransactionId = SPI_get_top_transaction_id();

    /*
     * First we evaluate the string expression after the EXECUTE keyword. Its
     * result is the querystring we have to execute.
     */
    query = exec_eval_expr(estate, stmt->query, &isnull, &restype);
    if (isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("query string argument of EXECUTE statement is null")));
    }

    /* Get the C-String representation */
    querystr = convert_value_to_string(estate, query, restype);

    /* copy it out of the temporary context before we clean up */
    querystr = pstrdup(querystr);

    exec_eval_cleanup(estate);
#ifndef ENABLE_MULTIPLE_NODES
    if (estate->autonomous_session && IsValidAutonomousTransactionQuery(STMT_DYNAMIC, stmt->query, stmt->into)) {
        estate->autonomous_session->ExecSimpleQuery(querystr);
        return PLPGSQL_RC_OK;
    }
#endif
    if (stmt->params != NULL) {
        stmt->ppd = (void*)exec_eval_using_params(estate, stmt->params);
    }

    if (stmt->isanonymousblock) {
        isblock = is_anonymous_block(querystr);
    }

    if (isblock == true) {
        FunctionCallInfoData fake_fcinfo;
        FmgrInfo flinfo;
        int ppdindex = 0;
        int datumindex = 0;

        /* Compile the anonymous code block */
        /* support pass external parameter in anonymous block */
        func = plpgsql_compile_inline(querystr);

        /* Mark the function as busy, just pro forma */
        func->use_count++;

        /* make parameters exchange between outer stmts and inner stmts for dynamic blocks */
        res = exchange_parameters(estate, stmt, func->action->body, &ppdindex, &datumindex);

        if (res < 0) {
            return -1;
        }

        /*
         * Set up a fake fcinfo with just enough info to satisfy
         * plpgsql_exec_function().  In particular note that this sets things up
         * with no arguments passed.
         */
        errno_t rc = memset_s(&fake_fcinfo, sizeof(fake_fcinfo), 0, sizeof(fake_fcinfo));
        securec_check(rc, "", "");
        rc = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
        securec_check(rc, "", "");
        fake_fcinfo.flinfo = &flinfo;
        flinfo.fn_oid = InvalidOid;
        flinfo.fn_mcxt = CurrentMemoryContext;

        (void)plpgsql_exec_function(func, &fake_fcinfo, true);
        /*
         * This is used for nested STP. If the transaction Id changed,
         * then need to create new econtext for the TopTransaction.
         */
        stp_check_transaction_and_create_econtext(estate,oldTransactionId);

        exec_set_sql_isopen(estate, false);
        exec_set_sql_cursor_found(estate, PLPGSQL_TRUE);
        exec_set_sql_notfound(estate, PLPGSQL_FALSE);
        exec_set_sql_rowcount(estate, 1);

        /* Function should now have no remaining use-counts ... */
        func->use_count--;
        if (unlikely(func->use_count != 0)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmodule(MOD_PLSQL),
                    errmsg("Function should now have no remaining use-counts")));
        }

        /* ... so we can free subsidiary storage */
        plpgsql_free_function_memory(func);

        if (stmt->ppd != NULL) {
            free_params_data((PreparedParamsData*)stmt->ppd);
            stmt->ppd = NULL;
        }
        pfree_ext(querystr);

        return PLPGSQL_RC_OK;
    }

    /* normal way: for only one statement in dynamic query */
    if (stmt->into) {
        tcount = stmt->strict ? 2 : 1;
    } else {
        tcount = 0;
    }

    Cursor_Data* saved_cursor_data = estate->cursor_return_data;
    if (stmt->row != NULL && stmt->row->nfields > 0) {
        estate->cursor_return_data = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * stmt->row->nfields);
    } else {
        estate->cursor_return_data = NULL;
    }

    plpgsql_estate = estate;

    /*
     * Execute the query without preparing a saved plan.
     */
    if (stmt->ppd != NULL) {
        PreparedParamsData* ppd = (PreparedParamsData*)stmt->ppd;
        exec_res = SPI_execute_with_args(
            querystr, ppd->nargs, ppd->types, ppd->values, ppd->nulls, estate->readonly_func, tcount, ppd->cursor_data);
        free_params_data(ppd);
        stmt->ppd = NULL;
    } else {
        exec_res = SPI_execute(querystr, estate->readonly_func, 0);
    }
    /*
     * This is used for nested STP. If the transaction Id changed,
     * then need to create new econtext for the TopTransaction.
     */
    stp_check_transaction_and_create_econtext(estate,oldTransactionId);

    plpgsql_estate = NULL;

    exec_set_attr_dynexecute(estate, exec_res, querystr);

    /* Save result info for GET DIAGNOSTICS */
    estate->eval_processed = SPI_processed;
    estate->eval_lastoid = u_sess->SPI_cxt.lastoid;

    /* Process INTO if present */
    if (stmt->into) {
        exec_into_dynexecute(estate, stmt);
    } else {
        /*
         * It might be a good idea to raise an error if the query returned
         * tuples that are being ignored, but historically we have not done
         * that.
         */
    }

    /* Release any result from SPI_execute, as well as the querystring */
    SPI_freetuptable(SPI_tuptable);
    pfree_ext(querystr);

    pfree_ext(estate->cursor_return_data);

    estate->cursor_return_data = saved_cursor_data;

    return PLPGSQL_RC_OK;
}

/*
 * Exchange parameters in dynamic statement when the dynamic statement is an anonymous block.
 * Since the parameters are all evaluated outside in outer runtime data, we need carefully assign those
 * parameters into each statement according to the placeholders. And only PLpgSQL_stmt_dynexecute
 * may have the flexibility to accept IN parameters with PPD data structure, so we need change
 * PLpgSQL_stmt_execsql structure to PLpgSQL_stmt_dynexecute.
 */
static int exchange_parameters(
    PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* dynstmt, List* stmts, int* ppdindex, int* datumindex)
{
    ListCell* s = NULL;
    int outcount = 0;
    PreparedParamsData* inppd = NULL;
    PLpgSQL_row* outrow = NULL;
    PLpgSQL_stmt_execsql* execstmt = NULL;

    if (stmts == NIL) {
        return PLPGSQL_RC_OK;
    }

    inppd = (PreparedParamsData*)dynstmt->ppd;
    outrow = dynstmt->row;

    foreach (s, stmts) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(s);
        outcount = 0;
        switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
            /*
             * support exception in anonymous block
             * executed by dynamic statement
             */
            case PLPGSQL_STMT_BLOCK: {
                PLpgSQL_stmt_block* blockstmt = (PLpgSQL_stmt_block*)stmt;
                exchange_parameters(estate, dynstmt, blockstmt->body, ppdindex, datumindex);
            } break;
            case PLPGSQL_STMT_EXECSQL:
                execstmt = (PLpgSQL_stmt_execsql*)stmt;
                if (execstmt->into && execstmt->row != NULL) {
                    /* this is the condition only for select into statement */
                    PLpgSQL_row* currow = execstmt->row;
                    outcount = currow->intoplaceholders;
                    if (outcount > 0) {
                        int i = 0;
                        if (outrow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmodule(MOD_PLSQL),
                                    errmsg("query has no destination for result data in EXECUTE statement")));
                            return 0;
                        }
                        currow->intodatums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * outcount);
                        /* add all out paramters */
                        for (i = 0; i < outcount; i++) {
                            PLpgSQL_datum* datum = NULL;
                            if (outrow->nfields <= *datumindex) {
                                ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmodule(MOD_PLSQL),
                                        errmsg("the number of place holders doesn't match the number of parameters.")));
                            }
                            datum = estate->datums[outrow->varnos[*datumindex]];
                            *datumindex = *datumindex + 1;
                            /* ppdindex is also needed to up because all out/in param in ppds */
                            *ppdindex = *ppdindex + 1;
                            currow->intodatums[i] = datum;
                        }
                    }
                }
                if (execstmt->placeholders > 0) {
                    PreparedParamsData* newppd = NULL;
                    PLpgSQL_stmt_dynexecute* newstmt =
                        (PLpgSQL_stmt_dynexecute*)palloc0(sizeof(PLpgSQL_stmt_dynexecute));
                    /* have in parameters */
                    int nargs = execstmt->placeholders;
                    int i = 0;

                    if (inppd == NULL) {
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                                errmodule(MOD_PLSQL),
                                errmsg("the number of place holders doesn't match the number of parameters.")));
                        return 0;
                    }

                    outcount = 0;
                    newstmt->cmd_type = PLPGSQL_STMT_DYNEXECUTE;
                    newstmt->lineno = execstmt->lineno;
                    newstmt->query = execstmt->sqlstmt;
                    /* add 'SELECT \' \'' outside the query and double the quotes in the query */
                    newstmt->query->query = transformAnonymousBlock(execstmt->sqlstmt->query);
                    newstmt->into = execstmt->into;
                    newstmt->strict = execstmt->strict;
                    newstmt->rec = execstmt->rec;
                    newstmt->row = execstmt->row;
                    newstmt->params = NIL;
                    newstmt->isinouttype = dynstmt->isinouttype;
                    newppd = (PreparedParamsData*)palloc0(sizeof(PreparedParamsData));
                    newppd->nargs = nargs;
                    newppd->types = (Oid*)palloc0(nargs * sizeof(Oid));
                    newppd->values = (Datum*)palloc0(nargs * sizeof(Datum));
                    newppd->nulls = (char*)palloc0(nargs * sizeof(char));
                    newppd->freevals = (bool*)palloc0(nargs * sizeof(bool));
                    newppd->isouttype = (bool*)palloc0(nargs * sizeof(bool));
                    for (i = 0; i < execstmt->placeholders; i++) {
                        if (inppd->nargs <= *ppdindex) {
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmodule(MOD_PLSQL),
                                    errmsg("the number of place holders doesn't match the number of parameters.")));
                        }
                        newppd->types[i] = inppd->types[*ppdindex];
                        newppd->values[i] = inppd->values[*ppdindex];
                        newppd->nulls[i] = inppd->nulls[*ppdindex];
                        /* inside values aren't freeable because it will be freed outside */
                        newppd->freevals[i] = false;
                        newppd->isouttype[i] = inppd->isouttype[*ppdindex];

                        if (inppd->isouttype[*ppdindex]) {
                            outcount++;
                        }

                        *ppdindex = *ppdindex + 1;
                    }

                    newstmt->ppd = (void*)newppd;

                    /* handle IN/OUT placeholder now */
                    if (outcount > 0) {
                        PLpgSQL_row* newrow = NULL;
                        if (execstmt->into && execstmt->row != NULL) {
                            /* only select have into statement */
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmodule(MOD_PLSQL),
                                    errmsg("syntax error at or near \"into\"")));
                            return 0;
                        }
                        if (outrow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmodule(MOD_PLSQL),
                                    errmsg("query has no destination for result data")));
                            return 0;
                        }

                        /* we have IN/OUT parameters , should copy from outrow */
                        newrow = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
                        newrow->dtype = PLPGSQL_DTYPE_ROW;
                        newrow->refname = pstrdup("*internal*");
                        newrow->lineno = outrow->lineno;
                        newrow->rowtupdesc = NULL;
                        newrow->nfields = 0;
                        newrow->fieldnames = NULL;
                        newrow->varnos = NULL;

                        /* add all out paramters */
                        newrow->intoplaceholders = outcount;
                        newrow->intodatums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * outcount);
                        for (i = 0; i < outcount; i++) {
                            PLpgSQL_datum* datum = NULL;
                            if (outrow->nfields <= *datumindex) {
                                ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmodule(MOD_PLSQL),
                                        errmsg("the number of place holders doesn't match the number of parameters.")));
                            }
                            datum = estate->datums[outrow->varnos[*datumindex]];
                            *datumindex = *datumindex + 1;
                            newrow->intodatums[i] = datum;
                        }
                        newstmt->row = newrow;
                        newstmt->into = true;
                    }

                    newstmt->isanonymousblock = false;
                    /* change stmt to dynstmt */
                    s->data.ptr_value = newstmt;
                    /* free the original statememt */
                    pfree_ext(stmt);
                }
                break;
            default:
                break;
        }
    }

    return PLPGSQL_RC_OK;
}

/*
 * Check whether the query is an anonyous block or not.
 */
static bool is_anonymous_block(const char* query)
{
    int index = 0;
    char ch;
    int len = (int) strlen(query);
    char str[10];
    int i;
    for (index = 0; index < len; index++) {
        ch = query[index];
        if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f') {
            continue;
        }
        break;
    }

    /* now we meet the first character */
    if ((len - index) < 8) {
        return false;
    }
    /* find begin */
    for (i = index; i < index + 5; i++) {
        str[i - index] = (char) tolower(query[i]);
    }
    str[i - index] = '\0';
    if (strcmp(str, "begin") == 0) {
        return true;
    }

    /* find declare */
    for (i = index; i < index + 7; i++) {
        str[i - index] = (char) tolower(query[i]);
    }
    str[i - index] = '\0';
    if (strcmp(str, "declare") == 0) {
        return true;
    }

    return false;
}

/* ----------
 * exec_stmt_dynfors			Execute a dynamic query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int exec_stmt_dynfors(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynfors* stmt)
{
    Portal portal;
    int rc;

    portal = exec_dynquery_with_params(estate, stmt->query, stmt->params, NULL, 0);

    /*
     * Execute the loop
     */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq*)stmt, portal, true, -1);

    /*
     * Close the implicit cursor
     */
    SPI_cursor_close(portal);

    return rc;
}

/*
 * Description: init portal's cursorOption.
 * Parameters:
 * @in portal: the cursor stored in portal.
 * @in estate: procedure execute estate.
 * @in varno: the cursor var number in datums.
 * Return: void
 */
static void BindCursorWithPortal(Portal portal, PLpgSQL_execstate *estate, int varno)
{
    portal->funcOid = estate->func->fn_oid;
    portal->funcUseCount = (int) estate->func->use_count;
    for (int i = 0; i < CURSOR_ATTRIBUTE_NUMBER; i++) {
        portal->cursorAttribute[i] = estate->datums[varno + i + 1];
    }
}

/* ----------
 * exec_stmt_open			Execute an OPEN cursor statement
 * ----------
 */
static int exec_stmt_open(PLpgSQL_execstate* estate, PLpgSQL_stmt_open* stmt)
{
    PLpgSQL_var* curvar = NULL;
    char* curname = NULL;
    PLpgSQL_expr* query = NULL;
    Portal portal = NULL;
    ParamListInfo paramLI = NULL;

    /* ----------
     * Get the cursor variable and if it has an assigned name, check
     * that it's not in use currently.
     * ----------
     */
    curvar = (PLpgSQL_var*)(estate->datums[stmt->curvar]);
    if (!curvar->isnull) {
        curname = TextDatumGetCString(curvar->value);
        if (SPI_cursor_find(curname) != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_CURSOR),
                    errmodule(MOD_PLSQL),
                    errmsg("cursor \"%s\" already in use in OPEN statement.", curname)));
        }
    }

    /* ----------
     * Process the OPEN according to it's type.
     * ----------
     */
    if (stmt->query != NULL) {
        /* ----------
         * This is an OPEN refcursor FOR SELECT ...
         *
         * We just make sure the query is planned. The real work is
         * done downstairs.
         * ----------
         */
        query = stmt->query;
        if (query->plan == NULL) {
            exec_prepare_plan(estate, query, stmt->cursor_options);
        }
    } else if (stmt->dynquery != NULL) {
        /* ----------
         * This is an OPEN refcursor FOR EXECUTE ...
         * ----------
         */
        portal = exec_dynquery_with_params(estate, stmt->dynquery, stmt->params, curname, stmt->cursor_options);

        /*
         * If cursor variable was NULL, store the generated portal name in it
         */
        if (curname == NULL) {
            assign_text_var(curvar, portal->name);
        }
        BindCursorWithPortal(portal, estate, curvar->dno);
        exec_set_isopen(estate, true, curvar->dno + CURSOR_ISOPEN);
        exec_set_rowcount(estate, 0, true, curvar->dno + CURSOR_ROWCOUNT);
        PinPortal(portal);
        return PLPGSQL_RC_OK;
    } else {
        /* ----------
         * This is an OPEN cursor
         *
         * Note: parser should already have checked that statement supplies
         * args iff cursor needs them, but we check again to be safe.
         * ----------
         */
        if (stmt->argquery != NULL) {
            /* ----------
             * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
             * statement to evaluate the args and put 'em into the
             * internal row.
             * ----------
             */
            PLpgSQL_stmt_execsql set_args;
            errno_t rc = EOK;

            if (curvar->cursor_explicit_argrow < 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmodule(MOD_PLSQL),
                        errmsg("arguments given for cursor without arguments in OPEN statement.")));
            }

            rc = memset_s(&set_args, sizeof(set_args), 0, sizeof(set_args));
            securec_check(rc, "\0", "\0");
            set_args.cmd_type = PLPGSQL_STMT_EXECSQL;
            set_args.lineno = stmt->lineno;
            set_args.sqlstmt = stmt->argquery;
            set_args.into = true;
            /* XXX historically this has not been STRICT */
            set_args.row = (PLpgSQL_row*)(estate->datums[curvar->cursor_explicit_argrow]);

            if (exec_stmt_execsql(estate, &set_args) != PLPGSQL_RC_OK) {
                ereport(ERROR,
                    (errcode(ERRCODE_SPI_CURSOR_OPEN_FAILURE),
                        errmodule(MOD_PLSQL),
                        errmsg("open cursor failed during argument processing in OPEN statement.")));
            }
        } else {
            if (curvar->cursor_explicit_argrow >= 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmodule(MOD_PLSQL),
                        errmsg("arguments required for cursor in OPEN statement.")));
            }
        }

        query = curvar->cursor_explicit_expr;
        if (query->plan == NULL) {
            exec_prepare_plan(estate, query, curvar->cursor_options);
        }
    }

    /*
     * Set up ParamListInfo (hook function and possibly data values)
     */
    paramLI = setup_param_list(estate, query);

    /*
     * Open the cursor
     */
    portal = SPI_cursor_open_with_paramlist(curname, query->plan, paramLI, estate->readonly_func);
    if (portal == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_CURSOR),
                errmodule(MOD_PLSQL),
                errmsg("could not open cursor: %s", SPI_result_code_string(SPI_result))));
    }

    /*
     * If cursor variable was NULL, store the generated portal name in it
     */
    if (curname == NULL) {
        assign_text_var(curvar, portal->name);
    }

    if (curname != NULL) {
        pfree_ext(curname);
    }
    if (paramLI != NULL) {
        pfree_ext(paramLI);
    }

    BindCursorWithPortal(portal, estate, curvar->dno);
    exec_set_isopen(estate, true, curvar->dno + CURSOR_ISOPEN);
    exec_set_rowcount(estate, 0, true, curvar->dno + CURSOR_ROWCOUNT);
    PinPortal(portal);
    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_fetch			Fetch from a cursor into a target, or just
 *							move the current position of the cursor
 * ----------
 */
static int exec_stmt_fetch(PLpgSQL_execstate* estate, PLpgSQL_stmt_fetch* stmt)
{
    PLpgSQL_var* curvar = NULL;
    PLpgSQL_rec* rec = NULL;
    PLpgSQL_row* row = NULL;
    long how_many = stmt->how_many;
    SPITupleTable* tuptab = NULL;
    Portal portal = NULL;
    char* curname = NULL;
    uint32 n;
    bool savedisAllowCommitRollback;
    bool needResetErrMsg;
    needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_OPEN_FOR);

    /* ----------
     * Get the portal of the cursor by name
     * ----------
     */
    curvar = (PLpgSQL_var*)(estate->datums[stmt->curvar]);
    if (curvar->isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("cursor variable \"%s\" is null in FETCH statement.", curvar->refname)));
    }
    curname = TextDatumGetCString(curvar->value);

    portal = SPI_cursor_find(curname);
    if (portal == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_CURSOR),
                errmodule(MOD_PLSQL),
                errmsg("cursor \"%s\" does not exist in FETCH statement.", curname)));
    }
    pfree_ext(curname);

    /* Calculate position for FETCH_RELATIVE or FETCH_ABSOLUTE */
    if (stmt->expr != NULL) {
        bool isnull = false;

        /* XXX should be doing this in LONG not INT width */
        how_many = exec_eval_integer(estate, stmt->expr, &isnull);

        if (isnull) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("relative or absolute cursor position is null in FETCH statement.")));
        }

        exec_eval_cleanup(estate);
    }

    if (!stmt->is_move) {
        /* ----------
         * Determine if we fetch into a record or a row
         * ----------
         */
        if (stmt->rec != NULL) {
            rec = (PLpgSQL_rec*)(estate->datums[stmt->rec->dno]);
        } else if (stmt->row != NULL) {
            row = (PLpgSQL_row*)(estate->datums[stmt->row->dno]);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_PLSQL),
                    errmsg("unsupported target in FETCH statement. use record or row instead.")));
        }

        /* ----------
         * Fetch 1 tuple from the cursor
         * ----------
         */
        SPI_scroll_cursor_fetch(portal, stmt->direction, how_many);
        tuptab = SPI_tuptable;
        n = SPI_processed;

        /* ----------
         * Set the target appropriately.
         * ----------
         */
        /*
         * when not found (n==0), pg9.2 set target var null,
         * but A db keep target var's origin value,
         * for adapting A db, just deal when n!=0
         */
        if (n != 0) {
            exec_move_row(estate, rec, row, tuptab->vals[0], tuptab->tupdesc);
        }

        exec_eval_cleanup(estate);
        SPI_freetuptable(tuptab);
    } else {
        /* Move the cursor */
        SPI_scroll_cursor_move(portal, stmt->direction, how_many);
        n = SPI_processed;
    }

    stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);

    /* Set the ROW_COUNT and the global FOUND variable appropriately. */
    estate->eval_processed = n;
    exec_set_found(estate, n != 0);
    exec_set_cursor_found(estate, (n != 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE, curvar->dno + CURSOR_FOUND);
    exec_set_notfound(estate, (n == 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE, curvar->dno + CURSOR_NOTFOUND);
    exec_set_rowcount(estate, n, false, curvar->dno + CURSOR_ROWCOUNT);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_close			Close a cursor
 * ----------
 */
static int exec_stmt_close(PLpgSQL_execstate* estate, PLpgSQL_stmt_close* stmt)
{
    PLpgSQL_var* curvar = NULL;
    Portal portal = NULL;
    char* curname = NULL;

    /* ----------
     * Get the portal of the cursor by name
     * ----------
     */
    curvar = (PLpgSQL_var*)(estate->datums[stmt->curvar]);
    if (curvar->isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("cursor variable \"%s\" is null in CLOSE statement.", curvar->refname)));
    }
    curname = TextDatumGetCString(curvar->value);

    portal = SPI_cursor_find(curname);
    if (portal == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_CURSOR),
                errmodule(MOD_PLSQL),
                errmsg("cursor \"%s\" does not exist in CLOSE statement.", curname)));
    }
    pfree_ext(curname);

    /* ----------
     * And close it.
     * ----------
     */
    UnpinPortal(portal);
    SPI_cursor_close(portal);
    exec_set_isopen(estate, false, curvar->dno + CURSOR_ISOPEN);
    exec_set_cursor_found(estate, PLPGSQL_NULL, curvar->dno + CURSOR_FOUND);
    exec_set_notfound(estate, PLPGSQL_NULL, curvar->dno + CURSOR_NOTFOUND);
    exec_set_rowcount(estate, -1, true, curvar->dno + CURSOR_ROWCOUNT);
    
    return PLPGSQL_RC_OK;
}

static void rebuild_exception_subtransaction_chain(PLpgSQL_execstate* estate)
{
    // Rebuild ResourceOwner chain, link Portal ResourceOwner to Top ResourceOwner.
    ResourceOwnerNewParent(t_thrd.utils_cxt.STPSavedResourceOwner, t_thrd.utils_cxt.CurrentResourceOwner);
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.STPSavedResourceOwner;
    int subTransactionCount = u_sess->SPI_cxt.portal_stp_exception_counter;
    while(subTransactionCount > 0) {
        if(u_sess->SPI_cxt.portal_stp_exception_counter > 0) {
            MemoryContext oldcontext = CurrentMemoryContext;

            estate->err_text = gettext_noop("during statement block entry");
#ifdef ENABLE_MULTIPLE_NODES
            /* CN should send savepoint command to remote nodes to begin sub transaction remotely. */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                RecordSavepoint("Savepoint s1", "s1", false, SUB_STMT_SAVEPOINT);
                pgxc_node_remote_savepoint("Savepoint s1", EXEC_ON_DATANODES, true, true);
            }
#endif
             BeginInternalSubTransaction(NULL);

             /* Want to run statements inside function's memory context */
             MemoryContextSwitchTo(oldcontext);

             plpgsql_create_econtext(estate);
             estate->err_text = NULL;
        }

        subTransactionCount--;
    }
}

/*
 * exec_stmt_transaction
 *
 * This function is used for start transaction action within stored procedure. It will commit/rollback 
 * the current transaction, Start a new transaction and connect the Portal to the new transaction.
 */
static int exec_stmt_transaction(PLpgSQL_execstate *estate, PLpgSQL_stmt* stmt)
{
    bool savedisAllowCommitRollback = false;
    bool needResetErrMsg = false;
    // 1. Check whether the calling context is supported.
    SPI_stp_transaction_check(estate->readonly_func);

    const char *PORTAL = "Portal";
    if(strcmp(PORTAL, ResourceOwnerGetName(t_thrd.utils_cxt.CurrentResourceOwner)) == 0) {
        if(ResourceOwnerGetNextChild(t_thrd.utils_cxt.CurrentResourceOwner)
            && (strcmp(PORTAL, ResourceOwnerGetName(
                ResourceOwnerGetNextChild(t_thrd.utils_cxt.CurrentResourceOwner))) == 0)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("commit with PE is not supported")));
        }
    }

    // 2. Hold portals.
    needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_OPEN_FOR);
    _SPI_hold_cursor();
    stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);

    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0) {
        // Recording Portal's ResourceOwner for rebuilding resource chain 
        // when procedure contain transaction statement.
        // Current ResourceOwner is Portal ResourceOwner.
        t_thrd.utils_cxt.STPSavedResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    }

    // 3. Save current transaction state for the outer transaction started by user's 
    // begin/start statement.
    SPI_save_current_stp_transaction_state();

    // 4. Commit/rollback
    switch((PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_COMMIT:
            SPI_commit();
            break;

        case PLPGSQL_STMT_ROLLBACK:
            SPI_rollback();
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("unsupported transaction statement type.")));
    }

    // 5. Start a new transaction.
    SPI_start_transaction();

    // 6. Restore the outer transaction state.
    SPI_restore_current_stp_transaction_state();
    
    // 7. Rebuild estate's context.
    u_sess->plsql_cxt.simple_eval_estate = NULL;
    plpgsql_create_econtext(estate);

    // 8. Rebuild subtransaction chain for exception.
    rebuild_exception_subtransaction_chain(estate);

    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0) {
        t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.STPSavedResourceOwner;
    }

    return PLPGSQL_RC_OK;

}

/* ----------
 * exec_stmt_null			Locate the execution status.
 * ----------
 */
static int exec_stmt_null(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt)
{
    Assert(estate && stmt->lineno >= 0);

    return PLPGSQL_RC_OK;
}

/*
 * @Description: copy cursor data from estate->datums to estate->datums
 * @in estate - estate
 * @in source_dno - source varno in datums
 * @in target_dno - target varno in datums
 * @return -void
 */
static void copy_cursor_var(PLpgSQL_execstate* estate, int source_dno, int target_dno)
{
    PLpgSQL_var* source_cursor = (PLpgSQL_var*)estate->datums[source_dno];
    PLpgSQL_var* target_cursor = (PLpgSQL_var*)estate->datums[target_dno];

    /* only copy cursor option between refcursor */
    if (source_cursor->datatype->typoid != REFCURSOROID || target_cursor->datatype->typoid != REFCURSOROID) {
        return;
    }

    source_cursor = (PLpgSQL_var *)estate->datums[source_dno + CURSOR_ISOPEN];
    target_cursor = (PLpgSQL_var *)estate->datums[target_dno + CURSOR_ISOPEN];
    target_cursor->value = source_cursor->value;
    target_cursor->isnull = source_cursor->isnull;

    source_cursor = (PLpgSQL_var*)estate->datums[source_dno + CURSOR_FOUND];
    target_cursor = (PLpgSQL_var*)estate->datums[target_dno + CURSOR_FOUND];
    target_cursor->value = source_cursor->value;
    target_cursor->isnull = source_cursor->isnull;

    source_cursor = (PLpgSQL_var*)estate->datums[source_dno + CURSOR_NOTFOUND];
    target_cursor = (PLpgSQL_var*)estate->datums[target_dno + CURSOR_NOTFOUND];
    target_cursor->value = source_cursor->value;
    target_cursor->isnull = source_cursor->isnull;

    source_cursor = (PLpgSQL_var*)estate->datums[source_dno + CURSOR_ROWCOUNT];
    target_cursor = (PLpgSQL_var*)estate->datums[target_dno + CURSOR_ROWCOUNT];
    target_cursor->value = source_cursor->value;
    target_cursor->isnull = source_cursor->isnull;
}
/* ----------
 * exec_assign_expr			Put an expression's result into a variable.
 * ----------
 */
static void exec_assign_expr(PLpgSQL_execstate* estate, PLpgSQL_datum* target, PLpgSQL_expr* expr)
{
    Datum value;
    Oid valtype;
    bool isnull = false;
    Cursor_Data* saved_cursor_data = estate->cursor_return_data;

    value = exec_eval_expr(estate, expr, &isnull, &valtype);

    /* copy cursor data to estate->datums */
    if (valtype == REFCURSOROID && target->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* var = (PLpgSQL_var*)target;
        int target_dno = var->dno;
        if (var->datatype->typoid == REFCURSOROID && expr->paramnos != NULL) {
            int source_dno = bms_first_member(expr->paramnos);
            if (source_dno >= 0) {
                int dtype = ((PLpgSQL_datum*)estate->datums[source_dno])->dtype;
                if (dtype == PLPGSQL_DTYPE_VAR || dtype == PLPGSQL_DTYPE_ROW) {
                    copy_cursor_var(estate, source_dno, target_dno);
                }
                expr->paramnos = bms_add_member(expr->paramnos, source_dno);
            }
        } else if (estate->cursor_return_data != NULL) {
            ExecCopyDataToDatum(estate->datums, target_dno, estate->cursor_return_data);
        }
    }
    exec_assign_value(estate, target, value, valtype, &isnull);
    if (saved_cursor_data != estate->cursor_return_data) {
        pfree_ext(estate->cursor_return_data);
        estate->cursor_return_data = saved_cursor_data;
    }
    exec_eval_cleanup(estate);
}

/* ----------
 * exec_assign_c_string		Put a C string into a text variable.
 *
 * We take a NULL pointer as signifying empty string, not SQL null.
 * ----------
 */
static void exec_assign_c_string(PLpgSQL_execstate* estate, PLpgSQL_datum* target, const char* str)
{
    text* value = NULL;
    bool isnull = false;

    if (str != NULL) {
        value = cstring_to_text(str);
    } else {
        value = cstring_to_text("");
    }
    exec_assign_value(estate, target, PointerGetDatum(value), TEXTOID, &isnull);
    pfree_ext(value);
}

/* ----------
 * exec_assign_value			Put a value into a target field
 *
 * Note: in some code paths, this will leak memory in the eval_econtext;
 * we assume that will be cleaned up later by exec_eval_cleanup.  We cannot
 * call exec_eval_cleanup here for fear of destroying the input Datum value.
 * ----------
 */
void exec_assign_value(PLpgSQL_execstate* estate, PLpgSQL_datum* target, Datum value, Oid valtype, bool* isNull)
{
    switch (target->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            /*
             * Target is a variable
             */
            PLpgSQL_var* var = (PLpgSQL_var*)target;
            Datum newvalue;

            newvalue = exec_cast_value(estate,
                value,
                valtype,
                var->datatype->typoid,
                &(var->datatype->typinput),
                var->datatype->typioparam,
                var->datatype->atttypmod,
                *isNull);

            if (*isNull && var->notnull) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_PLSQL),
                        errmsg("null value cannot be assigned to variable \"%s\" declared NOT NULL", var->refname)));
            }

            /*
             * If type is by-reference, copy the new value (which is
             * probably in the eval_econtext) into the procedure's memory
             * context.
             */
            if (!var->datatype->typbyval && !*isNull) {
                newvalue = datumCopy(newvalue, false, var->datatype->typlen);
            }

            /*
             * Now free the old value.	(We can't do this any earlier
             * because of the possibility that we are assigning the var's
             * old value to it, eg "foo := foo".  We could optimize out
             * the assignment altogether in such cases, but it's too
             * infrequent to be worth testing for.)
             */
            free_var(var);

            var->value = newvalue;
            var->isnull = *isNull;
            if (!var->datatype->typbyval && !*isNull) {
                var->freeval = true;
            }
            break;
        }

        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_RECORD: {
            /*
             * Target is a row variable
             */
            PLpgSQL_row* row = (PLpgSQL_row*)target;

            if (*isNull) {
                /* If source is null, just assign nulls to the row */
                exec_move_row(estate, NULL, row, NULL, NULL);
            } else {
                HeapTupleHeader td;
                Oid tupType;
                int32 tupTypmod;
                TupleDesc tupdesc;
                HeapTupleData tmptup;

                /* Source must be of RECORD or composite type */
                if (!type_is_rowtype(valtype)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("cannot assign non-composite value to a row variable")));
                }
                /* Source is a tuple Datum, so safe to do this: */
                td = DatumGetHeapTupleHeader(value);
                /* Extract rowtype info and find a tupdesc */
                tupType = HeapTupleHeaderGetTypeId(td);
                tupTypmod = HeapTupleHeaderGetTypMod(td);
                tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
                /* Build a temporary HeapTuple control structure */
                tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
                ItemPointerSetInvalid(&(tmptup.t_self));
                tmptup.t_tableOid = InvalidOid;
                tmptup.t_bucketId = InvalidBktId;
#ifdef PGXC
                tmptup.t_xc_node_id = 0;
#endif
                tmptup.t_data = td;
                exec_move_row(estate, NULL, row, &tmptup, tupdesc);
                ReleaseTupleDesc(tupdesc);
            }
            break;
        }

        case PLPGSQL_DTYPE_REC: {
            /*
             * Target is a record variable
             */
            PLpgSQL_rec* rec = (PLpgSQL_rec*)target;

            if (*isNull) {
                /* If source is null, just assign nulls to the record */
                exec_move_row(estate, rec, NULL, NULL, NULL);
            } else {
                HeapTupleHeader td;
                Oid tupType;
                int32 tupTypmod;
                TupleDesc tupdesc;
                HeapTupleData tmptup;

                /* Source must be of RECORD or composite type */
                if (!type_is_rowtype(valtype)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("cannot assign non-composite value to a record variable")));
                }

                /* Source is a tuple Datum, so safe to do this: */
                td = DatumGetHeapTupleHeader(value);
                /* Extract rowtype info and find a tupdesc */
                tupType = HeapTupleHeaderGetTypeId(td);
                tupTypmod = HeapTupleHeaderGetTypMod(td);
                tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
                /* Build a temporary HeapTuple control structure */
                tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
                ItemPointerSetInvalid(&(tmptup.t_self));
                tmptup.t_tableOid = InvalidOid;
                tmptup.t_bucketId = InvalidBktId;
#ifdef PGXC
                tmptup.t_xc_node_id = 0;
#endif
                tmptup.t_data = td;
                exec_move_row(estate, rec, NULL, &tmptup, tupdesc);
                ReleaseTupleDesc(tupdesc);
            }
            break;
        }

        case PLPGSQL_DTYPE_RECFIELD: {
            /*
             * Target is a field of a record
             */
            PLpgSQL_recfield* recfield = (PLpgSQL_recfield*)target;
            PLpgSQL_rec* rec = NULL;
            int fno;
            HeapTuple newtup = NULL;
            int natts;
            Datum* values = NULL;
            bool* nulls = NULL;
            bool* replaces = NULL;
            bool attisnull = false;
            Oid atttype;
            int32 atttypmod;
            errno_t rc = EOK;

            rec = (PLpgSQL_rec*)(estate->datums[recfield->recparentno]);

            /*
             * Check that there is already a tuple in the record. We need
             * that because records don't have any predefined field
             * structure.
             */
            if (!HeapTupleIsValid(rec->tup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }

            /*
             * Get the number of the records field to change and the
             * number of attributes in the tuple.  Note: disallow system
             * column names because the code below won't cope.
             */
            fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
            if (fno <= 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" has no field \"%s\" in assignment.", rec->refname, recfield->fieldname)));
            }
            fno--;
            natts = rec->tupdesc->natts;

            /*
             * Set up values/control arrays for heap_modify_tuple. For all
             * the attributes except the one we want to replace, use the
             * value that's in the old tuple.
             */
            values = (Datum*)palloc(sizeof(Datum) * natts);
            nulls = (bool*)palloc(sizeof(bool) * natts);
            replaces = (bool*)palloc(sizeof(bool) * natts);

            rc = memset_s(replaces, sizeof(bool) * natts, false, sizeof(bool) * natts);
            securec_check(rc, "\0", "\0");
            replaces[fno] = true;

            /*
             * Now insert the new value, being careful to cast it to the
             * right type.
             */
            atttype = SPI_gettypeid(rec->tupdesc, fno + 1);
            atttypmod = rec->tupdesc->attrs[fno]->atttypmod;
            attisnull = *isNull;
            values[fno] = exec_simple_cast_value(estate, value, valtype, atttype, atttypmod, attisnull);
            nulls[fno] = attisnull;

            /*
             * Now call heap_modify_tuple() to create a new tuple that
             * replaces the old one in the record.
             */
            newtup = heap_modify_tuple(rec->tup, rec->tupdesc, values, nulls, replaces);

            if (rec->freetup) {
                heap_freetuple_ext(rec->tup);
            }

            rec->tup = newtup;
            rec->freetup = true;

            pfree_ext(values);
            pfree_ext(nulls);
            pfree_ext(replaces);

            break;
        }

        case PLPGSQL_DTYPE_ARRAYELEM: {
            /*
             * Target is an element of an array
             */
            PLpgSQL_arrayelem* arrayelem = NULL;
            int nsubscripts;
            int i;
            PLpgSQL_expr* subscripts[MAXDIM];
            int subscriptvals[MAXDIM];
            Datum oldarraydatum, coerced_value;
            bool oldarrayisnull = false;
            Oid parenttypoid;
            int32 parenttypmod;
            ArrayType* oldarrayval = NULL;
            ArrayType* newarrayval = NULL;
            SPITupleTable* save_eval_tuptable = NULL;
            MemoryContext oldcontext = NULL;

            /*
             * We need to do subscript evaluation, which might require
             * evaluating general expressions; and the caller might have
             * done that too in order to prepare the input Datum.  We have
             * to save and restore the caller's SPI_execute result, if
             * any.
             */
            save_eval_tuptable = estate->eval_tuptable;
            estate->eval_tuptable = NULL;

            /*
             * To handle constructs like x[1][2] := something, we have to
             * be prepared to deal with a chain of arrayelem datums. Chase
             * back to find the base array datum, and save the subscript
             * expressions as we go.  (We are scanning right to left here,
             * but want to evaluate the subscripts left-to-right to
             * minimize surprises.)  Note that arrayelem is left pointing
             * to the leftmost arrayelem datum, where we will cache the
             * array element type data.
             */
            nsubscripts = 0;
            do {
                arrayelem = (PLpgSQL_arrayelem*)target;
                if (nsubscripts >= MAXDIM) {
                    ereport(ERROR,
                        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmodule(MOD_PLSQL),
                            errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d) in assignment.",
                                nsubscripts + 1,
                                MAXDIM)));
                }
                subscripts[nsubscripts++] = arrayelem->subscript;
                target = estate->datums[arrayelem->arrayparentno];
            } while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);

            /* Fetch current value of array datum */
            exec_eval_datum(estate, target, &parenttypoid, &parenttypmod, &oldarraydatum, &oldarrayisnull);

            /* Update cached type data if necessary */
            if (arrayelem->parenttypoid != parenttypoid || arrayelem->parenttypmod != parenttypmod) {
                Oid arraytypoid;
                int32 arraytypmod = parenttypmod;
                int16 arraytyplen;
                Oid elemtypoid;
                int16 elemtyplen;
                bool elemtypbyval = false;
                char elemtypalign;

                /* If target is domain over array, reduce to base type */
                arraytypoid = getBaseTypeAndTypmod(parenttypoid, &arraytypmod);

                /* ... and identify the element type */
                elemtypoid = get_element_type(arraytypoid);
                if (!OidIsValid(elemtypoid))
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("subscripted object in assignment is not an array")));

                /* Collect needed data about the types */
                arraytyplen = get_typlen(arraytypoid);

                get_typlenbyvalalign(elemtypoid, &elemtyplen, &elemtypbyval, &elemtypalign);

                /* Now safe to update the cached data */
                arrayelem->parenttypoid = parenttypoid;
                arrayelem->parenttypmod = parenttypmod;
                arrayelem->arraytypoid = arraytypoid;
                arrayelem->arraytypmod = arraytypmod;
                arrayelem->arraytyplen = arraytyplen;
                arrayelem->elemtypoid = elemtypoid;
                arrayelem->elemtyplen = elemtyplen;
                arrayelem->elemtypbyval = elemtypbyval;
                arrayelem->elemtypalign = elemtypalign;
            }

            /*
             * Evaluate the subscripts, switch into left-to-right order.
             * Like ExecEvalArrayRef(), complain if any subscript is null.
             */
            for (i = 0; i < nsubscripts; i++) {
                bool subisnull = false;

                subscriptvals[i] = exec_eval_integer(estate, subscripts[nsubscripts - 1 - i], &subisnull);
                if (subisnull) {
                    ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmodule(MOD_PLSQL),
                            errmsg("array subscript in assignment must not be null")));
                }

                /*
                 * Clean up in case the subscript expression wasn't
                 * simple. We can't do exec_eval_cleanup, but we can do
                 * this much (which is safe because the integer subscript
                 * value is surely pass-by-value), and we must do it in
                 * case the next subscript expression isn't simple either.
                 */
                if (estate->eval_tuptable != NULL) {
                    SPI_freetuptable(estate->eval_tuptable);
                }
                estate->eval_tuptable = NULL;
            }

            /* Now we can restore caller's SPI_execute result if any. */
            AssertEreport(estate->eval_tuptable == NULL, MOD_PLSQL, "eval tuptable should not be null");
            estate->eval_tuptable = save_eval_tuptable;

            /* Coerce source value to match array element type. */
            coerced_value =
                exec_simple_cast_value(estate, value, valtype, arrayelem->elemtypoid, arrayelem->arraytypmod, *isNull);

            /*
             * If the original array is null, cons up an empty array so
             * that the assignment can proceed; we'll end with a
             * one-element array containing just the assigned-to
             * subscript.  This only works for varlena arrays, though; for
             * fixed-length array types we skip the assignment.  We can't
             * support assignment of a null entry into a fixed-length
             * array, either, so that's a no-op too.  This is all ugly but
             * corresponds to the current behavior of ExecEvalArrayRef().
             */
            if (arrayelem->arraytyplen > 0 && /* fixed-length array? */
                (oldarrayisnull || *isNull)) {
                return;
            }

            /* oldarrayval and newarrayval should be short-lived */
            oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);

            if (oldarrayisnull) {
                oldarrayval = construct_empty_array(arrayelem->elemtypoid);
            } else {
                oldarrayval = (ArrayType*)DatumGetPointer(oldarraydatum);
            }

            /*
             * Build the modified array value.
             */
            newarrayval = array_set(oldarrayval,
                nsubscripts,
                subscriptvals,
                coerced_value,
                *isNull,
                arrayelem->arraytyplen,
                arrayelem->elemtyplen,
                arrayelem->elemtypbyval,
                arrayelem->elemtypalign);

            MemoryContextSwitchTo(oldcontext);

            if (oldarrayisnull) {
                pfree_ext(oldarrayval);
            }

            /*
             * Assign the new array to the base variable.  It's never NULL
             * at this point.  Note that if the target is a domain,
             * coercing the base array type back up to the domain will
             * happen within exec_assign_value.
             */
            *isNull = false;
            exec_assign_value(estate, target, PointerGetDatum(newarrayval), arrayelem->arraytypoid, isNull);
            break;
        }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized target data type: %d in assignment.", target->dtype)));
            break;
    }
}

/*
 * exec_eval_datum				Get current value of a PLpgSQL_datum
 *
 * The type oid, typmod, value in Datum format, and null flag are returned.
 *
 * At present this doesn't handle PLpgSQL_expr or PLpgSQL_arrayelem datums.
 *
 * NOTE: caller must not modify the returned value, since it points right
 * at the stored value in the case of pass-by-reference datatypes.	In some
 * cases we have to palloc a return value, and in such cases we put it into
 * the estate's short-term memory context.
 */
static void exec_eval_datum(
    PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typeId, int32* typetypmod, Datum* value, bool* isnull)
{
    MemoryContext oldcontext;

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;

            *typeId = var->datatype->typoid;
            *typetypmod = var->datatype->atttypmod;
            *value = var->value;
            *isnull = var->isnull;
            break;
        }
        case PLPGSQL_DTYPE_RECORD:
        case PLPGSQL_DTYPE_ROW: {
            PLpgSQL_row* row = (PLpgSQL_row*)datum;
            HeapTuple tup;

            if (!row->rowtupdesc) { /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_PLSQL),
                        errmsg("row variable has no tupdesc when evaluate datum")));
            }
            /* Make sure we have a valid type/typmod setting */
            BlessTupleDesc(row->rowtupdesc);
            oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
            tup = make_tuple_from_row(estate, row, row->rowtupdesc);
            if (tup == NULL) { /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_PLSQL),
                        errmsg("row not compatible with its own tupdesc when evaluate datum")));
            }
            MemoryContextSwitchTo(oldcontext);
            *typeId = row->rowtupdesc->tdtypeid;
            *typetypmod = row->rowtupdesc->tdtypmod;
            *value = HeapTupleGetDatum(tup);
            *isnull = false;
            break;
        }

        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* rec = (PLpgSQL_rec*)datum;
            HeapTupleData worktup;

            if (!HeapTupleIsValid(rec->tup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet when evaluate datum", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }
            AssertEreport(estate->eval_tuptable == NULL, MOD_PLSQL, "eval tuptable should not be null");
            /* Make sure we have a valid type/typmod setting */
            BlessTupleDesc(rec->tupdesc);

            /*
             * In a trigger, the NEW and OLD parameters are likely to be
             * on-disk tuples that don't have the desired Datum fields.
             * Copy the tuple body and insert the right values.
             */
            oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
            heap_copytuple_with_tuple(rec->tup, &worktup);
            HeapTupleHeaderSetDatumLength(worktup.t_data, worktup.t_len);
            HeapTupleHeaderSetTypeId(worktup.t_data, rec->tupdesc->tdtypeid);
            HeapTupleHeaderSetTypMod(worktup.t_data, rec->tupdesc->tdtypmod);
            MemoryContextSwitchTo(oldcontext);
            *typeId = rec->tupdesc->tdtypeid;
            *typetypmod = rec->tupdesc->tdtypmod;
            *value = HeapTupleGetDatum(&worktup);
            *isnull = false;
            break;
        }

        case PLPGSQL_DTYPE_RECFIELD: {
            PLpgSQL_recfield* recfield = (PLpgSQL_recfield*)datum;
            PLpgSQL_rec* rec = NULL;
            int fno;

            rec = (PLpgSQL_rec*)(estate->datums[recfield->recparentno]);
            if (!HeapTupleIsValid(rec->tup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet when evaluate datum", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }
            fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
            if (fno == SPI_ERROR_NOATTRIBUTE) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" has no field \"%s\" when evaluate datum",
                            rec->refname,
                            recfield->fieldname)));
            }
            *typeId = SPI_gettypeid(rec->tupdesc, fno);
            /* XXX there's no SPI_gettypmod, for some reason */
            if (fno > 0) {
                *typetypmod = rec->tupdesc->attrs[fno - 1]->atttypmod;
            } else {
                *typetypmod = -1;
            }
            *value = SPI_getbinval(rec->tup, rec->tupdesc, fno, isnull);
            break;
        }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized data type: %d when evaluate datum", datum->dtype)));
            break;
    }
}

/*
 * exec_get_datum_type				Get datatype of a PLpgSQL_datum
 *
 * This is the same logic as in exec_eval_datum, except that it can handle
 * some cases where exec_eval_datum has to fail; specifically, we may have
 * a tupdesc but no row value for a record variable.  (This currently can
 * happen only for a trigger's NEW/OLD records.)
 */
Oid exec_get_datum_type(PLpgSQL_execstate* estate, PLpgSQL_datum* datum)
{
    Oid typeId;

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;

            typeId = var->datatype->typoid;
            break;
        }

        case PLPGSQL_DTYPE_ROW: {
            PLpgSQL_row* row = (PLpgSQL_row*)datum;

            if (!row->rowtupdesc) /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_PLSQL),
                        errmsg("row variable has no tupdesc when get datum type")));
            /* Make sure we have a valid type/typmod setting */
            BlessTupleDesc(row->rowtupdesc);
            typeId = row->rowtupdesc->tdtypeid;
            break;
        }

        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* rec = (PLpgSQL_rec*)datum;

            if (rec->tupdesc == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet when get datum type", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }
            /* Make sure we have a valid type/typmod setting */
            BlessTupleDesc(rec->tupdesc);
            typeId = rec->tupdesc->tdtypeid;
            break;
        }

        case PLPGSQL_DTYPE_RECFIELD: {
            PLpgSQL_recfield* recfield = (PLpgSQL_recfield*)datum;
            PLpgSQL_rec* rec = NULL;
            int fno;

            rec = (PLpgSQL_rec*)(estate->datums[recfield->recparentno]);
            if (rec->tupdesc == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet when get datum type", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }
            fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
            if (fno == SPI_ERROR_NOATTRIBUTE) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" has no field \"%s\" when get datum type",
                            rec->refname,
                            recfield->fieldname)));
            }
            typeId = SPI_gettypeid(rec->tupdesc, fno);
            break;
        }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized data type: %d when get datum type", datum->dtype)));
            typeId = InvalidOid; /* keep compiler quiet */
            break;
    }

    return typeId;
}

/*
 * exec_get_datum_type_info			Get datatype etc of a PLpgSQL_datum
 *
 * An extended version of exec_get_datum_type, which also retrieves the
 * typmod and collation of the datum.
 */
void exec_get_datum_type_info(
    PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typeId, int32* typmod, Oid* collation, PLpgSQL_function* func)
{
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;

            *typeId = var->datatype->typoid;
            *typmod = var->datatype->atttypmod;
            *collation = var->datatype->collation;
            break;
        }

        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_RECORD: {
            PLpgSQL_row* row = (PLpgSQL_row*)datum;

            if (!row->rowtupdesc) { /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_PLSQL),
                        errmsg("row variable has no tupdesc when get datum type info")));
            }
            /* Make sure we have a valid type/typmod setting */
            BlessTupleDesc(row->rowtupdesc);
            *typeId = row->rowtupdesc->tdtypeid;
            /* do NOT return the mutable typmod of a RECORD variable */
            *typmod = -1;
            /* composite types are never collatable */
            *collation = InvalidOid;
            break;
        }

        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* rec = (PLpgSQL_rec*)datum;

            if (rec->tupdesc == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet when get datum type info", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }
            /* Make sure we have a valid type/typmod setting */
            BlessTupleDesc(rec->tupdesc);
            *typeId = rec->tupdesc->tdtypeid;
            /* do NOT return the mutable typmod of a RECORD variable */
            *typmod = -1;
            /* composite types are never collatable */
            *collation = InvalidOid;
            break;
        }

        case PLPGSQL_DTYPE_RECFIELD: {
            PLpgSQL_recfield* recfield = (PLpgSQL_recfield*)datum;
            PLpgSQL_rec* rec = NULL;
            int fno;

            if (estate != NULL) {
                rec = (PLpgSQL_rec*)(estate->datums[recfield->recparentno]);
            } else {
                rec = (PLpgSQL_rec*)(func->datums[recfield->recparentno]);
            }

            if (rec->tupdesc == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" is not assigned yet when get datum type info", rec->refname),
                        errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
            }
            fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
            if (fno == SPI_ERROR_NOATTRIBUTE) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmodule(MOD_PLSQL),
                        errmsg("record \"%s\" has no field \"%s\" when get datum type info",
                            rec->refname,
                            recfield->fieldname)));
            }
            *typeId = SPI_gettypeid(rec->tupdesc, fno);
            /* XXX there's no SPI_gettypmod, for some reason */
            if (fno > 0) {
                *typmod = rec->tupdesc->attrs[fno - 1]->atttypmod;
            } else {
                *typmod = -1;
            }
            /* XXX there's no SPI_getcollation either */
            if (fno > 0) {
                *collation = rec->tupdesc->attrs[fno - 1]->attcollation;
            } else {/* no system column types have collation */
                *collation = InvalidOid;
            }
            break;
        }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLSQL),
                    errmsg("unrecognized data type: %d when get datum type", datum->dtype)));
            *typeId = InvalidOid; /* keep compiler quiet */
            *typmod = -1;
            *collation = InvalidOid;
            break;
    }
}

/* ----------
 * exec_eval_integer		Evaluate an expression, coerce result to int4
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.  (We do this because the caller may be holding the
 * results of other, pass-by-reference, expression evaluations, such as
 * an array value to be subscripted.  Also see notes in exec_eval_simple_expr
 * about allocation of the parameter array.)
 * ----------
 */
static int exec_eval_integer(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull)
{
    Datum exprdatum;
    Oid exprtypeid;

    exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid);
    exprdatum = exec_simple_cast_value(estate, exprdatum, exprtypeid, INT4OID, -1, *isNull);
    return DatumGetInt32(exprdatum);
}

/* ----------
 * exec_eval_boolean		Evaluate an expression, coerce result to bool
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.
 * ----------
 */
static bool exec_eval_boolean(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull)
{
    Datum exprdatum;
    Oid exprtypeid;

    exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid);
    exprdatum = exec_simple_cast_value(estate, exprdatum, exprtypeid, BOOLOID, -1, *isNull);
    return DatumGetBool(exprdatum);
}

/* ----------
 * exec_eval_expr			Evaluate an expression and return
 *					the result Datum.
 *
 * NOTE: caller must do exec_eval_cleanup when done with the Datum.
 * ----------
 */
static Datum exec_eval_expr(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull, Oid* rettype)
{
    Datum result = 0;
    int rc;
    // forbid commit/rollback in the stp which is called to get value
    bool savedisAllowCommitRollback = false;
    bool needResetErrMsg = false;
    needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_USED_AS_EXPR);

    /*
     * If first time through, create a plan for this expression.
     */
    if (expr->plan == NULL) {
        exec_prepare_plan(estate, expr, 0);
    }
    if (ENABLE_CN_GPC && g_instance.plan_cache->CheckRecreateSPICachePlan(expr->plan)) {
        g_instance.plan_cache->RecreateSPICachePlan(expr->plan);
        expr->is_cachedplan_shared = false;
        expr->expr_simple_generation = 0;
    }

    /*
     * If this is a simple expression, bypass SPI and use the executor
     * directly
     */
    if (exec_eval_simple_expr(estate, expr, &result, isNull, rettype)) {
        stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);
        return result;
    }

    /*
     * Else do it the hard way via exec_run_select
     */
    rc = exec_run_select(estate, expr, 2, NULL);
    stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);

    if (rc != SPI_OK_SELECT) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("query \"%s\" did not return data when evaluate expression", expr->query)));
    }

    /*
     * Check that the expression returns exactly one column...
     */
    if (estate->eval_tuptable->tupdesc->natts != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_PLSQL),
                errmsg_plural("query \"%s\" returned %d column",
                    "query \"%s\" returned %d columns",
                    estate->eval_tuptable->tupdesc->natts,
                    expr->query,
                    estate->eval_tuptable->tupdesc->natts)));
    }

    /*
     * ... and get the column's datatype.
     */
    *rettype = SPI_gettypeid(estate->eval_tuptable->tupdesc, 1);

    /*
     * If there are no rows selected, the result is a NULL of that type.
     */
    if (estate->eval_processed == 0) {
        *isNull = true;
        return (Datum)0;
    }

    /*
     * Check that the expression returned no more than one row.
     */
    if (estate->eval_processed != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_CARDINALITY_VIOLATION),
                errmodule(MOD_PLSQL),
                errmsg("query \"%s\" returned more than one row (%u rows)when evaluate expression",
                    expr->query,
                    estate->eval_processed)));
    }

    /*
     * Return the single result Datum.
     */
    return SPI_getbinval(estate->eval_tuptable->vals[0], estate->eval_tuptable->tupdesc, 1, isNull);
}

/* ----------
 * exec_run_select			Execute a select query
 * ----------
 */
static int exec_run_select(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, long maxtuples, Portal* portalP)
{
    ParamListInfo paramLI;
    int rc;

    /*
     * On the first call for this expression generate the plan
     */
    if (expr->plan == NULL) {
        exec_prepare_plan(estate, expr, 0);
    }
    if (ENABLE_CN_GPC && g_instance.plan_cache->CheckRecreateSPICachePlan(expr->plan)) {
        g_instance.plan_cache->RecreateSPICachePlan(expr->plan);
    }
#ifndef ENABLE_MULTIPLE_NODES
    /* autonomous transaction */
    PLpgSQL_exectype etype = (maxtuples == 0 && portalP == NULL) ? STMT_PERFORM : STMT_UNKNOW;
    if (estate->autonomous_session && IsValidAutonomousTransactionQuery(etype, expr, false)) {
        ATResult atresult =  estate->autonomous_session->ExecSimpleQuery(expr->query);
        exec_set_found(estate, atresult.withtuple);
        return PLPGSQL_RC_OK;
    }
#endif

    /*
     * Set up ParamListInfo (hook function and possibly data values)
     */
    paramLI = setup_param_list(estate, expr);

    /*
     * If a portal was requested, put the query into the portal
     */
    if (portalP != NULL) {
        *portalP = SPI_cursor_open_with_paramlist(NULL, expr->plan, paramLI, estate->readonly_func);
        if (*portalP == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_CURSOR),
                    errmodule(MOD_PLSQL),
                    errmsg("could not open implicit cursor for query \"%s\": %s",
                        expr->query,
                        SPI_result_code_string(SPI_result))));
        }
        pfree_ext(paramLI);
        return SPI_OK_CURSOR;
    }

    plpgsql_estate = estate;
    MemoryContext oldcontext = estate->tuple_store_cxt;
    if (estate->tuple_store_cxt == NULL) {
        estate->tuple_store_cxt = CurrentMemoryContext;
    }

    /*
     * Execute the query
     */
    rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI, estate->readonly_func, maxtuples);
    plpgsql_estate = NULL;
    if (rc != SPI_OK_SELECT) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmodule(MOD_PLSQL), errmsg("query \"%s\" is not a SELECT", expr->query)));
    }

    /* Save query results for eventual cleanup */
    AssertEreport(estate->eval_tuptable == NULL, MOD_PLSQL, "eval tuptable should be null");
    estate->eval_tuptable = SPI_tuptable;
    estate->eval_processed = SPI_processed;
    estate->eval_lastoid = u_sess->SPI_cxt.lastoid;

    pfree_ext(paramLI);

    estate->tuple_store_cxt = oldcontext;

    return rc;
}

/*
 * exec_for_query --- execute body of FOR loop for each row from a portal
 *
 * Used by exec_stmt_fors, exec_stmt_forc and exec_stmt_dynfors
 */
static int exec_for_query(PLpgSQL_execstate* estate, PLpgSQL_stmt_forq* stmt, Portal portal, bool prefetch_ok, int dno)
{
    PLpgSQL_rec* rec = NULL;
    PLpgSQL_row* row = NULL;
    SPITupleTable* tuptab = NULL;
    bool found = false;
    int rc = PLPGSQL_RC_OK;
    int n;

    /*
     * Determine if we assign to a record or a row
     */
    if (stmt->rec != NULL) {
        rec = (PLpgSQL_rec*)(estate->datums[stmt->rec->dno]);
    } else if (stmt->row != NULL) {
        row = (PLpgSQL_row*)(estate->datums[stmt->row->dno]);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("unsupported target in FOP LOOP. use record or row instead.")));
    }

    /*
     * Make sure the portal doesn't get closed by the user statements we
     * execute.
     */
    PinPortal(portal);

    /*
     * Fetch the initial tuple(s).	If prefetching is allowed then we grab a
     * few more rows to avoid multiple trips through executor startup
     * overhead.
     */
    SPI_cursor_fetch(portal, true, prefetch_ok ? 10 : 1);
    tuptab = SPI_tuptable;
    n = SPI_processed;

    /*
     * If the query didn't return any rows, set the target to NULL and fall
     * through with found = false.
     */
    if (n <= 0) {
        exec_move_row(estate, rec, row, NULL, tuptab->tupdesc);
        exec_eval_cleanup(estate);
    } else {
        found = true; /* processed at least one tuple */
    }

    /*
     * Now do the loop
     */
    while (n > 0) {
        int i;

        for (i = 0; i < n; i++) {
            if (dno > 0) {
                exec_set_cursor_found(estate, PLPGSQL_TRUE, dno + CURSOR_FOUND);
                exec_set_notfound(estate, PLPGSQL_FALSE, dno + CURSOR_NOTFOUND);
                exec_set_isopen(estate, true, dno + CURSOR_ISOPEN);
                exec_set_rowcount(estate, 1, false, dno + CURSOR_ROWCOUNT);
            }
            exec_set_sql_cursor_found(estate, PLPGSQL_TRUE);
            exec_set_sql_notfound(estate, PLPGSQL_FALSE);
            exec_set_sql_isopen(estate, true);
            exec_set_sql_rowcount(estate, 1);
            /*
             * Assign the tuple to the target
             */
            exec_move_row(estate, rec, row, tuptab->vals[i], tuptab->tupdesc);
            exec_eval_cleanup(estate);

            /*
             * Execute the statements
             */
            rc = exec_stmts(estate, stmt->body);

            if (rc != PLPGSQL_RC_OK) {
                if (rc == PLPGSQL_RC_EXIT) {
                    if (estate->exitlabel == NULL) {
                        /* unlabelled exit, so exit the current loop */
                        rc = PLPGSQL_RC_OK;
                    } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                        /* label matches this loop, so exit loop */
                        estate->exitlabel = NULL;
                        rc = PLPGSQL_RC_OK;
                    }

                    /*
                     * otherwise, we processed a labelled exit that does not
                     * match the current statement's label, if any; return
                     * RC_EXIT so that the EXIT continues to recurse upward.
                     */
                } else if (rc == PLPGSQL_RC_CONTINUE) {
                    if (estate->exitlabel == NULL) {
                        /* unlabelled continue, so re-run the current loop */
                        rc = PLPGSQL_RC_OK;
                        continue;
                    } else if (stmt->label != NULL && strcmp(stmt->label, estate->exitlabel) == 0) {
                        /* label matches this loop, so re-run loop */
                        estate->exitlabel = NULL;
                        rc = PLPGSQL_RC_OK;
                        continue;
                    }

                    /*
                     * otherwise, we process a labelled continue that does not
                     * match the current statement's label, if any; return
                     * RC_CONTINUE so that the CONTINUE will propagate up the
                     * stack.
                     */
                }

                /*
                 * We're aborting the loop.  Need a goto to get out of two
                 * levels of loop...
                 */
                goto loop_exit;
            }
        }

        SPI_freetuptable(tuptab);

        /*
         * Fetch more tuples.  If prefetching is allowed, grab 50 at a time.
         */
        SPI_cursor_fetch(portal, true, prefetch_ok ? 50 : 1);
        tuptab = SPI_tuptable;
        n = SPI_processed;
    }

loop_exit:

    /*
     * Release last group of tuples (if any)
     */
    SPI_freetuptable(tuptab);

    UnpinPortal(portal);

    /*
     * Set the FOUND variable to indicate the result of executing the loop
     * (namely, whether we looped one or more times). This must be set last so
     * that it does not interfere with the value of the FOUND variable inside
     * the loop processing itself.
     */
    exec_set_found(estate, found);

    return rc;
}

/* ----------
 * exec_eval_simple_expr -		Evaluate a simple expression returning
 *								a Datum by directly calling ExecEvalExpr().
 *
 * If successful, store results into *result, *isNull, *rettype and return
 * TRUE.  If the expression cannot be handled by simple evaluation,
 * return FALSE.
 *
 * Because we only store one execution tree for a simple expression, we
 * can't handle recursion cases.  So, if we see the tree is already busy
 * with an evaluation in the current xact, we just return FALSE and let the
 * caller run the expression the hard way.	(Other alternatives such as
 * creating a new tree for a recursive call either introduce memory leaks,
 * or add enough bookkeeping to be doubtful wins anyway.)  Another case that
 * is covered by the expr_simple_in_use test is where a previous execution
 * of the tree was aborted by an error: the tree may contain bogus state
 * so we dare not re-use it.
 *
 * It is possible though unlikely for a simple expression to become non-simple
 * (consider for example redefining a trivial view).  We must handle that for
 * correctness; fortunately it's normally inexpensive to call
 * SPI_plan_get_cached_plan for a simple expression.  We do not consider the
 * other direction (non-simple expression becoming simple) because we'll still
 * give correct results if that happens, and it's unlikely to be worth the
 * cycles to check.
 *
 * Note: if pass-by-reference, the result is in the eval_econtext's
 * temporary memory context.  It will be freed when exec_eval_cleanup
 * is done.
 * ----------
 */
static bool exec_eval_simple_expr(
    PLpgSQL_execstate* estate, PLpgSQL_expr* expr, Datum* result, bool* isNull, Oid* rettype)
{
    ExprContext* econtext = estate->eval_econtext;
    LocalTransactionId curlxid = t_thrd.proc->lxid;
    CachedPlan* cplan = NULL;
    ParamListInfo paramLI = NULL;
    PLpgSQL_expr* save_cur_expr = NULL;
    MemoryContext oldcontext = NULL;

    /*
     * Forget it if expression wasn't simple before.
     */
    if (expr->expr_simple_expr == NULL) {
        return false;
    }

    /*
     * If expression is in use in current xact, don't touch it.
     */
    if (expr->expr_simple_in_use && expr->expr_simple_lxid == curlxid) {
        return false;
    }

    /*
     * Revalidate cached plan, so that we will notice if it became stale. (We
     * need to hold a refcount while using the plan, anyway.)
     */
    cplan = SPI_plan_get_cached_plan(expr->plan);

    /*
     * We can't get a failure here, because the number of CachedPlanSources in
     * the SPI plan can't change from what exec_simple_check_plan saw; it's a
     * property of the raw parsetree generated from the query text.
     */
    AssertEreport(estate->eval_tuptable == NULL, MOD_PLSQL, "eval tuptable should be null");

    if (ENABLE_CN_GPC && !expr->is_cachedplan_shared && cplan->isShared()) {
        exec_simple_recheck_plan(expr, cplan);
        if (expr->expr_simple_expr == NULL) {
            /* Ooops, release refcount and fail */
            ReleaseCachedPlan(cplan, true);
            return false;
        }
    }
    if (cplan->generation != expr->expr_simple_generation) {
        /* It got replanned ... is it still simple? */
        exec_simple_recheck_plan(expr, cplan);
        if (expr->expr_simple_expr == NULL) {
            /* Ooops, release refcount and fail */
            ReleaseCachedPlan(cplan, true);
            return false;
        }
    }
    /*
     * Pass back previously-determined result type.
     */
    *rettype = expr->expr_simple_type;

    /*
     * Prepare the expression for execution, if it's not been done already in
     * the current transaction.  (This will be forced to happen if we called
     * exec_simple_recheck_plan above.)
     */
    if (expr->expr_simple_lxid != curlxid) {
        oldcontext = MemoryContextSwitchTo(u_sess->plsql_cxt.simple_eval_estate->es_query_cxt);
        expr->expr_simple_state = ExecInitExpr(expr->expr_simple_expr, NULL);
        expr->expr_simple_in_use = false;
        expr->expr_simple_lxid = curlxid;
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * We have to do some of the things SPI_execute_plan would do, in
     * particular advance the snapshot if we are in a non-read-only function.
     * Without this, stable functions within the expression would fail to see
     * updates made so far by our own function.
     * GTM snapshot in distributed database hurts performance.
     * Do some optimizations here:
     * Previously, retrieving snapshot is behavior of function level. It means
     * simple expression in volatile function like 'a + 1' will also retrieve snapshot.
     * Some fine-grained control for each simple expression: if it doesn't have
     * functions or non-immutable operator functions. doesn't need snapshot.
     * Because functions can be defined by applications, it may need snapshot,
     * not support it now.
     */
    SPI_push();

    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    if (!estate->readonly_func && expr->expr_simple_need_snapshot) {
        CommandCounterIncrement();
        PushActiveSnapshot(GetTransactionSnapshot());
    }

    /*
     * Create the param list in econtext's temporary memory context. We won't
     * need to free it explicitly, since it will go away at the next reset of
     * that context.
     *
     * Just for paranoia's sake, save and restore the prior value of
     * estate->cur_expr, which setup_param_list() sets.
     */
    save_cur_expr = estate->cur_expr;

    paramLI = setup_param_list(estate, expr);
    econtext->ecxt_param_list_info = paramLI;

    /*
     * Mark expression as busy for the duration of the ExecEvalExpr call.
     */
    expr->expr_simple_in_use = true;

    /*
     * Finally we can call the executor to evaluate the expression
     */
    plpgsql_estate = estate;
    if (expr->expr_simple_state->resultType == REFCURSOROID && paramLI != NULL) {
        econtext->is_cursor = true;
    }
    *result = ExecEvalExpr(expr->expr_simple_state, econtext, isNull, NULL);
    econtext->is_cursor = false;

    plpgsql_estate = NULL;
    /* Assorted cleanup */
    expr->expr_simple_in_use = false;

    estate->cur_expr = save_cur_expr;

    if (!estate->readonly_func && expr->expr_simple_need_snapshot) {
        PopActiveSnapshot();
    }

    MemoryContextSwitchTo(oldcontext);

    SPI_pop();

    /*
     * Now we can release our refcount on the cached plan.
     */
    ReleaseCachedPlan(cplan, true);

    /*
     * That's it.
     */
    return true;
}

/*
 * Create a ParamListInfo to pass to SPI
 *
 * We fill in the values for any expression parameters that are plain
 * PLpgSQL_var datums; these are cheap and safe to evaluate, and by setting
 * them with PARAM_FLAG_CONST flags, we allow the planner to use those values
 * in custom plans.  However, parameters that are not plain PLpgSQL_vars
 * should not be evaluated here, because they could throw errors (for example
 * "no such record field") and we do not want that to happen in a part of
 * the expression that might never be evaluated at runtime.  To handle those
 * parameters, we set up a paramFetch hook for the executor to call when it
 * wants a not-presupplied value.
 *
 * The result is a locally palloc'd array that should be pfree'd after use;
 * but note it can be NULL.
 */
static ParamListInfo setup_param_list(PLpgSQL_execstate* estate, PLpgSQL_expr* expr)
{
    ParamListInfo paramLI;

    /*
     * We must have created the SPIPlan already (hence, query text has been
     * parsed/analyzed at least once); else we cannot rely on expr->paramnos.
     */
    AssertEreport(expr->plan != NULL, MOD_PLSQL, "plan should not be null");

    /*
     * Could we re-use these arrays instead of palloc'ing a new one each time?
     * However, we'd have to re-fill the array each time anyway, since new
     * values might have been assigned to the variables.
     */
    if (!bms_is_empty(expr->paramnos)) {
        Bitmapset* tmpset = NULL;
        int dno;

        paramLI =
            (ParamListInfo)palloc0(offsetof(ParamListInfoData, params) + estate->ndatums * sizeof(ParamExternData));
        paramLI->paramFetch = plpgsql_param_fetch;
        paramLI->paramFetchArg = (void*)estate;
        paramLI->parserSetup = (ParserSetupHook)plpgsql_parser_setup;
        paramLI->parserSetupArg = (void*)expr;
        paramLI->params_need_process = false;
        paramLI->numParams = estate->ndatums;

        /* Instantiate values for "safe" parameters of the expression */
        tmpset = bms_copy(expr->paramnos);
        while ((dno = bms_first_member(tmpset)) >= 0) {
            PLpgSQL_datum* datum = estate->datums[dno];

            if (datum->dtype == PLPGSQL_DTYPE_VAR) {
                PLpgSQL_var* var = (PLpgSQL_var*)datum;
                ParamExternData* prm = &paramLI->params[dno];

                prm->value = var->value;
                prm->isnull = var->isnull;
                prm->pflags = PARAM_FLAG_CONST;
                prm->ptype = var->datatype->typoid;

                /* cursor as a parameter, its option is also needed */
                if (var->datatype->typoid == REFCURSOROID) {
                    ExecCopyDataFromDatum(estate->datums, dno, &prm->cursor_data);
                }
            }
        }
        bms_free_ext(tmpset);

        /*
         * Set up link to active expr where the hook functions can find it.
         * Callers must save and restore cur_expr if there is any chance that
         * they are interrupting an active use of parameters.
         */
        estate->cur_expr = expr;

        /*
         * Also make sure this is set before parser hooks need it.	There is
         * no need to save and restore, since the value is always correct once
         * set.  (Should be set already, but let's be sure.)
         */
        expr->func = estate->func;
    } else {
        /*
         * Expression requires no parameters.  Be sure we represent this case
         * as a NULL ParamListInfo, so that plancache.c knows there is no
         * point in a custom plan.
         */
        paramLI = NULL;
    }
    return paramLI;
}

/*
 * plpgsql_param_fetch		paramFetch callback for dynamic parameter fetch
 */
static void plpgsql_param_fetch(ParamListInfo params, int paramid)
{
    int dno;
    PLpgSQL_execstate* estate = NULL;
    PLpgSQL_expr* expr = NULL;
    PLpgSQL_datum* datum = NULL;
    ParamExternData* prm = NULL;
    int32 prmtypmod;

    /* paramid's are 1-based, but dnos are 0-based */
    dno = paramid - 1;
    AssertEreport(dno >= 0 && dno < params->numParams, MOD_PLSQL, "dno is out or range.");

    /* fetch back the hook data */
    estate = (PLpgSQL_execstate*)params->paramFetchArg;
    expr = estate->cur_expr;
    AssertEreport(params->numParams == estate->ndatums, MOD_PLSQL, "It should be same para num.");

    /*
     * Do nothing if asked for a value that's not supposed to be used by this
     * SQL expression.	This avoids unwanted evaluations when functions such
     * as copyParamList try to materialize all the values.
     */
    if (!bms_is_member(dno, expr->paramnos)) {
        return;
    }

    /* OK, evaluate the value and store into the appropriate paramlist slot */
    datum = estate->datums[dno];
    prm = &params->params[dno];
    exec_eval_datum(estate, datum, &prm->ptype, &prmtypmod, &prm->value, &prm->isnull);
}

/*
 * Description:

 * Parameters: check the row's variable is a refcursor or not.
 * @in row: to row to check.
 * @in valtype: variable's type.
 * @in fnum: the attribute index of the row.
 * Return: True if current column is refcursor.
 */
FORCE_INLINE
static bool CheckTypeIsCursor(PLpgSQL_row *row, Oid valtype, int fnum)
{
    if (valtype == REFCURSOROID && row->rowtupdesc == NULL) {
        return true;
    }

    if (valtype == REFCURSOROID && row->rowtupdesc != NULL) {
        if (row->rowtupdesc->attrs == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("Accessing unexpected null value when checking row type.")));
        }
        if (row->rowtupdesc->attrs[fnum]->atttypid == REFCURSOROID) {
            return true;
        }
    }

    return false;
}

/* ----------
 * exec_move_row			Move one tuple's values into a record or row
 *
 * Since this uses exec_assign_value, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 * ----------
 */
static void exec_move_row(
    PLpgSQL_execstate* estate, PLpgSQL_rec* rec, PLpgSQL_row* row, HeapTuple tup, TupleDesc tupdesc)
{
    /*
     * Record is simple - just copy the tuple and its descriptor into the
     * record variable
     */
    if (rec != NULL) {
        /*
         * Copy input first, just in case it is pointing at variable's value
         */
        if (HeapTupleIsValid(tup)) {
            tup = heap_copytuple(tup);
        } else if (tupdesc) {
            /* If we have a tupdesc but no data, form an all-nulls tuple */
            bool* nulls = NULL;
            errno_t rc = EOK;

            nulls = (bool*)palloc(tupdesc->natts * sizeof(bool));
            rc = memset_s(nulls, tupdesc->natts * sizeof(bool), true, tupdesc->natts * sizeof(bool));
            securec_check(rc, "\0", "\0");

            tup = heap_form_tuple(tupdesc, NULL, nulls);

            pfree_ext(nulls);
        }

        if (tupdesc) {
            tupdesc = CreateTupleDescCopy(tupdesc);
        }

        /* Free the old value ... */
        if (rec->freetup) {
            heap_freetuple_ext(rec->tup);
            rec->freetup = false;
        }
        if (rec->freetupdesc) {
            FreeTupleDesc(rec->tupdesc);
            rec->freetupdesc = false;
        }

        /* ... and install the new */
        if (HeapTupleIsValid(tup)) {
            rec->tup = tup;
            rec->freetup = true;
        } else {
            rec->tup = NULL;
        }

        if (tupdesc) {
            rec->tupdesc = tupdesc;
            rec->freetupdesc = true;
        } else {
            rec->tupdesc = NULL;
        }

        return;
    }

    /*
     * Row is a bit more complicated in that we assign the individual
     * attributes of the tuple to the variables the row points to.
     *
     * NOTE: this code used to demand row->nfields ==
     * HeapTupleHeaderGetNatts(tup->t_data, tupdesc), but that's wrong.  The tuple
     * might have more fields than we expected if it's from an
     * inheritance-child table of the current table, or it might have fewer if
     * the table has had columns added by ALTER TABLE. Ignore extra columns
     * and assume NULL for missing columns, the same as heap_getattr would do.
     * We also have to skip over dropped columns in either the source or
     * destination.
     *
     * If we have no tuple data at all, we'll assign NULL to all columns of
     * the row variable.
     */
    if (row != NULL) {
        int td_natts = tupdesc ? tupdesc->natts : 0;
        int t_natts;
        int fnum;
        int anum;

        if (HeapTupleIsValid(tup)) {
            t_natts = HeapTupleHeaderGetNatts(tup->t_data, tupdesc);
        } else {
            t_natts = 0;
        }

        if (row->dtype == PLPGSQL_DTYPE_RECORD) {
            int m_natts = 0;

            for (int i = 0; i < td_natts; i++) {
                if (!tupdesc->attrs[i]->attisdropped) {
                    m_natts++; /* skip dropped column in tuple */
                }
            }

            if (m_natts != 0 && m_natts != row->nfields) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_PLSQL),
                        errmsg("mismatch between assignment and variable filed.")));
            }
        }

        anum = 0;

        /* for one dynamic statement only */
        if (row->nfields > 0) {
            Oid tupTypeOid = (tupdesc != NULL && tupdesc->attrs != NULL) ? tupdesc->attrs[0]->atttypid : InvalidOid;
            Oid rowTypeOid = (row->rowtupdesc != NULL) ? row->rowtupdesc->tdtypeid : InvalidOid;

            bool needSplitByNattrs = (td_natts == 1 && row->nfields > 1) || (td_natts == 1 && row->nfields == 1);
            bool needSplitByType = tupTypeOid == rowTypeOid && tupTypeOid != InvalidOid;
            /*
             * in this case, we have a tuple of a composite type and its type is same as
             * row type(in rowtupdesc), but the row has more attrs than tuple, so we need
             * to split the tuple using exec_assign_value.
             */
            if (needSplitByNattrs && needSplitByType && tup != NULL && get_typtype(tupTypeOid) == TYPTYPE_COMPOSITE) {
                Datum value;
                bool isnull = false;
                Oid valtype;

                value = SPI_getbinval(tup, tupdesc, 1, &isnull);
                valtype = SPI_gettypeid(tupdesc, 1);
                exec_assign_value(estate, (PLpgSQL_datum*)row, value, valtype, &isnull);
            } else {
                /* original way */
                for (fnum = 0; fnum < row->nfields; fnum++) {
                    PLpgSQL_var* var = NULL;
                    Datum value;
                    bool isnull = false;
                    Oid valtype;

                    if (row->varnos[fnum] < 0) {
                        continue; /* skip dropped column in row struct */
                    }

                    var = (PLpgSQL_var*)(estate->datums[row->varnos[fnum]]);

                    while (anum < td_natts && tupdesc->attrs[anum]->attisdropped) {
                        anum++; /* skip dropped column in tuple */
                    }

                    if (anum < td_natts) {
                        if (anum < t_natts) {
                            value = SPI_getbinval(tup, tupdesc, anum + 1, &isnull);
                        } else {
                            value = (Datum)0;
                            isnull = true;
                        }
                        valtype = SPI_gettypeid(tupdesc, anum + 1);
                        anum++;
                    } else {
                        value = (Datum)0;
                        isnull = true;

                        /*
                         * InvalidOid is OK because exec_assign_value doesn't care
                         * about the type of a source NULL
                         */
                        valtype = InvalidOid;
                    }

                    /* accept function's return value for cursor */
                    if (isnull == false && CheckTypeIsCursor(row, valtype, fnum) &&
                        estate->cursor_return_data != NULL) {
                        ExecCopyDataToDatum(estate->datums, var->dno, &estate->cursor_return_data[fnum]);
                    }

                    exec_assign_value(estate, (PLpgSQL_datum*)var, value, valtype, &isnull);
                }
            }
        } else if (row->intoplaceholders > 0 && row->intodatums != NULL) {
			/* for anonymous block, we need set those output parameters to intodatum list */
            for (fnum = 0; fnum < row->intoplaceholders; fnum++) {
                PLpgSQL_var* var = NULL;
                Datum value;
                bool isnull = false;
                Oid valtype;

                var = (PLpgSQL_var*)(row->intodatums[fnum]);

                while (anum < td_natts && tupdesc->attrs[anum]->attisdropped) {
                    anum++; /* skip dropped column in tuple */
                }

                if (anum < td_natts) {
                    if (anum < t_natts) {
                        value = SPI_getbinval(tup, tupdesc, anum + 1, &isnull);
                    } else {
                        value = (Datum)0;
                        isnull = true;
                    }
                    valtype = SPI_gettypeid(tupdesc, anum + 1);
                    anum++;
                } else {
                    value = (Datum)0;
                    isnull = true;

                    /*
                     * InvalidOid is OK because exec_assign_value doesn't care
                     * about the type of a source NULL
                     */
                    valtype = InvalidOid;
                }

                exec_assign_value(estate, (PLpgSQL_datum*)var, value, valtype, &isnull);
            }
        }

        return;
    }

    ereport(ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR),
            errmodule(MOD_PLSQL),
            errmsg("unsupported target when move values into record/row")));
}

/* ----------
 * make_tuple_from_row		Make a tuple from the values of a row object
 *
 * A NULL return indicates rowtype mismatch; caller must raise suitable error
 * ----------
 */
static HeapTuple make_tuple_from_row(PLpgSQL_execstate* estate, PLpgSQL_row* row, TupleDesc tupdesc)
{
    int natts = tupdesc->natts;
    HeapTuple tuple = NULL;
    Datum* dvalues = NULL;
    bool* nulls = NULL;
    int i;

    if (natts != row->nfields) {
        return NULL;
    }

    dvalues = (Datum*)palloc0(natts * sizeof(Datum));
    nulls = (bool*)palloc(natts * sizeof(bool));

    for (i = 0; i < natts; i++) {
        Oid fieldtypeid;
        int32 fieldtypmod;

        if (tupdesc->attrs[i]->attisdropped) {
            nulls[i] = true; /* leave the column as null */
            continue;
        }
        if (row->varnos[i] < 0) { /* should not happen */ 
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("dropped rowtype entry for non-dropped column when make tuple")));
        }

        exec_eval_datum(estate, estate->datums[row->varnos[i]], &fieldtypeid, &fieldtypmod, &dvalues[i], &nulls[i]);
        if (fieldtypeid != tupdesc->attrs[i]->atttypid) {
            pfree_ext(dvalues);
            pfree_ext(nulls);
            return NULL;
        }
        /* XXX should we insist on typmod match, too? */
    }

    tuple = heap_form_tuple(tupdesc, dvalues, nulls);

    pfree_ext(dvalues);
    pfree_ext(nulls);

    return tuple;
}

/* ----------
 * convert_value_to_string			Convert a non-null Datum to C string
 *
 * Note: the result is in the estate's eval_econtext, and will be cleared
 * by the next exec_eval_cleanup() call.  The invoked output function might
 * leave additional cruft there as well, so just pfree'ing the result string
 * would not be enough to avoid memory leaks if we did not do it like this.
 * In most usages the Datum being passed in is also in that context (if
 * pass-by-reference) and so an exec_eval_cleanup() call is needed anyway.
 *
 * Note: not caching the conversion function lookup is bad for performance.
 * ----------
 */
static char* convert_value_to_string(PLpgSQL_execstate* estate, Datum value, Oid valtype)
{
    char* result = NULL;
    MemoryContext oldcontext = NULL;
    Oid typoutput;
    bool typIsVarlena = false;

    oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
    getTypeOutputInfo(valtype, &typoutput, &typIsVarlena);
    result = OidOutputFunctionCall(typoutput, value);
    MemoryContextSwitchTo(oldcontext);

    return result;
}

/*
 * @Description: This is a special function called by the Postgres
 * database system to transform value to targetType by specified
 * typmode. The typmode of the attribute have to be applied on the
 * value. e.x: transform numeric data 1.0 to specified numeric(4, 2)
 * the result is 1.00
 *
 * @IN value: the source value.
 * @IN targetTypeId: target type oid.
 * @IN targetTypMod: target type mode.
 * @return: Datum - the result of specified typmode.
 */
static Datum pl_coerce_type_typmod(Datum value, Oid targetTypeId, int32 targetTypMod)
{
    CoercionPathType pathtype;
    Oid funcId;
    int nargs = 0;

    if (targetTypMod < 0) {
        return value;
    }

    pathtype = find_typmod_coercion_function(targetTypeId, &funcId);

    if (pathtype != COERCION_PATH_FUNC || !OidIsValid(funcId)) {
        return value;
    }

    if (OidIsValid(funcId)) {
        HeapTuple tp;
        Form_pg_proc procstruct;

        tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
        if (!HeapTupleIsValid(tp)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_PLSQL),
                    errmsg("cache lookup failed for function %u when coerce type in PLSQL function.", funcId)));

            return value;
        }

        procstruct = (Form_pg_proc)GETSTRUCT(tp);

        /*
         * These Asserts essentially check that function is a legal coercion
         * function.  We can't make the seemingly obvious tests on prorettype
         * and proargtypes[0], even in the COERCION_PATH_FUNC case, because of
         * various binary-compatibility cases.
         */
        nargs = procstruct->pronargs;
        AssertEreport(!procstruct->proretset && !procstruct->proisagg && !procstruct->proiswindow,
            MOD_PLSQL,
            "It should not be null.");
        AssertEreport(nargs >= 1 && nargs <= 3, MOD_PLSQL, "Args num is out of range.");
        AssertEreport(nargs < 2 || procstruct->proargtypes.values[1] == INT4OID, MOD_PLSQL, "Int is required.");
        AssertEreport(nargs < 3 || procstruct->proargtypes.values[2] == BOOLOID, MOD_PLSQL, "Bool is required.");

        ReleaseSysCache(tp);
    }

    if (nargs == 1) {
        value = OidFunctionCall1(funcId, value);
    } else if (nargs == 2) {
        value = OidFunctionCall2(funcId, value, Int32GetDatum(targetTypMod));
    } else if (nargs == 3) {
        value = OidFunctionCall3(funcId, value, Int32GetDatum(targetTypMod), ((Datum)0));
    }

    return value;
}

/* ----------
 * exec_cast_value			Cast a value if required
 *
 * Note: the estate's eval_econtext is used for temporary storage, and may
 * also contain the result Datum if we have to do a conversion to a pass-
 * by-reference data type.	Be sure to do an exec_eval_cleanup() call when
 * done with the result.
 * ----------
 */
static Datum exec_cast_value(PLpgSQL_execstate* estate, Datum value, Oid valtype, Oid reqtype, FmgrInfo* reqinput,
    Oid reqtypioparam, int32 reqtypmod, bool isnull)
{
    Oid funcid = InvalidOid;
    CoercionPathType result = COERCION_PATH_NONE;
    /*
     * If the type of the given value isn't what's requested, convert it.
     */
    if (valtype != reqtype || reqtypmod != -1) {
        MemoryContext oldcontext;

        oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
        if (!isnull) {
            char* extval = NULL;
            if (valtype == UNKNOWNOID) {
                valtype = TEXTOID;
                value = DirectFunctionCall1(textin, value);
            }

            /* get the implicit cast function from valtype to reqtype */
            result = find_coercion_pathway(reqtype, valtype, COERCION_ASSIGNMENT, &funcid);
            if (funcid != InvalidOid && !(result == COERCION_PATH_COERCEVIAIO || result == COERCION_PATH_ARRAYCOERCE)) {
                value = OidFunctionCall1(funcid, value);
                value = pl_coerce_type_typmod(value, reqtype, reqtypmod);
            } else {
                extval = convert_value_to_string(estate, value, valtype);
                value = InputFunctionCall(reqinput, extval, reqtypioparam, reqtypmod);
                pfree_ext(extval);
            }
        } else {
            value = InputFunctionCall(reqinput, NULL, reqtypioparam, reqtypmod);
        }
        MemoryContextSwitchTo(oldcontext);
    }

    return value;
}

/* ----------
 * exec_simple_cast_value			Cast a value if required
 *
 * As above, but need not supply details about target type.  Note that this
 * is slower than exec_cast_value with cached type info, and so should be
 * avoided in heavily used code paths.
 * ----------
 */
static Datum exec_simple_cast_value(
    PLpgSQL_execstate* estate, Datum value, Oid valtype, Oid reqtype, int32 reqtypmod, bool isnull)
{
    if (valtype != reqtype || reqtypmod != -1) {
        Oid typinput;
        Oid typioparam;
        FmgrInfo finfo_input;

        getTypeInputInfo(reqtype, &typinput, &typioparam);

        fmgr_info(typinput, &finfo_input);

        value = exec_cast_value(estate, value, valtype, reqtype, &finfo_input, typioparam, reqtypmod, isnull);
    }

    return value;
}

Datum exec_simple_cast_datum(
    PLpgSQL_execstate* estate, Datum value, Oid valtype, Oid reqtype, int32 reqtypmod, bool isnull)
{
    return exec_simple_cast_value(estate, value, valtype, reqtype, reqtypmod, isnull);
}

/* ----------
 * exec_simple_check_node -		Recursively check if an expression
 *								is made only of simple things we can
 *								hand out directly to ExecEvalExpr()
 *								instead of calling SPI.
 * ----------
 */
static bool exec_simple_check_node(Node* node)
{
    if (node == NULL) {
        return TRUE;
    }

    switch (nodeTag(node)) {
        case T_Const:
            return TRUE;

        case T_Param:
            return TRUE;

        case T_ArrayRef: {
            ArrayRef* expr = (ArrayRef*)node;

            if (!exec_simple_check_node((Node*)expr->refupperindexpr)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->reflowerindexpr)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->refexpr)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->refassgnexpr)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;

            if (expr->funcretset) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_OpExpr: {
            OpExpr* expr = (OpExpr*)node;

            if (expr->opretset) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_DistinctExpr: {
            DistinctExpr* expr = (DistinctExpr*)node;

            if (expr->opretset) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_NullIfExpr: {
            NullIfExpr* expr = (NullIfExpr*)node;

            if (expr->opretset) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;

            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;

            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_FieldSelect:
            return exec_simple_check_node((Node*)((FieldSelect*)node)->arg);

        case T_FieldStore: {
            FieldStore* expr = (FieldStore*)node;

            if (!exec_simple_check_node((Node*)expr->arg)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->newvals)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_RelabelType:
            return exec_simple_check_node((Node*)((RelabelType*)node)->arg);

        case T_CoerceViaIO:
            return exec_simple_check_node((Node*)((CoerceViaIO*)node)->arg);

        case T_ArrayCoerceExpr:
            return exec_simple_check_node((Node*)((ArrayCoerceExpr*)node)->arg);

        case T_ConvertRowtypeExpr:
            return exec_simple_check_node((Node*)((ConvertRowtypeExpr*)node)->arg);

        case T_CaseExpr: {
            CaseExpr* expr = (CaseExpr*)node;

            if (!exec_simple_check_node((Node*)expr->arg)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->defresult)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_CaseWhen: {
            CaseWhen* when = (CaseWhen*)node;

            if (!exec_simple_check_node((Node*)when->expr)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)when->result)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_CaseTestExpr:
            return TRUE;

        case T_ArrayExpr: {
            ArrayExpr* expr = (ArrayExpr*)node;

            if (!exec_simple_check_node((Node*)expr->elements)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_RowExpr: {
            RowExpr* expr = (RowExpr*)node;

            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_RowCompareExpr: {
            RowCompareExpr* expr = (RowCompareExpr*)node;

            if (!exec_simple_check_node((Node*)expr->largs)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->rargs)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_CoalesceExpr: {
            CoalesceExpr* expr = (CoalesceExpr*)node;

            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_MinMaxExpr: {
            MinMaxExpr* expr = (MinMaxExpr*)node;

            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_XmlExpr: {
            XmlExpr* expr = (XmlExpr*)node;

            if (!exec_simple_check_node((Node*)expr->named_args)) {
                return FALSE;
            }
            if (!exec_simple_check_node((Node*)expr->args)) {
                return FALSE;
            }

            return TRUE;
        }

        case T_NullTest:
            return exec_simple_check_node((Node*)((NullTest*)node)->arg);

        case T_HashFilter:
            return exec_simple_check_node((Node*)((HashFilter*)node)->arg);

        case T_BooleanTest:
            return exec_simple_check_node((Node*)((BooleanTest*)node)->arg);

        case T_CoerceToDomain:
            return exec_simple_check_node((Node*)((CoerceToDomain*)node)->arg);

        case T_CoerceToDomainValue:
            return TRUE;

        case T_List: {
            List* expr = (List*)node;
            ListCell* l = NULL;

            foreach (l, expr) {
                if (!exec_simple_check_node((Node*)lfirst(l))) {
                    return FALSE;
                }
            }

            return TRUE;
        }

        default:
            return FALSE;
    }
}

/* ----------
 * exec_simple_check_plan -		Check if a plan is simple enough to
 *								be evaluated by ExecEvalExpr() instead
 *								of SPI.
 * ----------
 */
static void exec_simple_check_plan(PLpgSQL_expr* expr)
{
    List* plansources = NIL;
    CachedPlanSource* plansource = NULL;
    Query* query = NULL;
    CachedPlan* cplan = NULL;

    /*
     * Initialize to "not simple", and remember the plan generation number we
     * last checked.  (If we don't get as far as obtaining a plan to check, we
     * just leave expr_simple_generation set to 0.)
     */
    expr->expr_simple_expr = NULL;
    expr->expr_simple_generation = 0;
    expr->expr_simple_need_snapshot = true;

    /*
     * We can only test queries that resulted in exactly one CachedPlanSource
     */
    plansources = SPI_plan_get_plan_sources(expr->plan);
    if (list_length(plansources) != 1) {
        return;
    }
    plansource = (CachedPlanSource*)linitial(plansources);

    /*
     * Do some checking on the analyzed-and-rewritten form of the query. These
     * checks are basically redundant with the tests in
     * exec_simple_recheck_plan, but the point is to avoid building a plan if
     * possible.  Since this function is only called immediately after
     * creating the CachedPlanSource, we need not worry about the query being
     * stale.
     */

    /*
     * 1. There must be one single querytree.
     */
    if (list_length(plansource->query_list) != 1)
        return;
    query = (Query*)linitial(plansource->query_list);

    /*
     * 2. It must be a plain SELECT query without any input tables
     */
    if (!IsA(query, Query)) {
        return;
    }
    if (query->commandType != CMD_SELECT) {
        return;
    }
    if (query->rtable != NIL) {
        return;
    }

    /*
     * 3. Can't have any subplans, aggregates, qual clauses either
     */
    bool has_subplans = query->hasAggs || query->hasWindowFuncs || query->hasSubLinks || query->hasForUpdate ||
                        query->cteList || query->jointree->quals || query->groupClause || query->havingQual ||
                        query->windowClause || query->distinctClause || query->sortClause || query->limitOffset ||
                        query->limitCount || query->setOperations;
    if (has_subplans) {
        return;
    }

    /*
     * 4. The query must have a single attribute as result
     */
    if (list_length(query->targetList) != 1) {
        return;
    }

    /*
     * OK, it seems worth constructing a plan for more careful checking.
     */

    /* Get the generic plan for the query */
    cplan = SPI_plan_get_cached_plan(expr->plan);

    /* Can't fail, because we checked for a single CachedPlanSource above */
    AssertEreport(cplan != NULL, MOD_PLSQL, "cplan should not be null");

    /* Share the remaining work with recheck code path */
    exec_simple_recheck_plan(expr, cplan);

    /* Release our plan refcount */
    ReleaseCachedPlan(cplan, true);
}

/*
 * exec_simple_recheck_plan --- check for simple plan once we have CachedPlan
 */
static void exec_simple_recheck_plan(PLpgSQL_expr* expr, CachedPlan* cplan)
{
    PlannedStmt* stmt = NULL;
    Plan* plan = NULL;
    TargetEntry* tle = NULL;

    /*
     * Initialize to "not simple", and remember the plan generation number we
     * last checked.
     */
    expr->expr_simple_expr = NULL;
    expr->expr_simple_generation = cplan->generation;
    expr->expr_simple_need_snapshot = true;
    expr->is_cachedplan_shared = cplan->isShared();

    /*
     * 1. There must be one single plantree
     */
    if (list_length(cplan->stmt_list) != 1) {
        return;
    }
    stmt = (PlannedStmt*)linitial(cplan->stmt_list);

    /*
     * 2. It must be a RESULT plan --> no scan's required
     */
    if (!IsA(stmt, PlannedStmt)) {
        return;
    }
    if (stmt->commandType != CMD_SELECT) {
        return;
    }
    plan = stmt->planTree;
    if (!IsA(plan, BaseResult)) {
        return;
    }

    /*
     * 3. Can't have any subplan or qual clause, either
     */
    if (plan->lefttree != NULL || plan->righttree != NULL || plan->initPlan != NULL || plan->qual != NULL ||
        ((BaseResult*)plan)->resconstantqual != NULL) {
        return;
    }

    /*
     * 4. The plan must have a single attribute as result
     */
    if (list_length(plan->targetlist) != 1) {
        return;
    }
    tle = (TargetEntry*)linitial(plan->targetlist);

    /*
     * 5. Check that all the nodes in the expression are non-scary.
     */
    if (!exec_simple_check_node((Node*)tle->expr)) {
        return;
    }
    /*
     * Yes - this is a simple expression.  Mark it as such, and initialize
     * state to "not valid in current transaction".
     */
    expr->expr_simple_expr = tle->expr;
    expr->expr_simple_state = NULL;
    expr->expr_simple_in_use = false;
    expr->expr_simple_lxid = InvalidLocalTransactionId;
    /* Also stash away the expression result type */
    expr->expr_simple_type = exprType((Node*)tle->expr);
    expr->expr_simple_need_snapshot = exec_simple_check_mutable_function((Node*)tle->expr);
}

/* ----------
 * exec_set_found			Set the global found variable to true/false
 * ----------
 */
static void exec_set_found(PLpgSQL_execstate* estate, bool state)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[estate->found_varno]);
    var->value = BoolGetDatum(state);
    var->isnull = false;
}

/*
 * Set the global explicit cursor attribute found variable to true/false/NULL
 * if state is -1, found is set NULL
 */
static void exec_set_cursor_found(PLpgSQL_execstate* estate, PLpgSQL_state state, int dno)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[dno]);
    /* if state is -1, explicit cursor found attribute is set NULL */
    if (state == PLPGSQL_NULL) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = (state == PLPGSQL_TRUE) ? (Datum)1 : (Datum)0;
        var->isnull = false;
    }
}

/*
 * Set the global explicit cursor attribute notfound variable to true/false/NULL
 * if state is -1, notfound is set NULL
 */
static void exec_set_notfound(PLpgSQL_execstate* estate, PLpgSQL_state state, int dno)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[dno]);
    /* if state is -1, notfound is set NULL */
    if (state == PLPGSQL_NULL) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = (state == PLPGSQL_TRUE) ? (Datum)1 : (Datum)0;
        var->isnull = false;
    }
}

/*
 * Set the global explicit cursor attribute isopen variable to true/false
 */
static void exec_set_isopen(PLpgSQL_execstate* estate, bool state, int varno)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[varno]);
    var->value = BoolGetDatum(state);
    var->isnull = false;
}

/*
 * Set the global explicit cursor attribute rowcount variable
 * reset == true and rowcount != -1,, set the global rowcount to rowcount,
 * reset == false, add the rowcount to global rowcount.
 * reset == true and rowcount == -1, rowcount is set NULL
 */
static void exec_set_rowcount(PLpgSQL_execstate* estate, int rowcount, bool reset, int dno)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[dno]);
    /* reset == true and rowcount == -1, rowcount is set NULL */
    if ((rowcount == -1) && reset) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value += rowcount;
        var->isnull = false;
    }
}

/*
 * Set the global implicit cursor attribute found variable to true/false/NULL
 * if state is -1, found is set NULL
 */
static void exec_set_sql_cursor_found(PLpgSQL_execstate* estate, PLpgSQL_state state)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]);
    /* if state is -1, found is set NULL */
    if (state == PLPGSQL_NULL) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = (state == PLPGSQL_TRUE) ? (Datum)1 : (Datum)0;
        var->isnull = false;
    }
}

/*
 * Set the global implicit cursor attribute notfound variable to true/false/NULL
 * if state is -1, notfound is set NULL
 */
static void exec_set_sql_notfound(PLpgSQL_execstate* estate, PLpgSQL_state state)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]);
    /* if state is -1, notfound is set NULL */
    if (state == PLPGSQL_NULL) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = (state == PLPGSQL_TRUE) ? (Datum)1 : (Datum)0;
        var->isnull = false;
    }
}

/*
 * Set the global implicit cursor attribute isopen variable to true/false
 */
static void exec_set_sql_isopen(PLpgSQL_execstate* estate, bool state)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]);
    var->value = BoolGetDatum(state);
    var->isnull = false;
}

/*
 * Set the global implicit cursor attribute rowcount variable
 * reset == true and rowcount != -1,, set the global rowcount to rowcount,
 * reset == false, add the rowcount to global rowcount.
 * reset == true and rowcount == -1, rowcount is set NULL
 */
static void exec_set_sql_rowcount(PLpgSQL_execstate* estate, int rowcount)
{
    PLpgSQL_var* var = NULL;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]);
    /* reset == true and rowcount == -1, rowcount is set NULL */
    if (rowcount == -1) {
        var->value = (Datum)0;
        var->isnull = true;
    } else {
        var->value = rowcount;
        var->isnull = false;
    }
}

/*
 * plpgsql_create_econtext --- create an eval_econtext for the current function
 *
 * We may need to create a new u_sess->plsql_cxt.simple_eval_estate too, if there's not one
 * already for the current transaction.  The EState will be cleaned up at
 * transaction end.
 */
static void plpgsql_create_econtext(PLpgSQL_execstate* estate)
{
    SimpleEcontextStackEntry* entry = NULL;

    /*
     * Create an EState for evaluation of simple expressions, if there's not
     * one already in the current transaction.	The EState is made a child of
     * u_sess->top_transaction_mem_cxt so it will have the right lifespan.
     */
    if (u_sess->plsql_cxt.simple_eval_estate == NULL) {
        MemoryContext oldcontext;

        oldcontext = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
        u_sess->plsql_cxt.simple_eval_estate = CreateExecutorState();
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * Create a child econtext for the current function.
     */
    estate->eval_econtext = CreateExprContext(u_sess->plsql_cxt.simple_eval_estate);

    /*
     * Make a stack entry so we can clean up the econtext at subxact end.
     * Stack entries are kept in u_sess->top_transaction_mem_cxt for simplicity.
     */
    entry = (SimpleEcontextStackEntry*)MemoryContextAlloc(
        u_sess->top_transaction_mem_cxt, sizeof(SimpleEcontextStackEntry));

    entry->stack_econtext = estate->eval_econtext;
    entry->xact_subxid = GetCurrentSubTransactionId();

    entry->next = u_sess->plsql_cxt.simple_econtext_stack;
    u_sess->plsql_cxt.simple_econtext_stack = entry;
}

/*
 * plpgsql_destroy_econtext --- destroy function's econtext
 *
 * We check that it matches the top stack entry, and destroy the stack
 * entry along with the context.
 */
static void plpgsql_destroy_econtext(PLpgSQL_execstate* estate)
{
    SimpleEcontextStackEntry* next = NULL;

    AssertEreport(u_sess->plsql_cxt.simple_econtext_stack != NULL &&
                      u_sess->plsql_cxt.simple_econtext_stack->stack_econtext == estate->eval_econtext,
        MOD_PLSQL,
        "wrong context state.");

    next = u_sess->plsql_cxt.simple_econtext_stack->next;
    pfree_ext(u_sess->plsql_cxt.simple_econtext_stack);
    u_sess->plsql_cxt.simple_econtext_stack = next;

    FreeExprContext(estate->eval_econtext, true);
    estate->eval_econtext = NULL;
}

/*
 * plpgsql_xact_cb --- post-transaction-commit-or-abort cleanup
 *
 * If a simple-expression EState was created in the current transaction,
 * it has to be cleaned up.
 */
void plpgsql_xact_cb(XactEvent event, void* arg)
{
#ifdef ENABLE_MOT
    /*
     * XACT_EVENT_PREROLLBACK_CLEANUP is added only for MOT FDW to cleanup some
     * internal resources. So, others can safely ignore this event.
     */
    if (event == XACT_EVENT_PREROLLBACK_CLEANUP) {
        return;
    }
#endif

    u_sess->plsql_cxt.simple_eval_estate = NULL;
    /*
     * If we are doing a clean transaction shutdown, free the EState (so that
     * any remaining resources will be released correctly). In an abort, we
     * expect the regular abort recovery procedures to release everything of
     * interest.
     */
    u_sess->plsql_cxt.simple_econtext_stack = NULL;

    if (event != XACT_EVENT_ABORT) {
        if (u_sess->plsql_cxt.simple_eval_estate)
            FreeExecutorState(u_sess->plsql_cxt.simple_eval_estate);
        u_sess->plsql_cxt.simple_eval_estate = NULL;
    } else {
        u_sess->plsql_cxt.simple_eval_estate = NULL;
    }
}

/*
 * plpgsql_subxact_cb --- post-subtransaction-commit-or-abort cleanup
 *
 * Make sure any simple-expression econtexts created in the current
 * subtransaction get cleaned up.  We have to do this explicitly because
 * no other code knows which child econtexts of u_sess->plsql_cxt.simple_eval_estate belong
 * to which level of subxact.
 */
void plpgsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg)
{
    if (event == SUBXACT_EVENT_START_SUB) {
        return;
    }

    while (u_sess->plsql_cxt.simple_econtext_stack != NULL &&
           u_sess->plsql_cxt.simple_econtext_stack->xact_subxid == mySubid) {
        SimpleEcontextStackEntry* next = NULL;

        FreeExprContext(u_sess->plsql_cxt.simple_econtext_stack->stack_econtext, (event == SUBXACT_EVENT_COMMIT_SUB));
        next = u_sess->plsql_cxt.simple_econtext_stack->next;
        pfree_ext(u_sess->plsql_cxt.simple_econtext_stack);
        u_sess->plsql_cxt.simple_econtext_stack = next;
    }
}

/*
 * free_var --- pfree any pass-by-reference value of the variable.
 *
 * This should always be followed by some assignment to var->value,
 * as it leaves a dangling pointer.
 */
static void free_var(PLpgSQL_var* var)
{
    if (var->freeval) {
        pfree(DatumGetPointer(var->value));
        var->freeval = false;
    }
}

/*
 * free old value of a text variable and assign new value from C string
 */
static void assign_text_var(PLpgSQL_var* var, const char* str)
{
    free_var(var);
    var->value = CStringGetTextDatum(str);
    var->isnull = false;
    var->freeval = true;
}

/*
 * exec_eval_using_params --- evaluate params of USING clause
 */
static PreparedParamsData* exec_eval_using_params(PLpgSQL_execstate* estate, List* params)
{
    PreparedParamsData* ppd = NULL;
    int nargs;
    int i;
    ListCell* lc = NULL;

    ppd = (PreparedParamsData*)palloc(sizeof(PreparedParamsData));
    nargs = list_length(params);

    ppd->nargs = nargs;
    ppd->types = (Oid*)palloc(nargs * sizeof(Oid));
    ppd->values = (Datum*)palloc(nargs * sizeof(Datum));
    ppd->nulls = (char*)palloc(nargs * sizeof(char));
    ppd->freevals = (bool*)palloc(nargs * sizeof(bool));
    ppd->isouttype = (bool*)palloc(nargs * sizeof(bool));
    ppd->cursor_data = (Cursor_Data*)palloc(nargs * sizeof(Cursor_Data));

    i = 0;
    foreach (lc, params) {
        PLpgSQL_expr* param = (PLpgSQL_expr*)lfirst(lc);
        bool isnull = false;

        ppd->values[i] = exec_eval_expr(estate, param, &isnull, &ppd->types[i]);
        ppd->nulls[i] = isnull ? 'n' : ' ';
        ppd->freevals[i] = false;

        ppd->isouttype[i] = param->isouttype;

        if (ppd->types[i] == UNKNOWNOID) {
            /*
             * Treat 'unknown' parameters as text, since that's what most
             * people would expect. SPI_execute_with_args can coerce unknown
             * constants in a more intelligent way, but not unknown Params.
             * This code also takes care of copying into the right context.
             * Note we assume 'unknown' has the representation of C-string.
             */
            ppd->types[i] = TEXTOID;
            if (!isnull) {
                ppd->values[i] = CStringGetTextDatum(DatumGetCString(ppd->values[i]));
                ppd->freevals[i] = true;
            }
        } else if (!isnull) {
			/* pass-by-ref non null values must be copied into plpgsql context */
            int16 typLen;
            bool typByVal = false;

            get_typlenbyval(ppd->types[i], &typLen, &typByVal);
            if (!typByVal) {
                ppd->values[i] = datumCopy(ppd->values[i], typByVal, typLen);
                ppd->freevals[i] = true;
            }
        }

        if (ppd->types[i] == REFCURSOROID) {
            ExprContext* econtext = estate->eval_econtext;
            CopyCursorInfoData(&ppd->cursor_data[i], &econtext->cursor_data);
        }

        exec_eval_cleanup(estate);

        i++;
    }

    return ppd;
}

/*
 * free_params_data --- pfree all pass-by-reference values used in USING clause
 */
static void free_params_data(PreparedParamsData* ppd)
{
    int i;

    for (i = 0; i < ppd->nargs; i++) {
        if (ppd->freevals[i]) {
            pfree(DatumGetPointer(ppd->values[i]));
        }
    }

    pfree_ext(ppd->types);
    pfree_ext(ppd->values);
    pfree_ext(ppd->nulls);
    pfree_ext(ppd->freevals);
    pfree_ext(ppd->isouttype);
    pfree_ext(ppd);
}

/*
 * Open portal for dynamic query
 */
static Portal exec_dynquery_with_params(
    PLpgSQL_execstate* estate, PLpgSQL_expr* dynquery, List* params, const char* portalname, int cursorOptions)
{
    Portal portal = NULL;
    Datum query;
    bool isnull = false;
    Oid restype;
    char* querystr = NULL;

    /*
     * Evaluate the string expression after the EXECUTE keyword. Its result is
     * the querystring we have to execute.
     */
    query = exec_eval_expr(estate, dynquery, &isnull, &restype);
    if (isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("query string argument of EXECUTE is null for dynamic query")));
    }

    /* Get the C-String representation */
    querystr = convert_value_to_string(estate, query, restype);

    /* copy it out of the temporary context before we clean up */
    querystr = pstrdup(querystr);

    exec_eval_cleanup(estate);

    /*
     * Open an implicit cursor for the query.  We use
     * SPI_cursor_open_with_args even when there are no params, because this
     * avoids making and freeing one copy of the plan.
     */
    if (params != NULL) {
        PreparedParamsData* ppd = NULL;

        ppd = exec_eval_using_params(estate, params);
        portal = SPI_cursor_open_with_args(portalname,
            querystr,
            ppd->nargs,
            ppd->types,
            ppd->values,
            ppd->nulls,
            estate->readonly_func,
            cursorOptions);
        free_params_data(ppd);
    } else {
        portal =
            SPI_cursor_open_with_args(portalname, querystr, 0, NULL, NULL, NULL, estate->readonly_func, cursorOptions);
    }

    if (portal == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_CURSOR),
                errmodule(MOD_PLSQL),
                errmsg("could not open implicit cursor for query \"%s\": %s for dynamic query",
                    querystr,
                    SPI_result_code_string(SPI_result))));
    pfree_ext(querystr);

    return portal;
}

// Set the sqlcode variable of estate
static void exec_set_sqlcode(PLpgSQL_execstate* estate, int sqlcode)
{
    PLpgSQL_var* sqlcode_var = NULL;
    sqlcode_var = (PLpgSQL_var*)estate->datums[estate->sqlcode_varno];
    sqlcode_var->value = Int32GetDatum(sqlcode);
    sqlcode_var->freeval = false;
    sqlcode_var->isnull = false;
}

/* transform anonymous block in dynamic statments. */
#define SQLSTART "SELECT \'"
#define SQLEND "\'"
/*
 * add SELECT for dynamic statements, and double the quotes.
 */
static char* transformAnonymousBlock(char* query)
{
    /* nquotes is the number of single quetations in sql stmt */
    int nquotes = 0;
    char* str_left = NULL;
    char* quotePos = NULL;
    char* newquery = NULL;
    size_t query_len = 0;
    errno_t errorno = EOK;

    str_left = query;
    /* search number of quotes in sql statement */
    quotePos = strchr(str_left, '\'');
    while (quotePos != NULL) {
        nquotes++;
        quotePos = strchr(quotePos + 1, '\'');
    }
    /* the reassembling statements need bigger room than old one */
    query_len = strlen(query) + strlen(SQLSTART) + strlen(SQLEND) + nquotes + 1;
    newquery = (char*)palloc0(query_len);

    errorno = strncpy_s(newquery, query_len, SQLSTART, strlen(SQLSTART) + 1);
    securec_check(errorno, "\0", "\0");

    quotePos = strchr(str_left, '\'');
    while (quotePos != NULL) {
        quotePos += 1;
        /* copy the str before the quote and add another quote */
        errorno = strncat_s(newquery, query_len, str_left, quotePos - str_left);
        securec_check(errorno, "\0", "\0");
        errorno = strncat_s(newquery, query_len, "\'", 1);
        securec_check(errorno, "\0", "\0");
        str_left = quotePos;
        quotePos = strchr(quotePos, '\'');
    }

    errorno = strncat_s(newquery, query_len, str_left, query + strlen(query) - str_left);
    securec_check(errorno, "\0", "\0");
    errorno = strncat_s(newquery, query_len, SQLEND, strlen(SQLEND));
    securec_check(errorno, "\0", "\0");
    newquery[query_len - 1] = '\0';
    return newquery;
}

/*
 * Function name: needRecompilePlan
 * 		Check if query in stored procedure need to be recompiled.
 * input Parameter:
 * 		plan: the plan need to be checked if it need to be recompiled.
 * output result:
 * 		True : need to redo Prepare proc before exec stored procedure.
 *		False: could execute SP execution directly.
 */
static bool needRecompilePlan(SPIPlanPtr plan)
{
    bool ret_val = false;
    ListCell* l = NULL;

    /* If there is no plan for query in stored procedure, then must do first time preparing. */
    if (plan == NULL) {
        return true;
    }

    /* Find if there is query that has been enabled auto truncation. */
    foreach (l, SPI_plan_get_plan_sources(plan)) {
        CachedPlanSource* plansource = (CachedPlanSource*)lfirst(l);
        /*
         * Create Table As will rewrite to Insert statement in plansource.
         * If execute the function multiple times,
         * it will execute multiple INSERT rather than multiple CREATE.
         * So recompile the query to avoid this situation.
         */
        if (plansource->raw_parse_tree != NULL && IsA(plansource->raw_parse_tree, CreateTableAsStmt)) {
            return true;
        }

        ret_val = checkRecompileCondition(plansource);

        /* Once we find one need re-compile stmt in SP, all stored procedure should be re-compiled. */
        if (ret_val) {
            return ret_val;
        }
    }
    return ret_val;
}

/*
 * Description: reset cursor's option. we only reset pointer when reset is true.
 * Parameters:
 * @in portal: the cursor saved in portal.
 * @in reset: wether reset cursor's option or not.
 * Return: void
 */
void ResetCursorOption(Portal portal, bool reset)
{
    /* only reset %ISOPEN (at index 0) attribute and keep the rest as they were */
    if (reset && portal->cursorAttribute[0] != NULL) {
        PLpgSQL_var *var = (PLpgSQL_var*)portal->cursorAttribute[0];
        var->isnull = false;
        var->value = 0;
    }

    /* unbind cursor attributes with portal */
    for (int i = 0; i < CURSOR_ATTRIBUTE_NUMBER; i++) {
        portal->cursorAttribute[i] = NULL;
    }

    portal->funcOid = InvalidOid;
    portal->funcUseCount = 0;
}

static void stp_check_transaction_and_set_resource_owner(ResourceOwner oldResourceOwner,TransactionId oldTransactionId)
{
    if (oldTransactionId != SPI_get_top_transaction_id()) {
        t_thrd.utils_cxt.CurrentResourceOwner = get_last_transaction_resourceowner();
    } else {
        t_thrd.utils_cxt.CurrentResourceOwner = oldResourceOwner;
    }    
}

static void stp_check_transaction_and_create_econtext(PLpgSQL_execstate* estate,TransactionId oldTransactionId)
{
    if (oldTransactionId != SPI_get_top_transaction_id()) {
        u_sess->plsql_cxt.simple_eval_estate = NULL;
        plpgsql_create_econtext(estate);
    }     
}

void plpgsql_HashTableDeleteAll()
{
    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, u_sess->plsql_cxt.plpgsql_HashTable);
    plpgsql_HashEnt* hentry = NULL;
    while ((hentry = (plpgsql_HashEnt *)hash_seq_search(&hash_seq)) != NULL) {
        PLpgSQL_function* func = hentry->function;
        plpgsql_HashTableDelete(func);
        func->use_count = 0;
        plpgsql_free_function_memory(func);
    }
}

void plpgsql_HashTableDeleteFunc(Oid func_oid)
{
    if (unlikely(u_sess->plsql_cxt.plpgsql_HashTable == NULL))
        return;
    u_sess->plsql_cxt.is_delete_function = true;
    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, u_sess->plsql_cxt.plpgsql_HashTable);
    plpgsql_HashEnt* hentry = NULL;
    while ((hentry = (plpgsql_HashEnt *)hash_seq_search(&hash_seq)) != NULL) {
        if (hentry->key.funcOid == func_oid) {
            uint32 key = SPICacheHashFunc((void*)(&hentry->key), sizeof(PLpgSQL_func_hashkey));
            PLpgSQL_function* func = hentry->function;
            if (func->use_count == 0) {
                plpgsql_HashTableDelete(func);
                SPIPlanCacheTableDelete(key);
                plpgsql_free_function_memory(func);
            } else if (ENABLE_CN_GPC) {
                /* plan has dropped, but current session still set plancache invalid for gpc */
                SPIPlanCacheTableInvalidPlan(key);
            }
        }
    }
    u_sess->plsql_cxt.is_delete_function = false;
}

/*
 * Description: record the current stp is execute with a exception or not,it used in the situation
 * that function or procedure have exception declare, DDL or DML SQL, is pushed down to a DN node,it
 * will cause the data inconsistency,for example.
 * create function return int shippable
 * insert into test values(1);
 * return 1;
 * exception when others then
 * return 2;
 * /
 * Parameters: void
 * Return: void
 */
bool plpgsql_get_current_value_stp_with_exception() 
{
    return u_sess->SPI_cxt.current_stp_with_exception;
}

/*
 * Description: restore the current stp is execute with a exception or not,it used in the sitation
 * that a procedure with procedure called by another procedure.Other information in the 
 * plpgsql_get_current_value_stp_with_exception function's commented.
 * Parameters: void
 * Return: void
 */
void plpgsql_restore_current_value_stp_with_exception(bool saved_current_stp_with_exception) 
{
    u_sess->SPI_cxt.current_stp_with_exception = saved_current_stp_with_exception;
}

/*
 * Description: set current stp with exception value true
 */
void plpgsql_set_current_value_stp_with_exception() 
{
    u_sess->SPI_cxt.current_stp_with_exception = true;
}
