/* -------------------------------------------------------------------------
 *
 * pl_exec.cpp		- Executor for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_exec.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utils/plpgsql.h"
#include "utils/pl_package.h"

#include <ctype.h>

#include "access/tuptoaster.h"
#include "access/transam.h"
#include "access/tupconvert.h"
#include "access/xact.h"
#include "auditfuncs.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "funcapi.h"
#include "gssignal/gs_signal.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/scansup.h"
#include "pgaudit.h"
#include "pgstat.h"
#include "optimizer/clauses.h"
#include "storage/proc.h"
#include "tcop/autonomoustransaction.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
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
#include "commands/sqladvisor.h"
#include "access/hash.h"

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
    int64 statckEntryId;                   /* ID for current StackEntry */
    struct SimpleEcontextStackEntry* next; /* next stack entry up */
} SimpleEcontextStackEntry;

/************************************************************
 * Local function forward declarations
 ************************************************************/
static void plpgsql_exec_error_callback(void* arg);
extern PLpgSQL_datum* copy_plpgsql_datum(PLpgSQL_datum* datum);

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
static int exec_stmt_null(PLpgSQL_execstate* estate, PLpgSQL_stmt_null* stmt);
static int exec_stmt_exit(PLpgSQL_execstate* estate, PLpgSQL_stmt_exit* stmt);
static int exec_stmt_return(PLpgSQL_execstate* estate, PLpgSQL_stmt_return* stmt);
static int exec_stmt_return_next(PLpgSQL_execstate* estate, PLpgSQL_stmt_return_next* stmt);
static int exec_stmt_return_query(PLpgSQL_execstate* estate, PLpgSQL_stmt_return_query* stmt);
static int exec_stmt_raise(PLpgSQL_execstate* estate, PLpgSQL_stmt_raise* stmt);
static int exec_stmt_execsql(PLpgSQL_execstate* estate, PLpgSQL_stmt_execsql* stmt);
static int exec_stmt_dynexecute(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* stmt);
static int exec_stmt_transaction(PLpgSQL_execstate *estate, PLpgSQL_stmt* stmt);
static void exec_savepoint_rollback(PLpgSQL_execstate *estate, const char *spName);
static int exec_stmt_savepoint(PLpgSQL_execstate *estate, PLpgSQL_stmt* stmt);

static int exchange_parameters(
    PLpgSQL_execstate* estate, PLpgSQL_stmt_dynexecute* dynstmt, List* stmts, int* ppdindex, int* datumindex);
static bool is_anonymous_block(const char* query);
static int exec_stmt_dynfors(PLpgSQL_execstate* estate, PLpgSQL_stmt_dynfors* stmt);
static void plpgsql_estate_setup(PLpgSQL_execstate* estate, PLpgSQL_function* func, ReturnSetInfo* rsi);
void exec_eval_cleanup(PLpgSQL_execstate* estate);

static void exec_prepare_plan(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, int cursorOptions);
static bool exec_simple_check_node(Node* node);
static void exec_simple_check_plan(PLpgSQL_execstate *estate, PLpgSQL_expr* expr);
static void exec_simple_recheck_plan(PLpgSQL_expr* expr, CachedPlan* cplan);
static void exec_save_simple_expr(PLpgSQL_expr *expr, CachedPlan *cplan);
static bool exec_eval_simple_expr(
    PLpgSQL_execstate* estate, PLpgSQL_expr* expr, Datum* result, bool* isNull, Oid* rettype, HTAB** tableOfIndex);

static void exec_assign_c_string(PLpgSQL_execstate* estate, PLpgSQL_datum* target, const char* str);
static void exec_eval_datum(PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typeId,
    int32* typetypmod, Datum* value, bool* isnull);
static int exec_eval_integer(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull);
static bool exec_eval_boolean(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull);
static Datum exec_eval_varchar(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull);
static Datum exec_eval_expr(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull, Oid* rettype,
    HTAB** tableOfIndex = NULL);
static int exec_run_select(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, long maxtuples,
                           Portal* portalP, bool isCollectParam = false);
static int exec_for_query(PLpgSQL_execstate* estate, PLpgSQL_stmt_forq* stmt, Portal portal, bool prefetch_ok, int dno);
static ParamListInfo setup_param_list(PLpgSQL_execstate* estate, PLpgSQL_expr* expr);
static void plpgsql_param_fetch(ParamListInfo params, int paramid);
static void exec_move_row(PLpgSQL_execstate* estate,
    PLpgSQL_rec* rec, PLpgSQL_row* row, HeapTuple tup, TupleDesc tupdesc, bool fromExecSql = false);
static void exec_read_bulk_collect(PLpgSQL_execstate *estate, PLpgSQL_row *row, SPITupleTable *tuptab);
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
static void plpgsql_create_econtext(PLpgSQL_execstate *estate, MemoryContext saveCxt = NULL);
static void plpgsql_destroy_econtext(PLpgSQL_execstate *estate);
static void free_var(PLpgSQL_var* var);
static PreparedParamsData* exec_eval_using_params(PLpgSQL_execstate* estate, List* params);
static void free_params_data(PreparedParamsData* ppd);
static Portal exec_dynquery_with_params(
    PLpgSQL_execstate* estate, PLpgSQL_expr* dynquery, List* params, const char* portalname, int cursorOptions);
static void exec_set_sqlcode(PLpgSQL_execstate* estate, int sqlcode);
static void exec_set_prev_sqlcode(PLpgSQL_execstate* estate, PLpgSQL_execstate* estate_prev);
#ifndef ENABLE_MULTIPLE_NODES
static void exec_set_cursor_att_var(PLpgSQL_execstate* estate, PLpgSQL_execstate* estate_prev);
static bool IsAutoOutParam(PLpgSQL_execstate* estate, PLpgSQL_stmt_open* stmt, int dno = -1);
static void CheckAssignTarget(PLpgSQL_execstate* estate, int dno);
#endif
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

static void rebuild_exception_subtransaction_chain(PLpgSQL_execstate* estate, List* transactionList);

static void stp_check_transaction_and_set_resource_owner(ResourceOwner oldResourceOwner,TransactionId oldTransactionId);
static void stp_check_transaction_and_create_econtext(PLpgSQL_execstate* estate,TransactionId oldTransactionId);
static void RecordSetGeneratedField(PLpgSQL_rec *recNew);

static HTAB* evalSubscipts(PLpgSQL_execstate* estate, int nsubscripts, int pos, int* subscriptvals,
    PLpgSQL_expr** subscripts, Oid subscriptType, HTAB* tableOfIndex);
static int insertTableOfIndexByDatumValue(TableOfIndexKey key, HTAB** tableOfIndex, PLpgSQL_var* var = NULL);
static void exec_assign_list(PLpgSQL_execstate* estate, PLpgSQL_datum* assigntarget,
    const List* assignlist, Datum value, Oid valtype, bool* isNull);
static PLpgSQL_datum* get_indirect_target(PLpgSQL_execstate* estate,
    PLpgSQL_datum* assigntarget, const char* attrname);
static void evalSubscriptList(PLpgSQL_execstate* estate, const List* subscripts,
    int* subscriptvals, int nsubscripts, PLpgSQL_datum** target, HTAB** elemTableOfIndex);
static PLpgSQL_temp_assignvar* build_temp_assignvar_from_datum(PLpgSQL_datum* target,
    int* subscriptvals, int nsubscripts);
static PLpgSQL_temp_assignvar* extractArrayElem(PLpgSQL_execstate* estate, PLpgSQL_temp_assignvar* target,
    int* subscriptvals, int nsubscripts);
static PLpgSQL_temp_assignvar* extractAttrValue(PLpgSQL_execstate* estate, PLpgSQL_temp_assignvar* target,
    char* attrname);
static Datum formDatumFromTargetList(PLpgSQL_execstate* estate, DList* targetlist,
    Datum value, Oid valtype, Oid* resultvaltype, bool* isNull);
static Datum formDatumFromAttrTarget(PLpgSQL_execstate* estate, const PLpgSQL_temp_assignvar* target, int attnum,
    Datum value, Oid* valtype, bool* isNull);
static Datum formDatumFromArrayTarget(PLpgSQL_execstate* estate, const PLpgSQL_temp_assignvar* target,
    int* subscriptvals, int nsubscripts, Datum value, Oid* resultvaltype, bool* isNull);

bool plpgsql_get_current_value_stp_with_exception();
void plpgsql_restore_current_value_stp_with_exception(bool saved_current_stp_with_exception);

static int addNewNestedTable(PLpgSQL_execstate* estate, TableOfIndexKey key, PLpgSQL_var* base_table);
static PLpgSQL_var* evalSubsciptsNested(PLpgSQL_execstate* estate, PLpgSQL_var* tablevar, PLpgSQL_expr** subscripts,
                                 int nsubscripts, int pos, int* subscriptvals, Oid subscriptType, HTAB* tableOfIndex);
static void assignNestTableOfValue(PLpgSQL_execstate* estate, PLpgSQL_var* var, Datum oldvalue, HTAB* tableOfIndex);

static PLpgSQL_row* copyPLpgsqlRow(PLpgSQL_row* src);
static PLpgSQL_type* copyPLpgsqlType(PLpgSQL_type* src);
static PLpgSQL_rec* copyPLpgsqlRec(PLpgSQL_rec* src);
static PLpgSQL_recfield* copyPLpgsqlRecfield(PLpgSQL_recfield* src);
static List* invalid_depend_func_and_packgae(Oid pkgOid);
static void ReportCompileConcurrentError(const char* objName, bool isPackage);
static Datum CopyFcinfoArgValue(Oid typOid, Datum value);
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


/*
 * Append a SQL string literal representing "val" to buf.
 */
static void deparseStringLiteral(StringInfo buf, const char *val)
{
    const char *valptr = NULL;

    /*
     * Rather than making assumptions about the remote server's value of
     * standard_conforming_strings, always use E'foo' syntax if there are any
     * backslashes.  This will fail on remote servers before 8.1, but those
     * are long out of support.
     */
    if (strchr(val, '\\') != NULL) {
        appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
    }
    appendStringInfoChar(buf, '\'');
    for (valptr = val; *valptr; valptr++) {
        char ch = *valptr;

        if (SQL_STR_DOUBLE(ch, true)) {
            appendStringInfoChar(buf, ch);
        }
        appendStringInfoChar(buf, ch);
    }
    appendStringInfoChar(buf, '\'');
}


void TypeValueToString(StringInfoData* buf, Oid consttype, Datum constvalue, bool constisnull)
{
    Oid typeOutput;
    bool typIsVarlena;
    bool isFloat = false;
    bool needLabel = false;

    if (constisnull) {
        appendStringInfoString(buf, "NULL");
        appendStringInfo(buf, "::%s", format_type_with_typemod(consttype, -1));
        return;
    }

    getTypeOutputInfo(consttype, &typeOutput, &typIsVarlena);
    char *extval = OidOutputFunctionCall(typeOutput, constvalue);

    switch (consttype) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID: {
            /*
             * No need to quote unless it's a special value such as 'NaN'.
             * See comments in get_const_expr().
             */
            if (strspn(extval, "0123456789+-eE.") == strlen(extval)) {
                if (extval[0] == '+' || extval[0] == '-') {
                    appendStringInfo(buf, "(%s)", extval);
                } else {
                    appendStringInfoString(buf, extval);
                }
                if (strcspn(extval, "eE.") != strlen(extval)) {
                    isFloat = true; /* it looks like a float */
                }
            } else {
                appendStringInfo(buf, "'%s'", extval);
            }
        } break;
        case BITOID:
        case VARBITOID:
            appendStringInfo(buf, "B'%s'", extval);
            break;
        case BOOLOID:
            if (strcmp(extval, "t") == 0) {
                appendStringInfoString(buf, "true");
            } else {
                appendStringInfoString(buf, "false");
            }
            break;
        default:
            deparseStringLiteral(buf, extval);
            break;
    }

    /*
     * Append ::typename unless the constant will be implicitly typed as the
     * right type when it is read in.
     *
     * XXX this code has to be kept in sync with the behavior of the parser,
     * especially make_const.
     */
    switch (consttype) {
        case BOOLOID:
        case INT4OID:
        case UNKNOWNOID:
            needLabel = false;
            break;
        case NUMERICOID:
            needLabel = !isFloat;
            break;
        case REFCURSOROID:
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("Un-support:ref_cursor parameter is not supported for autonomous transactions.")));
            break;
        default:
            needLabel = true;
            break;
    }
    if (needLabel) {
        appendStringInfo(buf, "::%s", format_type_with_typemod(consttype, -1));
    }

}

static char* AssembleAutomnousStatement(PLpgSQL_function* func, FunctionCallInfo fcinfo, char* sourceText)
{
    Oid procedureOid = fcinfo->flinfo->fn_oid;
    HeapTuple procTup = NULL;
    StringInfoData buf;
    char* nspName = NULL;

    initStringInfo(&buf);

    if (procedureOid != InvalidOid) {
        /*
         * Lookup the pg_proc tuple by Oid; we'll need it in any case
         */
        procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procedureOid));
        if (!HeapTupleIsValid(procTup)) {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for function %u, while compile function", procedureOid)));
        }
        Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(procTup);
        char* proname = NameStr(procform->proname);
        int nargs = procform->pronargs;
        if (is_function_with_plpgsql_language_and_outparam(func->fn_oid)) {
            appendStringInfoString(&buf, "SELECT * from ");
        } else {
            appendStringInfoString(&buf, "SELECT ");
        }

        /*
         * Would this proc be found (given the right args) by regprocedurein?
         * If not, we need to qualify it.
         */
        if (FunctionIsVisible(procedureOid))
            nspName = NULL;
        else
            nspName = get_namespace_name(procform->pronamespace);

        appendStringInfo(&buf, "%s", quote_qualified_identifier(nspName, NULL));
        bool isNull = true;
#ifndef ENABLE_MULTIPLE_NODES
        Datum datum = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_packageid, &isNull);
        Oid proPackageId = DatumGetObjectId(datum);

        if (proPackageId != InvalidOid) {
            NameData* pkgName = GetPackageName(proPackageId);
            if (pkgName != NULL) {
                appendStringInfo(&buf, "%s", quote_qualified_identifier(NameStr(*pkgName), NULL));
            } else {
                ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("The package name cannot be found."),
                        errdetail("proname: %s ,proPackageId: %d", proname,proPackageId),
                        errcause("System error."),
                        erraction("Please check PG_PROC table.")));

            }
        }
#endif

        appendStringInfo(&buf, "%s(", quote_qualified_identifier(NULL, proname));

        isNull = false;
        bool isNULL2 = false;
        Datum proAllArgTypes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proallargtypes, &isNull);
        Datum proArgModes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proargmodes, &isNULL2);

        if (!isNull && !isNULL2) {
            ArrayType* arr = DatumGetArrayTypeP(proAllArgTypes); /* ensure not toasted */
            ArrayType* arr2 = DatumGetArrayTypeP(proArgModes); /* ensure not toasted */
            int allNumArgs = ARR_DIMS(arr)[0];
            char* argmodes = (char*)ARR_DATA_PTR(arr2);
            Oid* argTypes = (Oid*)palloc(allNumArgs * sizeof(Oid));
            errno_t rc = memcpy_s(argTypes, allNumArgs * sizeof(Oid), ARR_DATA_PTR(arr), allNumArgs * sizeof(Oid));
            securec_check(rc, "\0", "\0");
            for (int i = 0; i < allNumArgs; i++) {
#ifndef ENABLE_MULTIPLE_NODES
                if (argTypes[i] == REFCURSOROID && (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_INOUT)) {
#else
                if (argTypes[i] == REFCURSOROID) {
#endif
                    pfree_ext(argTypes);
                    ReleaseSysCache(procTup);
                    ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("Un-support:ref_cursor parameter is not supported for autonomous transactions.")));
                }
                if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_INOUT
                    || argmodes[i] == PROARGMODE_VARIADIC) {
                    if (i > 0)
                        appendStringInfoChar(&buf, ',');
                    TypeValueToString(&buf, fcinfo->argTypes[i], fcinfo->arg[i], fcinfo->argnull[i]);
#ifndef ENABLE_MULTIPLE_NODES
                } else {
                    bool prokindIsNULL = false;
                    Datum prokindDatum = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prokind, &prokindIsNULL);
                    char prokind = CharGetDatum(prokindDatum);
                    if ((!prokindIsNULL && PROC_IS_PRO(prokind) && enable_out_param_override()) ||
                        (PROC_IS_FUNC(prokind) && is_function_with_plpgsql_language_and_outparam(procedureOid))) {
                        if (i > 0)
                            appendStringInfoChar(&buf, ',');
                        TypeValueToString(&buf, argTypes[i], (Datum)0, true);
                    }
#endif
                }
            }
            pfree_ext(argTypes);
        } else {
            for (int i = 0; i < nargs; i++) {
                if (i > 0)
                    appendStringInfoChar(&buf, ',');
                if (fcinfo->argTypes[i] == REFCURSOROID) {
                    ReleaseSysCache(procTup);
                    ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("Un-support:ref_cursor parameter is not supported for autonomous transactions.")));
                }
                TypeValueToString(&buf, fcinfo->argTypes[i], fcinfo->arg[i], fcinfo->argnull[i]);
            }
        }

        appendStringInfo(&buf, "); %s", "commit;");

        ReleaseSysCache(procTup);

    } else {
        if (sourceText == NULL) {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Invalid input anonymous block")));
        } else {
            appendStringInfo(&buf, "%s", sourceText);
            appendStringInfo(&buf, "; %s", "commit;");
        }
    }

    return buf.data;
}

/*
 * Subtransction's resowner can't be released right now since its
 * invoker may refer it. We keep them and their scope in one list,
 * and then try to release them at later.
 */
typedef struct {
    /* reserved subxact's resourceowner */
    ResourceOwner resowner;

    /*
     * resourceowner's scope, no one refers it before this stackId.
     * '-1' means it can be released at any time.
     */
    int64 stackId;

    /* next item in the list */
    void *next;
} XactContextItem;

/*
 * reserve current subxact's Resowner into session list.
 *
 * NOTE: resowner's scope(u_sess->plsql_cxt.minSubxactStackId) should be ready in advance.
 */
void stp_reserve_subxact_resowner(ResourceOwner resowner)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    XactContextItem *item = (XactContextItem*)palloc(sizeof(XactContextItem));
    item->resowner = resowner;
    item->stackId = u_sess->plsql_cxt.minSubxactStackId;

    ResourceOwnerNewParent(item->resowner, NULL);
    ResourceOwnerMarkInvalid(item->resowner);
    item->next = u_sess->plsql_cxt.spi_xact_context;
    MemoryContextSwitchTo(oldcxt);

    u_sess->plsql_cxt.spi_xact_context = item;
}

/*
 * release reserved subxact resowner after 'stackId'.
 */
void stp_cleanup_subxact_resowner(int64 stackId)
{
    XactContextItem *item = (XactContextItem*)u_sess->plsql_cxt.spi_xact_context;
    XactContextItem *preItem = NULL;

    while (item != NULL) {
        /*
         * (1) those at the upper stack can be clean at this level.
         * (2) -1 means it can be clean at any time.
         */
        if (item->stackId > stackId || item->stackId == -1) {
            ResourceOwnerDelete(item->resowner);

            /* remove it and process the next one. */
            if (preItem != NULL) {
                preItem->next = item->next;
                pfree(item);
                item = (XactContextItem*)preItem->next;
            } else {
                u_sess->plsql_cxt.spi_xact_context = item->next;
                pfree(item);
                item = (XactContextItem*)u_sess->plsql_cxt.spi_xact_context;
            }
        } else {
            preItem = item;
            item = (XactContextItem*)item->next;
        }
    }
}

/* reset store procedure context for each transaction. */
void stp_reset_xact()
{
    /* cleanup all reserved subxact resourceowner. */
    stp_cleanup_subxact_resowner(-1);

    /* reset stack counter for xact. */
    u_sess->plsql_cxt.nextStackEntryId = 0;
}

/* reset store procedure context for each statement. */
void stp_reset_stmt()
{
    stp_reset_opt_values();

    /* any reserved subxact resowner in previous statement can be released now. */
    stp_cleanup_subxact_resowner(-1);
}

bool recheckTableofType(TupleDesc tupdesc, TupleDesc retdesc)
{
    int n = tupdesc->natts;
    bool has_change = false;

    for (int i = 0; i < n; i++) {
        Form_pg_attribute att = tupdesc->attrs[i];
        if (att->attisdropped)
            continue; 
        Oid baseOid = InvalidOid;
        if (isTableofType(att->atttypid, &baseOid, NULL)) {
            Oid typOid = baseOid;
            char colname[NAMEDATALEN] = {0};
            errno_t rc = memcpy_s(colname, NAMEDATALEN, tupdesc->attrs[i]->attname.data, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            TupleDescInitEntry(tupdesc, i + 1, colname, typOid, retdesc->attrs[i]->atttypmod, 0);
            has_change = true;
        }
    }
    return has_change;
}

static void free_func_tableof_index()
{
    if (u_sess->plsql_cxt.func_tableof_index == NULL) {
        return;
    }
    ListCell* l = NULL;
    foreach (l, u_sess->plsql_cxt.func_tableof_index) {
        PLpgSQL_func_tableof_index* func_tableof = (PLpgSQL_func_tableof_index*)lfirst(l);
        hash_destroy(func_tableof->tableOfIndex);
    }

    list_free_deep(u_sess->plsql_cxt.func_tableof_index);
    u_sess->plsql_cxt.func_tableof_index = NIL;
}

static void init_implicit_cursor_attr(PLpgSQL_execstate *estate)
{
    /*
     * Set the magic variable FOUND to false
     */
    exec_set_found(estate, false);
    /* Set the magic implicit cursor attribute variable FOUND to false */
    exec_set_sql_cursor_found(estate, PLPGSQL_NULL);
    
    /* Set the magic implicit cursor attribute variable NOTFOUND to true */
    exec_set_sql_notfound(estate, PLPGSQL_NULL);
    
    /* Set the magic implicit cursor attribute variable ISOPEN to false */
    exec_set_sql_isopen(estate, false);
    
    /* Set the magic implicit cursor attribute variable ROWCOUNT to 0 */
    exec_set_sql_rowcount(estate, -1);

}

#ifndef ENABLE_MULTIPLE_NODES
static void reset_implicit_cursor_attr(PLpgSQL_execstate *estate)
{
    /*
     * Set the magic variable FOUND to false
     */
    exec_set_found(estate, false);
    /* Set the magic implicit cursor attribute variable FOUND to false */
    exec_set_sql_cursor_found(estate, PLPGSQL_FALSE);
    
    /* Set the magic implicit cursor attribute variable NOTFOUND to true */
    exec_set_sql_notfound(estate, PLPGSQL_TRUE);
    
    /* Set the magic implicit cursor attribute variable ISOPEN to false */
    exec_set_sql_isopen(estate, false);
    
    /* Set the magic implicit cursor attribute variable ROWCOUNT to 0 */
    exec_set_sql_rowcount(estate, 0);
}
#endif

/* ----------
 * plpgsql_exec_autonm_function	Called by the call handler for
 *				autonomous function execution.
 * ----------
 */
Datum plpgsql_exec_autonm_function(PLpgSQL_function* func, 
                                            FunctionCallInfo fcinfo, char* sourceText)
{
    TupleDesc tupDesc = NULL;
    PLpgSQL_execstate estate;
    FormatCallStack plcallstack;
    /*
     * Setup the execution state
     */
    plpgsql_estate_setup(&estate, func, (ReturnSetInfo*)fcinfo->resultinfo);
    func->debug = NULL;

    /* setup call stack for format_call_stack */
    plcallstack.elem = &estate;
    plcallstack.prev = t_thrd.log_cxt.call_stack;
    t_thrd.log_cxt.call_stack = &plcallstack;
    /*
     * Make local execution copies of all the datums
     */
    estate.err_text = gettext_noop("during initialization of execution state");
    /*
     * if the datum type is unknown, it means it's a package variable,so we need
     * compile the package , and replace the unknown type.
     */
    for (int i = 0; i < estate.ndatums; i++) {
        if (!func->datums[i]->ispkg) {
            estate.datums[i] = copy_plpgsql_datum(func->datums[i]);
        } else {
            estate.datums[i] = func->datums[i];
        }
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_DATANODE) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), 
                errmodule(MOD_PLSQL), errmsg("Un-support:DN does not support invoking autonomous transactions.")));
    }
#endif

#ifndef ENABLE_MULTIPLE_NODES
    if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
        exec_set_cursor_att_var(&estate, estate_tmp);
    } else {
#endif
    init_implicit_cursor_attr(&estate);
#ifndef ENABLE_MULTIPLE_NODES
    }
#endif

    if (estate.sqlcode_varno) {
        if (plcallstack.prev != NULL) {
            PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
            exec_set_prev_sqlcode(&estate, estate_tmp);
        }
    }

    /*
     * libpq link establishment
     * If no, create it. If yes, check its link status.
     */
    CreateAutonomousSession();

#ifndef ENABLE_MULTIPLE_NODES
    uint64 sessionId = IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;
    /* add session package values to global for autonm session, to restore package values */
    BuildSessionPackageRuntimeForAutoSession(sessionId, u_sess->autonomous_parent_sessionid, &estate, func);
#endif

    /* Statement concatenation. If the block is an anonymous block, the entire anonymous block is returned. */
    char* sql = AssembleAutomnousStatement(func, fcinfo, sourceText);

    /* If the return value of the function is of the record type, add the function to the temporary cache. */
    bool returnTypeNotMatch = false;
    if (is_function_with_plpgsql_language_and_outparam(func->fn_oid)) {
        Oid fn_rettype = func->fn_rettype;
        TypeFuncClass typclass;
        construct_func_param_desc(fcinfo->flinfo->fn_oid, &typclass, &tupDesc, &fn_rettype);
        returnTypeNotMatch = sourceText == NULL && func->fn_retistuple;
    } else {
        returnTypeNotMatch = sourceText == NULL && func->fn_retistuple
            && get_func_result_type(fcinfo->flinfo->fn_oid, NULL, &tupDesc) != TYPEFUNC_COMPOSITE;
    }

    if (returnTypeNotMatch) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), 
                errmodule(MOD_PLSQL), errmsg("unrecognized return type for PLSQL function.")));
    }

    List* search_path = fetch_search_path(false);

    if (search_path != NIL) {
        StringInfoData buf;
        ListCell* l = NULL;
        char* nspname = NULL;
        initStringInfo(&buf);
        appendStringInfo(&buf, "SET search_path TO ");
        foreach (l, search_path) {
            nspname = get_namespace_name(lfirst_oid(l));
            if (nspname != NULL) { /* watch out for deleted namespace */
                appendStringInfo(&buf, " \"%s\"", nspname);
                if (lnext(l) != NULL) {
                    appendStringInfoChar(&buf, ',');
                }
            }
        }
        appendStringInfoChar(&buf, ';');
        (void)u_sess->SPI_cxt.autonomous_session->ExecSimpleQuery(buf.data, NULL, 0);
        list_free_ext(search_path);
        if (buf.data != NULL) {
            pfree(buf.data);
        }
    }
    StringInfoData buf;
    initStringInfo(&buf);
#ifndef ENABLE_MULTIPLE_NODES
    if (is_function_with_plpgsql_language_and_outparam(func->fn_oid)) {
        appendStringInfo(&buf, "set behavior_compat_options='proc_outparam_override';");
    }
#endif
    if (COMPAT_CURSOR) {
        appendStringInfo(&buf, "set behavior_compat_options='COMPAT_CURSOR';");
    }
    appendStringInfo(&buf, "start transaction;");
    (void)u_sess->SPI_cxt.autonomous_session->ExecSimpleQuery(buf.data, NULL, 0);
    if (buf.data != NULL) {
        pfree(buf.data);
    }

    ATResult res = u_sess->SPI_cxt.autonomous_session->ExecSimpleQuery("select txid_current();", NULL, 0);
    int64 automnXid = res.ResTup;

    /* Call the libpq interface to send function names and parameters. */
    res = u_sess->SPI_cxt.autonomous_session->ExecSimpleQuery(
        sql, tupDesc, automnXid, true, is_function_with_plpgsql_language_and_outparam(func->fn_oid));
    fcinfo->isnull = res.resisnull;

    /* Process the information whose return value is of the record type. */
    if (sourceText == NULL && func->fn_retistuple) {
        /*
         * We have to check that the returned tuple actually matches the
         * expected result type.  XXX would be better to cache the tupdesc
         * instead of repeating get_call_result_type()
         */
        HeapTuple retTup = (HeapTuple)DatumGetPointer(res.ResTup);
        TupleDesc outTupdesc;
        TupleConversionMap* tupMap = NULL;
        switch (get_call_result_type(fcinfo, NULL, &outTupdesc)) {
            case TYPEFUNC_COMPOSITE:
                tupMap = convert_tuples_by_position(tupDesc,
                    outTupdesc,
                    gettext_noop("returned record type does not match expected record type"),
                    func->fn_oid);
                /* it might need conversion */
                if (tupMap != NULL) {
                    retTup = do_convert_tuple(retTup, tupMap);
                }
                /* no need to free map, we're about to return anyway */
                break;
            default:
                /* shouldn't get here if retistuple is true ... */
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL), errmsg("unrecognized return type for PLSQL function.")));
                break;
        }
        /*
         * Copy tuple to upper executor memory, as a tuple Datum. Make
         * sure it is labeled with the caller-supplied tuple type.
         */
        res.ResTup = PointerGetDatum(SPI_returntuple(retTup, outTupdesc));
    } else {
        /*
        * If the function's return type isn't by value, copy the value
        * into upper executor memory context.
        */
        if (!fcinfo->isnull && !func->fn_retbyval) {
            Size len;
            void* tmp = NULL;
            errno_t rc = EOK;

            len = datumGetSize(res.ResTup, false, func->fn_rettyplen);
            tmp = SPI_palloc(len);
            rc = memcpy_s(tmp, len, DatumGetPointer(res.ResTup), len);
            securec_check(rc, "\0", "\0");
            res.ResTup = PointerGetDatum(tmp);
        }
    }
    if (sql != NULL) {
        pfree(sql);
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* for restore parent session and automn session package var values */
    List *autonmsList = processAutonmSessionPkgs(func, NULL, true);
    if (autonmsList != NULL) {
        reset_implicit_cursor_attr(&estate);
    }
    if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
        exec_set_cursor_att_var(estate_tmp, &estate);
    }
#endif
    /* Clean up any leftover temporary memory */
    plpgsql_destroy_econtext(&estate);
    exec_eval_cleanup(&estate);

    /* pop the call stack */
    t_thrd.log_cxt.call_stack = plcallstack.prev;
    /*
     * Return the function's result
     */
    return res.ResTup;
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
    FormatCallStack plcallstack;
    int i;
    int rc;
    bool saved_current_stp_with_exception;
    /*
     * Setup the execution state
     */
    plpgsql_estate_setup(&estate, func, (ReturnSetInfo*)fcinfo->resultinfo);
    func->debug = NULL;

#ifndef ENABLE_MULTIPLE_NODES
    check_debug(func, &estate);
    bool isExecAutoFunc = u_sess->is_autonomous_session == true && u_sess->SPI_cxt._connected == 0;
    /* when exec autonomous transaction procedure, need update package values by parent session */
    if (isExecAutoFunc) {
        initAutoSessionPkgsValue(u_sess->autonomous_parent_sessionid);
    }
#endif

    saved_current_stp_with_exception = plpgsql_get_current_value_stp_with_exception();
    /*
     * Setup error traceback support for ereport()
     */
    plerrcontext.callback = plpgsql_exec_error_callback;
    plerrcontext.arg = &estate;
    plerrcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &plerrcontext;

    
    /* setup call stack for format_call_stack */
    plcallstack.elem = &estate;
    plcallstack.prev = t_thrd.log_cxt.call_stack;
    t_thrd.log_cxt.call_stack = &plcallstack;

    /*
     * Make local execution copies of all the datums
     */
    estate.err_text = gettext_noop("during initialization of execution state");

    /*
     * if the datum type is unknown, it means it's a package variable,so we need
     * compile the package , and replace the unknown type.
     */

    for (i = 0; i < estate.ndatums; i++) {
        if (!func->datums[i]->ispkg) {
            estate.datums[i] = copy_plpgsql_datum(func->datums[i]);
        } else {
            estate.datums[i] = func->datums[i];
        }
    }

    /*
     * Store the actual call argument values into the appropriate variables
     */
    estate.err_text = gettext_noop("while storing call arguments into local variables");
    int outArgCnt = 0;
    bool isNULL = false;
    char *argmodes = NULL;
    HeapTuple procTup = NULL;
    Datum proArgModes = 0;
    bool is_plpgsql_function_with_outparam = is_function_with_plpgsql_language_and_outparam(func->fn_oid);
    if (func->fn_nargs != fcinfo->nargs && is_plpgsql_function_with_outparam) {
        procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
        proArgModes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proargmodes, &isNULL);
        Assert(!isNULL);
        if (!isNULL) {
            ArrayType *arr = DatumGetArrayTypeP(proArgModes);
            argmodes = (char *)ARR_DATA_PTR(arr);
        }
    }
    for (i = 0; i < func->fn_nargs; i++) {
        int n = func->fn_argvarnos[i];

        switch (estate.datums[n]->dtype) {
            case PLPGSQL_DTYPE_VARRAY:
            case PLPGSQL_DTYPE_TABLE:
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)estate.datums[n];
                if (func->fn_nargs != fcinfo->nargs && is_plpgsql_function_with_outparam) {
                    while (argmodes[outArgCnt] == PROARGMODE_OUT) {
                        outArgCnt++;
                    }
                    var->value = CopyFcinfoArgValue(fcinfo->argTypes[outArgCnt], fcinfo->arg[outArgCnt]);
                    var->isnull = fcinfo->argnull[outArgCnt];
                    var->freeval = false;
                    outArgCnt++;
                } else {
                    var->value = CopyFcinfoArgValue(fcinfo->argTypes[i], fcinfo->arg[i]);
                    var->isnull = fcinfo->argnull[i];
                    var->freeval = false;
                }
                if (u_sess->plsql_cxt.func_tableof_index != NULL) {
                    ListCell* l = NULL;
                    foreach (l, u_sess->plsql_cxt.func_tableof_index) {
                        PLpgSQL_func_tableof_index* func_tableof = (PLpgSQL_func_tableof_index*)lfirst(l);
                        if (func_tableof->varno == i) {
                            var->tableOfIndexType = func_tableof->tableOfIndexType;
                            var->tableOfIndex = copyTableOfIndex(func_tableof->tableOfIndex);
                        }
                    }
                }
                
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
                if (procTup != NULL) {
                    ReleaseSysCache(procTup);
                }
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized argument data type: %d for PLSQL function.", func->datums[i]->dtype)));
                break;
        }
    }
    if (procTup != NULL) {
        ReleaseSysCache(procTup);
    }
    free_func_tableof_index();
    estate.err_text = gettext_noop("during function entry");

    estate.cursor_return_data = fcinfo->refcursor_data.returnCursor;
    estate.cursor_return_numbers = fcinfo->refcursor_data.return_number;

#ifndef ENABLE_MULTIPLE_NODES
    if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
        exec_set_cursor_att_var(&estate, estate_tmp);
    } else if (isExecAutoFunc && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        init_implicit_cursor_attr(&estate);
        initAutoSessionFuncInfoValue(u_sess->autonomous_parent_sessionid, &estate);
    } else {
#endif
        init_implicit_cursor_attr(&estate);
#ifndef ENABLE_MULTIPLE_NODES
    }
#endif

    if (estate.sqlcode_varno) {
        if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
            PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
            exec_set_prev_sqlcode(&estate, estate_tmp);
#ifndef ENABLE_MULTIPLE_NODES
        } else if (isExecAutoFunc && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
            /*
             * No processing is performed because the SQL code has been updated by
             * the initAutoSessionFuncInfoValue function.
             */
#endif
        } else if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
            exec_set_sqlcode(&estate, 0);
        }
    }


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
                    errmodule(MOD_PLSQL), errmsg("CONTINUE cannot be used outside a loop")));
        } else if (rc == PLPGSQL_RC_GOTO_UNRESOLVED) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg(
                        "illegal GOTO statement; this GOTO cannot branch to label \"%s\"", estate.goto_target_label)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
                    errmodule(MOD_PLSQL), errmsg("control reached end of function without RETURN")));
        }
    }

    pfree_ext(u_sess->plsql_cxt.pass_func_tupdesc);

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
    } else if (!estate.retisnull || !estate.paramisnull) {
        if (estate.retistuple) {
            /*
             * We have to check that the returned tuple actually matches the
             * expected result type.  XXX would be better to cache the tupdesc
             * instead of repeating get_call_result_type()
             */
            HeapTuple rettup = (HeapTuple)DatumGetPointer(estate.retval);
            TupleDesc tupdesc;
            TupleConversionMap* tupmap = NULL;
            TupleDesc retdesc = (estate.paramtupdesc) ? estate.paramtupdesc : estate.rettupdesc;

            switch (get_call_result_type(fcinfo, NULL, &tupdesc)) {
                case TYPEFUNC_COMPOSITE:
                    /* got the expected result rowtype, now check it */
                    if (retdesc == NULL && estate.func->out_param_varno >= 0) {
                        PLpgSQL_datum* out_param_datum = estate.datums[estate.func->out_param_varno];
                        if ((out_param_datum->dtype == PLPGSQL_DTYPE_VARRAY ||
                            out_param_datum->dtype == PLPGSQL_DTYPE_TABLE ||
                            out_param_datum->dtype == PLPGSQL_DTYPE_VAR) &&
                            ((PLpgSQL_var*)out_param_datum)->datatype != NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                                    errmodule(MOD_PLSQL),
                                    errmsg("function result type must be %s because of OUT parameters",
                                        ((PLpgSQL_var*)out_param_datum)->datatype->typname)));
                        }
                    }
                    /* if tuple has tableof type, we replace its base type, and pass to upper function's */
                    if (recheckTableofType(tupdesc, retdesc)) {
                        MemoryContext old = MemoryContextSwitchTo(u_sess->temp_mem_cxt);
                        u_sess->plsql_cxt.pass_func_tupdesc = CreateTupleDescCopy(tupdesc);
                        MemoryContextSwitchTo(old);
                    }

                    tupmap = convert_tuples_by_position(retdesc,
                        tupdesc,
                        gettext_noop("returned record type does not match expected record type"),
                        estate.func->fn_oid);
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
                    tupdesc = retdesc;
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
                            errmodule(MOD_PLSQL), errmsg("unrecognized return type for PLSQL function.")));
                    break;
            }

            /*
             * Copy tuple to upper executor memory, as a tuple Datum. Make
             * sure it is labeled with the caller-supplied tuple type.
             */
            estate.retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));
        } else {
            if (estate.rettype == InvalidOid && estate.func->out_param_varno >= 0) {
                PLpgSQL_datum *out_param_datum = estate.datums[estate.func->out_param_varno];
                if (out_param_datum->dtype == PLPGSQL_DTYPE_ROW && ((PLpgSQL_row *)out_param_datum)->nfields > 1) {
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

        if (estate.paramtupdesc != NULL || OidIsValid(estate.paramtype)) {
            HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
            if (!HeapTupleIsValid(tp)) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmodule(MOD_PLSQL),
                                errmsg("Cache lookup failed for function %u", func->fn_oid),
                                errdetail("Fail to get the function param type oid.")));
            }
            int out_args_num = 0;
            TupleDesc tupdesc = get_func_param_desc(tp, func->fn_rettype, &out_args_num);
            if (OidIsValid(estate.paramtype) || out_args_num == 1) { // For func has one out param
                Datum values[] = {estate.retval, estate.paramval};
                bool nulls[] = {estate.retisnull, estate.paramisnull};
                HeapTuple rettup = heap_form_tuple(tupdesc, values, nulls);
                estate.retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));
            } else { // For func has multiple out params
                Datum *values = (Datum*)palloc(sizeof(Datum) * (out_args_num + 1));
                bool *nulls = (bool*)palloc(sizeof(bool) * (out_args_num + 1));
                heap_deform_tuple((HeapTuple)DatumGetPointer(estate.paramval), estate.paramtupdesc, (values + 1),
                                  (nulls + 1));
                values[0] = estate.retval;
                nulls[0] = estate.retisnull;
                HeapTuple rettup = heap_form_tuple(tupdesc, values, nulls);
                estate.retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));
                pfree(values);
                pfree(nulls);
            }
            ReleaseSysCache(tp);
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
            if (estate.rettupdesc && i < estate.rettupdesc->natts &&
                IsClientLogicType(estate.rettupdesc->attrs[i]->atttypid) && newm->datatype) {
                newm->datatype->atttypmod = estate.rettupdesc->attrs[i]->atttypmod;
            }
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
        exec_set_cursor_att_var(estate_tmp, &estate);
    }
#endif
    estate.cursor_return_data = NULL;
    estate.cursor_return_numbers = 0;

#ifndef ENABLE_MULTIPLE_NODES
    /* for restore parent session and automn session package var values */
    (void)processAutonmSessionPkgs(func, &estate);
#endif
    /* Clean up any leftover temporary memory */
    plpgsql_destroy_econtext(&estate);
    exec_eval_cleanup(&estate);

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = plerrcontext.previous;
    plpgsql_restore_current_value_stp_with_exception(saved_current_stp_with_exception);

    /* pop the call stack */
    t_thrd.log_cxt.call_stack = plcallstack.prev;
    /*
     * Return the function's result
     */
    return estate.retval;
}

static Datum CopyFcinfoArgValue(Oid typOid, Datum value)
{
#ifdef ENABLE_MULTIPLE_NODES
    return value;
#endif
    if (!OidIsValid(typOid)) {
        return value;
    }
    /*
     * For centralized database, package value may be in param.
     * In this case, the value should be copyed, becaues the ref
     * value may be influenced by procedure.
     */
    bool typByVal = false;
    int16 typLen;
    get_typlenbyval(typOid, &typLen, &typByVal);
    return datumCopy(value, typByVal, typLen);
}

static void RecordSetGeneratedField(PLpgSQL_rec *recNew)
{
    int natts = recNew->tupdesc->natts;
    Datum* values = NULL;
    bool* nulls = NULL;
    bool* replaces = NULL;
    HeapTuple newtup = NULL;
    errno_t rc = EOK;

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

    for (int i = 0; i < recNew->tupdesc->natts; i++) {
        if (GetGeneratedCol(recNew->tupdesc, i) == ATTRIBUTE_GENERATED_STORED) {
            replaces[i] = true;
            values[i] = (Datum)0;
            nulls[i] = true;
        }
    }
    newtup = heap_modify_tuple(recNew->tup, recNew->tupdesc, values, nulls, replaces);
    if (recNew->freetup) {
        heap_freetuple_ext(recNew->tup);
    }

    recNew->tup = newtup;
    recNew->freetup = true;

    pfree_ext(values);
    pfree_ext(nulls);
    pfree_ext(replaces);
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
    FormatCallStack plcallstack;
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

    /* setup call stack for format_call_stack */
    plcallstack.elem = &estate;
    plcallstack.prev = t_thrd.log_cxt.call_stack;
    t_thrd.log_cxt.call_stack = &plcallstack;

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

        /*
         * In BEFORE trigger, stored generated columns are not computed yet,
         * so make them null in the NEW row.  (Only needed in UPDATE branch;
         * in the INSERT case, they are already null, but in UPDATE, the field
         * still contains the old value.)  Alternatively, we could construct a
         * whole new row structure without the generated columns, but this way
         * seems more efficient and potentially less confusing.
         */
        if (rec_new->tupdesc->constr && rec_new->tupdesc->constr->has_generated_stored &&
            TRIGGER_FIRED_BEFORE(trigdata->tg_event)) {
            RecordSetGeneratedField(rec_new);
        }
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
#ifndef ENABLE_MULTIPLE_NODES
    if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
        exec_set_cursor_att_var(&estate, estate_tmp);
        exec_set_prev_sqlcode(&estate, estate_tmp);
    } else {
#endif
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

        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
            exec_set_sqlcode(&estate, 0);
        }

#ifndef ENABLE_MULTIPLE_NODES
    }
#endif

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
                    errmodule(MOD_PLSQL), errmsg("control reached end of trigger procedure without RETURN")));
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
            gettext_noop("returned row structure does not match the structure of the triggering table"),
            estate.func->fn_oid);
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

#ifndef ENABLE_MULTIPLE_NODES
    if (plcallstack.prev != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PLpgSQL_execstate* estate_tmp = (PLpgSQL_execstate*)(plcallstack.prev->elem);
        exec_set_cursor_att_var(estate_tmp, &estate);
    }
#endif

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

    /* pop the call stack */
    t_thrd.log_cxt.call_stack = plcallstack.prev;

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
PLpgSQL_datum* copy_plpgsql_datum(PLpgSQL_datum* datum)
{
    PLpgSQL_datum* result = NULL;
    errno_t errorno = EOK;

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VARRAY:
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
        case PLPGSQL_DTYPE_TABLEELEM:
        case PLPGSQL_DTYPE_ASSIGNLIST:
        case PLPGSQL_DTYPE_COMPOSITE:
        case PLPGSQL_DTYPE_RECORD_TYPE:
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

/*
 * Action at exception block's entry
 *
 * starting a new subtransaction and save context.
 */
static void exec_exception_begin(PLpgSQL_execstate* estate, ExceptionContext *context)
{
    plpgsql_set_current_value_stp_with_exception();

    context->old_edata = estate->cur_error;
    context->cur_edata = NULL;
    context->spi_connected = SPI_connectid();

    /* save old memory context */
    context->oldMemCxt = CurrentMemoryContext;
    context->oldResOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    context->oldTransactionId = SPI_get_top_transaction_id();
    context->stackId = u_sess->plsql_cxt.nextStackEntryId;

    /* recording stmt's Top Portal ResourceOwner before any subtransaction. */
    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0 && u_sess->plsql_cxt.stp_savepoint_cnt == 0) {
        t_thrd.utils_cxt.STPSavedResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    }

    /* Execute statements in the block's body inside a sub-transaction */
    SPI_savepoint_create(NULL);

    /* save current Exception some mess information into context. */
    context->curExceptionCounter = u_sess->SPI_cxt.portal_stp_exception_counter;
    context->subXid = GetCurrentSubTransactionId();

    context->hasReleased = false;

    MemoryContextSwitchTo(context->oldMemCxt);
}

/*
 * Action at exception block's normal exit
 */
static void exec_exception_end(PLpgSQL_execstate* estate, ExceptionContext *context)
{
    context->hasReleased = true;

    /* Commit the exception's transaction while there is no savepoint in this block. */
    if (context->curExceptionCounter == u_sess->SPI_cxt.portal_stp_exception_counter &&
        GetCurrentTransactionName() == NULL) {
        /*
         * This is an internal subtransaction for exception. Normal, we destory it as if it's
         * outside STP. However, its id may changes after the rollback/commit. It means some
         * wrong was may related to it by id, such as SPI connect, search path and etc.
         *
         * Luckily, the previous same subtransaction has terminated with keeping its STP resources
         * while the exception's subtransaction owns none about these in the scenario. Hence, here
         * we terminate this exception's subtransaction as if it is inside STP.
         */
        SPI_savepoint_release(NULL);

        stp_cleanup_subxact_resowner(context->stackId);
    }

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

    /* Get last transaction's ResourceOwner. */
    stp_check_transaction_and_set_resource_owner(context->oldResOwner, context->oldTransactionId);

    MemoryContextSwitchTo(context->oldMemCxt);
}

/*
 * Action at exception block's abnormal exit
 *
 * do some cleanup before handling captured ERROR.
 */
static void exec_exception_cleanup(PLpgSQL_execstate* estate, ExceptionContext *context)
{
    MemoryContextSwitchTo(context->oldMemCxt);

    /* Set bInAbortTransaction to avoid core dump caused by recursive memory alloc exception */
    t_thrd.xact_cxt.bInAbortTransaction = true;

    /* Save error info */
    ErrorData* edata = CopyErrorData();
    t_thrd.xact_cxt.bInAbortTransaction = false;
    FlushErrorState();

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
    context->cur_edata =  edata;

    if (context->hasReleased) {
        ereport(FATAL,
                (errmsg("exception happens after current savepoint released error message is: %s",
                    edata->message ? edata->message : " ")));
    }

    /*
     * process resource's leak between subXid and latest subtransaction, such as ActiveSnapshot, catrefs
     * and etc. If the top transaction has changed, clean all the subtransaction.
     *
     * NOTE: More consideration is required to clean up subtransaction on DN to support multinode.
     */
    XactCleanExceptionSubTransaction(
        context->oldTransactionId != SPI_get_top_transaction_id() ? 0 : context->subXid);

    char *txnName = GetCurrentTransactionName();

    /* Abort the inner transaction */
    if (context->curExceptionCounter == u_sess->SPI_cxt.portal_stp_exception_counter && txnName == NULL) {
        ResetPortalCursor(GetCurrentSubTransactionId(), InvalidOid, 0);

        /*
         * destory this internal subtransaction as if it is inside STP.
         */
        if (PLSTMT_IMPLICIT_SAVEPOINT) {
            /* release this exception's subtransaction in time */
            SPI_savepoint_release(NULL);
        } else {
            SPI_savepoint_rollbackAndRelease(NULL, InvalidTransactionId);
        }

        /*
         * None should has references to this ResouceOwner of exception's subtransaction. Since ResourceOwner
         * is reserved during above destorying, Deal it specially to release its memroy as soon as poosible.
         */
        stp_cleanup_subxact_resowner(context->stackId);
    } else if (!PLSTMT_IMPLICIT_SAVEPOINT) {
        /*
         * rollback to the lastest savepoint which would be user's savepoint or exception's.
         */
        exec_savepoint_rollback(estate, txnName);
    }

    /* Since above AbortSubTransaction may has destory connections, we can't go ahead. */
    if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
        ereport(ERROR, (errcode(ERRCODE_TRANSACTION_ROLLBACK), errmodule(MOD_PLSQL),
                errmsg("Transaction aborted as connection handles were destroyed due to clean up stream "
                "failed. error message is: %s", edata->message ? edata->message : " ")));
    }

    /* destory SPI connects created in this exception block. */
    SPI_disconnect(context->spi_connected + 1);

    /*
     * Revert to outer eval_econtext.  (The inner one was
     * automatically cleaned up during subxact exit.)
     */
    estate->eval_econtext = u_sess->plsql_cxt.simple_econtext_stack->stack_econtext;

    /*
     * With subtransaction aborted or not, SPI may be in a disconnected state,
     * We need this hack to return to connected state.
     */
    SPI_restore_connection();

    t_thrd.xact_cxt.isSelectInto = false;

    /* Get last transaction's ResourceOwner. */
    stp_check_transaction_and_set_resource_owner(context->oldResOwner, context->oldTransactionId);

    MemoryContextSwitchTo(context->oldMemCxt);

    /* Must clean up the econtext too */
    exec_eval_cleanup(estate);
}

/*
 * handle exception as use defined handler.
 *
 * if no handler is defiend for exception, exception will be re-throw out.
 */
static int exec_exception_handler(PLpgSQL_execstate* estate, PLpgSQL_stmt_block* block, ExceptionContext *context)
{
    ListCell* e = NULL;
    ErrorData* edata = context->cur_edata;
    int rc = -1;

    estate->is_exception = true;
    /* no error can be ignored once connection was destoryed */
    if (t_thrd.xact_cxt.handlesDestroyedInCancelQuery) {
        estate->cur_error = context->old_edata;
        ReThrowError(edata);
    }

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

            state_var = (PLpgSQL_var*)estate->datums[estate->sqlstate_varno];
            errm_var = (PLpgSQL_var*)estate->datums[estate->sqlerrm_varno];
            assign_text_var(state_var, plpgsql_get_sqlstate(edata->sqlerrcode));
            assign_text_var(errm_var, edata->message);

            /*
             * Also set up cur_error so the error data is accessible
             * inside the handler.
             */
            estate->cur_error = edata;

            estate->err_text = NULL;

            exec_set_sqlcode(estate, edata->sqlerrcode);

            ExceptionContext* saved_cxt = u_sess->plsql_cxt.cur_exception_cxt;
            u_sess->plsql_cxt.cur_exception_cxt = context;
            rc = exec_stmts(estate, exception->action);
            u_sess->plsql_cxt.cur_exception_cxt = saved_cxt;
            saved_cxt = NULL;

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
    estate->cur_error = context->old_edata;

    /* If no match found, re-throw the error */
    if (e == NULL) {
        ReThrowError(edata);
    } else {
        FreeErrorData(edata);
    }

    return rc;
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
    int n_initvars = 0; 
    int* initvarnos;
#ifndef ENABLE_MULTIPLE_NODES
    bool initPackageVar = false;
#endif

    /*
     * First initialize all variables declared in this block
     */
    bool savedisAllowCommitRollback = u_sess->SPI_cxt.is_allow_commit_rollback;
    bool savedIsSTP = u_sess->SPI_cxt.is_stp;
    bool savedProConfigIsSet = u_sess->SPI_cxt.is_proconfig_set;
    estate->err_text = gettext_noop("during statement block local variable initialization");

    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        /*
         * when compile package , inline_code_block means it's a package instantiation block,
         * so it's only has package variables,and the variable must be declared in package.
         */
        if (strcmp(estate->func->fn_signature, "inline_code_block") ==0 ) {
            n_initvars = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->n_initvars;
            initvarnos = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->initvarnos;
#ifndef ENABLE_MULTIPLE_NODES
            initPackageVar = true;
#endif
        } else {
            n_initvars = block->n_initvars;
            initvarnos = block->initvarnos;      
        }
    } else {
        n_initvars = block->n_initvars;
        initvarnos = block->initvarnos;
    }

    for (i = 0; i < n_initvars; i++) {
        n = initvarnos[i];
        if (unlikely(n >= estate->ndatums || n < 0)) {
            ereport(WARNING,
                    (errmsg("excepted dno in 0 to %d, but get dno %d", estate->ndatums - 1, n)));
            continue;
        }
        switch (estate->datums[n]->dtype) {
            case PLPGSQL_DTYPE_VARRAY:
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[n]);
                /* free any old value, in case re-entering block */
                if (!var->ispkg) {
                    free_var(var);
                } else {
                    var->freeval = false;
                }

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

            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD: {
                PLpgSQL_row* row = (PLpgSQL_row*)(estate->datums[n]);
                if (row->default_val != NULL) {
                    exec_assign_expr(estate, (PLpgSQL_datum*)row, row->default_val);
                }
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

#ifndef ENABLE_MULTIPLE_NODES
    bool needInitAutoPkg = initPackageVar && u_sess->is_autonomous_session;
    if (needInitAutoPkg) {
        /* autonomous session, init package values from parent session */
        initAutonomousPkgValue(u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package,
            u_sess->autonomous_parent_sessionid);
    }
#endif

    if (block->exceptions != NULL) {
        estate->err_text = gettext_noop("during statement block entry");

        ExceptionContext excptContext;
        Cursor_Data* saved_cursor_data = estate->cursor_return_data;
        int saved_cursor_numbers = estate->cursor_return_numbers;

        exec_exception_begin(estate, &excptContext);
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

            /* Run the block's statements */
            rc = exec_stmts(estate, block->body);

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

            exec_exception_end(estate, &excptContext);
            stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
        }
        PG_CATCH();
        {
            if (estate->func->debug) {
                PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[estate->func->debug->comm->comm_idx];
                /* client has error and debug on inner funciton, throw current error */
                if (debug_comm->hasClientErrorOccured) {
                    ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                            errmsg("Debug client has some error occured.")));
                }
            }
            stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
            u_sess->SPI_cxt.is_stp = savedIsSTP;
            u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;

            estate->cursor_return_data = saved_cursor_data;
            estate->cursor_return_numbers = saved_cursor_numbers;

            /* reset stream for-loop flag */
            u_sess->SPI_cxt.has_stream_in_cursor_or_forloop_sql = false;

            /* gs_signal_handle maybe block sigusr2 when accept SIGINT */
            gs_signal_unblock_sigusr2();

            estate->err_text = gettext_noop("during exception cleanup");

            exec_exception_cleanup(estate, &excptContext);

            rc = exec_exception_handler(estate, block, &excptContext);

            stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
            u_sess->SPI_cxt.is_stp = savedIsSTP;
            u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
        }
        PG_END_TRY();

        AssertEreport(excptContext.old_edata == estate->cur_error,
            MOD_PLSQL,
            "save current error should be same error  as estate current error.");
    } else {
        /*
         * Just execute the statements in the block's body
         */
        estate->err_text = NULL;

        rc = exec_stmts(estate, block->body);

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

#ifndef ENABLE_MULTIPLE_NODES
    if (estate->func->debug) {
        estate->func->debug->debugCallback(estate->func, estate);
    }
#endif
    /* clear table of index every stmt */
    if (u_sess->SPI_cxt.cur_tableof_index != NULL) {
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = InvalidOid;
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = NULL;
    }

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
            rc = exec_stmt_transaction(estate, stmt);
            break;

        case PLPGSQL_STMT_NULL:
            rc = exec_stmt_null(estate, (PLpgSQL_stmt_null*)stmt);
            break;
        case PLPGSQL_STMT_SAVEPOINT:
            rc = exec_stmt_savepoint(estate, stmt);
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

static Oid get_func_oid_from_expr(PLpgSQL_expr *expr)
{
    Assert(expr != NULL);
    if (!expr->is_funccall) {
        return InvalidOid;
    }

    if (expr->plan != NULL && expr->plan->stmt_list != NULL) {
        Query *query = (Query *)linitial(expr->plan->stmt_list);
        Assert(IsA(query, Query));
        if (query->rtable != NULL) {
            RangeTblEntry *rte = (RangeTblEntry *)linitial(query->rtable);
            if (rte->rtekind == RTE_FUNCTION) {
                return ((FuncExpr *)rte->funcexpr)->funcid;
            }
        }
    }
 
    if (expr->expr_simple_expr != NULL && IsA(expr->expr_simple_expr, FuncExpr)) {
        return ((FuncExpr*)expr->expr_simple_expr)->funcid;
    }

    return InvalidOid;
}

/*
 * @brief exec_set_outparam_value
 *  Seperate OUT param values from RETURN values of function call if exists.
 * @param estate            execute state
 * @param expr              expr of execsql or perform or assign stmt
 * @param value             raw RETURN value
 * @param return_is_null    raw RETURN value null flag
 */
static void plpgsql_set_outparam_value(PLpgSQL_execstate* estate, PLpgSQL_expr* expr,
                                       Datum* return_value = NULL, bool* return_is_null = NULL)
{
    /* Use this before exec_eval_cleanup */
    if (estate->eval_tuptable == NULL) {
        return;
    }

    /* make sure we are dealing with a function call with OUT params under A_FORMAT compatibility */
    if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT || expr->out_param_dno < 0 || !expr->is_funccall) {
        return;
    }

    Oid funcoid = get_func_oid_from_expr(expr);
    if (!is_function_with_plpgsql_language_and_outparam(funcoid)) {
        return;
    }

    /*
     * Seperate OUT param tuple from RETURN tuple.
     * We should always have the value of this form at this point:
     *  (RETURN, (OUT, OUT, ..., OUT))
     */
    TupleDesc tupdesc = estate->eval_tuptable->tupdesc;
    int attrsnum = estate->eval_tuptable->tupdesc->natts;
    Datum *values = (Datum*)palloc(sizeof(Datum) * attrsnum);
    bool *nulls = (bool*)palloc(sizeof(bool) * attrsnum);
    heap_deform_tuple(estate->eval_tuptable->vals[0], tupdesc, values, nulls);

    /* Set true RETURN value */
    if (return_value) {
        *return_value = values[0];
    }
    if (return_is_null) {
        *return_is_null = nulls[0];
    }

    /* Set OUT param value */
    HeapTuple tuple = NULL;
    TupleDesc paramtupdesc = NULL;
    if (attrsnum == 2) {
        Datum paramval = values[1];
        bool paramisnull = nulls[1];
        /* If we have a single OUT param, it is not going to be a RECORD */
        paramtupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);
        TupleDescInitEntry(paramtupdesc, (AttrNumber)1, NameStr(tupdesc->attrs[1]->attname),
                           tupdesc->attrs[1]->atttypid,
                           tupdesc->attrs[1]->atttypmod, 0);
        Datum vals[] = {paramval};
        bool ns[] = {paramisnull};
        tuple = heap_form_tuple(paramtupdesc, vals, ns);
    } else {
        /* Multiple OUT params */
        paramtupdesc = CreateTemplateTupleDesc(attrsnum - 1, false, TAM_HEAP);
        for (int i = 1; i < attrsnum; i++) {
            TupleDescInitEntry(paramtupdesc, (AttrNumber)i, NameStr(tupdesc->attrs[i]->attname),
                               tupdesc->attrs[i]->atttypid, tupdesc->attrs[i]->atttypmod, 0);
        }
        tuple = heap_form_tuple(paramtupdesc, (values + 1), (nulls + 1));
    }
    PLpgSQL_row* row = (PLpgSQL_row*)estate->datums[expr->out_param_dno];
    exec_move_row(estate, NULL, row, tuple, paramtupdesc);
    pfree(values);
    pfree(nulls);
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
#ifndef ENABLE_MULTIPLE_NODES
    CheckAssignTarget(estate, stmt->varno);
#endif
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

    plpgsql_set_outparam_value(estate, stmt->expr);

    /*
     * This is used for nested STP. If the transaction Id changed,
     * then need to create new econtext for the TopTransaction.
     */
    stp_check_transaction_and_create_econtext(estate,oldTransactionId);

    exec_set_found(estate, (estate->eval_processed != 0));
    exec_eval_cleanup(estate);

    /* reset the flag, used for restoreAutonmSessionCursors */
    u_sess->plsql_cxt.call_after_auto = false;

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
                exec_assign_c_string(estate, var, plpgsql_get_sqlstate(estate->cur_error->sqlerrcode));
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

const char* SE_SAVEPOINT_NAME = "save_exception_implicit_savepoint";

static Datum FormBulkExceptionDatum(int index, int errcode, const char* errmsg)
{
    Oid typoid = get_typeoid(PG_CATALOG_NAMESPACE, "bulk_exception");
    TupleDesc tupDesc = lookup_rowtype_tupdesc(typoid, -1);
    Datum values[tupDesc->natts];
    bool isnull[tupDesc->natts];
    int i = 0;
    /* index */
    values[i] = Int32GetDatum(index);
    isnull[i++] = false;
    /* errcode */
    values[i] = Int32GetDatum(errcode);
    isnull[i++] = false;
    /* errmsg */
    values[i] = CStringGetTextDatum(errmsg);
    isnull[i++] = false;
    HeapTuple result = heap_form_tuple(tupDesc, values, isnull);
    ReleaseTupleDesc(tupDesc);
    return HeapTupleGetDatum(result);
}

static void AppendBulkExceptionArray(Datum *target, Datum newelem)
{
    Oid typoid = get_typeoid(PG_CATALOG_NAMESPACE, "bulk_exception");
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    get_typlenbyvalalign(typoid, &typlen, &typbyval, &typalign);
    if (*target == 0) {
        /* Initialized array with single bulk error */
        int dims[] = {1};
        int lbs[] = {1};
        Datum values[] = {newelem};
        bool isNULL[] = {false};
        ArrayType *arr = construct_md_array(values, isNULL, 1, dims, lbs, typoid, typlen, typbyval, typalign);
        *target = (Datum)arr;
    } else {
        /* Append new error to array */
        ArrayType *arr = (ArrayType *)(*target);
        int indx = ARR_DIMS(arr)[0] + 1;
        *target = (Datum)array_set(arr, 1, &indx, newelem, false, typlen, -1, typbyval, typalign);
    }
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
    bool exception_saved = false;
    int rc = PLPGSQL_RC_OK;
    MemoryContext oldcontext = CurrentMemoryContext;
    int64 stackId = u_sess->plsql_cxt.nextStackEntryId;

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

    /* Initialize bulk exception list if save_exceptions is defined */
    if (stmt->save_exceptions) {
        SPI_stp_transaction_check(estate->readonly_func, true);
        PLpgSQL_var* be_var = (PLpgSQL_var*)estate->datums[estate->sql_bulk_exceptions_varno];
        if (!be_var->isnull && be_var->value != (Datum)NULL) {
            ArrayType *arr = (ArrayType *)be_var->value;
            ArrayIterator iter = array_create_iterator(arr, 0);
            array_free_iterator(iter);
            pfree(arr);
            be_var->isnull = true;
            be_var->value = (Datum)NULL;
        }
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
        if (stmt->save_exceptions) {
            ResourceOwner oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
            /*
             * When save exceptions is defined, a save point is created before
             * execution of the loop body. Release if successed; save exception
             * information and rollback if failed.
             */
            SPI_savepoint_create(SE_SAVEPOINT_NAME);

            PG_TRY();
            {
                rc = exec_stmts(estate, stmt->body);
                SPI_savepoint_release(SE_SAVEPOINT_NAME);
                plpgsql_create_econtext(estate);
                stp_cleanup_subxact_resowner(stackId);
                t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
                SPI_restore_connection();
            }
            PG_CATCH();
            {
                ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
                int errcode = edata->sqlerrcode;
                char* errmsg = edata->message;
                (void)MemoryContextSwitchTo(oldcontext);
                SPI_restore_connection();

                PLpgSQL_datum* bulk_exceptions = estate->datums[estate->sql_bulk_exceptions_varno];
                PLpgSQL_var* be_var = (PLpgSQL_var*)bulk_exceptions;

                Datum newelem = FormBulkExceptionDatum(loop_value, errcode, errmsg);
                AppendBulkExceptionArray(&(be_var->value), newelem);
                be_var->isnull = false;

                FlushErrorState();
                SPI_savepoint_rollbackAndRelease(SE_SAVEPOINT_NAME, InvalidTransactionId);
                stp_cleanup_subxact_resowner(stackId);
                t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
                exception_saved = true;
            }
            PG_END_TRY();
        } else {
            rc = exec_stmts(estate, stmt->body);
        }

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

    if (exception_saved) {
        /*
         * If FORALL saved any exception, create a savepoint for the last statement
         * and raise a dedicated error for forall dml execution.
         */
        SPI_savepoint_create(NULL);
        ereport(ERROR,
                (errmodule(MOD_PLSQL), errcode(ERRCODE_FORALL_DML_ERROR),
                    errmsg("forall dml statments contain saved failure"),
                    errdetail("please check SQL%%BULK_EXCEPTIONS for detail"),
                    errcause("one or more execution of the loop body raised exceptions"),
                    erraction("please handle errors in SQL%%BULK_EXCEPTIONS")));
    }

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
    /* Check to see if it is being collected by sqladvsior */
    bool isCollectParam = false;
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState()) {
        isCollectParam = true;
    }
#endif

    /*
     * Open the implicit cursor for the statement using exec_run_select
     */
    rc = exec_run_select(estate, stmt->query, 0, &portal, isCollectParam);
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
        if (curvar->pkg != NULL) {
            MemoryContext temp = MemoryContextSwitchTo(curvar->pkg->pkg_cxt);
            assign_text_var(curvar, portal->name);
            temp = MemoryContextSwitchTo(temp);
        } else {
            assign_text_var(curvar, portal->name);
        }
    }

    /* save the status of display cursor before Modify attributes of cursor */
    isopen.value = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ISOPEN]))->value;
    isopen.isnull = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ISOPEN]))->isnull;
    cursor_found.value = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_FOUND]))->value;
    cursor_found.isnull = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_FOUND]))->isnull;
    notfound.value = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_NOTFOUND]))->value;
    notfound.isnull = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_NOTFOUND]))->isnull;
    rowcount.value = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ROWCOUNT]))->value;
    rowcount.isnull = ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ROWCOUNT]))->isnull;

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
    exec_set_isopen(estate, false, stmt->curvar + CURSOR_ISOPEN);
    exec_set_cursor_found(estate, PLPGSQL_FALSE, stmt->curvar + CURSOR_FOUND);
    exec_set_notfound(estate, PLPGSQL_TRUE, stmt->curvar + CURSOR_NOTFOUND);
    exec_set_rowcount(estate, 0, true, stmt->curvar + CURSOR_ROWCOUNT);
    exec_set_sql_isopen(estate, false);
    exec_set_sql_cursor_found(estate, PLPGSQL_FALSE);
    exec_set_sql_notfound(estate, PLPGSQL_TRUE);
    exec_set_sql_rowcount(estate, 0);

    /*
     * Execute the loop.  We can't prefetch because the cursor is accessible
     * to the user, for instance via UPDATE WHERE CURRENT OF within the loop.
     */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq*)stmt, portal, false, stmt->curvar);

    /* restore the status of display cursor after Modify attributes of cursor */
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ISOPEN]))->value = isopen.value;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ISOPEN]))->isnull = isopen.isnull;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_FOUND]))->value = cursor_found.value;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_FOUND]))->isnull = cursor_found.isnull;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_NOTFOUND]))->value = notfound.value;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_NOTFOUND]))->isnull = notfound.isnull;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ROWCOUNT]))->value = rowcount.value;
    ((PLpgSQL_var*)(estate->datums[stmt->curvar + CURSOR_ROWCOUNT]))->isnull = rowcount.isnull;

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

static void set_outparam_info_of_record_type(PLpgSQL_execstate* estate, PLpgSQL_row* row)
{
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(estate->func->fn_oid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmodule(MOD_PLSQL),
                        errmsg("Cache lookup failed for function %u", estate->func->fn_oid),
                        errdetail("Fail to get the function param type oid.")));
    }
    Oid *p_argtypes = NULL;
    char **p_argnames = NULL;
    char *p_argmodes = NULL;
    int p_nargs = get_func_arg_info(tp, &p_argtypes, &p_argnames, &p_argmodes);
    int out_args_num = 0;
    int out_args_index = 0;
    for (int i = 0; i < p_nargs; i++) {
        if (p_argmodes[i] == 'o' || p_argmodes[i] == 'b') {
            out_args_num++;
            out_args_index = i;
        }
        if (out_args_num > 1) {
            break;
        }
    }
    /*
     * When a function has an out parameter, the parameter information needs to be supplemented
     * because the build_row_from_vars function does not handle this situation.
     */
    if (out_args_num == 1 && row->varname != NULL && strcmp(row->varname, p_argnames[out_args_index]) == 0) {
        row->rowtupdesc->tdtypeid = p_argtypes[out_args_index];
        row->rowtupdesc->tdtypmod = p_argmodes[out_args_index];
        estate->paramval = PointerGetDatum(make_tuple_from_row(estate, row, row->rowtupdesc));
        HeapTuple rettup = (HeapTuple)DatumGetPointer(estate->paramval);
        estate->paramval = PointerGetDatum(SPI_returntuple(rettup, row->rowtupdesc));
        estate->paramtupdesc = row->rowtupdesc;
    } else {
        estate->paramval = PointerGetDatum(make_tuple_from_row(estate, row, row->rowtupdesc));
        estate->paramtupdesc = row->rowtupdesc;
    }
    estate->paramisnull = false;
    pfree_ext(p_argtypes);
    pfree_ext(p_argmodes);
    if (p_argnames != NULL) {
        for (int i = 0; i < p_nargs; i++) {
            pfree_ext(p_argnames[i]);
        }
        pfree_ext(p_argnames);
    }
    ReleaseSysCache(tp);
}

/* ----------
 * exec_stmt_return			Evaluate an expression and start
 *					returning from the function.
 * ----------
 */
static int exec_stmt_return(PLpgSQL_execstate* estate, PLpgSQL_stmt_return* stmt)
{
    free_func_tableof_index();
    /*
     * If processing a set-returning PL/pgSQL function, the final RETURN
     * indicates that the function is finished producing tuples.  The rest of
     * the work will be done at the top level.
     */
    if (estate->retisset) {
        return PLPGSQL_RC_RETURN;
    }

    bool need_param_seperation = estate->func->is_plpgsql_func_with_outparam;
    if (need_param_seperation && stmt->expr == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                        errmsg("Value assignment for the out parameter in plpgsql language functions, Unsupported "
                               "return nothing in PL/pgSQL function"),
                        errdetail("N/A"), errcause("Missing return value"), erraction("Please add return value")));
    }

    /* initialize for null result (possibly a tuple) */
    estate->retval = (Datum)0;
    estate->rettupdesc = NULL;
    estate->retisnull = true;

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
            plpgsql_set_outparam_value(estate, stmt->expr);
            if (estate->rettype == REFCURSOROID) {
                CopyCursorInfoData(estate->cursor_return_data, &estate->eval_econtext->cursor_data);
            }
        }

        if (!need_param_seperation) {
            return PLPGSQL_RC_RETURN;
        }
    }

    if (stmt->retvarno >= 0) {
        PLpgSQL_datum* retvar = estate->datums[stmt->retvarno];

        switch (retvar->dtype) {
            case PLPGSQL_DTYPE_VARRAY:
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)retvar;
                Datum value = var->value;
                if (is_huge_clob(var->datatype->typoid, var->isnull, value)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("huge clob do not support as return parameter")));
                }
                if (need_param_seperation) {
                    estate->paramval = value;
                    estate->paramtype = var->datatype->typoid;
                    estate->paramisnull = var->isnull;
                } else {
                    estate->retval = value;
                    estate->retisnull = var->isnull;
                    estate->rettype = var->datatype->typoid;
                    if (estate->is_exception && estate->rettype == REFCURSOROID) {
                        if (DatumGetPointer(estate->retval) != NULL) {
                            char* curname = NULL;
                            curname = TextDatumGetCString(estate->retval);
                            Portal portal = SPI_cursor_find(curname);
                            if (portal == NULL || portal->status == PORTAL_FAILED) {
                                estate->retval = (Datum)0;
                                estate->retisnull = true;
                                estate->rettype = 0;
                            }
                            ereport(DEBUG3, (errmodule(MOD_PLSQL), errcode(ERRCODE_LOG),
                                errmsg("RESET CURSOR NULL LOG: function: %s, set cursor: %s to null due to exception",
                                    estate->func->fn_signature, curname)));
                            pfree_ext(curname);
                        } else {
                            estate->retval = (Datum)0;
                            estate->retisnull = true;
                            estate->rettype = 0;
                        }
                    }
                }
                if (estate->rettype == REFCURSOROID) {
                    ExecCopyDataFromDatum(estate->datums, var->dno, estate->cursor_return_data);
                }
                bool isTableVal = var->datatype != NULL &&
                                  var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE &&
                                  OidIsValid(var->tableOfIndexType) &&
                                  var->tableOfIndex != NULL;
                if (isTableVal) {
                    MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
                    PLpgSQL_func_tableof_index* func_tableof = (PLpgSQL_func_tableof_index*)palloc0(sizeof(PLpgSQL_func_tableof_index));
                    func_tableof->varno = 0;
                    func_tableof->tableOfIndexType = var->tableOfIndexType;
                    func_tableof->tableOfIndex = copyTableOfIndex(var->tableOfIndex);
                    u_sess->plsql_cxt.func_tableof_index = lappend(u_sess->plsql_cxt.func_tableof_index, func_tableof);
                    MemoryContextSwitchTo(oldCxt);
                }

            } break;

            case PLPGSQL_DTYPE_REC: {
                PLpgSQL_rec* rec = (PLpgSQL_rec*)retvar;

                if (HeapTupleIsValid(rec->tup)) {
                    if (need_param_seperation) {
                        estate->paramval = PointerGetDatum(rec->tup);
                        estate->paramtupdesc = rec->tupdesc;
                        estate->paramisnull = false;
                    } else {
                        estate->retval = PointerGetDatum(rec->tup);
                        estate->rettupdesc = rec->tupdesc;
                        estate->retisnull = false;
                    }
                }
            } break;

            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD:
            {
                PLpgSQL_row* row = (PLpgSQL_row*)retvar;

                AssertEreport(row->rowtupdesc != NULL, MOD_PLSQL, "row's tuple description is required.");
                if (need_param_seperation) {
                    set_outparam_info_of_record_type(estate, row);
                } else {
                    estate->retval = PointerGetDatum(make_tuple_from_row(estate, row, row->rowtupdesc));
                    estate->rettupdesc = row->rowtupdesc;
                    estate->retisnull = false;
                }
                /* should not happen */
                if (DatumGetPointer(estate->retval) == NULL && DatumGetPointer(estate->paramval) == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmodule(MOD_PLSQL),
                            errmsg("row not compatible with its own tupdesc in RETURN statement.")));
                }

                /* table of index */
                for (int i = 0; i < row->rowtupdesc->natts; i++) {
                    int dno = row->varnos[i];
                    PLpgSQL_var* tableof_var = NULL;
                    PLpgSQL_datum* var_datum = NULL;
                    if (row->ispkg) {
                        var_datum = row->pkg->datums[dno];
                    } else {
                        var_datum = estate->datums[dno];
                    }
                    if (var_datum->dtype == PLPGSQL_DTYPE_TABLE || var_datum->dtype == PLPGSQL_DTYPE_VAR) {
                        tableof_var = (PLpgSQL_var*)var_datum;
                    } else {
                        continue;
                    }
                    bool isTableVar = tableof_var->datatype != NULL &&
                                      tableof_var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE &&
                                      OidIsValid(tableof_var->tableOfIndexType) &&
                                      tableof_var->tableOfIndex != NULL;
                    if (isTableVar) {
                        MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
                        PLpgSQL_func_tableof_index* func_tableof =
                            (PLpgSQL_func_tableof_index*)palloc0(sizeof(PLpgSQL_func_tableof_index));
                        func_tableof->varno = i;
                        func_tableof->tableOfIndexType = tableof_var->tableOfIndexType;
                        func_tableof->tableOfIndex = copyTableOfIndex(tableof_var->tableOfIndex);
                        u_sess->plsql_cxt.func_tableof_index =
                            lappend(u_sess->plsql_cxt.func_tableof_index, func_tableof);
                        MemoryContextSwitchTo(oldCxt);
                    }
                }

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
            case PLPGSQL_DTYPE_VARRAY:
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
                    rec->tupdesc, tupdesc, gettext_noop("wrong record type supplied in RETURN NEXT"),
                    estate->func->fn_oid);
                tuple = rec->tup;
                /* it might need conversion */
                if (tupmap != NULL) {
                    tuple = do_convert_tuple(tuple, tupmap);
                    free_conversion_map(tupmap);
                    free_tuple = true;
                }
            } break;

            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD: {
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
        portal->tupDesc, estate->rettupdesc, gettext_noop("structure of query does not match function result type"),
        estate->func->fn_oid);

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

static void checkNestTableOfLayer()
{
    if (u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer >= 0 &&
        u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer + 1 <
        u_sess->SPI_cxt.cur_tableof_index->tableOfNestLayer) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_PLSQL),
                errmsg("Don't print entire nest table of value in raise statement")));
    }
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

    if (stmt->hasExceptionInit) {
        err_code = plpgsql_code_cstring2int(stmt->condname);
        condname = pstrdup(stmt->condname);
    } else if (stmt->condname != NULL) {
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
                PLpgSQL_execstate* save_plpgsql_estate = plpgsql_estate;
                u_sess->SPI_cxt.cur_tableof_index->tableOfNestLayer = -1;
                u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer = -1;
                plpgsql_estate = estate;
                paramvalue = exec_eval_expr(estate, (PLpgSQL_expr*)lfirst(current_param), &paramisnull, &paramtypeid);
                checkNestTableOfLayer();
                u_sess->SPI_cxt.cur_tableof_index->tableOfNestLayer = -1;
                u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer = -1;
                plpgsql_estate = save_plpgsql_estate;
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
            err_message = pstrdup(plpgsql_get_sqlstate(err_code));
        }
    }
    /*
     * If has EXCEPTION_INIT, err_message is generated by err_code
     * for example, if sqlcode = -60, then sqlerrm = " 60: non-GaussDB Exception"
     * the length of sqlerrm is: 1 + sizeof(-sqlcode) + sizeof (msg_tail)
     */
    if (stmt->hasExceptionInit) {
        Assert(err_code < 0);
        pfree(err_message);

        const char *msg_tail = ": non-GaussDB Exception";
        int msg_len = SQL_STATE_BUF_LEN + strlen(msg_tail) + 1;
        err_message = (char *)palloc(msg_len);
        errno_t rc = snprintf_s(err_message, msg_len, msg_len - 1, " %d%s", -err_code, msg_tail);
        securec_check_ss(rc, "\0", "\0");
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
    estate->paramval = (Datum)0;
    estate->paramisnull = true;
    estate->paramtype = InvalidOid;

    estate->fn_rettype = func->fn_rettype;
    estate->retistuple = func->fn_retistuple;
    estate->retisset = func->fn_retset;

    estate->readonly_func = func->fn_readonly;

    estate->rettupdesc = NULL;
    estate->paramtupdesc = NULL;
    estate->exitlabel = NULL;
    estate->cur_error = NULL;
    estate->tuple_store = NULL;
    estate->cursor_return_data = NULL;
    estate->cursor_return_numbers = 0;
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
    estate->sqlstate_varno = func->sqlstate_varno;
    estate->sqlerrm_varno = func->sqlerrm_varno;
    estate->sql_bulk_exceptions_varno = func->sql_bulk_exceptions_varno;

    estate->ndatums = func->ndatums;
    estate->datums_alloc = estate->ndatums;
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
    estate->stack_entry_start = u_sess->plsql_cxt.nextStackEntryId + 1;
    estate->curr_nested_table_type = InvalidOid;
    estate->is_exception = false;

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
    exec_simple_check_plan(estate, expr);
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
    ListCell* l = NULL;
    bool isforbid = true;
    bool savedisAllowCommitRollback = false;
    bool needResetErrMsg = false;
    foreach (l, SPI_plan_get_plan_sources(expr->plan)) {
        CachedPlanSource* plansource = (CachedPlanSource*)lfirst(l);
        isforbid = CheckElementParsetreeTag(plansource->raw_parse_tree);
        if (isforbid) {
            needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_COMPL_SQL);
        }
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
        if (!stmt->mod_stmt & !stmt->bulk_collect) {
            stmt->strict = true;
        }

        if (stmt->bulk_collect) {
            tcount = 0;
        } else if (stmt->strict || stmt->mod_stmt) {
            tcount = 2;
        } else {
            tcount = 1;
        }
    } else {
        tcount = 0;
    }

    saved_cursor_data = estate->cursor_return_data;
    int saved_cursor_numbers = estate->cursor_return_numbers;
    if (stmt->row != NULL && stmt->row->nfields > 0) {
        estate->cursor_return_data = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * stmt->row->nfields);
        estate->cursor_return_numbers = stmt->row->nfields;
        has_alloc = true;
    } else {
        estate->cursor_return_numbers = 0;
        estate->cursor_return_data = NULL;
    }

    plpgsql_estate = estate;

    /*
     * Execute the plan
     */
    rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI, estate->readonly_func, tcount);
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState() && checkSPIPlan(expr->plan)) {
        collectDynWithArgs(expr->query, paramLI, expr->plan->cursor_options);
    }
#endif
    // This is used for nested STP. If the transaction Id changed,
    // then need to create new econtext for the TopTransaction.
    stp_check_transaction_and_create_econtext(estate,oldTransactionId);
#ifndef ENABLE_MULTIPLE_NODES
    if (isforbid) {
        stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);
    }
#endif 
    plpgsql_estate = NULL;

    /*
     * Check for error, and set FOUND if appropriate (for historical reasons
     * we set FOUND only for certain query types).	Also Assert that we
     * identified the statement type the same as SPI did.
     */
    switch (rc) {
        case SPI_OK_SELECT:
            AssertEreport(!stmt->mod_stmt, MOD_PLSQL, "It should not be mod stmt.");
#ifndef ENABLE_MULTIPLE_NODES
            if (stmt->sqlstmt->is_funccall && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                break;
            }
#endif
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
            row = stmt->row;
            if (row->ispkg) {
                row = (PLpgSQL_row*)(row->pkg->datums[stmt->row->dno]);
            } else {
                row = (PLpgSQL_row*)(estate->datums[stmt->row->dno]);
            }
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
            if (stmt->bulk_collect) {
                exec_read_bulk_collect(estate, row, tuptab);
            } else {
                /* set the target to NULL(s) */
                exec_move_row(estate, rec, row, NULL, tuptab->tupdesc, true);
            }
        } else {
            if (n > 1 && (stmt->strict || stmt->mod_stmt) && !stmt->bulk_collect) {
                ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_ROWS),
                        errmodule(MOD_PLSQL),
                        errmsg("query returned %u rows more than one row", n)));
            }
            if (stmt->bulk_collect) {
                exec_read_bulk_collect(estate, row, tuptab);
            } else if (expr->out_param_dno > 0 &&
                       is_function_with_plpgsql_language_and_outparam(get_func_oid_from_expr(expr))) {
                estate->eval_tuptable = tuptab;
                plpgsql_set_outparam_value(estate, stmt->sqlstmt);
                estate->eval_tuptable = NULL;
            } else {
                /* Put the first result row into the target */
                exec_move_row(estate, rec, row, tuptab->vals[0], tuptab->tupdesc, true);
#ifndef ENABLE_MULTIPLE_NODES
                if (stmt->sqlstmt->is_funccall && row != NULL) {
                    restoreAutonmSessionCursors(estate, row);
                }
#endif
            }
        }

        /* Clean up */
        exec_eval_cleanup(estate);
        /*
         * SPI_tuptable will be modified by the subsequent non simple expression.
         * Therefore, the saved tuptab table is used to clear data.
         */
        SPI_freetuptable(tuptab);
        SPI_tuptable = NULL;
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
    estate->cursor_return_numbers =  saved_cursor_numbers;
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
#ifndef ENABLE_MULTIPLE_NODES
    // forbid commit/rollback in the stp which is called to get value
    bool savedisAllowCommitRollback = false;
    bool needResetErrMsg = false;
    needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_USED_AS_EXPR);
#else
    /* Saves the status of whether to send commandId. */
    bool saveSetSendCommandId = IsSendCommandId();
#endif
    /*
     * First we evaluate the string expression after the EXECUTE keyword. Its
     * result is the querystring we have to execute.
     */
    query = exec_eval_expr(estate, stmt->query, &isnull, &restype);
#ifndef ENABLE_MULTIPLE_NODES
    stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);
#endif
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
        /* save flag for nest plpgsql compile */
        PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
        int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
        int save_compile_status = u_sess->plsql_cxt.compile_status;

        /* Compile the anonymous code block */
        /* support pass external parameter in anonymous block */
        PG_TRY();
        {
            func = plpgsql_compile_inline(querystr);
        }
        PG_CATCH();
        {
            /* package may use this ptr for check, need reset it */
            ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
                errmsg("%s clear curr_compile_context because of error.", __func__)));
            /* reset nest plpgsql compile */
            u_sess->plsql_cxt.curr_compile_context = save_compile_context;
            u_sess->plsql_cxt.compile_status = save_compile_status;
            clearCompileContextList(save_compile_list_length);
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* Mark packages the function use, so them can't be deleted from under us */
        AddPackageUseCount(func);
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
        PG_TRY();
        {
            (void)plpgsql_exec_function(func, &fake_fcinfo, true);
        }
        PG_CATCH();
        {
            FormatCallStack* plcallstack = t_thrd.log_cxt.call_stack;
#ifndef ENABLE_MULTIPLE_NODES
            estate_cursor_set(plcallstack);
#else
            SetSendCommandId(saveSetSendCommandId);
#endif
            if (plcallstack != NULL) {
                t_thrd.log_cxt.call_stack = plcallstack->prev;
            }
            /* Decrement package use-count */
            DecreasePackageUseCount(func);
            PG_RE_THROW();
        }
        PG_END_TRY();

#ifdef ENABLE_MULTIPLE_NODES
        SetSendCommandId(saveSetSendCommandId);
#endif

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
        /* Decrement package use-count */
        DecreasePackageUseCount(func);
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
    int saved_cursor_numbers = estate->cursor_return_numbers;
    if (stmt->row != NULL && stmt->row->nfields > 0) {
        estate->cursor_return_data = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * stmt->row->nfields);
        estate->cursor_return_numbers = stmt->row->nfields;
    } else {
        estate->cursor_return_numbers = 0;
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
        bool isCollectParam = false;
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState()) {
        isCollectParam = true;
    }
#endif
        exec_res = SPI_execute(querystr, estate->readonly_func, 0, isCollectParam);
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
    estate->cursor_return_numbers = saved_cursor_numbers;

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

#ifndef ENABLE_MULTIPLE_NODES
/* find the cursor var if is autonomoust session procedure out param cursor */
static bool IsAutoOutParam(PLpgSQL_execstate* estate, PLpgSQL_stmt_open* stmt, int dno)
{
    int curVarDno = -1;
    /* only conside autonomous transaction procedure */
    if (u_sess->is_autonomous_session != true || u_sess->SPI_cxt._connected != 0) {
        return false;
    }

    /* means no out param */
    if (estate->func->out_param_varno == -1) {
        return false;
    }

    /* only consider open curosr for SELECT... or open cursor for EXECUTE... */
    if (stmt != NULL) {
        if (stmt->query == NULL && stmt->dynquery == NULL) {
            return false;
        }
        curVarDno = stmt->curvar;
    } else {
        curVarDno = dno;
    }

    PLpgSQL_datum* outDatum = estate->datums[estate->func->out_param_varno];
    if (outDatum->dtype == PLPGSQL_DTYPE_VAR) {
        if (curVarDno == estate->func->out_param_varno) {
            return true;
        }
        return false;
    }

    PLpgSQL_row* outRow = (PLpgSQL_row*)outDatum;
    if (outRow->refname != NULL) {
        /* means out param is just one normal row variable */
        return false;
    }
    for (int i = 0; i < outRow->nfields; i++) {
        if (outRow->varnos[i] == curVarDno) {
            return true;
        }
    }

    return false;
}
#endif

#ifdef ENABLE_MULTIPLE_NODES
static void hold_portal_if_necessary(Portal portal)
{
    if (IS_PGXC_COORDINATOR && ENABLE_SQL_BETA_FEATURE(PLPGSQL_STREAM_FETCHALL) &&
        portal->hasStreamForPlpgsql) {
        _SPI_hold_cursor();
    }
}
#endif

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
        if (curvar->pkg != NULL) {
            MemoryContext temp;
            PLpgSQL_package* pkg = curvar->pkg;
            temp = MemoryContextSwitchTo(pkg->pkg_cxt);
            curname = TextDatumGetCString(curvar->value);
            MemoryContextSwitchTo(temp);
        } else {
            curname = TextDatumGetCString(curvar->value);
        }
        if (SPI_cursor_find(curname) != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_CURSOR),
                    errmodule(MOD_PLSQL),
                    errmsg("cursor \"%s\" already in use in OPEN statement.", curname)));
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    /* In distributed mode, the commandId is sent when a cursor is opened. */
    SetSendCommandId(true);
#endif
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
            if (curvar->pkg != NULL) {
                MemoryContext temp = MemoryContextSwitchTo(curvar->pkg->pkg_cxt);
                assign_text_var(curvar, portal->name);
                temp = MemoryContextSwitchTo(temp);
            } else {
                assign_text_var(curvar, portal->name);
            }
        }
        BindCursorWithPortal(portal, estate, stmt->curvar);
        exec_set_isopen(estate, true, stmt->curvar + CURSOR_ISOPEN);
        exec_set_rowcount(estate, 0, true, stmt->curvar + CURSOR_ROWCOUNT);
        PinPortal(portal);
#ifndef ENABLE_MULTIPLE_NODES
    if (IsAutoOutParam(estate, stmt)) {
        portal->isAutoOutParam = true;
    }
    if (curvar->ispkg) {
        portal->isPkgCur = true;
    }
#endif
        curvar->cursor_closed = false;
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
            if (curvar->ispkg) {
                set_args.row = (PLpgSQL_row*)(curvar->pkg->datums[curvar->cursor_explicit_argrow]);
            } else {
                set_args.row = (PLpgSQL_row*)(estate->datums[curvar->cursor_explicit_argrow]);
            }

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
    /* Check to see if it is being collected by sqladvsior */
    bool isCollectParam = false;
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState()) {
        isCollectParam = true;
    }
#endif
    /*
     * Set up ParamListInfo (hook function and possibly data values)
     */
    paramLI = setup_param_list(estate, query);
    /*
     * Open the cursor
     */
    portal = SPI_cursor_open_with_paramlist(curname, query->plan, paramLI, estate->readonly_func, isCollectParam);

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
        if (curvar->pkg != NULL) {
            MemoryContext temp = MemoryContextSwitchTo(curvar->pkg->pkg_cxt);
            assign_text_var(curvar, portal->name);
            MemoryContextSwitchTo(temp);
        } else {
            assign_text_var(curvar, portal->name);
        }
    }

    if (curname != NULL) {
        pfree_ext(curname);
    }
    if (paramLI != NULL) {
        pfree_ext(paramLI);
    }

    BindCursorWithPortal(portal, estate, stmt->curvar);
    exec_set_isopen(estate, true, stmt->curvar + CURSOR_ISOPEN);
    exec_set_rowcount(estate, 0, true, stmt->curvar + CURSOR_ROWCOUNT);
    PinPortal(portal);
#ifndef ENABLE_MULTIPLE_NODES
    if (IsAutoOutParam(estate, stmt)) {
        portal->isAutoOutParam = true;
    }
    if (curvar->ispkg) {
        portal->isPkgCur = true;
    }
#endif
    curvar->cursor_closed = false;
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

#ifdef ENABLE_MULTIPLE_NODES
        /* For redistribute and broadcast stream, if cursor's sql in loop need to communicate with dn
         * which wait for stream operator, hang will occurs.
         * To avoid this, we need get all tuples for this fetch sql. */
        hold_portal_if_necessary(portal);
#endif

    if (!stmt->is_move) {
        /* ----------
         * Determine if we fetch into a record or a row
         * ----------
         */
        if (stmt->rec != NULL) {
            rec = (PLpgSQL_rec*)(estate->datums[stmt->rec->dno]);
        } else if (stmt->row != NULL) {
            row = stmt->row;
            if (row->ispkg) {
                row = (PLpgSQL_row*)(row->pkg->datums[stmt->row->dno]);
            } else {
                row = (PLpgSQL_row*)(estate->datums[stmt->row->dno]);
            }
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
            if (stmt->bulk_collect) {
                exec_read_bulk_collect(estate, row, tuptab);
            } else {
                exec_move_row(estate, rec, row, tuptab->vals[0], tuptab->tupdesc);
            }
        } else {
            if (stmt->bulk_collect) {
                exec_read_bulk_collect(estate, row, tuptab);
            }
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
    exec_set_cursor_found(estate, (n != 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE, stmt->curvar + CURSOR_FOUND);
    exec_set_notfound(estate, (n == 0) ? PLPGSQL_TRUE : PLPGSQL_FALSE, stmt->curvar + CURSOR_NOTFOUND);
    exec_set_rowcount(estate, n, false, stmt->curvar + CURSOR_ROWCOUNT);

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
    /* sometime close cursor can be called after exception, don't report error */
    if (portal == NULL && !curvar->cursor_closed && estate->cur_error != NULL) {
        pfree_ext(curname);
        exec_set_isopen(estate, false, stmt->curvar + CURSOR_ISOPEN);
        exec_set_cursor_found(estate, PLPGSQL_NULL, stmt->curvar + CURSOR_FOUND);
        exec_set_notfound(estate, PLPGSQL_NULL, stmt->curvar + CURSOR_NOTFOUND);
        exec_set_rowcount(estate, -1, true, stmt->curvar + CURSOR_ROWCOUNT);
        curvar->cursor_closed = true;
        return PLPGSQL_RC_OK;
    } else if (portal == NULL) {
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
#ifndef ENABLE_MULTIPLE_NODES
    if (portal->isAutoOutParam) {
        ResetAutoPortalConext(portal);
    }
#endif
    SPI_cursor_close(portal);
    exec_set_isopen(estate, false, stmt->curvar + CURSOR_ISOPEN);
    exec_set_cursor_found(estate, PLPGSQL_NULL, stmt->curvar + CURSOR_FOUND);
    exec_set_notfound(estate, PLPGSQL_NULL, stmt->curvar + CURSOR_NOTFOUND);
    exec_set_rowcount(estate, -1, true, stmt->curvar + CURSOR_ROWCOUNT);
    curvar->cursor_closed = true;
    return PLPGSQL_RC_OK;
}

static void rebuild_exception_subtransaction_chain(PLpgSQL_execstate* estate, List* transactionList)
{
    // Rebuild ResourceOwner chain, link Portal ResourceOwner to Top ResourceOwner.
    ResourceOwnerNewParent(t_thrd.utils_cxt.STPSavedResourceOwner, t_thrd.utils_cxt.CurrentResourceOwner);
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.STPSavedResourceOwner;
    int subTransactionCount = u_sess->SPI_cxt.portal_stp_exception_counter;
    while(subTransactionCount > 0) {
        Oid savedCurrentUser = InvalidOid;
        int saveSecContext = 0;
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
            PG_TRY();
            {
                GetUserIdAndSecContext(&savedCurrentUser, &saveSecContext);
                transactionNode* node = (transactionNode*)lfirst(list_head(transactionList));
                SetUserIdAndSecContext(node->userId, node->secContext);
                list_delete(transactionList, node);
                pfree(node);
                BeginInternalSubTransaction(NULL);
            }
            PG_CATCH();
            {
                SetUserIdAndSecContext(savedCurrentUser, saveSecContext);
                PG_RE_THROW();
            }
            PG_END_TRY();
            SetUserIdAndSecContext(savedCurrentUser, saveSecContext);
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
    List* transactionHead = NULL;
    transactionHead = GetTransactionList(transactionHead);
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
    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0 && u_sess->plsql_cxt.stp_savepoint_cnt == 0) {
        // Recording Portal's ResourceOwner for rebuilding resource chain 
        // when procedure contain transaction statement.
        // Current ResourceOwner is Portal ResourceOwner.
        t_thrd.utils_cxt.STPSavedResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    }
    // 3. Save current transaction state for the outer transaction started by user's 
    // begin/start statement.
    SPI_save_current_stp_transaction_state();
    /* Saving es_query_cxt, transaction commit, or rollback will no longer delete es_query_cxt information */
    MemoryContext saveCxt = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    saveCxt = u_sess->plsql_cxt.simple_eval_estate->es_query_cxt;
    MemoryContextSetParent(saveCxt, t_thrd.mem_cxt.portal_mem_cxt);
#endif
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
    /* all savepoint in PL has destoryed or released. */
    u_sess->plsql_cxt.stp_savepoint_cnt = 0;
    /* Update transaction start time. */
    SetCurrentStatementStartTimestamp();
    // 5. Start a new transaction.
    SPI_start_transaction(transactionHead);
    // 6. Restore the outer transaction state.
    SPI_restore_current_stp_transaction_state();
    
    // 7. Rebuild estate's context.
    u_sess->plsql_cxt.simple_eval_estate = NULL;
    u_sess->plsql_cxt.shared_simple_eval_resowner = NULL;
    plpgsql_create_econtext(estate, saveCxt);

#ifndef ENABLE_MULTIPLE_NODES
    /* old savedcxt has freed, use new context */
    for (int i = u_sess->SPI_cxt._connected; i > 0; i--) {
        u_sess->SPI_cxt._stack[i].savedcxt = u_sess->SPI_cxt._stack[i - 1].execCxt;
    }
    /* implicit cursor attribute variable should reset when commit/rollback */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        reset_implicit_cursor_attr(estate);
    }
#endif
    // 8. Rebuild subtransaction chain for exception.
    rebuild_exception_subtransaction_chain(estate, transactionHead);
    // 9. move old subtransaction' remain resource into Current Transaction
    stp_cleanup_subxact_resowner(estate->stack_entry_start);
    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0) {
        t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.STPSavedResourceOwner;
    }
    return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmt_null			Locate the execution status.
 * ----------
 */
static int exec_stmt_null(PLpgSQL_execstate* estate, PLpgSQL_stmt_null* stmt)
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
void exec_assign_expr(PLpgSQL_execstate* estate, PLpgSQL_datum* target, PLpgSQL_expr* expr)
{
    Datum value;
    Oid valtype;
    bool isnull = false;
    Cursor_Data* saved_cursor_data = estate->cursor_return_data;
    int saved_cursor_numbers = estate->cursor_return_numbers;
    HTAB* tableOfIndex = NULL;

    value = exec_eval_expr(estate, expr, &isnull, &valtype, &tableOfIndex);

    /* Under A_FORMAT compatibility, we need to seperate OUT param from RETURN */
    plpgsql_set_outparam_value(estate, expr, &value, &isnull);

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
    exec_assign_value(estate, target, value, valtype, &isnull, tableOfIndex);
    if (saved_cursor_data != estate->cursor_return_data) {
        pfree_ext(estate->cursor_return_data);
        estate->cursor_return_data = saved_cursor_data;
        estate->cursor_return_numbers = saved_cursor_numbers;
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
void exec_assign_value(PLpgSQL_execstate* estate, PLpgSQL_datum* target, Datum value, Oid valtype,
    bool* isNull, HTAB* tableOfIndex)
{
    switch (target->dtype) {
        case PLPGSQL_DTYPE_VARRAY:
        case PLPGSQL_DTYPE_TABLE:
        case PLPGSQL_DTYPE_VAR: {
            /*
             * Target is a variable
             */
            PLpgSQL_var* var = (PLpgSQL_var*)target;
            Datum newvalue;

            if (!t_thrd.xact_cxt.isSelectInto && is_external_clob(valtype, *isNull, value)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("clob from execute into do not support assign.")));
            }

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
            bool isByReference = !var->datatype->typbyval && !*isNull;
            if (isByReference) {
                if (var->ispkg) {
                    MemoryContext temp = MemoryContextSwitchTo(var->pkg->pkg_cxt);
                    newvalue = datumCopy(newvalue, false, var->datatype->typlen);
                    MemoryContextSwitchTo(temp);
                } else {
                    newvalue = datumCopy(newvalue, false, var->datatype->typlen);
                }
            }

            /*
             * Now free the old value.	(We can't \do this any earlier
             * because of the possibility that we are assigning the var's
             * old value to it, eg "foo := foo".  We could optimize out
             * the assignment altogether in such cases, but it's too
             * infrequent to be worth testing for.)
             */
            free_var(var);

            var->value = newvalue;
            var->isnull = *isNull;
            if (isByReference) {
                var->freeval = true;
            }
            if (var->nest_table != NULL && !var->isnull) {
                assignNestTableOfValue(estate, var, var->value, tableOfIndex);
            } else if (tableOfIndex != NULL) {
                if (var->ispkg) {
                    MemoryContext temp = MemoryContextSwitchTo(var->pkg->pkg_cxt);
                    HTAB* newTableOfIndex = copyTableOfIndex(tableOfIndex);
                    if (var->tableOfIndex != NULL) {
                        hash_destroy(var->tableOfIndex);
                    }
                    var->tableOfIndex = newTableOfIndex;
                    MemoryContextSwitchTo(temp);
                } else {
                    HTAB* newTableOfIndex = copyTableOfIndex(tableOfIndex);
                    if (var->tableOfIndex != NULL) {
                        hash_destroy(var->tableOfIndex);
                    }
                    var->tableOfIndex = newTableOfIndex;
                }
            } else if (*isNull) {
                hash_destroy(var->tableOfIndex);
                var->tableOfIndex = NULL;
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
                tmptup.t_multi_base = InvalidTransactionId;
                tmptup.t_xid_base = InvalidTransactionId;
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
            AttrNumber attrno = ((PLpgSQL_arrayelem*)target)->assignattrno;

            if (is_huge_clob(valtype, *isNull, value)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("huge clob do not support as array element.")));
            }
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

            /* attrno not equal to -1 means assign value to attribute of array element. */
            if (attrno != -1) {
                /* get array element tupledesc from element typoid. */
                TupleDesc elemtupledesc =
                    lookup_rowtype_tupdesc_noerror(arrayelem->elemtypoid, arrayelem->arraytypmod, true);
                Datum coerced_attr_value = 0;
                /* Coerce source value to match array element attribute type. */
                if (elemtupledesc != NULL) {
                    coerced_attr_value = exec_simple_cast_value(estate, value, valtype,
                        elemtupledesc->attrs[attrno]->atttypid, elemtupledesc->attrs[attrno]->atttypmod, *isNull);
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("array element type is not composite in assignment"),
                            errdetail("array variable \"%s\" must be composite when assign value to attibute", 
                                ((PLpgSQL_var*)target)->refname),
                            errcause("incorrectly referencing variables"),
                            erraction("modify assign variable")));
                }
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
                    ReleaseTupleDesc(elemtupledesc);
                    return;
                }

                /* oldarrayval and newarrayval should be short-lived */
                oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
                if (oldarrayisnull) {
                    oldarrayval = construct_empty_array(arrayelem->elemtypoid);
                } else {
                    oldarrayval = (ArrayType*)DatumGetPointer(oldarraydatum);
                }

                /* Get array element value and deform the tuple. */
                bool elemisNull = false;
                Datum oldarrayelemval = array_ref(oldarrayval,
                    nsubscripts,
                    subscriptvals,
                    arrayelem->arraytyplen,
                    arrayelem->elemtyplen,
                    arrayelem->elemtypbyval,
                    arrayelem->elemtypalign,
                    &elemisNull);
                bool arrayelemisnull[elemtupledesc->natts];
                Datum arrayelemvalues[elemtupledesc->natts];
                if (elemisNull) {
                    /* If array element is null, set arrayelemisnull to true for all elements. */
                    errno_t rc = memset_s(arrayelemisnull, sizeof(arrayelemisnull), true, sizeof(arrayelemisnull));
                    securec_check(rc, "\0", "\0");
                } else {
                    /* Else build a tmp tuple and deform it. */
                    HeapTupleData tmptup;
                    HeapTupleHeader oldelemdata = DatumGetHeapTupleHeader(oldarrayelemval);
                    /* Build a temporary HeapTuple control structure */
                    tmptup.t_len = HeapTupleHeaderGetDatumLength(oldelemdata);
                    ItemPointerSetInvalid(&(tmptup.t_self));
                    tmptup.t_tableOid = InvalidOid;
                    tmptup.t_bucketId = InvalidBktId;
                    HeapTupleSetZeroBase(&tmptup);
#ifdef PGXC
                    tmptup.t_xc_node_id = 0;
#endif
                    tmptup.t_data = oldelemdata;
                    heap_deform_tuple(&tmptup, elemtupledesc, arrayelemvalues, arrayelemisnull);
                }

                /* Assign attribute value by attrno, and form the new tuple. */
                arrayelemvalues[attrno] = coerced_attr_value;
                arrayelemisnull[attrno] = *isNull;
                for (int k = 0; k < elemtupledesc->natts; k++) {
                    if (!arrayelemisnull[k]) {
                        *isNull = false;
                        break;
                    }
                }
                HeapTuple result = heap_form_tuple(elemtupledesc, arrayelemvalues, arrayelemisnull);
                coerced_value = HeapTupleGetDatum(result);
                ReleaseTupleDesc(elemtupledesc);
            } else {
                /* attrno equals to -1, means assign value to array element.
                 * Coerce source value to match array element type.
                 */
                coerced_value =
                    exec_simple_cast_value(estate, value, valtype, arrayelem->elemtypoid,
                        arrayelem->arraytypmod, *isNull);

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

        case PLPGSQL_DTYPE_TABLEELEM: {
            /*
             * Target is an element of a table
             */
            PLpgSQL_tableelem* tableelem = NULL;
            int nsubscripts;
            PLpgSQL_expr* subscripts[MAXDIM];
            int subscriptvals[MAXDIM];
            Datum oldtabledatum, coerced_value;
            bool oldtableisnull = false;
            Oid parenttypoid;
            int32 parenttypmod;
            ArrayType* oldtableval = NULL;
            ArrayType* newtableval = NULL;
            SPITupleTable* save_eval_tuptable = NULL;
            MemoryContext oldcontext = NULL;
            AttrNumber attrno = ((PLpgSQL_tableelem*)target)->assignattrno;

            if (is_huge_clob(valtype, *isNull, value)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("huge clob do not support as table of element.")));
            }

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
             * be prepared to deal with a chain of tableelem datums. Chase
             * back to find the base table datum, and save the subscript
             * expressions as we go.  (We are scanning right to left here,
             * but want to evaluate the subscripts left-to-right to
             * minimize surprises.)  Note that tableelem is left pointing
             * to the leftmost tableelem datum, where we will cache the
             * table element type data.
             */
            nsubscripts = 0;
            do {
                tableelem = (PLpgSQL_tableelem*)target;
                if (nsubscripts >= MAXDIM) {
                    ereport(ERROR,
                        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmodule(MOD_PLSQL),
                            errmsg("number of table dimensions (%d) exceeds the maximum allowed (%d) in assignment.",
                                nsubscripts + 1,
                                MAXDIM)));
                }
                subscripts[nsubscripts++] = tableelem->subscript;
                target = estate->datums[tableelem->tableparentno];
            } while (target->dtype == PLPGSQL_DTYPE_TABLEELEM);

            Assert(target->dtype == PLPGSQL_DTYPE_VAR);
            /* Fetch current value of table datum */
            exec_eval_datum(estate, target, &parenttypoid, &parenttypmod, &oldtabledatum, &oldtableisnull);

            /* Update cached type data if necessary */
            if (tableelem->parenttypoid != parenttypoid || tableelem->parenttypmod != parenttypmod) {
                Oid tabletypoid;
                int32 tabletypmod = parenttypmod;
                int16 tabletyplen;
                Oid elemtypoid;
                int16 elemtyplen;
                bool elemtypbyval = false;
                char elemtypalign;

                /* 
                 * If target is domain over table, reduce to base type
                 * e.g ：  tableA table of typeA, 
                 * parenttypoid： oid of _typeA 
                 * elemtypoid:    oid of typeA
                 * if should check tableA changed or not?
                 */
                tabletypoid = getBaseTypeAndTypmod(parenttypoid, &tabletypmod);

                /* ... and identify the element type */
                elemtypoid = get_element_type(tabletypoid);
                if (!OidIsValid(elemtypoid))
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("subscripted object in assignment is not a table")));

                /* Collect needed data about the types */
                tabletyplen = get_typlen(tabletypoid);

                get_typlenbyvalalign(elemtypoid, &elemtyplen, &elemtypbyval, &elemtypalign);

                /* Now safe to update the cached data */
                tableelem->parenttypoid = parenttypoid;
                tableelem->parenttypmod = parenttypmod;
                tableelem->tabletypoid = tabletypoid;
                tableelem->tabletypmod = tabletypmod;
                tableelem->tabletyplen = tabletyplen;
                tableelem->elemtypoid = elemtypoid;
                tableelem->elemtyplen = elemtyplen;
                tableelem->elemtypbyval = elemtypbyval;
                tableelem->elemtypalign = elemtypalign;
            }
            bool is_nested_table = ((PLpgSQL_var*)target)->nest_table != NULL;
            Oid subscriptType = ((PLpgSQL_var*)target)->datatype->tableOfIndexType;
            HTAB* elemTableOfIndex = ((PLpgSQL_var*)target)->tableOfIndex;
            if (is_nested_table) {
                PLpgSQL_var* innerVar = evalSubsciptsNested(estate, (PLpgSQL_var*)target, subscripts, nsubscripts,
                                                   0, subscriptvals, subscriptType, elemTableOfIndex);
                target = (PLpgSQL_datum*)innerVar;
                /* should assign inner var as a table, copy value's index */
                innerVar->tableOfIndex = copyTableOfIndex(tableOfIndex);
                exec_assign_value(estate, target, PointerGetDatum(value),
                                  valtype, isNull, innerVar->tableOfIndex);
                break;
            } else {
                MemoryContext temp = NULL;
                if (((PLpgSQL_var*)target)->ispkg) {
                    temp = MemoryContextSwitchTo(((PLpgSQL_var*)target)->pkg->pkg_cxt);
                }
                for (int i = 0; i < nsubscripts; i++) {
                   elemTableOfIndex = evalSubscipts(estate, nsubscripts, i, subscriptvals,
                                    subscripts, subscriptType, elemTableOfIndex);
                }
                if (((PLpgSQL_var*)target)->ispkg) {
                    MemoryContextSwitchTo(temp);
                }
            }
            /* Now we can restore caller's SPI_execute result if any. */
            AssertEreport(estate->eval_tuptable == NULL, MOD_PLSQL, "eval tuptable should not be null");
            estate->eval_tuptable = save_eval_tuptable;

            /* attrno not equal to -1 means assign value to attribute of array element. */
            if (attrno != -1) {
                /* get array element tupledesc from element typoid. */
                TupleDesc elemtupledesc =
                lookup_rowtype_tupdesc_noerror(tableelem->elemtypoid, tableelem->tabletypmod, true);
                Datum coerced_attr_value = 0;
                /* Coerce source value to match array element attribute type. */
                if (elemtupledesc != NULL) {
                coerced_attr_value = exec_simple_cast_value(estate, value, valtype,
                    elemtupledesc->attrs[attrno]->atttypid, elemtupledesc->attrs[attrno]->atttypmod, *isNull);
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmodule(MOD_PLSQL),
                            errmsg("table element type is not composite in assignment"),
                            errdetail("table variable \"%s\" must be composite when assign value to attibute", 
                                ((PLpgSQL_var*)target)->refname),
                            errcause("incorrectly referencing variables"),
                            erraction("modify table variable")));
                }
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
                if (tableelem->tabletyplen > 0 && /* fixed-length array? */
                    (oldtableisnull || *isNull)) {
                    ReleaseTupleDesc(elemtupledesc);
                    return;
                }

                /* oldarrayval and newarrayval should be short-lived */
                oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
                if (oldtableisnull) {
                    oldtableval = construct_empty_array(tableelem->elemtypoid);
                } else {
                    oldtableval = (ArrayType*)DatumGetPointer(oldtabledatum);
                }

                /* Get array element value and deform the tuple. */
                bool elemisNull = false;
                Datum oldtableelemval = array_ref(oldtableval,
                nsubscripts,
                subscriptvals,
                tableelem->tabletyplen,
                tableelem->elemtyplen,
                tableelem->elemtypbyval,
                tableelem->elemtypalign,
                &elemisNull);
                bool tableelemisnull[elemtupledesc->natts];
                Datum tableelemvalues[elemtupledesc->natts];
                if (elemisNull) {
                    /* If array element is null, set arrayelemisnull to true for all elements. */
                    errno_t rc = memset_s(tableelemisnull, sizeof(tableelemisnull), true, sizeof(tableelemisnull));
                    securec_check(rc, "\0", "\0");
                } else {
                    /* Else build a tmp tuple and deform it. */
                    HeapTupleData tmptup;
                    HeapTupleHeader oldelemdata = DatumGetHeapTupleHeader(oldtableelemval);
                    /* Build a temporary HeapTuple control structure */
                    tmptup.t_len = HeapTupleHeaderGetDatumLength(oldelemdata);
                    ItemPointerSetInvalid(&(tmptup.t_self));
                    tmptup.t_tableOid = InvalidOid;
                    tmptup.t_bucketId = InvalidBktId;
                    HeapTupleSetZeroBase(&tmptup);
#ifdef PGXC
                    tmptup.t_xc_node_id = 0;
#endif
                    tmptup.t_data = oldelemdata;
                    heap_deform_tuple(&tmptup, elemtupledesc, tableelemvalues, tableelemisnull);
                }

                /* Assign attribute value by attrno, and form the new tuple. */
                tableelemvalues[attrno] = coerced_attr_value;
                tableelemisnull[attrno] = *isNull;
                for (int k = 0; k < elemtupledesc->natts; k++) {
                    if (!tableelemisnull[k]) {
                        *isNull = false;
                        break;
                    }
                }
                HeapTuple result = heap_form_tuple(elemtupledesc, tableelemvalues, tableelemisnull);
                coerced_value = HeapTupleGetDatum(result);
                ReleaseTupleDesc(elemtupledesc);
            } else {
                /* attrno equals to -1, means assign value to array element.
                 * Coerce source value to match array element type.
                 */

                /* Coerce source value to match table element type. */
                coerced_value =exec_simple_cast_value(estate, value, valtype, tableelem->elemtypoid,
                        tableelem->tabletypmod, *isNull);

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
                if (tableelem->tabletyplen > 0 && /* fixed-length array? */
                    (oldtableisnull || *isNull)) {
                    return;
                }

                /* oldtableval and newtableval should be short-lived */
                oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);

                if (oldtableisnull) {
                    oldtableval = construct_empty_array(tableelem->elemtypoid);
                } else {
                    oldtableval = (ArrayType*)DatumGetPointer(oldtabledatum);
                }
            }
            /*
             * Build the modified array value.
             */
            newtableval = array_set(oldtableval,
                nsubscripts,
                subscriptvals,
                coerced_value,
                *isNull,
                tableelem->tabletyplen,
                tableelem->elemtyplen,
                tableelem->elemtypbyval,
                tableelem->elemtypalign);

            MemoryContextSwitchTo(oldcontext);

            if (oldtableisnull) {
                pfree_ext(oldtableval);
            }

            /*
             * Assign the new array to the base variable.  It's never NULL
             * at this point.  Note that if the target is a domain,
             * coercing the base array type back up to the domain will
             * happen within exec_assign_value.
             */
            *isNull = false;
            ((PLpgSQL_var*)target)->tableOfIndexType = subscriptType;
            ((PLpgSQL_var*)target)->tableOfIndex = elemTableOfIndex;
            exec_assign_value(estate, target, PointerGetDatum(newtableval),
                tableelem->tabletypoid, isNull, NULL);
            break;
        }

        case PLPGSQL_DTYPE_ASSIGNLIST: {
            /*
             * Target has a assign list
             */
            PLpgSQL_assignlist* assignvar = (PLpgSQL_assignlist*)target;
            List* assignlist = assignvar->assignlist;
            PLpgSQL_datum* assigntarget = estate->datums[assignvar->targetno];
            exec_assign_list(estate, assigntarget, assignlist, value, valtype, isNull);
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

static void exec_assign_list(PLpgSQL_execstate* estate, PLpgSQL_datum* assigntarget,
    const List* assignlist, Datum value, Oid valtype, bool* isNull)
{
    Datum resultval = value;
    Oid resultvaltype = valtype;
    PLpgSQL_datum* resulttarget = assigntarget;
    PLpgSQL_datum* dummytarget = NULL;
    PLpgSQL_temp_assignvar* resultvar = NULL;
    List* subscripts = NIL;
    ListCell* lc = NULL;
    DList* targetlist = NULL;
    HTAB* elemTableOfIndex = NULL;
    bool hasarray = false;
    int subscriptvals[MAXDIM];
    int nsubscripts;
    foreach(lc, assignlist) {
        Node* n = (Node*)lfirst(lc);
        if (IsA(n, A_Indices)) {
            subscripts = lappend(subscripts, n);
            hasarray = true;
        } else {
            if (hasarray) {
                if (subscripts != NIL) {
                    if (resultvar == NULL) {
                        nsubscripts = list_length(subscripts);
                        PLpgSQL_datum* oldresulttarget = resulttarget;
                        evalSubscriptList(estate, subscripts, subscriptvals, nsubscripts,
                            &resulttarget, &elemTableOfIndex);
                        /* nested table get inner target, only need one subscripts */
                        if (resulttarget != oldresulttarget) {
                            subscriptvals[0] = subscriptvals[nsubscripts - 1];
                            nsubscripts = 1;
                        }
                        resultvar = build_temp_assignvar_from_datum(resulttarget, subscriptvals, nsubscripts);
                        targetlist = dlappend(targetlist, resultvar);
                    } else {
                        nsubscripts = list_length(subscripts);
                        evalSubscriptList(estate, subscripts, subscriptvals, nsubscripts,
                            &dummytarget, &elemTableOfIndex);
                    }
                    resultvar = extractArrayElem(estate, resultvar, subscriptvals, nsubscripts);
                    targetlist = dlappend(targetlist, resultvar);
                    list_free_ext(subscripts);
                }
                resultvar = extractAttrValue(estate, resultvar, strVal(n));
                targetlist = dlappend(targetlist, resultvar);
            } else {
                resulttarget = get_indirect_target(estate, resulttarget, strVal(n));
            }
        }
    }
    if (subscripts != NIL) {
        if (resultvar == NULL) {
            nsubscripts = list_length(subscripts);
            PLpgSQL_datum* oldresulttarget = resulttarget;
            evalSubscriptList(estate, subscripts, subscriptvals, nsubscripts, &resulttarget, &elemTableOfIndex);
            /* nested table already get inner target, only need one subscripts */
            if (resulttarget != oldresulttarget) {
                subscriptvals[0] = subscriptvals[nsubscripts - 1];
                nsubscripts = 1;
            }
            resultvar = build_temp_assignvar_from_datum(resulttarget, subscriptvals, nsubscripts);
            targetlist = dlappend(targetlist, resultvar);
        } else {
            nsubscripts = list_length(subscripts);
            evalSubscriptList(estate, subscripts, subscriptvals, nsubscripts, &dummytarget, &elemTableOfIndex);
        }
        resultvar = extractArrayElem(estate, resultvar, subscriptvals, nsubscripts);
        targetlist = dlappend(targetlist, resultvar);
        list_free_ext(subscripts);
    }

    if (targetlist != NULL) {
        resultval = formDatumFromTargetList(estate, targetlist, value, valtype, &resultvaltype, isNull);
    }
    ((PLpgSQL_var*)resulttarget)->tableOfIndex = elemTableOfIndex;
    exec_assign_value(estate, resulttarget, resultval, resultvaltype, isNull, NULL);
    /* deep free target list */
    for (DListCell* dlc = dlist_tail_cell(targetlist); dlc != NULL && lprev(dlc) != NULL; dlc = lprev(dlc)) {
        PLpgSQL_temp_assignvar* curresultvar = (PLpgSQL_temp_assignvar*)lfirst(dlc);
        pfree_ext(curresultvar->subscriptvals);
    }
    dlist_free(targetlist, true);
}

static Datum formDatumFromTargetList(PLpgSQL_execstate* estate, DList* targetlist,
    Datum value, Oid valtype, Oid* resultvaltype, bool* isNull)
{
    DListCell* dlc = NULL;
    Datum resultvalue = value;
    Oid resulttyp = valtype;
    for (dlc = dlist_tail_cell(targetlist); lprev(dlc) != NULL; dlc = lprev(dlc)) {
        PLpgSQL_temp_assignvar* n = (PLpgSQL_temp_assignvar*)lfirst(dlc);
        if (n->isarrayelem) {
            int* subscriptvals = n->subscriptvals;
            int nsubscripts = n->nsubscripts;
            resultvalue = formDatumFromArrayTarget(estate, (PLpgSQL_temp_assignvar*)lfirst(lprev(dlc)),
                subscriptvals, nsubscripts, resultvalue, &resulttyp, isNull);
        } else {
            int attnum =  n->attnum;
            resultvalue = formDatumFromAttrTarget(estate, (PLpgSQL_temp_assignvar*)lfirst(lprev(dlc)),
                attnum, resultvalue, &resulttyp, isNull);
        }
    }
    *resultvaltype = resulttyp;
    return resultvalue;
}

static Datum formDatumFromAttrTarget(PLpgSQL_execstate* estate, const PLpgSQL_temp_assignvar* target, int attnum,
    Datum value, Oid* valtype, bool* isNull)
{
    Oid parenttypoid = target->typoid;
    int32 parenttypmod = target->typmod;
    TupleDesc tupDesc = lookup_rowtype_tupdesc_noerror(parenttypoid, parenttypmod, true);

    if (tupDesc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("array element type is not composite in assignment"),
                errdetail("array variable  must be composite when assign value to attibute"),
                errcause("incorrectly referencing variables"),
                erraction("modify assign variable")));
    }
    Oid targettypoid = tupDesc->attrs[attnum]->atttypid;
    int32 targettypmod = tupDesc->attrs[attnum]->atttypmod;
    bool attrisnull[tupDesc->natts];
    Datum attrvalues[tupDesc->natts];
    Datum coerced_value = exec_simple_cast_value(estate, value, *valtype, targettypoid, targettypmod, *isNull);

    if (target->isnull) {
        errno_t rc = memset_s(attrisnull, sizeof(attrisnull), true, sizeof(attrisnull));
        securec_check(rc, "\0", "\0");
    } else {
        HeapTupleData tmptup;
        HeapTupleHeader olddata = DatumGetHeapTupleHeader(target->value);
        tmptup.t_len = HeapTupleHeaderGetDatumLength(olddata);
        ItemPointerSetInvalid(&(tmptup.t_self));
        tmptup.t_tableOid = InvalidOid;
        tmptup.t_bucketId = InvalidBktId;
#ifdef PGXC
        tmptup.t_xc_node_id = 0;
#endif
        tmptup.t_data = olddata;
        heap_deform_tuple(&tmptup, tupDesc, attrvalues, attrisnull);
    }

    attrvalues[attnum] = coerced_value;
    attrisnull[attnum] = *isNull;
    HeapTuple result = heap_form_tuple(tupDesc, attrvalues, attrisnull);
    Datum newresult = HeapTupleGetDatum(result);

    ReleaseTupleDesc(tupDesc);
    *valtype = parenttypoid;
    *isNull = false;
    return newresult;
}

static Datum formDatumFromArrayTarget(PLpgSQL_execstate* estate, const PLpgSQL_temp_assignvar* target,
    int* subscriptvals, int nsubscripts, Datum value, Oid* resultvaltype, bool* isNull)
{
    Oid parenttypoid = target->typoid;
    int32 parenttypmod = target->typmod;
    ArrayType* oldarrayval = NULL;
    ArrayType* newarrayval = NULL;
    Datum oldarraydatum = target->value;
    bool oldarrayisnull = target->isnull;
    int32 arraytypmod = parenttypmod;

    Oid arraytypoid = getBaseTypeAndTypmod(parenttypoid, &arraytypmod);
    Oid elemtypoid = get_element_type(arraytypoid);
    if (!OidIsValid(elemtypoid))
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("subscripted object in assignment is not an array")));
    int16 arraytyplen = get_typlen(arraytypoid);
    int16 elemtyplen;
    bool elemtypbyval = false;
    char elemtypalign;
    get_typlenbyvalalign(elemtypoid, &elemtyplen, &elemtypbyval, &elemtypalign);

    Datum coerced_value =
        exec_simple_cast_value(estate, value, *resultvaltype, elemtypoid,
            arraytypmod, *isNull);

    arraytyplen = -1; /* need to adjust */
    if (arraytyplen > 0 && /* fixed-length array? */
        (oldarrayisnull || *isNull)) {
        *resultvaltype = parenttypoid;
        return (Datum)0;
    }
    MemoryContext oldcontext = MemoryContextSwitchTo(estate->eval_econtext->ecxt_per_tuple_memory);
    if (oldarrayisnull) {
        oldarrayval = construct_empty_array(elemtypoid);
    } else {
        oldarrayval = (ArrayType*)DatumGetPointer(oldarraydatum);
    }
    newarrayval = array_set(oldarrayval,
        nsubscripts,
        subscriptvals,
        coerced_value,
        *isNull,
        arraytyplen,
        elemtyplen,
        elemtypbyval,
        elemtypalign);
    MemoryContextSwitchTo(oldcontext);

    if (oldarrayisnull) {
        pfree_ext(oldarrayval);
    }
    *isNull = false;
    *resultvaltype = arraytypoid;
    return PointerGetDatum(newarrayval);
}

static PLpgSQL_temp_assignvar* extractAttrValue(PLpgSQL_execstate* estate,
    PLpgSQL_temp_assignvar* target, char* attrname)
{
    PLpgSQL_temp_assignvar* result = NULL;
    bool isNull = false;
    Oid parenttypoid = target->typoid;
    int32 parenttypmod = target->typmod;
    int i;

    TupleDesc tupDesc = lookup_rowtype_tupdesc_noerror(parenttypoid, parenttypmod, true);
    if (tupDesc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("array element type is not composite in assignment"),
                errdetail("array variable \"%s\" must be composite when assign value to attibute", attrname),
                errcause("incorrectly referencing variables"),
                erraction("modify assign variable")));
    }
    AttrNumber attrno = InvalidAttrNumber;
    for (i = 0; i < tupDesc->natts; i++) {
        if (namestrcmp(&(tupDesc->attrs[i]->attname), attrname) == 0) {
            attrno = tupDesc->attrs[i]->attnum;
            break;
        }
    }
    if (attrno == InvalidAttrNumber)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ATTRIBUTE),
                errmodule(MOD_EXECUTOR),
                errmsg("attribute \"%s\" does not exist", attrname)));

    Datum resultvalue = (Datum)0;
    if (!target->isnull) {
        HeapTupleHeader olddata = DatumGetHeapTupleHeader(target->value);
        resultvalue = GetAttributeByNum(olddata, attrno, &isNull);
    }
    result = (PLpgSQL_temp_assignvar*)palloc0(sizeof(PLpgSQL_temp_assignvar));
    result->isarrayelem = false;
    result->isnull = target->isnull || isNull;
    result->typoid = tupDesc->attrs[i]->atttypid;
    result->typmod = tupDesc->attrs[i]->atttypmod;
    result->attnum = i;
    result->value = resultvalue;
    result->attrname = pstrdup(attrname);

    ReleaseTupleDesc(tupDesc);
    return result;
}

static PLpgSQL_temp_assignvar* extractArrayElem(PLpgSQL_execstate* estate, PLpgSQL_temp_assignvar* target,
    int* subscriptvals, int nsubscripts)
{
    PLpgSQL_temp_assignvar* result = NULL;
    Oid parenttypoid = target->typoid;
    int32 arraytypmod = target->typmod;
    Oid arraytypoid = getBaseTypeAndTypmod(parenttypoid, &arraytypmod);
    Oid elemtypoid = get_element_type(arraytypoid);
    ArrayType* arrayval = NULL;
    if (!OidIsValid(elemtypoid))
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("subscripted object in assignment is not an array")));
    int16 arraytyplen =  get_typlen(arraytypoid);
    int16 elemtyplen;
    bool elemtypbyval = false;
    char elemtypalign;
    get_typlenbyvalalign(elemtypoid, &elemtyplen, &elemtypbyval, &elemtypalign);

    if (target->isnull) {
        arrayval = construct_empty_array(elemtypoid);
    } else {
        arrayval = (ArrayType*)DatumGetPointer(target->value);
    }
    bool elemisNull = false;
    Datum arrayelemval = array_ref(arrayval,
        nsubscripts,
        subscriptvals,
        arraytyplen,
        elemtyplen,
        elemtypbyval,
        elemtypalign,
        &elemisNull);
    result = (PLpgSQL_temp_assignvar*)palloc0(sizeof(PLpgSQL_temp_assignvar));
    result->isarrayelem = true;
    result->isnull = elemisNull;
    result->typoid = elemtypoid;
    result->typmod = arraytypmod;
    result->value = arrayelemval;
    result->subscriptvals = (int *)palloc(sizeof(int) * nsubscripts);
    for (int i = 0; i < nsubscripts; i++) {
        result->subscriptvals[i] = subscriptvals[i];
    }
    result->nsubscripts = nsubscripts;
    if (target->isnull) {
        pfree_ext(arrayval);
    }
    return result;
}

static PLpgSQL_temp_assignvar* build_temp_assignvar_from_datum(PLpgSQL_datum* target,
    int* subscriptvals, int nsubscripts)
{
    PLpgSQL_var* var = (PLpgSQL_var*)target;
    PLpgSQL_temp_assignvar* result = NULL;
    if (var->datatype->typinput.fn_oid == F_ARRAY_IN) {
        result = (PLpgSQL_temp_assignvar*)palloc0(sizeof(PLpgSQL_temp_assignvar));
        result->isarrayelem = true;
        result->isnull = var->isnull;
        result->typoid = var->datatype->typoid;
        result->typmod = var->datatype->atttypmod;
        result->value = var->value;
        result->subscriptvals = (int *)palloc(sizeof(int) * nsubscripts);
        for (int i = 0; i < nsubscripts; i++) {
            result->subscriptvals[i] = subscriptvals[i];
        }
        result->nsubscripts = nsubscripts;
        return result;
    }
    if (var->datatype->typinput.fn_oid == F_DOMAIN_IN) {
        HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
        if (HeapTupleIsValid(type_tuple)) {
            Form_pg_type type_form = (Form_pg_type)GETSTRUCT(type_tuple);
            if (F_ARRAY_OUT == type_form->typoutput) {
                result = (PLpgSQL_temp_assignvar*)palloc0(sizeof(PLpgSQL_temp_assignvar));
                result->isarrayelem = true;
                result->isnull = var->isnull;
                result->typoid = var->datatype->typoid;
                result->typmod = var->datatype->atttypmod;
                result->value = var->value;
                result->subscriptvals = (int *)palloc(sizeof(int) * nsubscripts);
                for (int i = 0; i < nsubscripts; i++) {
                    result->subscriptvals[i] = subscriptvals[i];
                }
                result->nsubscripts = nsubscripts;
                ReleaseSysCache(type_tuple);
                return result;
            }
        }
        ReleaseSysCache(type_tuple);
    }
    ereport(ERROR,
        (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmodule(MOD_PLSQL),
            errmsg("subscripted object in assignment is not an array")));
    return result;
}

static void evalSubscriptList(PLpgSQL_execstate* estate, const List* subscripts,
    int* subscriptvals, int nsubscripts, PLpgSQL_datum** target, HTAB** elemTableOfIndex)
{
    if (nsubscripts > MAXDIM) {
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmodule(MOD_PLSQL),
                errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d) in assignment.",
                    nsubscripts, MAXDIM)));
    }
    int i = 0;
    ListCell* lc = NULL;
    SPITupleTable* save_eval_tuptable = NULL;
    save_eval_tuptable = estate->eval_tuptable;
    estate->eval_tuptable = NULL;

    if (*target != NULL && (*target)->dtype == PLPGSQL_DTYPE_VAR &&
        ((PLpgSQL_var*)(*target))->datatype->collectionType == PLPGSQL_COLLECTION_TABLE) {
        int tableof_level = 0;
        char* valname = ((PLpgSQL_var*)(*target))->refname;
        PLpgSQL_expr* subexprs[nsubscripts];
        foreach (lc, subscripts) {
            HTAB* tableOfIndex = ((PLpgSQL_var*)(*target))->tableOfIndex;
            Oid subscriptType = ((PLpgSQL_var*)(*target))->datatype->tableOfIndexType;
            /* for table type, if has more than one input arg list,
             * it should get arg list from left to right as index from outter to inner */
            bool is_nested_table = (((PLpgSQL_var*)(*target))->nest_table != NULL);
            Node* n = (Node*)lfirst(lc);
            if (!IsA(n, A_Indices)) {
                ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmodule(MOD_PLSQL),
                        errmsg("subscripts list has other members")));
            }
            subexprs[nsubscripts - i - 1] =  (PLpgSQL_expr*)(((A_Indices*)n)->uidx);
            PLpgSQL_var* nestVar = NULL;
            if (is_nested_table) {
                nestVar = evalSubsciptsNested(estate, (PLpgSQL_var*)(*target), subexprs, nsubscripts, i,
                                                   subscriptvals, subscriptType, tableOfIndex);
                *target = (PLpgSQL_datum*)nestVar;
                tableof_level++;
            } else {
                /* nest tableof value's nest level should match subexprs's number. */
                if (i > tableof_level) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmodule(MOD_PLSQL),
                            errmsg("subscripts list has members more than tableof value %s expected", valname)));
                }
                MemoryContext temp = NULL;
                if (((PLpgSQL_var*)(*target))->ispkg) {
                    temp = MemoryContextSwitchTo(((PLpgSQL_var*)(*target))->pkg->pkg_cxt);
                }
                tableOfIndex = evalSubscipts(estate, nsubscripts, i, subscriptvals,
                                             subexprs, subscriptType, tableOfIndex);
                if (((PLpgSQL_var*)(*target))->ispkg) {
                    MemoryContextSwitchTo(temp);
                }
                if (*elemTableOfIndex == NULL) {
                    *elemTableOfIndex = tableOfIndex;
                }
            }
            i++;
        }
        if (i - tableof_level != 1) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_PLSQL),
                    errmsg("subscripts list has members less than tableof value %s expected", valname)));
        }
        Assert(estate->eval_tuptable == NULL);
        estate->eval_tuptable = save_eval_tuptable;
        return;
    }

    PLpgSQL_expr* subexpr = NULL;
    bool subisnull = false;

    foreach(lc, subscripts) {
        Node* n = (Node*)lfirst(lc);
        if (!IsA(n, A_Indices)) {
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmodule(MOD_PLSQL),
                    errmsg("subscripts list has other members")));
        }

        subexpr =  (PLpgSQL_expr*)(((A_Indices*)n)->uidx);
        *(subscriptvals + i) = exec_eval_integer(estate, subexpr, &subisnull);
        if (subisnull) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("array subscript in assignment must not be null")));
        }
        i++;
        if (estate->eval_tuptable != NULL) {
            SPI_freetuptable(estate->eval_tuptable);
        }
        estate->eval_tuptable = NULL;
    }
    Assert(estate->eval_tuptable == NULL);
    estate->eval_tuptable = save_eval_tuptable;
}

static PLpgSQL_datum* get_indirect_target(PLpgSQL_execstate* estate, PLpgSQL_datum* assigntarget, const char* filed)
{
    if (assigntarget->dtype != PLPGSQL_DTYPE_ROW && assigntarget->dtype != PLPGSQL_DTYPE_RECORD) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("no filed in schalar target.")));
    }

    PLpgSQL_row* row = (PLpgSQL_row*)assigntarget;
    for (int i = 0; i < row->nfields; i++) {
        if (row->fieldnames[i] && strcmp(row->fieldnames[i], filed) == 0) {
            if (row->ispkg) {
                return row->pkg->datums[row->varnos[i]];
            } else {
                return estate->datums[row->varnos[i]];
            }
        }
    }

    ereport(ERROR,
        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmodule(MOD_PLSQL),
             errmsg("no filed in row target.")));
    return NULL;
}

/* return nested table var if get inner table, return NULL if get nested table's elem */
static PLpgSQL_var* evalSubsciptsNested(PLpgSQL_execstate* estate, PLpgSQL_var* tablevar, PLpgSQL_expr** subscripts,
                      int nsubscripts, int pos, int* subscriptvals, Oid subscriptType, HTAB* tableOfIndex)
{
    if (tablevar == NULL || tablevar->nest_table == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmodule(MOD_PLSQL),
                 errmsg("table subscript in assignment not match nested table.")));
    }
    bool subisnull = false;
    Datum exprdatum;
    /* get this level's index and then call evalSubsciptsNested to get inner table type's subscripts */
    if (subscriptType == VARCHAROID) {
        exprdatum = exec_eval_varchar(estate, subscripts[nsubscripts - 1 - pos], &subisnull);
    } else {
        /* subcript type is integer */
        exprdatum = (Datum)exec_eval_integer(estate, subscripts[nsubscripts - 1 - pos], &subisnull);
    }
    if (subisnull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmodule(MOD_PLSQL),
                errmsg("table subscript in assignment must not be null")));
    }
    TableOfIndexKey key;
    key.exprtypeid = subscriptType;
    key.exprdatum = exprdatum;
    PLpgSQL_var* old_var = NULL;
    int index = getTableOfIndexByDatumValue(key, tableOfIndex, &old_var);
    if (index < 0) {
        // if not exits new nested table var
        index = addNewNestedTable(estate, key, tablevar);
    } else {
        Assert(old_var != NULL);
        index = old_var->dno;
    }
    subscriptvals[pos] = index;

    if (estate->eval_tuptable != NULL) {
        SPI_freetuptable(estate->eval_tuptable);
    }
    estate->eval_tuptable = NULL;

    PLpgSQL_var* nest_var = NULL;
    if (tablevar->ispkg) {
        nest_var = (PLpgSQL_var*)tablevar->pkg->datums[index];
    } else {
        nest_var = (PLpgSQL_var*)estate->datums[index];
    }
    return nest_var;
}

/*
 * Evaluate the subscripts, switch into left-to-right order.
 * Like ExecEvalArrayRef(), complain if any subscript is null.
 */
static HTAB* evalSubscipts(PLpgSQL_execstate* estate, int nsubscripts, int pos, int* subscriptvals,
                           PLpgSQL_expr** subscripts, Oid subscriptType, HTAB* tableOfIndex)
{
    bool subisnull = false;
    Oid exprtypeid;
    /* subcript type is index by varchar/integer */
    if (OidIsValid(subscriptType)) {
        bool isTran = false;
        MemoryContext savedContext = CurrentMemoryContext;
        Datum exprdatum = exec_eval_expr(estate, subscripts[nsubscripts - 1 - pos], &subisnull, &exprtypeid);
        MemoryContextSwitchTo(savedContext);
        exprdatum = exec_simple_cast_value(estate, exprdatum, exprtypeid, subscriptType, -1, subisnull);
        if (subscriptType == VARCHAROID && !subisnull && VARATT_IS_1B(exprdatum)) {
            exprdatum = transVaratt1BTo4B(exprdatum);
            isTran = true;
        }
        if (subisnull) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("table subscript in assignment must not be null")));
        }

        TableOfIndexKey key;
        key.exprtypeid = subscriptType;
        key.exprdatum = exprdatum;
        /* type is varchar/integer */
        int index = getTableOfIndexByDatumValue(key, tableOfIndex);
        if (index >= 0) {
            subscriptvals[pos] = index;
        } else {
            subscriptvals[pos] = insertTableOfIndexByDatumValue(key, &tableOfIndex);
        }
        if (isTran) {
            pfree(DatumGetPointer(exprdatum));
        }
    } else {
        /* subcript type is integer */
        subscriptvals[pos] = exec_eval_integer(estate, subscripts[nsubscripts - 1 - pos], &subisnull);
        if (subisnull) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLSQL),
                    errmsg("array subscript in assignment must not be null")));
        }
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

    return tableOfIndex;
}

Datum transVaratt1BTo4B(Datum value)
{
    Size data_size = VARSIZE_SHORT(value) - VARHDRSZ_SHORT;
    Size new_size = data_size + VARHDRSZ;
    struct varlena *new_value;
    errno_t rc = EOK;

    new_value = (struct varlena *)palloc(new_size);
    SET_VARSIZE(new_value, new_size);
    rc = memcpy_s(VARDATA(new_value), new_size, VARDATA_SHORT(value), data_size);
    securec_check(rc, "", "");
    return PointerGetDatum(new_value);
}

static uint32 tableOfIndexHashFunc(const void* key, Size keysize)
{
    const TableOfIndexKey *item = (const TableOfIndexKey *) key;
    int16 typLen;
    bool typByVal = false;
    get_typlenbyval(item->exprtypeid, &typLen, &typByVal);
    Size size = datumGetSize(item->exprdatum, typByVal, typLen);
    if (item->exprtypeid == VARCHAROID)
        return DatumGetUInt32(hash_any((const unsigned char*)item->exprdatum, size));
    else
        return DatumGetUInt32(hash_uint32((uint32)item->exprdatum));
}

static int tableOfIndexKeyMatch(const void *left, const void *right, uint32 keysize)
{
    TableOfIndexKey* leftItem = (TableOfIndexKey*)left;
    TableOfIndexKey* rightItem = (TableOfIndexKey*)right;
    Assert(NULL != leftItem);
    Assert(NULL != rightItem);

    if (leftItem->exprtypeid != rightItem->exprtypeid) {
        return 1;
    }

    int16 typLen;
    bool typByVal = false;
    get_typlenbyval(leftItem->exprtypeid, &typLen, &typByVal);
    if (!datumIsEqual(leftItem->exprdatum, rightItem->exprdatum, typByVal, typLen)) {
        return 1;
    }
    return 0;
}

static HTAB* createTableOfIndex()
{
    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(TableOfIndexKey);
    ctl.entrysize = sizeof(TableOfIndexEntry);
    ctl.hash = (HashValueFunc)tableOfIndexHashFunc;
    ctl.match = (HashCompareFunc)tableOfIndexKeyMatch;
    MemoryContext indexContext = NULL;
    if (CurrentMemoryContext->is_shared) {
        indexContext = AllocSetContextCreate(CurrentMemoryContext,
            "tableOfIndexContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    } else {
        indexContext = AllocSetContextCreate(CurrentMemoryContext,
            "tableOfIndexContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }
    ctl.hcxt = indexContext;
    int flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_EXTERN_CONTEXT;
    HTAB* hashTable = hash_create("tableof_index", TABLEOFINDEXBUCKETNUM, &ctl, flags);
    return hashTable;
}

HTAB* copyTableOfIndex(HTAB* oldValue)
{
    if (oldValue == NULL) {
        return NULL;
    }
    HTAB* newValue = createTableOfIndex();
    HASH_SEQ_STATUS hashSeq;
    hash_seq_init(&hashSeq, oldValue);
    TableOfIndexEntry* srcEntry = NULL;
    while ((srcEntry = (TableOfIndexEntry*)hash_seq_search(&hashSeq)) != NULL) {
        bool found = false;
        TableOfIndexEntry* desEntry = (TableOfIndexEntry*)hash_search(
            newValue, (const void*)&srcEntry->key, HASH_ENTER, &found);
        Assert(!found);
        if (desEntry == NULL) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("palloc hash element TableOfIndexEntry failed"),
                            errdetail("N/A"), errcause("out of memory"),
                            erraction("set more memory")));
        }
        int16 typLen;
        bool typByVal = false;
        get_typlenbyval(srcEntry->key.exprtypeid, &typLen, &typByVal);
        desEntry->key.exprdatum = datumCopy(srcEntry->key.exprdatum, typByVal, typLen);
        desEntry->key.exprtypeid = srcEntry->key.exprtypeid;
        desEntry->index = srcEntry->index;
        desEntry->var = srcEntry->var;
    }

    return newValue;
}

int getTableOfIndexByDatumValue(TableOfIndexKey key, HTAB* tableOfIndex, PLpgSQL_var** node)
{
    if (tableOfIndex == NULL) {
        return -1;
    }
    
    bool found = false;
    TableOfIndexEntry* entry = (TableOfIndexEntry*)hash_search(
        tableOfIndex, (const void*)&key, HASH_FIND, &found);
    if (found) {
        if (node != NULL)
            *node = entry->var;
        return entry->index;
    } else {
        return -1;
    }
}

static PLpgSQL_var* makeNewNestedPlpgsqlVar(PLpgSQL_var* src)
{
    if (src == NULL) {
        return NULL;
    }
    PLpgSQL_var* dest = (PLpgSQL_var*)palloc(sizeof(PLpgSQL_var));
    dest->dtype = src->dtype;
    dest->dno = 0;
    dest->pkg = src->pkg;
    dest->pkg_name = list_copy(src->pkg_name);
    dest->varname = pstrdup(src->varname);
    dest->ispkg = src->ispkg;
    dest->refname = pstrdup(src->refname);
    dest->lineno = src->lineno;
    dest->datatype = src->datatype;
    dest->isconst = src->isconst;
    dest->notnull = src->notnull;
    dest->default_val = NULL;
    dest->cursor_explicit_expr = NULL;
    dest->value = InvalidOid;
    dest->isnull = src->isnull;
    dest->freeval = src->freeval;
    dest->is_cursor_var = src->is_cursor_var;
    dest->is_cursor_open = src->is_cursor_open;
    dest->tableOfIndexType = src->tableOfIndexType; 
    dest->tableOfIndex = NULL;
    dest->nest_table = makeNewNestedPlpgsqlVar(src->nest_table);
    dest->nest_layers = src->nest_layers;
    return dest;
}

/* 
 * add new PLpgSQL_var into estate and save correspond index into bast table var's array.
 * array's elem is int4, correspond index save in tableofindex.
 * and then save index into base table var's indexoftable.
 */
static int addNewNestedTable(PLpgSQL_execstate* estate, TableOfIndexKey key, PLpgSQL_var* base_table)
{
    MemoryContext old = NULL;
    if (base_table->ispkg) {
        old = MemoryContextSwitchTo(base_table->pkg->pkg_cxt);
    }
    PLpgSQL_var* origin_table = base_table->nest_table;
    PLpgSQL_var* new_nest_table = makeNewNestedPlpgsqlVar(origin_table);
    Oid elemtypoid = INT4OID;
    ArrayType* arrayval = NULL;
    if (base_table->value == 0) {
        arrayval = construct_empty_array(elemtypoid);
        base_table->freeval = true;
        base_table->isnull = false;
    } else {
        arrayval = (ArrayType*)base_table->value;
    }
    int dno = plpgsql_estate_adddatum(estate, (PLpgSQL_datum*)new_nest_table);
    int idx = insertTableOfIndexByDatumValue(key, &base_table->tableOfIndex, new_nest_table);
    /* save nest table's dno into array */
    base_table->value =
        fillNestedTableArray(arrayval, base_table->datatype->typoid, INT4OID, dno, idx);
    base_table->isnull = false;
    if (base_table->ispkg) {
        (void)MemoryContextSwitchTo(old);
    }
    return dno;
}

Datum fillNestedTableArray(ArrayType* arrayval, Oid parenttypoid, Oid elemtypoid, int value, int idx)
{
    int32 tabletypmod = 0;
    Oid tabletypoid = getBaseTypeAndTypmod(parenttypoid, &tabletypmod);
    if (get_element_type(tabletypoid) != elemtypoid) {
        elog(ERROR, "wrong input elemtypoid");
    }
    int16 tabletyplen = get_typlen(tabletypoid);
    int16 elemtyplen;
    bool elemtypbyval = false;
    char elemtypalign;
    get_typlenbyvalalign(elemtypoid, &elemtyplen, &elemtypbyval, &elemtypalign);
    ArrayType* resultArray = array_set(arrayval,
                                       1,
                                       &idx,
                                       value,
                                       false,
                                       tabletyplen,
                                       elemtyplen,
                                       elemtypbyval,
                                       elemtypalign);
    return (Datum)resultArray;
}

/* recursivly set nest table's inner value */
static void assignNestTableOfValue(PLpgSQL_execstate* estate, PLpgSQL_var* var, Datum oldvalue, HTAB* tableOfIndex)
{
    /* for last layer of tableof, assign array value and copy index */
    if (var->nest_table == NULL) {
        exec_assign_value(estate, (PLpgSQL_datum*)var, oldvalue,
                          var->datatype->typoid, &var->isnull, tableOfIndex);
        return;
    }
    if (tableOfIndex == NULL) {
        return;
    }
    HASH_SEQ_STATUS hashSeq;
    hash_seq_init(&hashSeq, tableOfIndex);
    /* clear var's old index and value */
    bool freevalLater = false;
    bool freeindexLater = false;
    if (var->value != oldvalue) {
        free_var(var);
    } else {
        var->value = (Datum)0;
        var->freeval = false;
        freevalLater = true;
    }
    if (var->tableOfIndex != tableOfIndex) {
        hash_destroy(var->tableOfIndex);
        var->tableOfIndex = NULL;
    } else {
        var->tableOfIndex = NULL;
        freeindexLater = true;
    }
    TableOfIndexEntry* srcEntry = NULL;
    while ((srcEntry = (TableOfIndexEntry*)hash_seq_search(&hashSeq)) != NULL) {
        PLpgSQL_var* oldvar = srcEntry->var;
        PLpgSQL_var* nest_var = NULL;
        if (unlikely(oldvar == NULL || oldvar->nest_layers != var->nest_table->nest_layers)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_PLSQL),
                    errmsg("Nest tableof var %s assigned is mismatch excepted nest layers.", var->refname)));
        }
        /* first add new var */
        int index = addNewNestedTable(estate, srcEntry->key, var);
        if (var->ispkg) {
            nest_var = (PLpgSQL_var*)var->pkg->datums[index];
        } else {
            nest_var = (PLpgSQL_var*)estate->datums[index];
        }
        Assert(nest_var->nest_layers == oldvar->nest_layers);
        nest_var->isnull = oldvar->isnull;
        /* recursive assign inner nest table value */
        assignNestTableOfValue(estate, nest_var, oldvar->value, oldvar->tableOfIndex);
    }
    if (freeindexLater && oldvalue != 0)
        pfree(DatumGetPointer(oldvalue));
    if (freeindexLater)
        hash_destroy(tableOfIndex);
}

static int insertTableOfIndexByDatumValue(TableOfIndexKey key, HTAB** tableOfIndex, PLpgSQL_var* var)
{
    if (*tableOfIndex == NULL) {
        *tableOfIndex = createTableOfIndex();
    }
    bool found = false;
    long numTotalIndex = hash_get_num_entries(*tableOfIndex);
    int16 typLen;
    bool typByVal = false;
    get_typlenbyval(key.exprtypeid, &typLen, &typByVal);
    key.exprdatum = datumCopy(key.exprdatum, typByVal, typLen);
    key.exprtypeid = key.exprtypeid;
    TableOfIndexEntry* desEntry =
        (TableOfIndexEntry*)hash_search(*tableOfIndex, (const void*)&key, HASH_ENTER, &found);
    Assert(!found);
    desEntry->index = numTotalIndex + 1;
    desEntry->var = var;
    return desEntry->index;
}

/*
 * exec_eval_datum        Get current value of a PLpgSQL_datum
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
static void exec_eval_datum(PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typeId,
    int32* typetypmod, Datum* value, bool* isnull)
{
    MemoryContext oldcontext;

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VARRAY:
        case PLPGSQL_DTYPE_TABLE:
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
        case PLPGSQL_DTYPE_VARRAY:
        case PLPGSQL_DTYPE_TABLE:
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
void exec_get_datum_type_info(PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typeId, int32* typmod,
    Oid* collation, Oid* tableOfIndexType, PLpgSQL_function* func)
{
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VARRAY:
        case PLPGSQL_DTYPE_TABLE:
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            if (var->nest_table != NULL) {
                PLpgSQL_var* nest = var->nest_table;
                while (nest != NULL) {
                    *typeId = nest->datatype->typoid;
                    *typmod = nest->datatype->atttypmod;
                    nest = nest->nest_table;
                }
            } else {
                *typeId = var->datatype->typoid;
                *typmod = var->datatype->atttypmod;
            }
            *collation = var->datatype->collation;
            *tableOfIndexType = var->tableOfIndexType;
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

static Datum exec_eval_varchar(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull)
{
    Datum exprdatum;
    Oid exprtypeid;

    exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid);
    exprdatum = exec_simple_cast_value(estate, exprdatum, exprtypeid, VARCHAROID, -1, *isNull);
    return PointerGetDatum(exprdatum);
}

/* ----------
 * exec_eval_expr			Evaluate an expression and return
 *					the result Datum.
 *
 * NOTE: caller must do exec_eval_cleanup when done with the Datum.
 * ----------
 */
static Datum exec_eval_expr(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull, Oid* rettype,
    HTAB** tableOfIndex)
{
    Datum result = 0;
    int rc;
#ifdef ENABLE_MULTIPLE_NODES
    // forbid commit/rollback in the stp which is called to get value
    bool savedisAllowCommitRollback = false;
    bool needResetErrMsg = false;
    needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_USED_AS_EXPR);
#else
    TransactionId oldTransactionId = SPI_get_top_transaction_id();
#endif
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
    if (exec_eval_simple_expr(estate, expr, &result, isNull, rettype, tableOfIndex)) {
#ifdef ENABLE_MULTIPLE_NODES
        stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);
#else
        stp_check_transaction_and_create_econtext(estate, oldTransactionId);
#endif
        return result;
    }

    /*
     * Else do it the hard way via exec_run_select
     */
    rc = exec_run_select(estate, expr, 2, NULL);
#ifdef ENABLE_MULTIPLE_NODES
    stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);
#else
    stp_check_transaction_and_create_econtext(estate, oldTransactionId);
#endif

    if (rc != SPI_OK_SELECT) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("query \"%s\" did not return data when evaluate expression", expr->query)));
    }

    bool allowMultiColumn = false;
    if (is_function_with_plpgsql_language_and_outparam(get_func_oid_from_expr(expr))) {
        allowMultiColumn = true;
    }

    /*
     * Check that the expression returns exactly one column...
     */
    if (estate->eval_tuptable->tupdesc->natts != 1 && !allowMultiColumn) {
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
    if (plpgsql_estate && plpgsql_estate->curr_nested_table_type != InvalidOid) {
        *rettype = plpgsql_estate->curr_nested_table_type;
        plpgsql_estate->curr_nested_table_type = InvalidOid;
    }

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
 * @isCollectParam: default false, is used to collect sql info in sqladvisor online model.
 */
static int exec_run_select(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, long maxtuples,
                           Portal* portalP, bool isCollectParam)
{
    ParamListInfo paramLI = NULL;
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

    /*
     * Set up ParamListInfo (hook function and possibly data values)
     * For validation estate->datums should be NULL, since there is
     * no parameter set vo validation
     */
    if (estate->datums) {
        paramLI = setup_param_list(estate, expr);
    }

    /*
     * If a portal was requested, put the query into the portal
     */
    if (portalP != NULL) {
        *portalP = SPI_cursor_open_with_paramlist(NULL, expr->plan, paramLI, estate->readonly_func, isCollectParam);
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
#ifdef ENABLE_MULTIPLE_NODES
    if (isCollectParam && checkSPIPlan(expr->plan)) {
        collectDynWithArgs(expr->query, paramLI, expr->plan->cursor_options);
    }
#endif
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
        row = stmt->row;
        if (row->ispkg) {
            row = (PLpgSQL_row*)(row->pkg->datums[stmt->row->dno]);
        } else {
            row = (PLpgSQL_row*)(estate->datums[stmt->row->dno]);
        }
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

#ifdef ENABLE_MULTIPLE_NODES
            /* For redistribute and broadcast stream, if sql in loop need to communicate with dn which wait for
             * stream operator which wait for stream operator, hang will occurs.
             * To avoid this, we need get all tuples for this fetch sql. */
            hold_portal_if_necessary(portal);
#endif

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
    PLpgSQL_execstate* estate, PLpgSQL_expr* expr, Datum* result, bool* isNull, Oid* rettype, HTAB** tableOfIndex)
{
    ExprContext* econtext = estate->eval_econtext;
    LocalTransactionId curlxid = t_thrd.proc->lxid;
    CachedPlan* cplan = NULL;
    ParamListInfo paramLI = NULL;
    PLpgSQL_expr* save_cur_expr = NULL;
    MemoryContext oldcontext = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    TransactionId oldTransactionId = SPI_get_top_transaction_id();
#endif
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
     * Check to see if the cached plan has been invalidated.  If not, and this
     * is the first use in the current transaction, save a plan refcount in
     * the simple-expression resowner.
     */
    if (likely(CachedPlanIsSimplyValid(expr->expr_simple_plansource,
                                       expr->expr_simple_plan,
                                       (expr->expr_simple_plan_lxid != curlxid ?
                                           u_sess->plsql_cxt.shared_simple_eval_resowner : NULL)))) {
        /*
         * It's still good, so just remember that we have a refcount on the
         * plan in the current transaction.  (If we already had one, this
         * assignment is a no-op.)
         */
        expr->expr_simple_plan_lxid = curlxid;
    } else {
        /*
        * If we have a valid refcount on some previous version of the plan,
        * release it, so we don't leak plans intra-transaction.
        */
        if (expr->expr_simple_plan_lxid == curlxid) {
            ResourceOwner saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
            t_thrd.utils_cxt.CurrentResourceOwner = u_sess->plsql_cxt.shared_simple_eval_resowner;
            ReleaseCachedPlan(expr->expr_simple_plan, true);
            t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
            expr->expr_simple_plan = NULL;
            expr->expr_simple_plan_lxid = InvalidLocalTransactionId;
        }

        /*
         * Revalidate cached plan, so that we will notice if it became stale. (We
         * need to hold a refcount while using the plan, anyway.)
         */
        cplan = SPI_plan_get_cached_plan(expr->plan);
        if (cplan == NULL) {
            return false;
        }

        /*
         * This test probably can't fail either, but if it does, cope by
         * declaring the plan to be non-simple.  On success, we'll acquire a
         * refcount on the new plan, stored in simple_eval_resowner.
         */
        if (CachedPlanAllowsSimpleValidityCheck(expr->expr_simple_plansource, cplan,
                                                u_sess->plsql_cxt.shared_simple_eval_resowner)) {
            /* Remember that we have the refcount */
            expr->expr_simple_plan = cplan;
            expr->expr_simple_plan_lxid = curlxid;
        } else {
            /* Release SPI_plan_get_cached_plan's refcount */
            ReleaseCachedPlan(cplan, true);
            /* Mark expression as non-simple, and fail */
            expr->expr_simple_expr = NULL;
            return false;
        }
        /*
         * SPI_plan_get_cached_plan acquired a plan refcount stored in the
         * active resowner.  We don't need that anymore, so release it.
         */
        ReleaseCachedPlan(cplan, true);

        /* Extract desired scalar expression from cached plan */
        exec_save_simple_expr(expr, cplan);

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
    SPI_STACK_LOG("push", NULL, NULL);
    SPI_push();

    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    bool need_snapshot = (expr->expr_simple_mutable && !estate->readonly_func);
    if (need_snapshot) {
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
    /*
     * For validation estate->datums should be NULL
     * since there is no parameter set vo validation
     */
    if (estate->datums) {
        paramLI = setup_param_list(estate, expr);
    }
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
    /* get tableof index from param */
    if (expr->is_have_tableof_index_var || expr->is_have_tableof_index_func) {
        ExecTableOfIndexInfo execTableOfIndexInfo;
        initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
        ExecEvalParamExternTableOfIndex((Node*)expr->expr_simple_state->expr, &execTableOfIndexInfo);

        if (tableOfIndex != NULL) {
            *tableOfIndex = execTableOfIndexInfo.tableOfIndex;
        }

        u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = execTableOfIndexInfo.tableOfIndex;
        u_sess->SPI_cxt.cur_tableof_index->tableOfNestLayer = execTableOfIndexInfo.tableOfLayers;
        /* for nest table of output, save layer of this var tableOfGetNestLayer in ExecEvalArrayRef,
           or set to zero for get whole nest table. */
        u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer =
            (execTableOfIndexInfo.tableOfLayers > 0 && IsA(expr->expr_simple_state->expr, Param)) ? 0 : -1;
    }
    plpgsql_estate->curr_nested_table_type = InvalidOid;

    *result = ExecEvalExpr(expr->expr_simple_state, econtext, isNull, NULL);
    /* for nested table, we need use nested table type as result type */
    if (plpgsql_estate && plpgsql_estate->curr_nested_table_type != InvalidOid) {
        HeapTuple tp;
        Form_pg_type typtup;
        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(plpgsql_estate->curr_nested_table_type));
        if (!HeapTupleIsValid(tp)) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for type %u", plpgsql_estate->curr_nested_table_type)));
        }
        typtup = (Form_pg_type)GETSTRUCT(tp);
        if (typtup->typtype != TYPTYPE_COMPOSITE)
            *rettype = plpgsql_estate->curr_nested_table_type;
        ReleaseSysCache(tp);
        plpgsql_estate->curr_nested_table_type = InvalidOid;
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (SPI_get_top_transaction_id() == oldTransactionId) {
        econtext->is_cursor = false;
    }
#else
    econtext->is_cursor = false;
#endif

    plpgsql_estate = NULL;
    /* Assorted cleanup */
    expr->expr_simple_in_use = false;

    estate->cur_expr = save_cur_expr;

    if (need_snapshot) {
        PopActiveSnapshot();
    }

    MemoryContextSwitchTo(oldcontext);

    SPI_STACK_LOG("pop", NULL, NULL);
    SPI_pop();

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

            if (datum->dtype == PLPGSQL_DTYPE_VAR ||
                datum->dtype == PLPGSQL_DTYPE_VARRAY ||
                datum->dtype == PLPGSQL_DTYPE_TABLE) {

                PLpgSQL_var* var = (PLpgSQL_var*)datum;
                ParamExternData* prm = &paramLI->params[dno];
                prm->value = var->value;
                prm->isnull = var->isnull;
                prm->pflags = PARAM_FLAG_CONST;
                prm->tabInfo = NULL;
                if (var->nest_table) {
                    PLpgSQL_var* nest = var->nest_table;
                    while (nest != NULL && nest->datatype != NULL) {
                        prm->ptype = nest->datatype->typoid;
                        nest = nest->nest_table;
                    }
                } else {
                    prm->ptype = var->datatype->typoid;
                }
                if (var->tableOfIndexType != InvalidOid) {
                    prm->tabInfo = (TableOfInfo*)palloc0(sizeof(TableOfInfo));
                    prm->tabInfo->isnestedtable = (var->nest_table != NULL);
                    prm->tabInfo->tableOfIndexType = var->tableOfIndexType;
                    prm->tabInfo->tableOfIndex = var->tableOfIndex;
                    prm->tabInfo->tableOfLayers =  var->nest_layers;
                }
                

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

/*
 * @brief exec_tuple_get_composite
 *  Transform tuple into a specified composite type.
 */
static Datum exec_tuple_get_composite(PLpgSQL_execstate* estate, TupleDesc tupdesc, HeapTuple tup, Oid typeoid)
{
    int td_natts = tupdesc ? tupdesc->natts : 0;
    StringInfoData buf;   /* record string */
    int t_natts = (HeapTupleIsValid(tup)) ? HeapTupleHeaderGetNatts(tup->t_data, tupdesc) : 0;
    initStringInfo(&buf);
    appendStringInfoChar(&buf, '(');

    int cnt = 0;
    int last_anum = 0;
    bool first = true;
    for (int anum = 0; anum < td_natts; anum++) {
        if (tupdesc->attrs[anum]->attisdropped) {
            continue;
        }

        if (!first) {
            appendStringInfoChar(&buf, ',');
        }
        first = false;

        if (anum < td_natts && anum < t_natts) {
            char *val_str = SPI_getvalue(tup, tupdesc, anum + 1);
            last_anum = anum;
            if (val_str) {
                appendStringInfoString(&buf, val_str);
            }
        }
        cnt++;
    }
    appendStringInfoChar(&buf, ')');

    /*
     * For tuple with only ONE complex type. The following rule is applied：
     *  1. If tuple is a RECORD, we assume that the datum is a generalized form of the request type.
     *  2. If tuple already is request type, use that instead.
     *  3. For other types, we assume that the datum is member of the request type.
     */
    Oid tuptype = SPI_gettypeid(tupdesc, last_anum + 1);
    if (cnt == 1 && (tuptype == RECORDOID || tuptype == typeoid)) {
        resetStringInfo(&buf);
        char *val_str = SPI_getvalue(tup, tupdesc, last_anum + 1);
        if (val_str) {
            appendStringInfoString(&buf, val_str);
        } else {
            appendStringInfoString(&buf, "()");
        }
    }

    /* reconstruct record */
    Oid typinput;
    Oid typioparam;
    FmgrInfo finfo_input;
    getTypeInputInfo(typeoid, &typinput, &typioparam);
    fmgr_info(typinput, &finfo_input);

    Datum rec_datum = InputFunctionCall(&finfo_input, buf.data, typioparam, -1);
    pfree_ext(buf.data);
    return rec_datum;
}

/*
 * @brief get_bulk_collect_target
 *  Get bulk collect into target.
 */
static PLpgSQL_var* get_bulk_collect_target(PLpgSQL_execstate* estate, PLpgSQL_row* row, int fnum, Oid* elemtype)
{
    PLpgSQL_datum* var = (PLpgSQL_datum*)(estate->datums[row->varnos[fnum]]);
    if (var->dtype != PLPGSQL_DTYPE_VAR) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_PLSQL),
                        errmsg("Unsupported bulk collect into target"),
                        errdetail("Unable to recognize the given bulk collect into target"),
                        errcause("N/A"),
                        erraction("Please modify bulk collect into target")));
    }
    *elemtype = get_element_type(((PLpgSQL_var*)var)->datatype->typoid);
    if (*elemtype == InvalidOid) {   /* It is not an array */
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_PLSQL),
                        errmsg("Unsupported bulk collect into target"),
                        errdetail("Unable to find any array elements in target"),
                        errcause("Given into target is not an array"),
                        erraction("Please check bulk collect into target")));
    }
    return (PLpgSQL_var*)var;
}

/*
 * @brief bulk_collect_precheck
 *  Bulk collect precheck.
 */
static int bulk_collect_precheck(PLpgSQL_execstate* estate, PLpgSQL_row* row, TupleDesc tupdesc, int ntup)
{
    if (row == NULL || row->nfields <= 0) {
        /* error out for now */
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                        errmsg("Unsupported bulk collect into target"),
                        errdetail("Unable to recognize the given bulk collect into target"), errcause("N/A"),
                        erraction("Please modify bulk collect into target")));
    }

    if (row->dtype == PLPGSQL_DTYPE_RECORD) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                        errmsg("Unsupported bulk collect into target"), errdetail("Cannot bulk collect records"),
                        errcause("N/A"), erraction("N/A")));
    }

    if ((Size)ntup > BULK_COLLECT_MAX) { /* Memory saving */
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_BUFFER), errmodule(MOD_PLSQL),
                        errmsg("Too many rows fetched at once"),
                        errdetail("Statement returns more than %zu rows", BULK_COLLECT_MAX), errcause("N/A"),
                        erraction("N/A")));
    }

    int anum = 0;
    int td_natts = tupdesc ? tupdesc->natts : 0;
    for (int i = 0; i < td_natts; i++) {
        /*
         * If td_natts is a positive number, tupdesc cannot be empty,
         * so there is no need to double check here
         */
        if (!tupdesc->attrs[i]->attisdropped) {
            anum++;
        }
    }

    int fnum = 0;
    int pos = 0;
    for (int i = 0; i < row->nfields; i++) {
        if (row->varnos[i] >= 0) {
            pos = i;
            fnum++;
        }
    }

    if (fnum == 1) { /* we can try to form a composite record for each row(tuple) */
        Oid elemtype = InvalidOid;
        (void)get_bulk_collect_target(estate, row, pos, &elemtype);
        bool try_coerce = (fnum == 1) & (TypeCategory(elemtype) == TYPCATEGORY_COMPOSITE);
        if (try_coerce) {
            return true;
        }
    }

    if (anum != fnum) { /* uneven number of attrs and bulk collect target */
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                        errmsg("Cannot bulk collect into targets"),
                        errdetail("Query returns more columns than available targets"), errcause("N/A"),
                        erraction("Please modify bulk collect into target")));
    }

    return false;
}

/*
 * @brief bulk_collect_context
 *  Context for handling BULK COLLECT INTO.
 */
typedef struct bulk_collect_context {
    PLpgSQL_var *var;       /* INTO target ARRAY/TABLE */
    Datum       *values;    /* Datum values from tuple table */
    bool        *isnull;    /* NULLs */
    Oid         elemtype;   /* elemtype Oid of target */
    Oid         valtype;    /* data source type Oid */
    int         ntup;       /* total number of tuples */
    TupleDesc   tupdesc;    /* tupdesc from tuptab */
} bulk_collect_context;

/*
 * @brief exec_assign_bulk_collect
 *  Execute bulk collect, transform tuples into an collection.
 * @param estate        execute state
 * @param context       bulk collect context
 * @param entire_row    needs to do row transform
 */
static void exec_assign_bulk_collect(PLpgSQL_execstate *estate, bulk_collect_context context)
{
    int dims[] = {context.ntup};
    int lbs[] = {1};
    int16 elmlen = 0;
    bool elmbyval = false;
    char elmalign;
    ArrayType *arrtype = NULL;

    /* initialize typelen, typbyval, typalign */
    get_typlenbyvalalign(context.elemtype, &elmlen, &elmbyval, &elmalign);

    /* coerce should be avoided by user, since it is a huge cost on performance */
    if (context.valtype == InvalidOid) { /* with all returning values are NULL */
        context.valtype = context.elemtype;
    }
    if (context.valtype != context.elemtype) {
        Oid typinput;
        Oid typioparam;
        FmgrInfo finfo_input;

        /* cache all common information for exec_cast_value() */
        getTypeInputInfo(context.elemtype, &typinput, &typioparam);
        fmgr_info(typinput, &finfo_input);

        /* try one cast, if it is not possible to cast type, should throw an error here */
        Oid funcid = InvalidOid;
        if (find_coercion_pathway(context.elemtype, context.valtype, COERCION_ASSIGNMENT, &funcid) \
            == COERCION_PATH_NONE) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                            errmsg("Fail to perform bulk collect"),
                            errdetail("Cannot bulk collect %s values into %s collections",
                            format_type_be(context.valtype), format_type_be(context.elemtype)),
                            errcause("N/A"), erraction("Please modify bulk collect into target")));
        }

        /* now try to cast every elements */
        for (int i = 0; i < context.ntup; i++) {
            Datum newdatum = exec_cast_value(estate, context.values[i], context.valtype, context.elemtype,
                                             &finfo_input, typioparam, -1, context.isnull[i]);
            context.values[i] = datumCopy(newdatum, elmbyval, elmlen);
            exec_eval_cleanup(estate);
        }
        context.valtype = context.elemtype;
    }
    arrtype = (context.ntup == 0) ? construct_empty_array(context.valtype)
                          : construct_md_array(context.values, context.isnull, 1, dims, lbs, context.valtype, elmlen,
                                               elmbyval, elmalign);
    HTAB* newTableOfIndex = NULL;
    /* table of index should build hash table */
    if (context.var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE &&
        context.var->datatype->tableOfIndexType == INT4OID) {
        /* clear origin tableOfIndex */
        if (context.var->tableOfIndex != NULL) {
            hash_destroy(context.var->tableOfIndex);
            context.var->tableOfIndex = NULL;
        }
        if (context.ntup != 0) {
            for (int i = 1; i <= context.ntup; i++) {
                TableOfIndexKey key;
                key.exprtypeid = context.var->datatype->tableOfIndexType;
                key.exprdatum = Int32GetDatum(i);
                (void)insertTableOfIndexByDatumValue(key, &newTableOfIndex);
            }
        }
    }
    bool array_is_null = false;
    exec_assign_value(estate, (PLpgSQL_datum*)context.var, PointerGetDatum(arrtype),
                      context.var->datatype->typoid, &array_is_null, newTableOfIndex);
}

/*
 * exec_read_bulk_collect
 * @brief This is a proprietary method for BULK COLLECT assign.
 *        It converts the tuples into an array type datum, then
 *        do the assign like regular INTO.
 *
 * @param estate    exec state
 * @param row       row (rec is not accepted)
 * @param tuptab    tuptab (with all returned tuples)
 */
static void exec_read_bulk_collect(PLpgSQL_execstate* estate, PLpgSQL_row* row, SPITupleTable* tuptab)
{
    bulk_collect_context context;
    context.tupdesc = tuptab->tupdesc;
    int td_natts = context.tupdesc ? context.tupdesc->natts : 0;
    context.ntup = tuptab->alloced - tuptab->free;
    bool need_entire_row = bulk_collect_precheck(estate, row, context.tupdesc, context.ntup);

    int fnum = 0;
    int anum = 0;
    for (fnum = 0; fnum < row->nfields; fnum++) {
        /* skip dropped column in row struct */
        if (row->varnos[fnum] < 0) {
            continue;
        }

        /* get associated var -- the target varray */
        context.elemtype = InvalidOid;
        context.var = get_bulk_collect_target(estate, row, fnum, &context.elemtype);
        if (context.ntup == 0) {
            context.valtype = context.elemtype;
            exec_assign_bulk_collect(estate, context);
        } else {
            /* Get all datums for the corresponding field */
            context.values = (Datum*)palloc0(sizeof(Datum) * context.ntup);   /* all datums are stored here */
            context.isnull = (bool*)palloc0(sizeof(bool) * context.ntup);     /* mark null datums */
            for (int i = 0; i < context.ntup; i++) {
                int t_natts = 0;
                HeapTuple tup = tuptab->vals[i];
                if (need_entire_row) {
                    context.valtype = context.elemtype;
                    context.values[i] = exec_tuple_get_composite(estate, context.tupdesc, tup, context.elemtype);
                    context.isnull[i] = false;
                } else {
                    /* skip dropped column in tuple */
                    t_natts = (HeapTupleIsValid(tup)) ? HeapTupleHeaderGetNatts(tup->t_data, context.tupdesc) : 0;
                    while (anum < td_natts && context.tupdesc->attrs[anum]->attisdropped) {
                        anum++;
                    }

                    if (anum < td_natts) {
                        if (anum < t_natts) {
                            context.values[i] = SPI_getbinval(tup, context.tupdesc, anum + 1, &context.isnull[i]);
                        } else {
                            context.values[i] = (Datum)0;
                            context.isnull[i] = true;
                        }
                        context.valtype = SPI_gettypeid(context.tupdesc, anum + 1);
                    } else {
                        context.values[i] = (Datum)0;
                        context.isnull[i] = true;
                    }

                    if (context.isnull[i] == false && CheckTypeIsCursor(row, context.valtype, fnum) &&
                        estate->cursor_return_data != NULL) {
                        /* error out for cursors */
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PLSQL),
                                errmsg("Unsupported bulk collect into target"),
                                errdetail("Cursors in bulk collect are not supported"), errcause("N/A"),
                                erraction("Please modify bulk collect into target")));
                    }
                }
            }
            /* make a single array datum from the datums we collected before, and assign to var */
            exec_assign_bulk_collect(estate, context);

            pfree_ext(context.values);
            pfree_ext(context.isnull);
        }
        
        anum++; /* anum increment at last */
    }
}

/* ----------
 * exec_move_row			Move one tuple's values into a record or row
 *
 * Since this uses exec_assign_value, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 * ----------
 */
static void exec_move_row(PLpgSQL_execstate* estate,
    PLpgSQL_rec* rec, PLpgSQL_row* row, HeapTuple tup, TupleDesc tupdesc, bool fromExecSql)
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
            if (rec->pkg != NULL) {
                MemoryContext temp = MemoryContextSwitchTo(rec->pkg->pkg_cxt);
                tup = heap_copytuple(tup);
                MemoryContextSwitchTo(temp);
            } else {
                tup = heap_copytuple(tup);
            }
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
            if (rec->pkg != NULL) {
                MemoryContext temp = MemoryContextSwitchTo(rec->pkg->pkg_cxt);
                tupdesc = CreateTupleDescCopy(tupdesc);
                MemoryContextSwitchTo(temp);              
            } else {
                tupdesc = CreateTupleDescCopy(tupdesc);
            }
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
            if (u_sess->plsql_cxt.pass_func_tupdesc && tupdesc &&
                u_sess->plsql_cxt.pass_func_tupdesc->natts == tupdesc->natts && fromExecSql) {
                tupdesc = u_sess->plsql_cxt.pass_func_tupdesc;
            }
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
#ifndef ENABLE_MULTIPLE_NODES
                    if (row->ispkg) {
                        PLpgSQL_package* pkg = row->pkg;
                        var = (PLpgSQL_var*)(pkg->datums[row->varnos[fnum]]);
                    } else {
                        var = (PLpgSQL_var*)(estate->datums[row->varnos[fnum]]);
                    }
#else
                    var = (PLpgSQL_var*)(estate->datums[row->varnos[fnum]]);
#endif
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

                    HTAB* tableOfIndex = NULL;
                    if (u_sess->plsql_cxt.func_tableof_index != NULL && fromExecSql) {
                        ListCell* l = NULL;
                        foreach (l, u_sess->plsql_cxt.func_tableof_index) {
                            PLpgSQL_func_tableof_index* func_tableof = (PLpgSQL_func_tableof_index*)lfirst(l);
                            if (func_tableof->varno == fnum) {
                                tableOfIndex = func_tableof->tableOfIndex;
                            }
                        }
                    }
                    exec_assign_value(estate, (PLpgSQL_datum*)var, value, valtype, &isnull, tableOfIndex);
                }
                if (fromExecSql) {
                    free_func_tableof_index();
                    pfree_ext(u_sess->plsql_cxt.pass_func_tupdesc);
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
HeapTuple make_tuple_from_row(PLpgSQL_execstate* estate, PLpgSQL_row* row, TupleDesc tupdesc)
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
        if (row->ispkg) {
            PLpgSQL_package* pkg = row->pkg;
            if (pkg == NULL) {
                ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("package is null"),
                    errdetail("N/A"),
                    errcause("compile error."),
                    erraction("check package error and redefine package")));  
            }
            exec_eval_datum(estate, pkg->datums[row->varnos[i]], &fieldtypeid, &fieldtypmod, &dvalues[i], &nulls[i]);
        } else {
            exec_eval_datum(estate, estate->datums[row->varnos[i]], &fieldtypeid, &fieldtypmod, &dvalues[i], &nulls[i]);
        }

        if (is_huge_clob(fieldtypeid, nulls[i], dvalues[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("huge clob do not support concat a row")));
        }

        if (estate->is_exception && fieldtypeid == REFCURSOROID) {
            if (DatumGetPointer(dvalues[i]) != NULL) {
                char* curname = NULL;
                curname = TextDatumGetCString(dvalues[i]);
                Portal portal = SPI_cursor_find(curname);
                if (portal == NULL || portal->status == PORTAL_FAILED) {
                    dvalues[i] = 0;
                    nulls[i] = true;
                }
                ereport(DEBUG3, (errmodule(MOD_PLSQL), errcode(ERRCODE_LOG),
                    errmsg("RESET CURSOR NULL LOG: function: %s, set cursor: %s to null due to exception",
                        estate->func->fn_signature, curname)));
                pfree_ext(curname);
            } else {
                dvalues[i] = 0;
                nulls[i] = true;
            }
        }
        if (fieldtypeid != tupdesc->attrs[i]->atttypid) {
            /* if table of type should check its array type */
            HeapTuple type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(tupdesc->attrs[i]->atttypid));
            if (HeapTupleIsValid(type_tup)) {
                Oid refTypOid = ((Form_pg_type)GETSTRUCT(type_tup))->typelem;
                char refTypType = ((Form_pg_type)GETSTRUCT(type_tup))->typtype;
                ReleaseSysCache(type_tup);
                if (refTypType == TYPTYPE_TABLEOF && refTypOid == fieldtypeid) {
                    continue;
                }
            }

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

        // No need to check Anum_pg_proc_proargtypesext attribute, because cast function
        // will not have more than FUNC_MAX_ARGS_INROW parameters
        Assert(procstruct->pronargs <= FUNC_MAX_ARGS_INROW);

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
static void exec_simple_check_plan(PLpgSQL_execstate *estate, PLpgSQL_expr* expr)
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

    /* Check whether the return value of the function is setof. If yes, directly return the value. */
    for (int i = 0; i < list_length(query->targetList); i++) {
        TargetEntry* tle = (TargetEntry*)list_nth(query->targetList, i);
        if (!exec_simple_check_node((Node*)tle->expr)) {
            return;
        }
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

    /*
     * Verify that plancache.c thinks the plan is simple enough to use
     * CachedPlanIsSimplyValid.  Given the restrictions above, it's unlikely
     * that this could fail, but if it does, just treat plan as not simple. On
     * success, save a refcount on the plan in the simple-expression resowner.
     */
    if (estate != NULL &&
        CachedPlanAllowsSimpleValidityCheck(plansource, cplan, u_sess->plsql_cxt.shared_simple_eval_resowner)) {
        /* Remember that we have the refcount */
        expr->expr_simple_plansource = plansource;
        expr->expr_simple_plan = cplan;
        expr->expr_simple_plan_lxid = t_thrd.proc->lxid;

        /* Share the remaining work with the replan code path */
        exec_save_simple_expr(expr, cplan);
    } 
    if (estate == NULL) {
        /* Share the remaining work with recheck code path */
        exec_simple_recheck_plan(expr, cplan);
    }

    /* Release our plan refcount */
    ReleaseCachedPlan(cplan, true);
}


/*
 * exec_save_simple_expr --- extract simple expression from CachedPlan
 */
static void exec_save_simple_expr(PLpgSQL_expr *expr, CachedPlan *cplan)
{
    PlannedStmt *stmt;
    Plan       *plan;
    Expr       *tle_expr;

    /*
     * Given the checks that exec_simple_check_plan did, none of the Asserts
     * here should ever fail.
     */

    /* Extract the single PlannedStmt */
    Assert(list_length(cplan->stmt_list) == 1);
    stmt = linitial_node(PlannedStmt, cplan->stmt_list);
    Assert(stmt->commandType == CMD_SELECT);

    /*
     * Ordinarily, the plan node should be a simple Result.  However, if
     * force_parallel_mode is on, the planner might've stuck a Gather node
     * atop that.  The simplest way to deal with this is to look through the
     * Gather node.  The Gather node's tlist would normally contain a Var
     * referencing the child node's output, but it could also be a Param, or
     * it could be a Const that setrefs.c copied as-is.
     */
    plan = stmt->planTree;
    for (;;) {
        /* Extract the single tlist expression */
        Assert(list_length(plan->targetlist) == 1);
        tle_expr = castNode(TargetEntry, linitial(plan->targetlist))->expr;

        if (IsA(plan, BaseResult)) {
            Assert(plan->lefttree == NULL &&
                   plan->righttree == NULL &&
                   plan->initPlan == NULL &&
                   plan->qual == NULL &&
                   ((BaseResult*) plan)->resconstantqual == NULL);
            break;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_PLSQL),
                    errmsg("unexpected plan node type: %d", (int)nodeTag(plan))));
        }
    }

    /*
     * Save the simple expression, and initialize state to "not valid in current transaction".
     */
    expr->expr_simple_expr = tle_expr;
    expr->expr_simple_state = NULL;
    expr->expr_simple_in_use = false;
    expr->expr_simple_lxid = InvalidLocalTransactionId;
    /* Also stash away the expression result type */
    expr->expr_simple_type = exprType((Node *) tle_expr);
    expr->expr_simple_typmod = exprTypmod((Node *) tle_expr);
    /* We also want to remember if it is immutable or not */
    expr->expr_simple_mutable = contain_mutable_functions((Node *) tle_expr);
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
static void plpgsql_create_econtext(PLpgSQL_execstate* estate, MemoryContext saveCxt)
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
        u_sess->plsql_cxt.simple_eval_estate = CreateExecutorState(saveCxt);

        if (saveCxt != NULL) {
            MemoryContextSetParent(saveCxt, u_sess->top_transaction_mem_cxt);
        }
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * Likewise for the simple-expression resource owner.
     */
    if (u_sess->plsql_cxt.shared_simple_eval_resowner == NULL)
    {
        u_sess->plsql_cxt.shared_simple_eval_resowner =
            ResourceOwnerCreate(t_thrd.utils_cxt.TopTransactionResourceOwner,
                                "PL/pgSQL simple expressions",
                                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    }

    /*
     * Create a child econtext for the current function.
     */
    estate->eval_econtext = CreateExprContext(u_sess->plsql_cxt.simple_eval_estate);

    /*
     * Make a stack entry so we can clean up the econtext at subxact end.
     * Stack entries are kept in u_sess->top_transaction_mem_cxt for simplicity.
     */
    entry = (SimpleEcontextStackEntry*)MemoryContextAlloc(u_sess->top_transaction_mem_cxt,
                                                          sizeof(SimpleEcontextStackEntry));

    entry->stack_econtext = estate->eval_econtext;
    entry->xact_subxid = GetCurrentSubTransactionId();
    entry->statckEntryId = ++u_sess->plsql_cxt.nextStackEntryId;

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
    SimpleEcontextStackEntry *pre = NULL;
    SimpleEcontextStackEntry *plcontext = u_sess->plsql_cxt.simple_econtext_stack;

    /*
     * As savepoint in STP appear after creating econtext, the quantity of subtransaction is either
     * more or less. The top stack entry may not be the pushed one at the begining of PL function.
     * Here we skip some, try to find and release the entry identified by estate->stack_entry_start.
     */
    while (plcontext != NULL &&  plcontext->statckEntryId > estate->stack_entry_start) {
        /*
         * Call any shutdown callbacks and reset the econtext created by this PL function
         * as possible. Don't free it since the subtransaction is still alive.
         */
        ReScanExprContext(plcontext->stack_econtext);

        pre = plcontext;
        plcontext = plcontext->next;
    }

    /* if find the stack_entry_start, destory it. */
    if (plcontext != NULL && plcontext->statckEntryId == estate->stack_entry_start) {
        if (pre == NULL) {
            u_sess->plsql_cxt.simple_econtext_stack = plcontext->next;
        } else {
            pre->next = plcontext->next;
        }

        FreeExprContext(plcontext->stack_econtext, true);
        pfree_ext(plcontext);
        estate->eval_econtext = NULL;
    }

    stp_cleanup_subxact_resowner(estate->stack_entry_start);
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
        if (u_sess->plsql_cxt.shared_simple_eval_resowner)
            ResourceOwnerReleaseAllPlanCacheRefs(u_sess->plsql_cxt.shared_simple_eval_resowner);
        u_sess->plsql_cxt.shared_simple_eval_resowner = NULL;
    } else {
        u_sess->plsql_cxt.simple_eval_estate = NULL;
        u_sess->plsql_cxt.shared_simple_eval_resowner = NULL;
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

    /*
     * "-1" means it can be freed anywhere.
     *
     * 1. SubTransaction started outside this statemenet except the last one can be freed
     *    at any time. such as:
     *
     *        BEGIN; savepoint s1; savepoint s2; proc1(); savepoint s3; savepoint s4; proc2();
     *
     *    1.1: s1's resowner can be freed anywhere in proc1();
     *    1.2: those before s4 can be freed anywhere in proc2();
     *
     * 2. subtransaction in PL without SimpleEcontextStackEntry
     */
    int64 minStackId = -1;

    while (u_sess->plsql_cxt.simple_econtext_stack != NULL &&
           u_sess->plsql_cxt.simple_econtext_stack->xact_subxid == mySubid) {
        SimpleEcontextStackEntry* next = NULL;

        minStackId = u_sess->plsql_cxt.simple_econtext_stack->statckEntryId;
        FreeExprContext(u_sess->plsql_cxt.simple_econtext_stack->stack_econtext, (event == SUBXACT_EVENT_COMMIT_SUB));
        next = u_sess->plsql_cxt.simple_econtext_stack->next;
        pfree_ext(u_sess->plsql_cxt.simple_econtext_stack);
        u_sess->plsql_cxt.simple_econtext_stack = next;
    }

    u_sess->plsql_cxt.minSubxactStackId = minStackId;
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
void assign_text_var(PLpgSQL_var* var, const char* str)
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
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        assign_text_var(sqlcode_var, plpgsql_get_sqlstate(sqlcode));
    } else {
        sqlcode_var->value = Int32GetDatum(sqlcode);
        sqlcode_var->freeval = false;
        sqlcode_var->isnull = false;
    }
}

static void exec_set_prev_sqlcode(PLpgSQL_execstate* estate, PLpgSQL_execstate* estate_prev)
{
    PLpgSQL_var* sqlcode_var = NULL;

    if (estate_prev != NULL && estate_prev->cur_error != NULL) {
        PLpgSQL_var* sqlerrm_var = NULL;
        PLpgSQL_var* state_var = NULL;
        ErrorData* prev_error = estate_prev->cur_error;
        sqlcode_var = (PLpgSQL_var*)estate->datums[estate->sqlcode_varno];

        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
            assign_text_var(sqlcode_var, plpgsql_get_sqlstate(prev_error->sqlerrcode));
        } else {
            sqlcode_var->value = Int32GetDatum(prev_error->sqlerrcode);
            sqlcode_var->freeval = false;
            sqlcode_var->isnull = false;
        }
        sqlerrm_var = (PLpgSQL_var*)estate->datums[estate->sqlerrm_varno];
        assign_text_var(sqlerrm_var, prev_error->message);
        state_var = (PLpgSQL_var*)estate->datums[estate->sqlstate_varno];
        assign_text_var(state_var, plpgsql_get_sqlstate(prev_error->sqlerrcode));
    } else {
        sqlcode_var = (PLpgSQL_var*)estate->datums[estate->sqlcode_varno];
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
            assign_text_var(sqlcode_var, plpgsql_get_sqlstate(Int32GetDatum(0)));
        } else {
            sqlcode_var->value = Int32GetDatum(0);
            sqlcode_var->freeval = false;
            sqlcode_var->isnull = false;
        }
    }
}

#ifndef ENABLE_MULTIPLE_NODES
static void exec_set_cursor_att_var(PLpgSQL_execstate* estate, PLpgSQL_execstate* estate_prev)
{
    PLpgSQL_var* var = NULL;
    PLpgSQL_var* prev_var = NULL;

    var = (PLpgSQL_var*)(estate->datums[estate->found_varno]);
    prev_var = (PLpgSQL_var*)(estate_prev->datums[estate_prev->found_varno]);
    var->value = prev_var->value;
    var->isnull = prev_var->isnull;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_cursor_found_varno]);
    prev_var = (PLpgSQL_var*)(estate_prev->datums[estate_prev->sql_cursor_found_varno]);
    var->value = prev_var->value;
    var->isnull = prev_var->isnull;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_notfound_varno]);
    prev_var = (PLpgSQL_var*)(estate_prev->datums[estate_prev->sql_notfound_varno]);
    var->value = prev_var->value;
    var->isnull = prev_var->isnull;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_isopen_varno]);
    prev_var = (PLpgSQL_var*)(estate_prev->datums[estate_prev->sql_isopen_varno]);
    var->value = prev_var->value;
    var->isnull = prev_var->isnull;

    var = (PLpgSQL_var*)(estate->datums[estate->sql_rowcount_varno]);
    prev_var = (PLpgSQL_var*)(estate_prev->datums[estate_prev->sql_rowcount_varno]);
    var->value = prev_var->value;
    var->isnull = prev_var->isnull;
}
void estate_cursor_set(FormatCallStack* plcallstack)
{
    if (plcallstack != NULL && plcallstack->prev != NULL &&
        u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        PLpgSQL_execstate* estate_pre = (PLpgSQL_execstate*)(plcallstack->prev->elem);
        PLpgSQL_execstate* estate_exec = (PLpgSQL_execstate*)plcallstack->elem;
        exec_set_cursor_att_var(estate_pre, estate_exec);
    }
}
#endif

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
#ifndef ENABLE_MULTIPLE_NODES
    if (!portal->isPkgCur) {
#endif
        for (int i = 0; i < CURSOR_ATTRIBUTE_NUMBER; i++) {
            portal->cursorAttribute[i] = NULL;
        }
#ifndef ENABLE_MULTIPLE_NODES
    }
#endif

    portal->funcOid = InvalidOid;
    portal->funcUseCount = 0;
}
#ifndef ENABLE_MULTIPLE_NODES
void ResetCursorAtrribute(Portal portal)
{
    if (!portal->isPkgCur) {
        return;
    }
    if (portal->cursorAttribute[0] == NULL) {
        return;
    }

    PLpgSQL_var *var = (PLpgSQL_var*)portal->cursorAttribute[CURSOR_ISOPEN - 1];
    var->isnull = false;
    var->value = 0;
    var = (PLpgSQL_var*)portal->cursorAttribute[CURSOR_FOUND - 1];
    var->isnull = true;
    var->value = (Datum)0;
    var = (PLpgSQL_var*)portal->cursorAttribute[CURSOR_NOTFOUND - 1];
    var->isnull = true;
    var->value = (Datum)0;
    var = (PLpgSQL_var*)portal->cursorAttribute[CURSOR_ROWCOUNT - 1];
    var->isnull = true;
    var->value = (Datum)0;
}
#endif

static void stp_check_transaction_and_set_resource_owner(ResourceOwner oldResourceOwner, TransactionId oldTransactionId)
{
    if (oldTransactionId != SPI_get_top_transaction_id()) {
        t_thrd.utils_cxt.CurrentResourceOwner = get_last_transaction_resourceowner();
    } else if (ResourceOwnerIsValid(oldResourceOwner)) {
        t_thrd.utils_cxt.CurrentResourceOwner = oldResourceOwner;
    }
}

static void stp_check_transaction_and_create_econtext(PLpgSQL_execstate* estate,TransactionId oldTransactionId)
{
    if (oldTransactionId != SPI_get_top_transaction_id()) {
        /* While old transaction terminated, econtext was destroyed. Create it again. */
        plpgsql_create_econtext(estate);
    } else {
        /*
         * Subtransaction may start or finished in the subroutine , we need switch into
         * the newest context for executing simple expressions.
         */
        Assert(u_sess->plsql_cxt.simple_econtext_stack != NULL);
        estate->eval_econtext = u_sess->plsql_cxt.simple_econtext_stack->stack_econtext;
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

static bool plpgsql_check_invalid_by_dependency(List* invalItems, int cacheid, uint32 objid)
{
    if (invalItems == NULL)
        return false;
    ListCell   *lc = NULL;
    foreach(lc, invalItems)
    {
        FuncInvalItem *item = (FuncInvalItem *) lfirst(lc);
        if (item->cacheId != cacheid) {
            continue;
        }
        // if has dependency in thic invalitem, return
        if (item->cacheId == PROCOID && item->objId == objid) {
            return true;
        } else if (item->cacheId == PACKAGEOID && item->objId == objid) {
            return true;
        }
    }
    return false;
}

/*
 * Check dependency for function and package's hash table,
 * and delete the invalid package or functio from session.
 * cacheId: oid of object's type
 * objId: oid of object
 */
void plpgsql_hashtable_delete_and_check_invalid_item(int classId, Oid objId)
{
    if (classId == PROCOID) {
        if (likely(u_sess->plsql_cxt.plpgsql_HashTable != NULL)) {
            HASH_SEQ_STATUS hash_seq;
            hash_seq_init(&hash_seq, u_sess->plsql_cxt.plpgsql_HashTable);
            plpgsql_HashEnt* hentry = NULL;
            while ((hentry = (plpgsql_HashEnt *)hash_seq_search(&hash_seq)) != NULL) {
                PLpgSQL_function* func = hentry->function;
                /* func in package will be invalid by package */
                if (hentry->key.funcOid == objId) {
                    if (!OidIsValid(func->pkg_oid)) {
                        delete_function(func);
                    }
                    hash_seq_term(&hash_seq);
                    return;
                }
            }
        }
        return;
    }

    /*
     * when compile, invalid the package may cause confilct, so ignore it.
     * maybe a better way to record it and handler it later, will support in
     * the future.
     */
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        return;
    }
    
    PLpgSQL_pkg_hashkey hashkey;
    hashkey.pkgOid = objId;
    PLpgSQL_package* getpkg = plpgsql_pkg_HashTableLookup(&hashkey);
    if (getpkg == NULL) {
        return;
    }

    /* delete the invalid package from session */
    delete_package(getpkg);

    /*
     * find packages and funcs depend on this package,
     * funcs will be invalided in invalid_depend_func_and_packgae(),
     * packages will be marked and return as a list
     */
    List* invalidPkgList = invalid_depend_func_and_packgae(objId);
    ListCell* cell = NULL;

    /* delete the marked packages */
    foreach(cell, invalidPkgList) {
        plpgsql_hashtable_delete_and_check_invalid_item(PACKAGEOID, lfirst_oid(cell));
    }
    list_free_ext(invalidPkgList);
}

/*
 * Check dependency for package's hash table,
 * and delete the invalid package or functio from session.
 * cacheId: oid of object's type
 * objId: oid of object
 */
void delete_package_and_check_invalid_item(Oid pkgOid)
{
    /* if compile the package now, report error */
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid == pkgOid) {
        ReportCompileConcurrentError(
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature, true);
    }

    PLpgSQL_pkg_hashkey hashkey;
    hashkey.pkgOid = pkgOid;
    PLpgSQL_package* getpkg = plpgsql_pkg_HashTableLookup(&hashkey);
    if (getpkg != NULL) {
        /* check if have conflict with current compile */
        CheckCurrCompileDependOnPackage(pkgOid);
        delete_package(getpkg);
    } else {
        return;
    }

    /*
     * find packages and funcs depend on this package,
     * funcs will be invalided in invalid_depend_func_and_packgae(),
     * packages will be marked and return as a list
     */
    List* invalidPkgList = invalid_depend_func_and_packgae(pkgOid);
    ListCell* cell = NULL;

    /* delete the marked packages */
    foreach(cell, invalidPkgList) {
        delete_package_and_check_invalid_item(lfirst_oid(cell));
    }
    list_free_ext(invalidPkgList);
}

/*
 * find packages and funcs depend on this package,
 * funcs will be invalided in invalid_depend_func_and_packgae(),
 * packages will be marked and return as a list
 */
static List* invalid_depend_func_and_packgae(Oid pkgOid)
{
    List* invalidPkgList = NIL;
    
    /* search depend func first */
    if (likely(u_sess->plsql_cxt.plpgsql_HashTable != NULL)) {
        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, u_sess->plsql_cxt.plpgsql_HashTable);
        plpgsql_HashEnt* hentry = NULL;
        while ((hentry = (plpgsql_HashEnt *)hash_seq_search(&hash_seq)) != NULL) {
            PLpgSQL_function* func = hentry->function;
            /* check invalid by dependency, func->invalItems record the dependency */
            bool funcIsInvalid = plpgsql_check_invalid_by_dependency(func->invalItems, PACKAGEOID, pkgOid);
            /* ignore func in this package */
            if (funcIsInvalid && func->pkg_oid != pkgOid) {
                if (func->use_count == 0 && !OidIsValid(func->pkg_oid)) {
                    delete_function(func);
                } else if (OidIsValid(func->pkg_oid)) {
                    /* mark the package, and delete them later */
                    invalidPkgList = list_append_unique_oid(invalidPkgList, func->pkg_oid);
                }
            }
        }
    }

    /* search depend package then */
    if (u_sess->plsql_cxt.plpgsql_pkg_HashTable != NULL) {
        HASH_SEQ_STATUS hash_pkgseq;
        hash_seq_init(&hash_pkgseq, u_sess->plsql_cxt.plpgsql_pkg_HashTable);
        plpgsql_pkg_HashEnt* pkghentry = NULL;
        while ((pkghentry = (plpgsql_pkg_HashEnt *)hash_seq_search(&hash_pkgseq)) != NULL) {
            PLpgSQL_package* pkg = pkghentry->package;
            /* check invalid by dependency, pkg->invalItems record the dependency */
            bool pkgIsInvalid = plpgsql_check_invalid_by_dependency(pkg->invalItems, PACKAGEOID, pkgOid);
            /* delete the invalid package, skipping the orignal one, we have delete it */
            if (pkgIsInvalid && pkg->pkg_oid != pkgOid) {
                /* mark the package, and delete them later */
                invalidPkgList = list_append_unique_oid(invalidPkgList, pkg->pkg_oid);
            }
        }
    }
    return invalidPkgList;
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

typedef struct {
    int nargs;      /* number of arguments */
    Oid* types;     /* types of arguments */
    int* param_ids; /* evaluated argument values */
} DynParamsData;

typedef struct {
    DynParamsData* dp;
    SQLFunctionParseInfoPtr pinfo;
} pl_validate_dynexpr_info;

/*
 * exec_eval_using_params --- evaluate params of USING clause
 */
static DynParamsData* validate_using_params(List* params, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo)
{
    DynParamsData* ppd = NULL;
    int nargs;
    int i;
    ListCell* lc = NULL;

    ppd = (DynParamsData*)palloc(sizeof(DynParamsData));
    nargs = list_length(params);

    ppd->nargs = nargs;
    ppd->types = (Oid*)palloc(nargs * sizeof(Oid));
    ppd->param_ids = (int*)palloc(nargs * sizeof(int));

    /* Get all types to verify param_index */
    bool isNull = false;
    char* argmodes = NULL;
    int n_modes = 0;
    HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmodule(MOD_EXECUTOR),
            errmsg("cache lookup failed for function %u when initialize function cache.", func->fn_oid)));
    }
    Datum proargmodes;
#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, tuple, Anum_pg_proc_proargmodes, &isNull);
    } else {
        proargmodes = SysCacheGetAttr(PROCALLARGS, tuple, Anum_pg_proc_proargmodes, &isNull);
    }
#else
    proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, tuple, Anum_pg_proc_proargmodes, &isNull);
#endif
    if (!isNull) {
        ArrayType* arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
        if (arr) {
            n_modes = ARR_DIMS(arr)[0];
            argmodes = (char*)ARR_DATA_PTR(arr);
        }
    }
    i = 0;
    foreach (lc, params) {
        PLpgSQL_expr* param = (PLpgSQL_expr*)lfirst(lc);
        SPIPlanPtr plan = NULL;
        pl_validate_expression(param, func, pinfo, &plan);
        if (plan) {
            SPI_keepplan(plan);
            param->plan = plan;
        }
        /* Check to see if it's a simple expression */
        exec_simple_check_plan(NULL, param);
        ppd->types[i] = param->expr_simple_type;


        if (ppd->types[i] == UNKNOWNOID) {
            /*
             * Treat 'unknown' parameters as text, since that's what most
             * people would expect. SPI_execute_with_args can coerce unknown
             * constants in a more intelligent way, but not unknown Params.
             * This code also takes care of copying into the right context.
             * Note we assume 'unknown' has the representation of C-string.
             */
            ppd->types[i] = TEXTOID;
        }
        if (param->expr_simple_expr && nodeTag(param->expr_simple_expr) == T_Param) {
            int allp_id = ((Param*)param->expr_simple_expr)->paramid;
            int param_id = 0;
            /* get input param id since dynexec uses allparamtypes */
            if (argmodes) {
                for (int i = 0; i < allp_id && i < n_modes; i++) {
                    if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_INOUT ||
                        argmodes[i] == PROARGMODE_VARIADIC) {
                        param_id++;
                    }
                }
            } else {
                param_id = allp_id;
            }
            ppd->param_ids[i] = param_id;
        } else {
            ppd->param_ids[i] = 0;
        }
        i++;
        if (param->plan) {
            SPI_freeplan(param->plan);
            param->plan = NULL;
        }
    }
    ReleaseSysCache(tuple);
    return ppd;
}

/*
 * plpgsql_dynparam_ref		parser callback for ParamRefs (dynamic execution)
 */
static Node* plpgsql_dynparam_ref(ParseState* pstate, ParamRef* pref)
{
    DynParamsData* dp = (DynParamsData*)pstate->p_ref_hook_state;
    int p_no = pref->number - 1;
    if (p_no < 0 || p_no >= dp->nargs) {
        return NULL; /* parameter not known to plpgsql */
    }
    Param* param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = dp->param_ids[p_no];
    param->paramtype = dp->types[p_no];
    param->location = pref->location;
    param->tableOfIndexType = InvalidOid;

    return (Node*)param;
}

static void plpgsql_crete_dynaproc_parser_setup(struct ParseState* pstate, pl_validate_dynexpr_info* dexpr_info)
{
    pstate->p_pre_columnref_hook = NULL;
    pstate->p_post_columnref_hook = NULL;
    pstate->p_paramref_hook = plpgsql_dynparam_ref;
    /* no need to use p_coerce_param_hook */
    pstate->p_ref_hook_state = (void*)dexpr_info->dp;
    pstate->p_create_proc_operator_hook = sql_create_proc_operator_ref;
    pstate->p_create_proc_insert_hook = sql_fn_parser_replace_param_type_for_insert;
    pstate->p_cl_hook_state = dexpr_info->pinfo;
}

void validate_stmt_dynexecute(PLpgSQL_stmt_dynexecute* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    List** stmt_list)
{
    Datum query;
    bool isnull = false;
    Oid restype;
    char* querystr = NULL;
    PLpgSQL_stmt stmtblock;
    stmtblock.cmd_type = stmt->cmd_type;
    stmtblock.lineno = stmt->lineno;
    PLpgSQL_execstate estate;
    ListCell* lc1 = NULL;
    plpgsql_estate_setup(&estate, func, (ReturnSetInfo*)NULL);
    /* For validation set  estate->datums to null */
    pfree_ext(estate.datums);
    estate.datums = NULL;

    query = exec_eval_expr(&estate, stmt->query, &isnull, &restype);
    if (isnull) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmodule(MOD_PLSQL),
            errmsg("query string argument of EXECUTE statement is null")));
    }

    /* Get the C-String representation */
    querystr = convert_value_to_string(&estate, query, restype);
    /* copy it out of the temporary context before we clean up */
    querystr = pstrdup(querystr);

    exec_eval_cleanup(&estate);
    if (!stmt->params) {
        return;
    }
    DynParamsData* dp = validate_using_params(stmt->params, func, pinfo);
    if (!dp) {
        return;
    }
    int res;
    _SPI_plan plan;
    SPI_STACK_LOG("begin", querystr, NULL);
    res = _SPI_begin_call(false);
    if (res < 0) {
        return;
    }
    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = 0;
    plan.nargs = 0;
    plan.argtypes = NULL;
    plan.parserSetup = (ParserSetupHook)plpgsql_crete_dynaproc_parser_setup;
    pl_validate_dynexpr_info dvi;
    dvi.dp = dp;
    dvi.pinfo = pinfo;
    plan.parserSetupArg = &dvi;

    _SPI_prepare_oneshot_plan_for_validator(querystr, &plan);
    ErrorContextCallback spi_err_context;

    foreach (lc1, plan.plancache_list) {
        CachedPlanSource* plansource = (CachedPlanSource*)lfirst(lc1);

        spi_err_context.arg = (void*)plansource->query_string;

        /*
         * If this is a one-shot plan, we still need to do parse analysis.
         */
        if (plan.oneshot) {
            Node* parsetree = plansource->raw_parse_tree;
            const char* src = plansource->query_string;
            List* statement_list = NIL;

            /*
             * Parameter datatypes are driven by parserSetup hook if provided,
             * otherwise we use the fixed parameter list.
             */
            if (plan.parserSetup != NULL) {
                Assert(plan.nargs == 0);
                statement_list = pg_analyze_and_rewrite_params(parsetree, src, plan.parserSetup, plan.parserSetupArg);
            } else {
                statement_list = pg_analyze_and_rewrite(parsetree, src, plan.argtypes, plan.nargs);
            }
            *stmt_list = list_concat(*stmt_list, list_copy(statement_list));
        }
    }
    if (NIL != plan.plancache_list) {
        list_free_deep(plan.plancache_list);
        plan.plancache_list = NIL;
    }
    SPI_STACK_LOG("end", querystr, NULL);
    _SPI_end_call(false);
}

void pl_validate_stmt_block_in_subtransaction(PLpgSQL_stmt_block *block, PLpgSQL_function *func,
    SQLFunctionParseInfoPtr pinfo, SPIPlanPtr *plan, List **dynexec_list)
{
    /*
     * Execute the statements in the block's body inside a sub-transaction
     */
    SubTransactionId subXid = InvalidSubTransactionId;
    /*
     * First initialize all variables declared in this block
     */
    bool savedisAllowCommitRollback = u_sess->SPI_cxt.is_allow_commit_rollback;
    bool savedIsSTP = u_sess->SPI_cxt.is_stp;
    bool savedProConfigIsSet = u_sess->SPI_cxt.is_proconfig_set;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    TransactionId oldTransactionId = SPI_get_top_transaction_id();
    bool need_rethrow = false;
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
        Assert(u_sess->catalog_cxt.overrideStack != NULL);
        u_sess->SPI_cxt.portal_stp_exception_counter--;
        entry = (OverrideStackEntry *)linitial(u_sess->catalog_cxt.overrideStack);
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
        stp_check_transaction_and_set_resource_owner(oldOwner, oldTransactionId);
        MemoryContextSwitchTo(oldcontext);
        /*
         * If AtEOSubXact_SPI() popped any SPI context of the subxact, it
         * will have left us in a disconnected state.  We need this hack
         * to return to connected state.
         */
        SPI_restore_connection();
        /* Must clean up the econtext too */
        PG_RE_THROW();
    }
    PG_END_TRY();
    subXid = GetCurrentSubTransactionId();
    /* Want to run statements inside function's memory context */
    MemoryContextSwitchTo(oldcontext);
    PG_TRY();
    {
        pl_validate_stmt_block(block, func, pinfo, plan, dynexec_list);
    }
    PG_CATCH();
    {
        stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
        u_sess->SPI_cxt.is_stp = savedIsSTP;
        u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
        /* gs_signal_handle maybe block sigusr2 when accept SIGINT */
        gs_signal_unblock_sigusr2();
        /* Save error info */
        MemoryContextSwitchTo(oldcontext);
        /* Set bInAbortTransaction to avoid core dump caused by recursive memory alloc exception */
        t_thrd.xact_cxt.bInAbortTransaction = true;
        t_thrd.xact_cxt.bInAbortTransaction = false;
        /*
         * If AtEOSubXact_SPI() popped any SPI context of the subxact, it
         * will have left us in a disconnected state.  We need this hack
         * to return to connected state.
         */
        SPI_restore_connection();
        FlushErrorState();
        need_rethrow = true;
    }
    PG_END_TRY();
    /*
     * rollback transaction unconditionally
     */
    // Get last transaction's ResourceOwner.
    stp_check_transaction_and_set_resource_owner(oldOwner, oldTransactionId);
    MemoryContextSwitchTo(oldcontext);
    /* Abort the inner transaction */
    if (GetCurrentSubTransactionId() == subXid) {
        ResetPortalCursor(subXid, InvalidOid, 0);
        RollbackAndReleaseCurrentSubTransaction();
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
    MemoryContextSwitchTo(oldcontext);
    // Get last transaction's ResourceOwner.
    stp_check_transaction_and_set_resource_owner(oldOwner, oldTransactionId);
    stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
    if (need_rethrow) {
        PG_RE_THROW();
    }
}

static void exec_savepoint_rollback(PLpgSQL_execstate *estate, const char *spName)
{
    SPI_savepoint_rollback(spName);
    stp_cleanup_subxact_resowner(estate->stack_entry_start);

    plpgsql_create_econtext(estate);

    /* if rollback to savepoint outside STP, re-link stmt's top portal with transaction. */
    if (u_sess->plsql_cxt.stp_savepoint_cnt == 0 && spName != NULL) {
        ResourceOwnerNewParent(
            t_thrd.utils_cxt.STPSavedResourceOwner, t_thrd.utils_cxt.CurrentResourceOwner);
    }
}

static int exec_stmt_savepoint(PLpgSQL_execstate *estate, PLpgSQL_stmt* stmt)
{
    PLpgSQL_stmt_savepoint *spstmt = (PLpgSQL_stmt_savepoint*)stmt;

    SPI_stp_transaction_check(estate->readonly_func, true);

    MemoryContext oldcontext = CurrentMemoryContext;

    /* recording stmt's Top Portal ResourceOwner before any subtransaction. */
    if (u_sess->SPI_cxt.portal_stp_exception_counter == 0 && u_sess->plsql_cxt.stp_savepoint_cnt == 0) {
        t_thrd.utils_cxt.STPSavedResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    }
#ifndef ENABLE_MULTIPLE_NODES
    /* implicit cursor attribute variable should reset when savepoint */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && COMPAT_CURSOR) {
        reset_implicit_cursor_attr(estate);
    }
#endif

    switch (spstmt->opType) {
        case PLPGSQL_SAVEPOINT_CREATE:
            SPI_savepoint_create(spstmt->spName);
            plpgsql_create_econtext(estate);
            break;
        case PLPGSQL_SAVEPOINT_ROLLBACKTO:
            exec_savepoint_rollback(estate, spstmt->spName);
            break;
        case PLPGSQL_SAVEPOINT_RELEASE:
            SPI_savepoint_release(spstmt->spName);
            stp_cleanup_subxact_resowner(estate->stack_entry_start);
            /*
             * Revert to outer eval_econtext.  (The inner one was
             * automatically cleaned up during subxact exit.)
             */
            estate->eval_econtext = u_sess->plsql_cxt.simple_econtext_stack->stack_econtext;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("Unknown plpgsql savepoint operate type: %d", spstmt->opType)));
            break;
    }

    /*
     * As the COMMIT/ROLLBACK/RollbackTo in STP appear soon afterwards, subtransaction
     * memory context would be destoryed at once. Objects, which would be visited after
     * subtransaction finish, shouldn't be allocated from transaction memory context.
     * Hence, it is necessary to switch to the stmt top portal's memory context here.
     */
    MemoryContextSwitchTo(oldcontext);

    return PLPGSQL_RC_OK;
}

int plpgsql_estate_adddatum(PLpgSQL_execstate* estate, PLpgSQL_datum* newm)
{
    /* resize if necessary */
    if (newm->ispkg) {
        PLpgSQL_var* var = (PLpgSQL_var*)newm;
        if (var->pkg->ndatums == var->pkg->datums_alloc) {
            var->pkg->datums_alloc *= 2;
            var->pkg->datums = (PLpgSQL_datum**)repalloc(
                var->pkg->datums, sizeof(PLpgSQL_datum*) * var->pkg->datums_alloc);
            var->pkg->datum_need_free = (bool*)repalloc(
                var->pkg->datum_need_free, sizeof(bool) * var->pkg->datums_alloc);
        }
        newm->dno = var->pkg->ndatums;
        var->pkg->datums[newm->dno] = newm;
        var->pkg->datum_need_free[newm->dno] = true;
        var->pkg->ndatums++;
    } else {
        if (estate->datums_alloc == estate->ndatums) {
            estate->datums_alloc *= 2;
            estate->datums = (PLpgSQL_datum**)repalloc(
                estate->datums, sizeof(PLpgSQL_datum*) * estate->datums_alloc);
        }
        newm->dno = estate->ndatums;
        estate->datums[newm->dno] = newm;
        estate->ndatums++;
    }
    return newm->dno;
}

PLpgSQL_var* copyPlpgsqlVar(PLpgSQL_var* src)
{
    if (src == NULL) {
        return NULL;
    }
    PLpgSQL_var* dest = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
    dest->dtype = src->dtype;
    dest->dno = src->dno;
    dest->ispkg = src->ispkg;
    dest->refname = pstrdup(src->refname);
    dest->lineno = src->lineno;
    dest->datatype = copyPLpgsqlType(src->datatype);
    dest->isconst = src->isconst;
    dest->notnull = src->notnull;
    dest->default_val = NULL;
    dest->cursor_explicit_expr = NULL;
    dest->value = datumCopy(src->value, dest->datatype->typbyval, dest->datatype->typlen);
    dest->isnull = src->isnull;
    dest->freeval = src->freeval;
    dest->is_cursor_var = src->is_cursor_var;
    dest->is_cursor_open = src->is_cursor_open;
    dest->pkg_name = NIL;
    dest->pkg = NULL;
    dest->tableOfIndexType = src->tableOfIndexType;
    dest->tableOfIndex = copyTableOfIndex(src->tableOfIndex);
    dest->nest_table = NULL;
    dest->nest_layers = src->nest_layers;
    return dest;
}

/* The copy parameters are all references from exec_get_datum_type() */
PLpgSQL_datum* deepCopyPlpgsqlDatum(PLpgSQL_datum* datum)
{
    PLpgSQL_datum* result = NULL;
    errno_t errorno = EOK;

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* newm =  copyPlpgsqlVar((PLpgSQL_var*)datum);
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* newm = copyPLpgsqlRec((PLpgSQL_rec*)datum);
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_RECFIELD: {
            PLpgSQL_recfield* newm = copyPLpgsqlRecfield((PLpgSQL_recfield*)datum);
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_EXPR: {
            PLpgSQL_expr* newm = (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
            errorno = memcpy_s(newm, sizeof(PLpgSQL_expr), datum, sizeof(PLpgSQL_expr));
            securec_check(errorno, "\0", "\0");
            newm->query = NULL;
            newm->plan = NULL;
            newm->paramnos = NULL;
            newm->func = NULL;
            newm->ns = NULL;
            newm->expr_simple_expr = NULL;
            newm->expr_simple_state = NULL;
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_RECORD: {
            PLpgSQL_row* newm = copyPLpgsqlRow((PLpgSQL_row*)datum);
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_RECORD_TYPE: {
            PLpgSQL_rec_type* newm = (PLpgSQL_rec_type*)palloc0(sizeof(PLpgSQL_rec_type));
            errorno = memcpy_s(newm, sizeof(PLpgSQL_rec_type), datum, sizeof(PLpgSQL_rec_type));
            securec_check(errorno, "\0", "\0");
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_ARRAYELEM:{
            PLpgSQL_arrayelem* newm = (PLpgSQL_arrayelem*)palloc0(sizeof(PLpgSQL_arrayelem));
            errorno = memcpy_s(newm, sizeof(PLpgSQL_arrayelem), datum, sizeof(PLpgSQL_arrayelem));
            securec_check(errorno, "\0", "\0");
            newm->subscript = NULL;
            result = (PLpgSQL_datum*)newm;
            break;
        }

        case PLPGSQL_DTYPE_TABLEELEM: {
            PLpgSQL_tableelem* newm = (PLpgSQL_tableelem*)palloc0(sizeof(PLpgSQL_tableelem));
            errorno = memcpy_s(newm, sizeof(PLpgSQL_tableelem), datum, sizeof(PLpgSQL_tableelem));
            securec_check(errorno, "\0", "\0");
            newm->subscript = NULL;
            result = (PLpgSQL_datum*)newm;
            break;
        }
        case PLPGSQL_NSTYPE_PROC:
        case PLPGSQL_NSTYPE_UNKNOWN:
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

static PLpgSQL_type* copyPLpgsqlType(PLpgSQL_type* src)
{
    if (src == NULL) {
        return NULL;
    }
    PLpgSQL_type* dest = (PLpgSQL_type*)palloc0(sizeof(PLpgSQL_type));
    dest->dtype = src->dtype;
    dest->dno = src->dno;
    dest->ispkg = src->ispkg;
    dest->typname = pstrdup(src->typname);
    dest->typoid = src->typoid;
    dest->ttype = src->ttype;
    dest->collectionType = src->collectionType;
    dest->tableOfIndexType = src->tableOfIndexType;
    dest->typlen = src->typlen;
    dest->typbyval = src->typbyval;
    dest->typrelid = src->typrelid;
    dest->typioparam = src->typioparam;
    dest->collation = src->collation;
    FmgrInfo* info = &dest->typinput;
    info = NULL;
    dest->atttypmod = src->atttypmod;
    dest->typnamespace = src->typnamespace;
    dest->collectionType = src->collectionType;
    dest->tableOfIndexType = src->tableOfIndexType;
    dest->cursorCompositeOid = src->cursorCompositeOid;
    return dest;
}

static PLpgSQL_row* copyPLpgsqlRow(PLpgSQL_row* src)
{
    PLpgSQL_row* dest =  (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    if (src->rowtupdesc == NULL) {
        dest->rowtupdesc = NULL;
    } else {
        dest->rowtupdesc = CreateTupleDescCopy(src->rowtupdesc);
    }
    dest->dtype = src->dtype;
    dest->dno = src->dno;
    dest->ispkg = src->ispkg;
    dest->intodatums = NULL;
    dest->refname = pstrdup(src->refname);
    dest->nfields = src->nfields;
    dest->fieldnames = (char**)palloc0(sizeof(char*) * dest->nfields);
    for (int i = 0; i < dest->nfields ; i++) {
        dest->fieldnames[i] = pstrdup(src->fieldnames[i]);
    }
    dest->varnos = (int*)palloc(sizeof(int) * dest->nfields);
    for (int i = 0; i < dest->nfields ; i++) {
        dest->varnos[i] = src->varnos[i];
    }
    dest->default_val = NULL;
    dest->recordVarTypOid = src->recordVarTypOid;
    return dest;
}

/* !!! Constraints tup is not copied !!! */
static PLpgSQL_rec* copyPLpgsqlRec(PLpgSQL_rec* src)
{
    PLpgSQL_rec* dest = (PLpgSQL_rec*)palloc0(sizeof(PLpgSQL_rec));
    dest->dtype = src->dtype;
    dest->dno = src->dno;
    dest->ispkg = src->ispkg;
    dest->refname = pstrdup(src->refname);
    dest->tup = NULL;
    dest->lineno = src->lineno;
    if (src->tupdesc == NULL) {
        dest->tupdesc = NULL;
    } else {
        dest->tupdesc = CreateTupleDescCopy(src->tupdesc);
    }
    dest->freetup = src->freetup;
    dest->freetupdesc = src->freetupdesc;
    dest->pkg_name = NULL;
    dest->pkg = NULL;
    return dest;
}

static PLpgSQL_recfield* copyPLpgsqlRecfield(PLpgSQL_recfield* src)
{
    PLpgSQL_recfield* dest = (PLpgSQL_recfield*)palloc0(sizeof(PLpgSQL_recfield));
    dest->dtype = src->dtype;
    dest->dno = src->dno;
    dest->ispkg = src->ispkg;
    dest->fieldname = pstrdup(src->fieldname);
    dest->recparentno = src->recparentno;
    return dest;
}

void CheckCurrCompileDependOnPackage(Oid pkgOid)
{
    /* not compile, just return */
    if (u_sess->plsql_cxt.curr_compile_context == NULL) {
        return;
    }

    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (curr_compile->plpgsql_curr_compile_package != NULL) {
        /* comile thi package now, report error */
        if (pkgOid == curr_compile->plpgsql_curr_compile_package->pkg_oid) {
            ReportCompileConcurrentError(curr_compile->plpgsql_curr_compile_package->pkg_signature, true);
        }
        ListCell* l;
        PLpgSQL_function* package_func = NULL;
        /* check curr compile package's func if depend on this package */
        foreach(l, curr_compile->plpgsql_curr_compile_package->proc_compiled_list) {
            package_func = (PLpgSQL_function*)lfirst(l);
            List *invalItems = package_func->invalItems;
            ListCell *lc = NULL;
            FuncInvalItem* item = NULL;
            foreach(lc, invalItems) {
                item = (FuncInvalItem*)lfirst(lc);
                if (item->objId == pkgOid) {
                    ReportCompileConcurrentError(package_func->fn_signature, false);
                }
            }
        }
    }

    /* check curr compile function */
    if (curr_compile->plpgsql_curr_compile != NULL) {
        /* check the function belong to this package? */
        if (curr_compile->plpgsql_curr_compile->pkg_oid == pkgOid) {
            ReportCompileConcurrentError(curr_compile->plpgsql_curr_compile->fn_signature, false);
        }

        /* check the function depend on this package? */
        List *invalItems = curr_compile->plpgsql_curr_compile->invalItems;
        ListCell *lc = NULL;
        FuncInvalItem* item = NULL;
        foreach(lc, invalItems) {
            item = (FuncInvalItem*)lfirst(lc);
            if (item->objId == pkgOid) {
                ReportCompileConcurrentError(curr_compile->plpgsql_curr_compile->fn_signature, false);
            }
        }
    }
}

void ReportCompileConcurrentError(const char* objName, bool isPackage)
{
    const char *className = isPackage? "package" : "procedure";
    ereport(ERROR,
        (errmodule(MOD_PLSQL), errcode(ERRCODE_PLPGSQL_ERROR),
         errmsg("concurrent error when compile package or procedure."),
         errdetail("when compile %s \"%s\", it has been invalidated by other session.",
             className, objName),
         errcause("excessive concurrency"),
         erraction("reduce concurrency and retry")));
}

/*
 * check the assign target if valid
 * now, only check the assign target can not be
 * autonomous procedure out ref_cursor arg
 */
#ifndef ENABLE_MULTIPLE_NODES
static void CheckAssignTarget(PLpgSQL_execstate* estate, int dno)
{
    /* only conside autonomous transaction procedure */
    if (u_sess->is_autonomous_session != true || u_sess->SPI_cxt._connected != 0) {
        return;
    }

    /* only conside ref cursor var */
    PLpgSQL_datum* target = estate->datums[dno];
    if (target->dtype != PLPGSQL_DTYPE_VAR) {
        return;
    }

    PLpgSQL_var* targetVar = (PLpgSQL_var*)target;
    if (targetVar->datatype->typoid != REFCURSOROID) {
        return;
    }

    if (IsAutoOutParam(estate, NULL, dno)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmodule(MOD_PLSQL),
            errmsg("autonomous procedure out ref cursor parameter can not be assigned"),
            errdetail("procedure \"%s\" out parameter \"%s\" not support to be assigned value",
                estate->func->fn_signature, targetVar->varname == NULL ? targetVar->refname : targetVar->varname),
            errcause("feature not supported"),
            erraction("use open cursor instead of assign value")));
    }
}
#endif
