/* -------------------------------------------------------------------------
 *
 * functions.cpp
 *	  Execution of SQL-language functions
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/functions.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "auditfuncs.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "client_logic/client_logic_proc.h"
#include "executor/functions.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "gs_ledger/ledger_utils.h"

#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#endif

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "commands/prepare.h"
#endif

#include "securec.h"
#include "client_logic/cache.h"
#include "catalog/gs_encrypted_columns.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc_fn.h"

/*
 * Specialized DestReceiver for collecting query output in a SQL function
 */
typedef struct {
    DestReceiver pub;        /* publicly-known function pointers */
    Tuplestorestate* tstore; /* where to put result tuples */
    MemoryContext cxt;       /* context containing tstore */
    JunkFilter* filter;      /* filter to convert tuple type */
} DR_sqlfunction;

/*
 * We have an execution_state record for each query in a function.	Each
 * record contains a plantree for its query.  If the query is currently in
 * F_EXEC_RUN state then there's a QueryDesc too.
 *
 * The "next" fields chain together all the execution_state records generated
 * from a single original parsetree.  (There will only be more than one in
 * case of rule expansion of the original parsetree.)
 */
typedef enum { F_EXEC_START, F_EXEC_RUN, F_EXEC_DONE } ExecStatus;

typedef struct execution_state {
    struct execution_state* next;
    ExecStatus status;
    bool setsResult; /* true if this query produces func's result */
    bool lazyEval;   /* true if should fetch one row at a time */
    Node* stmt;      /* PlannedStmt or utility statement */
    QueryDesc* qd;   /* null unless status == RUN */
} execution_state;

/*
 * An SQLFunctionCache record is built during the first call,
 * and linked to from the fn_extra field of the FmgrInfo struct.
 *
 * Note that currently this has only the lifespan of the calling query.
 * Someday we should rewrite this code to use plancache.c to save parse/plan
 * results for longer than that.
 *
 * Physically, though, the data has the lifespan of the FmgrInfo that's used
 * to call the function, and there are cases (particularly with indexes)
 * where the FmgrInfo might survive across transactions.  We cannot assume
 * that the parse/plan trees are good for longer than the (sub)transaction in
 * which parsing was done, so we must mark the record with the LXID/subxid of
 * its creation time, and regenerate everything if that's obsolete.  To avoid
 * memory leakage when we do have to regenerate things, all the data is kept
 * in a sub-context of the FmgrInfo's fn_mcxt.
 */
typedef struct {
    char* fname; /* function name (for error msgs) */
    char* src;   /* function body text (for error msgs) */

    SQLFunctionParseInfoPtr pinfo; /* data for parser callback hooks */

    Oid rettype;        /* actual return type */
    int32 rettype_orig = -1;        /* original return type */
    int16 typlen;       /* length of the return type */
    bool typbyval;      /* true if return type is pass by value */
    bool returnsSet;    /* true if returning multiple rows */
    bool returnsTuple;  /* true if returning whole tuple result */
    bool shutdown_reg;  /* true if registered shutdown callback */
    bool readonly_func; /* true to run in "read only" mode */
    bool lazyEval;      /* true if using lazyEval for result query */

    ParamListInfo paramLI; /* Param list representing current args */

    Tuplestorestate* tstore; /* where we accumulate result tuples */

    JunkFilter* junkFilter; /* will be NULL if function returns VOID */

    /*
     * func_state is a List of execution_state records, each of which is the
     * first for its original parsetree, with any additional records chained
     * to it via the "next" fields.  This sublist structure is needed to keep
     * track of where the original query boundaries are.
     */
    List* func_state;

    MemoryContext fcontext; /* memory context holding this struct and all subsidiary data */
    LocalTransactionId lxid; /* lxid in which cache was made */
    SubTransactionId subxid; /* subxid in which cache was made */
} SQLFunctionCache;

typedef SQLFunctionCache* SQLFunctionCachePtr;

/*
 * Data structure needed by the parser callback hooks to resolve parameter
 * references during parsing of a SQL function's body.  This is separate from
 * SQLFunctionCache since we sometimes do parsing separately from execution.
 */
typedef struct SQLFunctionParseInfo {
    char* fname;     /* function's name */
    int nargs;       /* number of input arguments */
    Oid* argtypes;   /* resolved types of input arguments */
    char** argnames; /* names of input arguments; NULL if none */
    /* Note that argnames[i] can be NULL, if some args are unnamed */
    Oid collation; /* function's input collation, if known */
    Oid* replaced_argtypes; /* for managed columns replaced argtypes */
    Oid* replaced_args_cl_oids; /* for managed colums - managed column oid for replaced column */
} SQLFunctionParseInfo;

/* non-export function prototypes */
static Node* sql_fn_param_ref(ParseState* p_state, ParamRef* p_ref);
static Node* sql_fn_post_column_ref(ParseState* p_state, ColumnRef* c_ref, Node* var);
static Node* sql_fn_make_param(SQLFunctionParseInfoPtr p_info, int param_no, int location);
static Node* sql_fn_resolve_param_name(SQLFunctionParseInfoPtr p_info, const char* param_name, int location);
static List* init_execution_state(List* query_tree_list, SQLFunctionCachePtr fcache, bool lazy_eval_ok);
static void init_sql_fcache(FmgrInfo* finfo, Oid collation, bool lazy_eval_ok);
static void postquel_start(execution_state* es, SQLFunctionCachePtr fcache);
static bool postquel_getnext(execution_state* es, SQLFunctionCachePtr fcache);
static void postquel_end(execution_state* es);
static void postquel_sub_params(SQLFunctionCachePtr fcache, FunctionCallInfo fcinfo);
static Datum postquel_get_single_result(
    TupleTableSlot* slot, FunctionCallInfo fcinfo, SQLFunctionCachePtr fcache, MemoryContext result_context);
static void sql_exec_error_callback(void* arg);
static void ShutdownSQLFunction(Datum arg);
static void sqlfunction_startup(DestReceiver* self, int operation, TupleDesc type_info);
static void sqlfunction_receive(TupleTableSlot* slot, DestReceiver* self);
static void sqlfunction_shutdown(DestReceiver* self);
static void sqlfunction_destroy(DestReceiver* self);

/*
 * Prepare the SQLFunctionParseInfo struct for parsing a SQL function body
 *
 * This includes resolving actual types of polymorphic arguments.
 *
 * call_expr can be passed as NULL, but then we will fail if there are any
 * polymorphic arguments.
 */
SQLFunctionParseInfoPtr prepare_sql_fn_parse_info(HeapTuple procedure_tuple, Node* call_expr, Oid input_collation)
{
    SQLFunctionParseInfoPtr p_info;
    Form_pg_proc procedure_struct = (Form_pg_proc)GETSTRUCT(procedure_tuple);
    int nargs;

    p_info = (SQLFunctionParseInfoPtr)palloc0(sizeof(SQLFunctionParseInfo));

    /* Function's name (only) can be used to qualify argument names */
    p_info->fname = pstrdup(NameStr(procedure_struct->proname));

    /* Save the function's input collation */
    p_info->collation = input_collation;

    /*
     * Copy input argument types from the pg_proc entry, then resolve any
     * polymorphic types.
     */
    p_info->nargs = nargs = procedure_struct->pronargs;
    if (nargs <= 0) {
        p_info->argnames = NULL;
        return p_info;
    }

    Oid* arg_oid_vect = NULL;
    int arg_num;
    errno_t rc = EOK;

    arg_oid_vect = (Oid*)palloc(nargs * sizeof(Oid));

    oidvector* proargs = ProcedureGetArgTypes(procedure_tuple);
    rc = memcpy_s(arg_oid_vect, nargs * sizeof(Oid), proargs->values, nargs * sizeof(Oid));
    securec_check(rc, "\0", "\0");

    for (arg_num = 0; arg_num < nargs; arg_num++) {
        Oid arg_type = arg_oid_vect[arg_num];

        if (IsPolymorphicType(arg_type)) {
            arg_type = get_call_expr_argtype(call_expr, arg_num);
            if (arg_type == InvalidOid) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("could not determine actual type of argument declared %s",
                            format_type_be(arg_oid_vect[arg_num]))));
            }
            arg_oid_vect[arg_num] = arg_type;
        }
    }

    p_info->argtypes = arg_oid_vect;

    /*
     * Collect names of arguments, too, if any
     */
    Datum pro_arg_names;
    Datum pro_arg_modes;
    int n_arg_names;
    bool is_null = false;

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        pro_arg_names = SysCacheGetAttr(PROCNAMEARGSNSP, procedure_tuple, Anum_pg_proc_proargnames, &is_null);
    } else {
        pro_arg_names = SysCacheGetAttr(PROCALLARGS, procedure_tuple, Anum_pg_proc_proargnames, &is_null);
    }
#else
    pro_arg_names = SysCacheGetAttr(PROCNAMEARGSNSP, procedure_tuple, Anum_pg_proc_proargnames, &is_null);
#endif
    if (is_null) {
        pro_arg_names = PointerGetDatum(NULL); /* just to be sure */
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        pro_arg_modes = SysCacheGetAttr(PROCNAMEARGSNSP, procedure_tuple, Anum_pg_proc_proargmodes, &is_null);
    } else {
        pro_arg_modes = SysCacheGetAttr(PROCALLARGS, procedure_tuple, Anum_pg_proc_proargmodes, &is_null);
    }
#else
    pro_arg_modes = SysCacheGetAttr(PROCNAMEARGSNSP, procedure_tuple, Anum_pg_proc_proargmodes, &is_null);
#endif
    if (is_null) {
        pro_arg_modes = PointerGetDatum(NULL); /* just to be sure */
    }
    
    n_arg_names = get_func_input_arg_names(pro_arg_names, pro_arg_modes, &p_info->argnames);
    /* Paranoia: ignore the result if too few array entries */
    if (n_arg_names < nargs) {
        p_info->argnames = NULL;
    }
    p_info->replaced_argtypes = (Oid*)palloc0(nargs * sizeof(Oid));
    p_info->replaced_args_cl_oids = (Oid*)palloc0(nargs * sizeof(Oid));
    return p_info;
}

/*
 * Parser setup hook for parsing a SQL function body.
 */
void sql_fn_parser_setup(struct ParseState* p_state, SQLFunctionParseInfoPtr p_info)
{
    p_state->p_pre_columnref_hook = NULL;
    p_state->p_post_columnref_hook = sql_fn_post_column_ref;
    p_state->p_paramref_hook = sql_fn_param_ref;
    /* no need to use p_coerce_param_hook */
    p_state->p_ref_hook_state = (void*)p_info;
}

/*
 * sql_fn_post_column_ref		parser callback for ColumnRefs
 */
static Node* sql_fn_post_column_ref(ParseState* p_state, ColumnRef* c_ref, Node* var)
{
    SQLFunctionParseInfoPtr p_info = (SQLFunctionParseInfoPtr)p_state->p_ref_hook_state;
    int n_names;
    Node* field1 = NULL;
    Node* sub_field = NULL;
    const char* name1 = NULL;
    const char* name2 = NULL;
    Node* param = NULL;

    /*
     * Never override a table-column reference.  This corresponds to
     * considering the parameter names to appear in a scope outside the
     * individual SQL commands, which is what we want.
     */
    if (var != NULL)
        return NULL;

    /* ----------
     * The allowed syntaxes are:
     *
     * A		A = parameter name
     * A.B		A = function name, B = parameter name
     *			OR: A = record-typed parameter name, B = field name
     *			(the first possibility takes precedence)
     * A.B.C	A = function name, B = record-typed parameter name,
     *			C = field name
     * A.*		Whole-row reference to composite parameter A.
     * A.B.*	Same, with A = function name, B = parameter name
     *
     * Here, it's sufficient to ignore the "*" in the last two cases --- the
     * main parser will take care of expanding the whole-row reference.
     * ----------
     */
    n_names = list_length(c_ref->fields);

    if (n_names > 3)
        return NULL;

    if (IsA(llast(c_ref->fields), A_Star))
        n_names--;

    field1 = (Node*)linitial(c_ref->fields);
    Assert(IsA(field1, String));
    name1 = strVal(field1);
    if (n_names > 1) {
        sub_field = (Node*)lsecond(c_ref->fields);
        Assert(IsA(sub_field, String));
        name2 = strVal(sub_field);
    }

    if (n_names == 3) {
        /*
         * Three-part name: if the first part doesn't match the function name,
         * we can fail immediately. Otherwise, look up the second part, and
         * take the third part to be a field reference.
         */
        if (strcmp(name1, p_info->fname) != 0)
            return NULL;

        param = sql_fn_resolve_param_name(p_info, name2, c_ref->location);

        sub_field = (Node*)lthird(c_ref->fields);
        Assert(IsA(sub_field, String));
    } else if (n_names == 2 && strcmp(name1, p_info->fname) == 0) {
        /*
         * Two-part name with first part matching function name: first see if
         * second part matches any parameter name.
         */
        param = sql_fn_resolve_param_name(p_info, name2, c_ref->location);
        if (param != NULL) {
            /* Yes, so this is a parameter reference, no subfield */
            sub_field = NULL;
        } else {
            /* No, so try to match as parameter name and subfield */
            param = sql_fn_resolve_param_name(p_info, name1, c_ref->location);
        }
    } else {
        /* Single name, or parameter name followed by subfield */
        param = sql_fn_resolve_param_name(p_info, name1, c_ref->location);
    }

    if (param == NULL)
        return NULL; /* No match */

    if (sub_field != NULL) {
        /*
         * Must be a reference to a field of a composite parameter; otherwise
         * ParseFuncOrColumn will return NULL, and we'll fail back at the
         * caller.
         */
        param = ParseFuncOrColumn(p_state, list_make1(sub_field), list_make1(param), NULL, c_ref->location);
    }

    return param;
}

/*
 * sql_fn_param_ref		parser callback for ParamRefs ($n symbols)
 */
static Node* sql_fn_param_ref(ParseState* p_state, ParamRef* p_ref)
{
    SQLFunctionParseInfoPtr p_info = (SQLFunctionParseInfoPtr)p_state->p_ref_hook_state;
    int param_no = p_ref->number;

    /* Check parameter number is valid */
    if (param_no <= 0 || param_no > p_info->nargs)
        return NULL; /* unknown parameter number */

    return sql_fn_make_param(p_info, param_no, p_ref->location);
}

/*
 * sql_fn_make_param		construct a Param node for the given paramno
 */
static Node* sql_fn_make_param(SQLFunctionParseInfoPtr p_info, int param_no, int location)
{
    Param* param = NULL;

    param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = param_no;
    param->paramtype = p_info->argtypes[param_no - 1];
    param->paramtypmod = -1;
    param->paramcollid = get_typcollation(param->paramtype);
    param->location = location;
    param->tableOfIndexType = InvalidOid;

    /*
     * If we have a function input collation, allow it to override the
     * type-derived collation for parameter symbols.  (XXX perhaps this should
     * not happen if the type collation is not default?)
     */
    if (OidIsValid(p_info->collation) && OidIsValid(param->paramcollid))
        param->paramcollid = p_info->collation;

    return (Node*)param;
}

/*
 * Search for a function parameter of the given name; if there is one,
 * construct and return a Param node for it.  If not, return NULL.
 * Helper function for sql_fn_post_column_ref.
 */
static Node* sql_fn_resolve_param_name(SQLFunctionParseInfoPtr p_info, const char* param_name, int location)
{
    int i;

    if (p_info->argnames == NULL)
        return NULL;

    if (param_name == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("paramname should not be NULL")));
    }

    for (i = 0; i < p_info->nargs; i++) {
        if (p_info->argnames[i] && strcmp(p_info->argnames[i], param_name) == 0)
            return sql_fn_make_param(p_info, i + 1, location);
    }

    return NULL;
}

/*
 * Set up the per-query execution_state records for a SQL function.
 *
 * The input is a List of Lists of parsed and rewritten, but not planned,
 * querytrees.	The sublist structure denotes the original query boundaries.
 */
static List* init_execution_state(List* query_tree_list, SQLFunctionCachePtr fcache, bool lazy_eval_ok)
{
    List* es_list = NIL;
    execution_state* last_tages = NULL;
    ListCell* lc1 = NULL;

    foreach (lc1, query_tree_list) {
        List* qt_list = (List*)lfirst(lc1);
        execution_state* first_es = NULL;
        execution_state* prev_es = NULL;
        ListCell* lc2 = NULL;

        foreach (lc2, qt_list) {
            Query* query_tree = (Query*)lfirst(lc2);
            Node* stmt = NULL;
            execution_state* new_es = NULL;

            Assert(IsA(query_tree, Query));

            /* Plan the query if needed */
            if (query_tree->commandType == CMD_UTILITY)
                stmt = query_tree->utilityStmt;
            else {
                int nest_level = apply_set_hint(query_tree);
                PG_TRY();
                {
                    stmt = (Node*)pg_plan_query(query_tree, 0, NULL);
                }
                PG_CATCH();
                {
                    recover_set_hint(nest_level);
                    PG_RE_THROW();
                }
                PG_END_TRY();

                recover_set_hint(nest_level);
            }

            /* Precheck all commands for validity in a function */
            if (IsA(stmt, TransactionStmt))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        /* translator: %s is a SQL statement name */
                        errmsg("%s is not allowed in a SQL function", CreateCommandTag(stmt))));

            if (fcache->readonly_func && !CommandIsReadOnly(stmt))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        /* translator: %s is a SQL statement name */
                        errmsg("%s is not allowed in a non-volatile function", CreateCommandTag(stmt))));

            /* OK, build the execution_state for this query */
            new_es = (execution_state*)palloc(sizeof(execution_state));
            if (prev_es != NULL)
                prev_es->next = new_es;
            else
                first_es = new_es;

            new_es->next = NULL;
            new_es->status = F_EXEC_START;
            new_es->setsResult = false; /* might change below */
            new_es->lazyEval = false;   /* might change below */
            new_es->stmt = stmt;
            new_es->qd = NULL;

            if (query_tree->canSetTag)
                last_tages = new_es;

            prev_es = new_es;
        }

        es_list = lappend(es_list, first_es);
    }

    /*
     * Mark the last canSetTag query as delivering the function result; then,
     * if it is a plain SELECT, mark it for lazy evaluation. If it's not a
     * SELECT we must always run it to completion.
     *
     * Note: at some point we might add additional criteria for whether to use
     * lazy eval.  However, we should prefer to use it whenever the function
     * doesn't return set, since fetching more than one row is useless in that
     * case.
     *
     * Note: don't set setsResult if the function returns VOID, as evidenced
     * by not having made a junkfilter.  This ensures we'll throw away any
     * output from a utility statement that check_sql_fn_retval deemed to not
     * have output.
     */
    if (last_tages != NULL && fcache->junkFilter) {
        last_tages->setsResult = true;
        if (lazy_eval_ok && IsA(last_tages->stmt, PlannedStmt)) {
            PlannedStmt* ps = (PlannedStmt*)last_tages->stmt;

            if (ps->commandType == CMD_SELECT && ps->utilityStmt == NULL && !ps->hasModifyingCTE)
                fcache->lazyEval = last_tages->lazyEval = true;
        }
    }

    return es_list;
}

/*
 * Initialize the SQLFunctionCache for a SQL function
 */
static void init_sql_fcache(FmgrInfo* finfo, Oid collation, bool lazy_eval_ok)
{
    Oid f_oid = finfo->fn_oid;
    MemoryContext f_context;
    MemoryContext old_context;
    Oid ret_type;
    HeapTuple procedure_tuple;
    Form_pg_proc procedure_struct;
    HeapTuple gs_proc_tuple;
    Form_gs_encrypted_proc gs_proc_struct;
    SQLFunctionCachePtr fcache;
    List* raw_parsetree_list = NIL;
    List* query_tree_list = NIL;
    List* flat_query_list = NIL;
    ListCell* lc = NULL;
    Datum tmp;
    bool is_null = false;

    /*
     * Create memory context that holds all the SQLFunctionCache data.	It
     * must be a child of whatever context holds the FmgrInfo.
     */
    f_context = AllocSetContextCreate(finfo->fn_mcxt,
        "SQL function data",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    old_context = MemoryContextSwitchTo(f_context);

    /*
     * Create the struct proper, link it to fcontext and fn_extra.	Once this
     * is done, we'll be able to recover the memory after failure, even if the
     * FmgrInfo is long-lived.
     */
    fcache = (SQLFunctionCachePtr)palloc0(sizeof(SQLFunctionCache));
    fcache->fcontext = f_context;
    finfo->fn_extra = (void*)fcache;

    /*
     * get the procedure tuple corresponding to the given function Oid
     */
    procedure_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(f_oid));
    if (!HeapTupleIsValid(procedure_tuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_EXECUTOR),
                errmsg("cache lookup failed for function %u when initialize function cache.", f_oid)));
    procedure_struct = (Form_pg_proc)GETSTRUCT(procedure_tuple);

    /*
     * copy function name immediately for use by error reporting callback
     */
    fcache->fname = pstrdup(NameStr(procedure_struct->proname));

    /*
     * get the result type from the procedure tuple, and check for polymorphic
     * result type; if so, find out the actual result type.
     */
    ret_type = procedure_struct->prorettype;

    if (IsPolymorphicType(ret_type)) {
        ret_type = get_fn_expr_rettype(finfo);
        if (ret_type == InvalidOid) /* this probably should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not determine actual result type for function declared to return type %s",
                        format_type_be(procedure_struct->prorettype))));
    }

    fcache->rettype = ret_type;
    if (IsClientLogicType(ret_type)) {
        /*
         * get the client logic proc tuple corresponding to the given function Oid
         */
        gs_proc_tuple = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(f_oid));
        if (!HeapTupleIsValid(gs_proc_tuple))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmodule(MOD_EXECUTOR),
                errmsg("cache lookup failed for function %u when initialize function cache.", f_oid)));
        gs_proc_struct = (Form_gs_encrypted_proc)GETSTRUCT(gs_proc_tuple);
        fcache->rettype_orig = gs_proc_struct->prorettype_orig;
        ReleaseSysCache(gs_proc_tuple);
    }

    /* Fetch the typlen and byval info for the result type */
    get_typlenbyval(ret_type, &fcache->typlen, &fcache->typbyval);

    /* Remember whether we're returning setof something */
    fcache->returnsSet = procedure_struct->proretset;

    /* Remember if function is STABLE/IMMUTABLE */
    fcache->readonly_func = (procedure_struct->provolatile != PROVOLATILE_VOLATILE);

    /*
     * We need the actual argument types to pass to the parser.  Also make
     * sure that parameter symbols are considered to have the function's
     * resolved input collation.
     */
    fcache->pinfo = prepare_sql_fn_parse_info(procedure_tuple, finfo->fn_expr, collation);

    /*
     * And of course we need the function body text.
     */
    tmp = SysCacheGetAttr(PROCOID, procedure_tuple, Anum_pg_proc_prosrc, &is_null);
    if (is_null)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("null prosrc for function %u when we need the function body text", f_oid)));
    fcache->src = TextDatumGetCString(tmp);

    /*
     * Parse and rewrite the queries in the function text.	Use sublists to
     * keep track of the original query boundaries.  But we also build a
     * "flat" list of the rewritten queries to pass to check_sql_fn_retval.
     * This is because the last canSetTag query determines the result type
     * independently of query boundaries --- and it might not be in the last
     * sublist, for example if the last query rewrites to DO INSTEAD NOTHING.
     * (It might not be unreasonable to throw an error in such a case, but
     * this is the historical behavior and it doesn't seem worth changing.)
     *
     * Note: since parsing and planning is done in fcontext, we will generate
     * a lot of cruft that lives as long as the fcache does.  This is annoying
     * but we'll not worry about it until the module is rewritten to use
     * plancache.c.
     */
    raw_parsetree_list = pg_parse_query(fcache->src);

    query_tree_list = NIL;
    flat_query_list = NIL;
    foreach (lc, raw_parsetree_list) {
        Node* parsetree = (Node*)lfirst(lc);
        List* queryTree_sublist = NIL;

        queryTree_sublist =
            pg_analyze_and_rewrite_params(parsetree, fcache->src, (ParserSetupHook)sql_fn_parser_setup, fcache->pinfo);
        query_tree_list = lappend(query_tree_list, queryTree_sublist);
        flat_query_list = list_concat(flat_query_list, list_copy(queryTree_sublist));
    }

    /*
     * Check that the function returns the type it claims to.  Although in
     * simple cases this was already done when the function was defined, we
     * have to recheck because database objects used in the function's queries
     * might have changed type.  We'd have to do it anyway if the function had
     * any polymorphic arguments.
     *
     * Note: we set fcache->returnsTuple according to whether we are returning
     * the whole tuple result or just a single column.	In the latter case we
     * clear returnsTuple because we need not act different from the scalar
     * result case, even if it's a rowtype column.  (However, we have to force
     * lazy eval mode in that case; otherwise we'd need extra code to expand
     * the rowtype column into multiple columns, since we have no way to
     * notify the caller that it should do that.)
     *
     * check_sql_fn_retval will also construct a JunkFilter we can use to
     * coerce the returned rowtype to the desired form (unless the result type
     * is VOID, in which case there's nothing to coerce to).
     */
    fcache->returnsTuple = check_sql_fn_retval(f_oid, ret_type, flat_query_list, NULL, &fcache->junkFilter, false);

    if (fcache->returnsTuple) {
        /* Make sure output rowtype is properly blessed */
        BlessTupleDesc(fcache->junkFilter->jf_resultSlot->tts_tupleDescriptor);
    } else if (fcache->returnsSet && type_is_rowtype(fcache->rettype)) {
        /*
         * Returning rowtype as if it were scalar --- materialize won't work.
         * Right now it's sufficient to override any caller preference for
         * materialize mode, but to add more smarts in init_execution_state
         * about this, we'd probably need a three-way flag instead of bool.
         */
        lazy_eval_ok = true;
    }

    /* Finally, plan the queries */
    fcache->func_state = init_execution_state(query_tree_list, fcache, lazy_eval_ok);

    /* Mark fcache with time of creation to show it's valid */
    fcache->lxid = t_thrd.proc->lxid;
    fcache->subxid = GetCurrentSubTransactionId();

    ReleaseSysCache(procedure_tuple);

    MemoryContextSwitchTo(old_context);
}

/* Start up execution of one execution_state node */
static void postquel_start(execution_state* es, SQLFunctionCachePtr fcache)
{
    DestReceiver* dest = NULL;

    Assert(es->qd == NULL);

    /* Caller should have ensured a suitable snapshot is active */
    Assert(ActiveSnapshotSet());

    /*
     * If this query produces the function result, send its output to the
     * tuplestore; else discard any output.
     */
    if (es->setsResult) {
        DR_sqlfunction* my_state = NULL;

        dest = CreateDestReceiver(DestSQLFunction);
        /* pass down the needed info to the dest receiver routines */
        my_state = (DR_sqlfunction*)dest;
        Assert(my_state->pub.mydest == DestSQLFunction);
        my_state->tstore = fcache->tstore;
        my_state->cxt = CurrentMemoryContext;
        my_state->filter = fcache->junkFilter;
    } else {
        dest = None_Receiver;
    }

    if (IsA(es->stmt, PlannedStmt)) {
        es->qd = CreateQueryDesc(
            (PlannedStmt*)es->stmt, fcache->src, GetActiveSnapshot(), InvalidSnapshot, dest, fcache->paramLI, 0);
    } else {
        es->qd = CreateUtilityQueryDesc(es->stmt, fcache->src, GetActiveSnapshot(), dest, fcache->paramLI);
    }

    /* Utility commands don't need Executor. */
    if (es->qd->utilitystmt == NULL) {
        /*
         * In lazyEval mode, do not let the executor set up an AfterTrigger
         * context.  This is necessary not just an optimization, because we
         * mustn't exit from the function execution with a stacked
         * AfterTrigger level still active.  We are careful not to select
         * lazyEval mode for any statement that could possibly queue triggers.
         */
        int eflags;

        if (es->lazyEval) {
            eflags = EXEC_FLAG_SKIP_TRIGGERS;
        } else {
            eflags = 0; /* default run-to-completion flags */
        }
        ExecutorStart(es->qd, eflags);
    }

    es->status = F_EXEC_RUN;
}

/* Run one execution_state; either to completion or to first result row */
/* Returns true if we ran to completion */
static bool postquel_getnext(execution_state* es, SQLFunctionCachePtr fcache)
{
    bool result = false;
    /* initialize the parameters of the main statement */
    Qid stroed_proc_qid = {0, 0, 0};
    unsigned char stroed_proc_parctl_state_except = 0;
    WLMStatusTag stroed_proc_g_collect_info_status = WLM_STATUS_RESERVE;
    bool stroed_proc_is_active_statements_reset = false;
    errno_t rc = EOK;

    if (es->qd->utilitystmt) {
        /* ProcessUtility needs the PlannedStmt for DECLARE CURSOR */
        ProcessUtility((es->qd->plannedstmt ? (Node*)es->qd->plannedstmt : es->qd->utilitystmt),
            fcache->src,
            es->qd->params,
            false, /* not top level */
            es->qd->dest,
#ifdef PGXC
            false,
#endif /* PGXC */
            NULL);
        result = true; /* never stops early */
    } else {
        /* Run regular commands to completion unless lazyEval */
        long count = (es->lazyEval) ? 1L : 0L;
        bool forced_control = (t_thrd.wlm_cxt.parctl_state.simple == 1 || u_sess->wlm_cxt->is_active_statements_reset);
        if (ENABLE_WORKLOAD_CONTROL && IS_PGXC_COORDINATOR && forced_control) {
            if (!u_sess->wlm_cxt->is_active_statements_reset && !u_sess->attr.attr_resource.enable_transaction_parctl) {
                u_sess->wlm_cxt->stroedproc_rp_reserve = t_thrd.wlm_cxt.parctl_state.rp_reserve;
                u_sess->wlm_cxt->stroedproc_rp_release = t_thrd.wlm_cxt.parctl_state.rp_release;
                u_sess->wlm_cxt->stroedproc_release = t_thrd.wlm_cxt.parctl_state.release;
            }

            /* Retain the parameters of the main statement */
            if (!IsQidInvalid(&u_sess->wlm_cxt->wlm_params.qid)) {
                rc = memcpy_s(&stroed_proc_qid, sizeof(Qid), &u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid));
                securec_check(rc, "\0", "\0");
            }
            stroed_proc_parctl_state_except = t_thrd.wlm_cxt.parctl_state.except;
            stroed_proc_g_collect_info_status = t_thrd.wlm_cxt.collect_info->status;
            stroed_proc_is_active_statements_reset = u_sess->wlm_cxt->is_active_statements_reset;

            t_thrd.wlm_cxt.parctl_state.subquery = 1;
            WLMInitQueryPlan(es->qd);
            dywlm_client_manager(es->qd);
        }
        /* Check if there is any modification on ledger user table. */
        if (es->qd->operation != CMD_SELECT && g_instance.role == VDATANODE &&
            querydesc_contains_ledger_usertable(es->qd)) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                            errmsg("Permission denied to modify ledger table in SQL function.")));
        }

        ExecutorRun(es->qd, ForwardScanDirection, count);

        if (ENABLE_WORKLOAD_CONTROL && IS_PGXC_COORDINATOR && forced_control) {
            t_thrd.wlm_cxt.parctl_state.except = 0;
            if (g_instance.wlm_cxt->dynamic_workload_inited && (t_thrd.wlm_cxt.parctl_state.simple == 0)) {
                dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
            } else {
                // only release resource pool count
                if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                    (u_sess->wlm_cxt->parctl_state_exit || IsQueuedSubquery())) {
                    WLMReleaseGroupActiveStatement();
                }
            }

            WLMSetCollectInfoStatus(WLM_STATUS_FINISHED);
            t_thrd.wlm_cxt.parctl_state.subquery = 0;
            t_thrd.wlm_cxt.parctl_state.except = stroed_proc_parctl_state_except;
            t_thrd.wlm_cxt.collect_info->status = stroed_proc_g_collect_info_status;
            u_sess->wlm_cxt->is_active_statements_reset = stroed_proc_is_active_statements_reset;
            if (!IsQidInvalid(&stroed_proc_qid)) {
                rc = memcpy_s(&u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid), &stroed_proc_qid, sizeof(Qid));
                securec_check(rc, "\0", "\0");
            }

            /* restore state condition if guc para is off since it contains unreleased count */
            if (!u_sess->attr.attr_resource.enable_transaction_parctl &&
                (u_sess->wlm_cxt->reserved_in_active_statements || u_sess->wlm_cxt->reserved_in_group_statements ||
                    u_sess->wlm_cxt->reserved_in_group_statements_simple)) {
                t_thrd.wlm_cxt.parctl_state.rp_reserve = u_sess->wlm_cxt->stroedproc_rp_reserve;
                t_thrd.wlm_cxt.parctl_state.rp_release = u_sess->wlm_cxt->stroedproc_rp_release;
                t_thrd.wlm_cxt.parctl_state.release = u_sess->wlm_cxt->stroedproc_release;
            }
        }

        /*
         * If we requested run to completion OR there was no tuple returned,
         * command must be complete.
         */
        result = (count == 0L || es->qd->estate->es_processed == 0);
    }

    return result;
}

/* Shut down execution of one execution_state node */
static void postquel_end(execution_state* es)
{
    /* mark status done to ensure we don't do ExecutorEnd twice */
    es->status = F_EXEC_DONE;

    /* Utility commands don't need Executor. */
    if (es->qd->utilitystmt == NULL) {
        ExecutorFinish(es->qd);
        ExecutorEnd(es->qd);
    }

    (*es->qd->dest->rDestroy)(es->qd->dest);

    FreeQueryDesc(es->qd);
    es->qd = NULL;
}

/* Build ParamListInfo array representing current arguments */
static void postquel_sub_params(SQLFunctionCachePtr fcache, FunctionCallInfo fcinfo)
{
    int nargs = fcinfo->nargs;

    if (nargs > 0) {
        ParamListInfo param_li;
        int i;

        if (fcache->paramLI == NULL) {
            param_li = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + nargs * sizeof(ParamExternData));
            /* we have static list of params, so no hooks needed */
            param_li->paramFetch = NULL;
            param_li->paramFetchArg = NULL;
            param_li->parserSetup = NULL;
            param_li->parserSetupArg = NULL;
            param_li->params_need_process = false;
            param_li->numParams = nargs;
            fcache->paramLI = param_li;
        } else {
            param_li = fcache->paramLI;
            Assert(param_li->numParams == nargs);
        }

        for (i = 0; i < nargs; i++) {
            ParamExternData* prm = &param_li->params[i];

            prm->value = fcinfo->arg[i];
            prm->isnull = fcinfo->argnull[i];
            prm->pflags = 0;
            prm->ptype = fcache->pinfo->argtypes[i];
            prm->tabInfo = NULL;
        }
    } else {
        fcache->paramLI = NULL;
    }
}

/*
 * Extract the SQL function's value from a single result row.  This is used
 * both for scalar (non-set) functions and for each row of a lazy-eval set
 * result.
 */
static Datum postquel_get_single_result(
    TupleTableSlot* slot, FunctionCallInfo fcinfo, SQLFunctionCachePtr fcache, MemoryContext result_context)
{
    Assert(slot != NULL);

    Datum value;
    MemoryContext old_context;

    /*
     * Set up to return the function value.  For pass-by-reference datatypes,
     * be sure to allocate the result in resultcontext, not the current memory
     * context (which has query lifespan).	We can't leave the data in the
     * TupleTableSlot because we intend to clear the slot before returning.
     */
    old_context = MemoryContextSwitchTo(result_context);

    if (fcache->returnsTuple) {
        /* We must return the whole tuple as a Datum. */
        fcinfo->isnull = false;
        value = ExecFetchSlotTupleDatum(slot);
        value = datumCopy(value, fcache->typbyval, fcache->typlen);
    } else {
        /*
         * Returning a scalar, which we have to extract from the first column
         * of the SELECT result, and then copy into result context if needed.
         */
        /* Get the Table Accessor Method*/
        Assert(slot->tts_tupleDescriptor != NULL);
        value = tableam_tslot_getattr(slot, 1, &(fcinfo->isnull));

        if (!fcinfo->isnull)
            value = datumCopy(value, fcache->typbyval, fcache->typlen);
    }

    MemoryContextSwitchTo(old_context);

    return value;
}

static void auditExecSQLFunction(Oid fn_oid, SQLFunctionCache* func, AuditResult result)
{
    char details[PGAUDIT_MAXLENGTH];

    Assert(func != NULL);
    if (fn_oid < FirstNormalObjectId)
        return;

    int ret = snprintf_s(details, sizeof(details), sizeof(details) - 1, "Execute SQL function(%s). ", func->fname);
    securec_check_ss(ret, "", "");
    audit_report(AUDIT_FUNCTION_EXEC, result, func->fname, details);
}

/*
 * fmgr_sql: function call manager for SQL functions
 */
Datum fmgr_sql(PG_FUNCTION_ARGS)
{
    SQLFunctionCachePtr fcache = NULL;
    ErrorContextCallback sql_err_context;
    MemoryContext old_context;
    bool random_access = false;
    bool lazy_eval_ok = false;
    bool is_first = false;
    bool pushed_snapshot = false;
    execution_state* es = NULL;
    TupleTableSlot* slot = NULL;
    Datum result;
    List* es_list = NIL;
    ListCell* eslc = NULL;
    bool old_running_in_fmgr = t_thrd.codegen_cxt.g_runningInFmgr;
    t_thrd.codegen_cxt.g_runningInFmgr = true;
    bool need_snapshot = !ActiveSnapshotSet();

#ifdef ENABLE_MULTIPLE_NODES
    bool outer_is_stream = false;
    bool outer_is_stream_support = false;

    if (IS_PGXC_COORDINATOR) {
        outer_is_stream = u_sess->opt_cxt.is_stream;
        outer_is_stream_support = u_sess->opt_cxt.is_stream_support;

        u_sess->opt_cxt.is_stream = true;
        u_sess->opt_cxt.is_stream_support = true;
    }
#else
    int outerDop = u_sess->opt_cxt.query_dop;
    u_sess->opt_cxt.query_dop = 1;
#endif

    /*
     * Setup error traceback support for ereport()
     */
    sql_err_context.callback = sql_exec_error_callback;
    sql_err_context.arg = fcinfo->flinfo;
    sql_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &sql_err_context;

    /* Check call context */
    if (fcinfo->flinfo->fn_retset) {
        ReturnSetInfo* rsi = (ReturnSetInfo*)fcinfo->resultinfo;

        /*
         * For simplicity, we require callers to support both set eval modes.
         * There are cases where we must use one or must use the other, and
         * it's not really worthwhile to postpone the check till we know. But
         * note we do not require caller to provide an expectedDesc.
         */
        if (rsi == NULL || !IsA(rsi, ReturnSetInfo) || (rsi->allowedModes & SFRM_ValuePerCall) == 0 ||
            (rsi->allowedModes & SFRM_Materialize) == 0)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("set-valued function called in context that cannot accept a set")));
        random_access = rsi->allowedModes & SFRM_Materialize_Random;
        lazy_eval_ok = !(rsi->allowedModes & SFRM_Materialize_Preferred);
    } else {
        random_access = false;
        lazy_eval_ok = true;
    }

    if (need_snapshot) {
        PushActiveSnapshot(GetTransactionSnapshot());
    }
    /*
     * Initialize fcache (build plans) if first time through; or re-initialize
     * if the cache is stale.
     */
    fcache = (SQLFunctionCachePtr)fcinfo->flinfo->fn_extra;

    if (fcache != NULL) {
        if (fcache->lxid != t_thrd.proc->lxid || !SubTransactionIsActive(fcache->subxid)) {
            /* It's stale; unlink and delete */
            fcinfo->flinfo->fn_extra = NULL;
            MemoryContextDelete(fcache->fcontext);
            fcache = NULL;
        }
    }

    if (fcache == NULL) {
        init_sql_fcache(fcinfo->flinfo, PG_GET_COLLATION(), lazy_eval_ok);
        fcache = (SQLFunctionCachePtr)fcinfo->flinfo->fn_extra;
    }

    /*
     * Switch to context in which the fcache lives.  This ensures that our
     * tuplestore etc will have sufficient lifetime.  The sub-executor is
     * responsible for deleting per-tuple information.	(XXX in the case of a
     * long-lived FmgrInfo, this policy represents more memory leakage, but
     * it's not entirely clear where to keep stuff instead.)
     */
    old_context = MemoryContextSwitchTo(fcache->fcontext);

    /*
     * Find first unfinished query in function, and note whether it's the
     * first query.
     */
    es_list = fcache->func_state;
    es = NULL;
    is_first = true;
    foreach (eslc, es_list) {
        es = (execution_state*)lfirst(eslc);

        while (es != NULL && es->status == F_EXEC_DONE) {
            is_first = false;
            es = es->next;
        }

        if (es != NULL)
            break;
    }

    /*
     * Convert params to appropriate format if starting a fresh execution. (If
     * continuing execution, we can re-use prior params.)
     */
    if (is_first && es != NULL && es->status == F_EXEC_START)
        postquel_sub_params(fcache, fcinfo);

    /*
     * Build tuplestore to hold results, if we don't have one already. Note
     * it's in the query-lifespan context.
     */
    if (fcache->tstore == NULL)
        fcache->tstore = tuplestore_begin_heap(random_access, false, u_sess->attr.attr_memory.work_mem);

    /*
     * Execute each command in the function one after another until we either
     * run out of commands or get a result row from a lazily-evaluated SELECT.
     *
     * Notes about snapshot management:
     *
     * In a read-only function, we just use the surrounding query's snapshot.
     *
     * In a non-read-only function, we rely on the fact that we'll never
     * suspend execution between queries of the function: the only reason to
     * suspend execution before completion is if we are returning a row from a
     * lazily-evaluated SELECT.  So, when first entering this loop, we'll
     * either start a new query (and push a fresh snapshot) or re-establish
     * the active snapshot from the existing query descriptor.	If we need to
     * start a new query in a subsequent execution of the loop, either we need
     * a fresh snapshot (and pushed_snapshot is false) or the existing
     * snapshot is on the active stack and we can just bump its command ID.
     */
    pushed_snapshot = false;
    while (es != NULL) {
        bool completed = false;

        if (es->status == F_EXEC_START) {
            /*
             * If not read-only, be sure to advance the command counter for
             * each command, so that all work to date in this transaction is
             * visible.  Take a new snapshot if we don't have one yet,
             * otherwise just bump the command ID in the existing snapshot.
             */
            if (!fcache->readonly_func) {
                CommandCounterIncrement();
                if (!pushed_snapshot) {
                    PushActiveSnapshot(GetTransactionSnapshot());
                    pushed_snapshot = true;
                } else
                    UpdateActiveSnapshotCommandId();
            }

            postquel_start(es, fcache);
        } else if (!fcache->readonly_func && !pushed_snapshot) {
            /* Re-establish active snapshot when re-entering function */
            PushActiveSnapshot(es->qd->snapshot);
            pushed_snapshot = true;
        }

        completed = postquel_getnext(es, fcache);
        /*
         * If we ran the command to completion, we can shut it down now. Any
         * row(s) we need to return are safely stashed in the tuplestore, and
         * we want to be sure that, for example, AFTER triggers get fired
         * before we return anything.  Also, if the function doesn't return
         * set, we can shut it down anyway because it must be a SELECT and we
         * don't care about fetching any more result rows.
         */
        if (completed || !fcache->returnsSet)
            postquel_end(es);

        /*
         * Break from loop if we didn't shut down (implying we got a
         * lazily-evaluated row).  Otherwise we'll press on till the whole
         * function is done, relying on the tuplestore to keep hold of the
         * data to eventually be returned.	This is necessary since an
         * INSERT/UPDATE/DELETE RETURNING that sets the result might be
         * followed by additional rule-inserted commands, and we want to
         * finish doing all those commands before we return anything.
         */
        if (es->status != F_EXEC_DONE)
            break;

        /*
         * Advance to next execution_state, which might be in the next list.
         */
        es = es->next;
        while (es == NULL) {
            eslc = lnext(eslc);
            if (eslc == NULL)
                break; /* end of function */

            es = (execution_state*)lfirst(eslc);

            /*
             * Flush the current snapshot so that we will take a new one for
             * the new query list.	This ensures that new snaps are taken at
             * original-query boundaries, matching the behavior of interactive
             * execution.
             */
            if (pushed_snapshot) {
                PopActiveSnapshot();
                pushed_snapshot = false;
            }
        }
    }

    if (AUDIT_EXEC_ENABLED) {
        auditExecSQLFunction(fcinfo->flinfo->fn_oid, fcache, AUDIT_OK);
    }

    /*
     * The tuplestore now contains whatever row(s) we are supposed to return.
     */
    if (fcache->returnsSet) {
        ReturnSetInfo* rsi = (ReturnSetInfo*)fcinfo->resultinfo;

        if (IsClientLogicType(fcache->rettype)) {
            for (int i = 0; i < rsi->expectedDesc->natts; i++) {
                if (IsClientLogicType(rsi->expectedDesc->attrs[i]->atttypid)) {
                    rsi->expectedDesc->attrs[i]->atttypmod = fcache->rettype_orig;
                }
            }
        }
        if (es != NULL) {
            /*
             * If we stopped short of being done, we must have a lazy-eval
             * row.
             */
            Assert(es->lazyEval);
            /* Re-use the junkfilter's output slot to fetch back the tuple */
            Assert(fcache->junkFilter);
            if (fcache->junkFilter == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_FETCH_DATA_FAILED),
                        errmsg("function returns VOID, failed to get junk filter's slot")));
            }
            slot = fcache->junkFilter->jf_resultSlot;
            if (!tuplestore_gettupleslot(fcache->tstore, true, false, slot))
                ereport(ERROR, (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("failed to fetch lazy-eval tuple")));
            /* Extract the result as a datum, and copy out from the slot */
            result = postquel_get_single_result(slot, fcinfo, fcache, old_context);
            /* Clear the tuplestore, but keep it for next time */
            /* NB: this might delete the slot's content, but we don't care */
            tuplestore_clear(fcache->tstore);

            /*
             * Let caller know we're not finished.
             */
            rsi->isDone = ExprMultipleResult;

            /*
             * Ensure we will get shut down cleanly if the exprcontext is not
             * run to completion.
             */
            if (!fcache->shutdown_reg) {
                RegisterExprContextCallback(rsi->econtext, ShutdownSQLFunction, PointerGetDatum(fcache));
                fcache->shutdown_reg = true;
            }
        } else if (fcache->lazyEval) {
            /*
             * We are done with a lazy evaluation.	Clean up.
             */
            tuplestore_clear(fcache->tstore);

            /*
             * Let caller know we're finished.
             */
            rsi->isDone = ExprEndResult;

            fcinfo->isnull = true;
            result = (Datum)0;

            /* Deregister shutdown callback, if we made one */
            if (fcache->shutdown_reg) {
                UnregisterExprContextCallback(rsi->econtext, ShutdownSQLFunction, PointerGetDatum(fcache));
                fcache->shutdown_reg = false;
            }
        } else {
            /*
             * We are done with a non-lazy evaluation.	Return whatever is in
             * the tuplestore.	(It is now caller's responsibility to free the
             * tuplestore when done.)
             */
            rsi->returnMode = SFRM_Materialize;
            rsi->setResult = fcache->tstore;
            fcache->tstore = NULL;
            /* must copy desc because execQual will free it */
            if (fcache->junkFilter != NULL)
                rsi->setDesc = CreateTupleDescCopy(fcache->junkFilter->jf_cleanTupType);

            fcinfo->isnull = true;
            result = (Datum)0;

            /* Deregister shutdown callback, if we made one */
            if (fcache->shutdown_reg) {
                UnregisterExprContextCallback(rsi->econtext, ShutdownSQLFunction, PointerGetDatum(fcache));
                fcache->shutdown_reg = false;
            }
        }
    } else {
        /*
         * Non-set function.  If we got a row, return it; else return NULL.
         */
        if (fcache->junkFilter != NULL) {
            /* Re-use the junkfilter's output slot to fetch back the tuple */
            slot = fcache->junkFilter->jf_resultSlot;
            if (tuplestore_gettupleslot(fcache->tstore, true, false, slot))
                result = postquel_get_single_result(slot, fcinfo, fcache, old_context);
            else {
                fcinfo->isnull = true;
                result = (Datum)0;
            }
        } else {
            /* Should only get here for VOID functions */
            Assert(fcache->rettype == VOIDOID);
            fcinfo->isnull = true;
            result = (Datum)0;
        }

        /* Clear the tuplestore, but keep it for next time */
        tuplestore_clear(fcache->tstore);
    }

    /* Pop snapshot if we have pushed one */
    if (pushed_snapshot)
        PopActiveSnapshot();

    if (need_snapshot)
        PopActiveSnapshot();
    /*
     * If we've gone through every command in the function, we are done. Reset
     * the execution states to start over again on next call.
     */
    if (es == NULL) {
        foreach (eslc, fcache->func_state) {
            es = (execution_state*)lfirst(eslc);
            while (es != NULL) {
                es->status = F_EXEC_START;
                es = es->next;
            }
        }
    }

    t_thrd.log_cxt.error_context_stack = sql_err_context.previous;

    MemoryContextSwitchTo(old_context);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        u_sess->opt_cxt.is_stream = outer_is_stream;
        u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
    }
#else
    u_sess->opt_cxt.query_dop = outerDop;
#endif
    t_thrd.codegen_cxt.g_runningInFmgr = old_running_in_fmgr;
    return result;
}

/*
 * error context callback to let us supply a call-stack traceback
 */
static void sql_exec_error_callback(void* arg)
{
    FmgrInfo* flinfo = (FmgrInfo*)arg;
    SQLFunctionCachePtr fcache = (SQLFunctionCachePtr)flinfo->fn_extra;
    int syntax_err_position;

    /*
     * We can do nothing useful if init_sql_fcache() didn't get as far as
     * saving the function name
     */
    if (fcache == NULL || fcache->fname == NULL)
        return;

    /*
     * If there is a syntax error position, convert to internal syntax error
     */
    syntax_err_position = geterrposition();
    if (syntax_err_position > 0 && fcache->src != NULL) {
        errposition(0);
        internalerrposition(syntax_err_position);
        internalerrquery(fcache->src);
    }

    if (AUDIT_EXEC_ENABLED) {
        int e_level;
        int sql_state;
        getElevelAndSqlstate(&e_level, &sql_state);
        if (e_level >= ERROR)
            auditExecSQLFunction(flinfo->fn_oid, fcache, AUDIT_FAILED);
    }
    /*
     * Try to determine where in the function we failed.  If there is a query
     * with non-null QueryDesc, finger it.	(We check this rather than looking
     * for F_EXEC_RUN state, so that errors during ExecutorStart or
     * ExecutorEnd are blamed on the appropriate query; see postquel_start and
     * postquel_end.)
     */
    if (fcache->func_state != NULL) {
        execution_state* es = NULL;
        int query_num;
        ListCell* lc = NULL;

        es = NULL;
        query_num = 1;
        foreach (lc, fcache->func_state) {
            es = (execution_state*)lfirst(lc);
            while (es != NULL) {
                if (es->qd != NULL) {
                    errcontext("SQL function \"%s\" statement %d", fcache->fname, query_num);
                    break;
                }
                es = es->next;
            }
            if (es != NULL)
                break;
            query_num++;
        }
        if (es == NULL) {
            /*
             * couldn't identify a running query; might be function entry,
             * function exit, or between queries.
             */
            errcontext("SQL function \"%s\"", fcache->fname);
        }
    } else {
        /*
         * Assume we failed during init_sql_fcache().  (It's possible that the
         * function actually has an empty body, but in that case we may as
         * well report all errors as being "during startup".)
         */
        errcontext("SQL function \"%s\" during startup", fcache->fname);
    }
}

/*
 * callback function in case a function-returning-set needs to be shut down
 * before it has been run to completion
 */
static void ShutdownSQLFunction(Datum arg)
{
    SQLFunctionCachePtr fcache = (SQLFunctionCachePtr)DatumGetPointer(arg);
    execution_state* es = NULL;
    ListCell* lc = NULL;

    foreach (lc, fcache->func_state) {
        es = (execution_state*)lfirst(lc);
        while (es != NULL) {
            /* Shut down anything still running */
            if (es->status == F_EXEC_RUN) {
                /* Re-establish active snapshot for any called functions */
                if (!fcache->readonly_func)
                    PushActiveSnapshot(es->qd->snapshot);

                postquel_end(es);

                if (!fcache->readonly_func)
                    PopActiveSnapshot();
            }

            /* Reset states to START in case we're called again */
            es->status = F_EXEC_START;
            es = es->next;
        }
    }

    /* Release tuplestore if we have one */
    if (fcache->tstore != NULL)
        tuplestore_end(fcache->tstore);
    fcache->tstore = NULL;

    /* execUtils will deregister the callback... */
    fcache->shutdown_reg = false;
}

/*
 * check_sql_fn_retval() -- check return value of a list of sql parse trees.
 *
 * The return value of a sql function is the value returned by the last
 * canSetTag query in the function.  We do some ad-hoc type checking here
 * to be sure that the user is returning the type he claims.  There are
 * also a couple of strange-looking features to assist callers in dealing
 * with allowed special cases, such as binary-compatible result types.
 *
 * For a polymorphic function the passed rettype must be the actual resolved
 * output type of the function; we should never see a polymorphic pseudotype
 * such as ANYELEMENT as rettype.  (This means we can't check the type during
 * function definition of a polymorphic function.)
 *
 * This function returns true if the sql function returns the entire tuple
 * result of its final statement, or false if it returns just the first column
 * result of that statement.  It throws an error if the final statement doesn't
 * return the right type at all.
 *
 * Note that because we allow "SELECT rowtype_expression", the result can be
 * false even when the declared function return type is a rowtype.
 *
 * If modifyTargetList isn't NULL, the function will modify the final
 * statement's targetlist in two cases:
 * (1) if the tlist returns values that are binary-coercible to the expected
 * type rather than being exactly the expected type.  RelabelType nodes will
 * be inserted to make the result types match exactly.
 * (2) if there are dropped columns in the declared result rowtype.  NULL
 * output columns will be inserted in the tlist to match them.
 * (Obviously the caller must pass a parsetree that is okay to modify when
 * using this flag.)  Note that this flag does not affect whether the tlist is
 * considered to be a legal match to the result type, only how we react to
 * allowed not-exact-match cases.  *modifyTargetList will be set true iff
 * we had to make any "dangerous" changes that could modify the semantics of
 * the statement.  If it is set true, the caller should not use the modified
 * statement, but for simplicity we apply the changes anyway.
 *
 * If junkFilter isn't NULL, then *junkFilter is set to a JunkFilter defined
 * to convert the function's tuple result to the correct output tuple type.
 * Exception: if the function is defined to return VOID then *junkFilter is
 * set to NULL.
 */
bool check_sql_fn_retval(Oid func_id, Oid ret_type, List* query_tree_list, bool* modify_target_list,
    JunkFilter** junk_filter, bool plpgsql_validation)
{
    Query* parse = NULL;
    List** tlist_ptr;
    List* tlist = NIL;
    int tlist_len;
    char fn_type;
    Oid res_type;
    ListCell* lc = NULL;
    bool gs_encrypted_proc_was_created = false;
    AssertArg(!IsPolymorphicType(ret_type));
    CommandCounterIncrement();
    if (modify_target_list != NULL)
        *modify_target_list = false; /* initialize for no change */
    if (junk_filter != NULL)
        *junk_filter = NULL; /* initialize in case of VOID result */

    /*
     * Find the last canSetTag query in the list.  This isn't necessarily the
     * last parsetree, because rule rewriting can insert queries after what
     * the user wrote.
     */
    parse = NULL;
    foreach (lc, query_tree_list) {
        Query* q = (Query*)lfirst(lc);

        if (q->canSetTag)
            parse = q;
    }

    /*
     * If it's a plain SELECT, it returns whatever the targetlist says.
     * Otherwise, if it's INSERT/UPDATE/DELETE with RETURNING, it returns
     * that. Otherwise, the function return type must be VOID.
     *
     * Note: eventually replace this test with QueryReturnsTuples?	We'd need
     * a more general method of determining the output type, though.  Also, it
     * seems too dangerous to consider FETCH or EXECUTE as returning a
     * determinable rowtype, since they depend on relatively short-lived
     * entities.
     */
    if (parse != NULL && parse->commandType == CMD_SELECT && parse->utilityStmt == NULL) {
        tlist_ptr = &parse->targetList;
        tlist = parse->targetList;
    } else if (parse != NULL &&
               (parse->commandType == CMD_INSERT || parse->commandType == CMD_UPDATE ||
                   parse->commandType == CMD_DELETE) &&
               (parse->returningList || plpgsql_validation)) {
        tlist_ptr = &parse->returningList;
        tlist = parse->returningList;
    } else {
        /* For plpgsql function return may be done through out/inout param so ret_type != VOIDOID */
        /* Empty function body, or last statement is a utility command */
        if (ret_type != VOIDOID)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                    errdetail("Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.")));
        return false;
    }

    /*
     * OK, check that the targetlist returns something matching the declared
     * type.  (We used to insist that the declared type not be VOID in this
     * case, but that makes it hard to write a void function that exits after
     * calling another void function.  Instead, we insist that the tlist
     * return void ... so void is treated as if it were a scalar type below.)
     */
    /*
     * Count the non-junk entries in the result targetlist.
     */
    tlist_len = ExecCleanTargetListLength(tlist);

    fn_type = get_typtype(ret_type);

    if (fn_type == TYPTYPE_BASE || fn_type == TYPTYPE_DOMAIN || fn_type == TYPTYPE_ENUM ||
        fn_type == TYPTYPE_RANGE || ret_type == VOIDOID) {
        /*
         * For scalar-type returns, the target list must have exactly one
         * non-junk entry, and its type must agree with what the user
         * declared; except we allow binary-compatible types too.
         */
        TargetEntry* tle = NULL;

        if (tlist_len != 1) {
            if (plpgsql_validation) {
                return true;
            }
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                    errdetail("Final statement must return exactly one column.")));

        }
        /* We assume here that non-junk TLEs must come first in tlists */
        tle = (TargetEntry*)linitial(tlist);
        Assert(!tle->resjunk);

        res_type = exprType((Node*)tle->expr);
        if (!IsBinaryCoercible(res_type, ret_type)) {
            if (IsClientLogicType(res_type) && IsBinaryCoercible(exprTypmod((Node*)tle->expr), ret_type)) {
                add_rettype_orig(func_id, ret_type, res_type);
            } else {
                if (IsClientLogicType(res_type)) {
                    res_type = exprTypmod((Node*)tle->expr);
                }
                ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                    errdetail("Actual return type is %s.", format_type_be(res_type))));
            }
        }
        if (modify_target_list != NULL && res_type != ret_type) {
            tle->expr = (Expr*)makeRelabelType(tle->expr, ret_type, -1, get_typcollation(ret_type), COERCE_DONTCARE);
            /* Relabel is dangerous if TLE is a sort/group or setop column */
            if (tle->ressortgroupref != 0 || parse->setOperations)
                *modify_target_list = true;
        }

        /* Set up junk filter if needed */
        if (junk_filter != NULL)
            *junk_filter = ExecInitJunkFilter(tlist, false, NULL);
    } else if (fn_type == TYPTYPE_COMPOSITE || ret_type == RECORDOID) {
        /* Returns a rowtype */
        TupleDesc tup_desc;
        int tup_natts;          /* physical number of columns in tuple */
        int tup_log_cols;        /* # of nondeleted columns in tuple */
        int col_index;          /* physical column index */
        List* new_tlist = NIL;  /* new non-junk tlist entries */
        List* junk_attrs = NIL; /* new junk tlist entries */
        Datum* all_types_orig = NULL; /* original data types for gs_encrypted_proc */
        Datum* all_types = NULL; /* will be used for replace data types in pg_proc */
        /*
         * If the target list is of length 1, and the type of the varnode in
         * the target list matches the declared return type, this is okay.
         * This can happen, for example, where the body of the function is
         * 'SELECT func2()', where func2 has the same composite return type as
         * the function that's calling it.
         *
         * XXX Note that if rettype is RECORD, the IsBinaryCoercible check
         * will succeed for any composite restype.	For the moment we rely on
         * runtime type checking to catch any discrepancy, but it'd be nice to
         * do better at parse time.
         */
        if (tlist_len == 1) {
            TargetEntry* tle = (TargetEntry*)linitial(tlist);

            Assert(!tle->resjunk);
            res_type = exprType((Node*)tle->expr);
            if (IsBinaryCoercible(res_type, ret_type)) {
                if (modify_target_list != NULL && res_type != ret_type) {
                    tle->expr =
                        (Expr*)makeRelabelType(tle->expr, ret_type, -1, get_typcollation(ret_type), COERCE_DONTCARE);
                    /* Relabel is dangerous if sort/group or setop column */
                    if (tle->ressortgroupref != 0 || parse->setOperations)
                        *modify_target_list = true;
                }
                /* Set up junk filter if needed */
                if (junk_filter != NULL)
                    *junk_filter = ExecInitJunkFilter(tlist, false, NULL);
                return false; /* NOT returning whole tuple */
            }
        }

        /* Is the rowtype fixed, or determined only at runtime? */
        if (get_func_result_type(func_id, NULL, &tup_desc) != TYPEFUNC_COMPOSITE) {
            /*
             * Assume we are returning the whole tuple. Crosschecking against
             * what the caller expects will happen at runtime.
             */
            if (junk_filter != NULL)
                *junk_filter = ExecInitJunkFilter(tlist, false, NULL);
            return true;
        }
        Assert(tup_desc);

        /*
         * Verify that the targetlist matches the return tuple type. We scan
         * the non-deleted attributes to ensure that they match the datatypes
         * of the non-resjunk columns.	For deleted attributes, insert NULL
         * result columns if the caller asked for that.
         */
        tup_natts = tup_desc->natts;
        tup_log_cols = 0; /* we'll count nondeleted cols as we go */
        col_index = 0;
        new_tlist = NIL; /* these are only used if modifyTargetList */
        junk_attrs = NIL;
        Oid gsrelid = InvalidOid;
        foreach (lc, tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            Form_pg_attribute attr;
            Oid tle_type;
            Oid att_type;

            if (tle->resjunk) {
                if (modify_target_list != NULL)
                    junk_attrs = lappend(junk_attrs, tle);
                continue;
            }

            do {
                col_index++;
                if (col_index > tup_natts)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                            errdetail("Final statement returns too many columns.")));
                attr = tup_desc->attrs[col_index - 1];
                if (attr->attisdropped && modify_target_list) {
                    Expr* null_expr = NULL;

                    /* The type of the null we insert isn't important */
                    null_expr = (Expr*)makeConst(INT4OID,
                        -1,
                        InvalidOid,
                        sizeof(int32),
                        (Datum)0,
                        true, /* isnull */
                        true /* byval */);
                    new_tlist = lappend(new_tlist, makeTargetEntry(null_expr, col_index, NULL, false));
                    /* NULL insertion is dangerous in a setop */
                    if (parse->setOperations)
                        *modify_target_list = true;
                }
            } while (attr->attisdropped);
            tup_log_cols++;

            tle_type = exprType((Node*)tle->expr);
            att_type = attr->atttypid;
            if (gs_encrypted_proc_was_created && !IsClientLogicType(tle_type)) {
                all_types_orig[col_index - 1] = -1;
                all_types[col_index - 1] = ObjectIdGetDatum(att_type);
            }
            /* return table */
            if (IsClientLogicType(tle_type) && !IsClientLogicType(att_type)) {
                if (!IsBinaryCoercible(exprTypmod((Node*)tle->expr), att_type)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                            errdetail("Final statement returns %s instead of %s at column %d.",
                                format_type_be(exprTypmod((Node*)tle->expr)),
                                format_type_be(att_type),
                                tup_log_cols)));
                }
                if (!gs_encrypted_proc_was_created) {
                    all_types_orig = (Datum*)palloc(tup_natts * sizeof(Datum));
                    all_types = (Datum*)palloc(tup_natts * sizeof(Datum));
                    /* if the column result type is diffent than the function table reuslt type */
                    if (attr->attrelid != tle->resorigtbl &&
                       /* The colunm relation is not temporal */
                       attr->attrelid != 0 &&
                       /* The colunm relation is not temporal */
                       attr->attnum == tle->resorigcol) {
                        /* if all the above conditions are correct - than we might return real table
                           with same structre but without client logic columns -  replace the data type */
                        gsrelid = ObjectIdGetDatum(tle->resorigtbl);
                    }

                    for (int j = 0; j < col_index - 1; j++) {
                        all_types_orig[j] = -1;
                        all_types[j] = ObjectIdGetDatum(tup_desc->attrs[j]->atttypid);
                    }
                }
                all_types_orig[col_index - 1] = ObjectIdGetDatum(att_type);
                all_types[col_index - 1] = ObjectIdGetDatum(tle_type);
                gs_encrypted_proc_was_created = true;
            } else if (!IsBinaryCoercible(tle_type, att_type) &&
                /*
                 * if the data type mismatch is because of it is client_logic, it's OK at this point
                 * we just need to validate that it does not conflict with the original data type 
                 */
                !(IsClientLogicType(att_type) && IsBinaryCoercible(tle_type, attr->atttypmod))) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                        errdetail("Final statement returns %s instead of %s at column %d.",
                            format_type_be(tle_type),
                            format_type_be(att_type),
                            tup_log_cols)));
            }
            if (modify_target_list != NULL) {
                if (tle_type != att_type) {
                    tle->expr =
                        (Expr*)makeRelabelType(tle->expr, att_type, -1, get_typcollation(att_type), COERCE_DONTCARE);
                    /* Relabel is dangerous if sort/group or setop column */
                    if (tle->ressortgroupref != 0 || parse->setOperations)
                        *modify_target_list = true;
                }
                tle->resno = col_index;
                new_tlist = lappend(new_tlist, tle);
            }
        }
        if (gs_encrypted_proc_was_created) {
            add_allargtypes_orig(func_id, all_types_orig, all_types, tup_natts, gsrelid);
        }

        /* remaining columns in tupdesc had better all be dropped */
        for (col_index++; col_index <= tup_natts; col_index++) {
            if (!tup_desc->attrs[col_index - 1]->attisdropped)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("return type mismatch in function declared to return %s", format_type_be(ret_type)),
                        errdetail("Final statement returns too few columns.")));
            if (modify_target_list != NULL) {
                Expr* null_expr = NULL;

                /* The type of the null we insert isn't important */
                null_expr = (Expr*)makeConst(INT4OID,
                    -1,
                    InvalidOid,
                    sizeof(int32),
                    (Datum)0,
                    true, /* isnull */
                    true /* byval */);
                new_tlist = lappend(new_tlist, makeTargetEntry(null_expr, col_index, NULL, false));
                /* NULL insertion is dangerous in a setop */
                if (parse->setOperations)
                    *modify_target_list = true;
            }
        }
        if (modify_target_list != NULL) {
            /* ensure resjunk columns are numbered correctly */
            foreach (lc, junk_attrs) {
                TargetEntry* tle = (TargetEntry*)lfirst(lc);

                tle->resno = col_index++;
            }
            /* replace the tlist with the modified one */
            *tlist_ptr = list_concat(new_tlist, junk_attrs);
        }

        /* Set up junk filter if needed */
        if (junk_filter != NULL)
            *junk_filter = ExecInitJunkFilterConversion(tlist, CreateTupleDescCopy(tup_desc), NULL);

        /* Report that we are returning entire tuple result */
        return true;
    } else
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("return type %s is not supported for SQL functions with ID %u.",
                    format_type_be(ret_type),
                    func_id)));

    return false;
}

/*
 * CreateSQLFunctionDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver* CreateSQLFunctionDestReceiver(void)
{
    DR_sqlfunction* self = (DR_sqlfunction*)palloc0(sizeof(DR_sqlfunction));

    self->pub.receiveSlot = sqlfunction_receive;
    self->pub.rStartup = sqlfunction_startup;
    self->pub.rShutdown = sqlfunction_shutdown;
    self->pub.rDestroy = sqlfunction_destroy;
    self->pub.mydest = DestSQLFunction;
    self->pub.tmpContext = NULL;

    /* private fields will be set by postquel_start */
    return (DestReceiver*)self;
}

/*
 * sqlfunction_startup --- executor startup
 */
static void sqlfunction_startup(DestReceiver* self, int operation, TupleDesc type_info)
{
    /* no-op */
}

/*
 * sqlfunction_receive --- receive one tuple
 */
static void sqlfunction_receive(TupleTableSlot* slot, DestReceiver* self)
{
    DR_sqlfunction* my_state = (DR_sqlfunction*)self;

    /* Filter tuple as needed */
    slot = ExecFilterJunk(my_state->filter, slot);

    /* Store the filtered tuple into the tuplestore */
    tuplestore_puttupleslot(my_state->tstore, slot);
}

/*
 * sqlfunction_shutdown --- executor end
 */
static void sqlfunction_shutdown(DestReceiver* self)
{
    /* no-op */
}

/*
 * sqlfunction_destroy --- release DestReceiver object
 */
static void sqlfunction_destroy(DestReceiver* self)
{
    pfree_ext(self);
}

/*
 * Store input parameters substitution info in parser info
 */
void sql_fn_parser_replace_param_type(struct ParseState* pstate, int param_no, Var* var)
{
    SQLFunctionParseInfoPtr f_info = (SQLFunctionParseInfoPtr)pstate->p_cl_hook_state;
    if (param_no >= f_info->nargs) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("param_num is not valid")));
        return;
    }
    f_info->replaced_argtypes[param_no] = var->vartype;
    /* find the column in RTE (pg_class and pg_attribute) and retrieve the gs_encrypted_columns record based on it */
    RangeTblEntry* rte = GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
    if (!rte) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("RTE not found (internal error)")));
    }
    ListCell* c = list_nth_cell(rte->eref->colnames, var->varattno - 1);
    if (c == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("AttrNum is not found in RTE (internal error)")));
    }
    /* get the encrypted column (gs_encrypted_columns) */
    HeapTuple ce_tuple =  search_sys_cache_copy_ce_col_name(rte->relid, strVal(lfirst(c)));
    if (!HeapTupleIsValid(ce_tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg("failed to find encrypted column: \"%s\"", strVal(lfirst(c)))));
    }
    f_info->replaced_args_cl_oids[param_no] = HeapTupleGetOid(ce_tuple);
    heap_freetuple_ext(ce_tuple);
}

void update_gs_encrypted_proc(const Oid func_id, SQLFunctionParseInfoPtr p_info, Datum* allargs_orig, int allnumargs)
{
    bool gs_nulls[Natts_gs_encrypted_proc] = {0};
    Datum gs_values[Natts_gs_encrypted_proc] = {0};
    bool gs_replaces[Natts_gs_encrypted_proc] = {0};
    oidvector* parameterTypes = buildoidvector(p_info->replaced_args_cl_oids, p_info->nargs);
    HeapTuple gs_tup;
    Oid gs_oid;
    Relation gs_rel = NULL;
    HeapTuple gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(func_id));
    gs_rel = heap_open(ClientLogicProcId, RowExclusiveLock);
    TupleDesc gs_tupDesc = RelationGetDescr(gs_rel);
    gs_values[Anum_gs_encrypted_proc_last_change - 1] =
        DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    if (allargs_orig) {
        ArrayType* all_parameter_types_orig =
            construct_array(allargs_orig, allnumargs, INT4OID, sizeof(int4), true, 'i');
        gs_values[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = PointerGetDatum(all_parameter_types_orig);
        gs_replaces[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = true;
    } else {
        gs_nulls[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = true;
    }
    if (!HeapTupleIsValid(gs_oldtup)) {
        gs_values[Anum_gs_encrypted_proc_func_id - 1] = ObjectIdGetDatum(func_id);
        gs_nulls[Anum_gs_encrypted_proc_prorettype_orig - 1] = true;
        gs_values[Anum_gs_encrypted_proc_proargcachedcol - 1] = PointerGetDatum(parameterTypes);
        gs_tup = heap_form_tuple(gs_tupDesc, gs_values, gs_nulls);
        gs_oid = simple_heap_insert(gs_rel, gs_tup);
        record_proc_depend(func_id, gs_oid);
    } else {
        gs_values[Anum_gs_encrypted_proc_proargcachedcol - 1] = PointerGetDatum(parameterTypes);
        gs_replaces[Anum_gs_encrypted_proc_proargcachedcol - 1] = true;
        gs_replaces[Anum_gs_encrypted_proc_last_change - 1] = true;
        /* Okay, do it... */
        gs_tup = heap_modify_tuple(gs_oldtup, gs_tupDesc, gs_values, gs_nulls, gs_replaces);
        simple_heap_update(gs_rel, &gs_tup->t_self, gs_tup);
        ReleaseSysCache(gs_oldtup);
    }
    CatalogUpdateIndexes(gs_rel, gs_tup);
    CommandCounterIncrement();
    heap_close(gs_rel, RowExclusiveLock);
    ce_cache_refresh_type |= 0x20; /* refresh proc cache */
    pfree_ext(allargs_orig);
}

static inline bool is_proargmode_any_input(char arg)
{
    return (arg == PROARGMODE_IN || arg == PROARGMODE_INOUT || arg == PROARGMODE_VARIADIC);
}

/*
 * Replace the parameters data types requested by the client with the encrypted "bytea" data types
 * add column setting oid info to gs_encrypted_proc data for stored procedure
 */
bool sql_fn_cl_rewrite_params(const Oid func_id, SQLFunctionParseInfoPtr p_info, bool is_replace)
{
    bool is_supported_outparams_override = false;
#ifndef ENABLE_MULTIPLE_NODES
    is_supported_outparams_override = (t_thrd.proc->workingVersionNum >= 92470);
#endif // ENABLE_MULTIPLE_NODES

    CommandCounterIncrement(); // precaution

    HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmodule(MOD_EXECUTOR),
            errmsg("cache lookup failed for function %u when initialize function cache.", func_id)));
    }

    /* get tuple from pg_proc */
    Form_pg_proc oldproc = (Form_pg_proc)GETSTRUCT(tuple);

    /* get argmodes from tuple */
    bool isNull = false;
    Datum proargmodes = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proargmodes, &isNull);
    char *argmodes = NULL;
    ArrayType *argmodes_arr = NULL;
    int n_modes = 0;
    if (!isNull) {
        argmodes_arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
        n_modes = ARR_DIMS(argmodes_arr)[0];
        bool is_char_oid_array =
            ARR_NDIM(argmodes_arr) != 1 || ARR_HASNULL(argmodes_arr) || ARR_ELEMTYPE(argmodes_arr) != CHAROID;
        if (is_char_oid_array) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        argmodes = (char *)ARR_DATA_PTR(argmodes_arr);
    }

    /* get allargs from tuple if available */
    oidvector *tup_allargs = NULL;
    if (is_supported_outparams_override) {
        tup_allargs = (oidvector *)DatumGetPointer(ProcedureGetAllArgTypes(tuple, &isNull));
    }

    /* replace argtypes and allargs data types from original to real data types */
    bool is_any_replacement = false;
    int out_count = 0;
    for (int i = 0; i < p_info->nargs; i++) {
        if (p_info->replaced_argtypes[i] == 0 || oldproc->proargtypes.values[i] == p_info->replaced_argtypes[i]) {
            continue;
        }

        is_any_replacement = true;
        oldproc->proargtypes.values[i] = p_info->replaced_argtypes[i];

        if (argmodes != NULL) { /* skip the out params here. Allargs will have nargs input params */
            while (out_count <= (n_modes - p_info->nargs) && !is_proargmode_any_input(argmodes[i + out_count])) {
                out_count++;
            }
        }

        if (tup_allargs != NULL) {
            tup_allargs->values[i + out_count] = p_info->replaced_argtypes[i];
        }
    }
    if (!is_any_replacement) {
        ReleaseSysCache(tuple);
        return false;
    }

    /*
        check if tuple already exists.
        if is_replace == true then remove old tuple
        otherwise return error to client
    */
    HeapTuple oldtup = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    /* support CREATE OR REPLACE with the same parameter types */
    Datum packageidDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_packageid, &isNull);
    if (!is_supported_outparams_override) {
        oldtup = SearchSysCache3(PROCNAMEARGSNSP, CStringGetDatum(NameStr(oldproc->proname)),
                                 PointerGetDatum(&oldproc->proargtypes), ObjectIdGetDatum(oldproc->pronamespace));
    } else {
        oldtup = SearchSysCacheForProcAllArgs(CStringGetDatum(NameStr(oldproc->proname)), PointerGetDatum(tup_allargs),
            ObjectIdGetDatum(oldproc->pronamespace), packageidDatum, proargmodes);
    }
#else
    oldtup = SearchSysCache3(PROCNAMEARGSNSP, CStringGetDatum(NameStr(oldproc->proname)),
        PointerGetDatum(&oldproc->proargtypes), ObjectIdGetDatum(oldproc->pronamespace));
#endif  // ENABLE_MULTIPLE_NODES

    Relation rel = NULL;
    Relation gs_rel = NULL;
    if (HeapTupleIsValid(oldtup) && is_replace == true) {
        Assert(oldtup != tuple);

        /* remove dependent record from gs_encrypted_proc */
        HeapTuple old_gs_tup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(HeapTupleGetOid(oldtup)));
        if (HeapTupleIsValid(old_gs_tup)) {
            gs_rel = heap_open(ClientLogicProcId, RowExclusiveLock);
            deleteDependencyRecordsFor(ClientLogicProcId, HeapTupleGetOid(old_gs_tup), true);
            simple_heap_delete(gs_rel, &old_gs_tup->t_self);
            heap_close(gs_rel, RowExclusiveLock);
            ReleaseSysCache(old_gs_tup);
        }

        /* remove record from pg_proc */
        rel = heap_open(ProcedureRelationId, RowExclusiveLock);
        deleteDependencyRecordsFor(ProcedureRelationId, HeapTupleGetOid(oldtup), true);
        simple_heap_delete(rel, &oldtup->t_self);
        ReleaseSysCache(oldtup);
    } else if (HeapTupleIsValid(oldtup) && is_replace == false) {
        ReleaseSysCache(oldtup);
        ReleaseSysCache(tuple);
        /* caller should handle this case - function already exists and it is not replaced */
        return true;
    }

    bool nulls[Natts_pg_proc] = {0};
    Datum values[Natts_pg_proc] = {0};
    bool replaces[Natts_pg_proc] = {0};
    values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(&oldproc->proargtypes);
    replaces[Anum_pg_proc_proargtypes - 1] = true;
    /* verify replace for allargtypes */
    int proallargtypes_size = 0;
    Datum *proallargtypes_oids_orig = NULL;
    Datum proallargtypes = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proallargtypes, &isNull);
    if (!isNull) {
        ArrayType* proallargtypes_arr = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
        proallargtypes_size = ARR_DIMS(proallargtypes_arr)[0];
        Assert(proallargtypes_size >= p_info->nargs);

        /* check proallargtypes is not an array */
        bool is_char_oid_array = ARR_NDIM(proallargtypes_arr) != 1 || proallargtypes_size < 0 ||
            ARR_HASNULL(proallargtypes_arr) || ARR_ELEMTYPE(proallargtypes_arr) != OIDOID;
        if (is_char_oid_array) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        if (argmodes_arr != NULL && proallargtypes_size != ARR_DIMS(argmodes_arr)[0]) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        Oid *proallargtypes_oids = (Oid *)ARR_DATA_PTR(proallargtypes_arr);
        proallargtypes_oids_orig = (Datum *)palloc(proallargtypes_size * sizeof(Datum));
        errno_t rc = memset_s(proallargtypes_oids_orig, proallargtypes_size * sizeof(Datum), -1,
            proallargtypes_size * sizeof(Datum));
        securec_check_c(rc, "\0", "\0");
        int input_args_idx = 0;
        for (int i = 0; i < proallargtypes_size && input_args_idx < p_info->nargs; i++) {
            /* check if input argument */
            if (argmodes == NULL || !is_proargmode_any_input(argmodes[i])) {
                continue;
            }

            /* check if data type needs to be replaced */
            if (p_info->replaced_argtypes[input_args_idx] != 0) {
                /* type has been replaced for input params */
                proallargtypes_oids_orig[i] = Int32GetDatum(proallargtypes_oids[i]);
                proallargtypes_oids[i] = p_info->replaced_argtypes[input_args_idx];
            }

            input_args_idx++;
        }
        values[Anum_pg_proc_proallargtypes - 1] = PointerGetDatum(proallargtypes_arr);
        replaces[Anum_pg_proc_proallargtypes - 1] = true;
    }
    if (values[Anum_pg_proc_proallargtypes - 1] != 0) {
        values[Anum_pg_proc_allargtypes - 1] = values[Anum_pg_proc_proallargtypes - 1];
    } else {
        values[Anum_pg_proc_allargtypes - 1] = values[Anum_pg_proc_proargtypes - 1];
    }
    replaces[Anum_pg_proc_allargtypes - 1] = true;

    /* update catalog tables pg_proc and gs_encrypted_proc */
    if (!rel) {
        rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    }
    TupleDesc tupDesc = RelationGetDescr(rel);
    HeapTuple newtup = heap_modify_tuple(tuple, tupDesc, values, nulls, replaces);
    simple_heap_update(rel, &tuple->t_self, newtup);
    CatalogUpdateIndexes(rel, newtup);
    heap_freetuple_ext(newtup);
    heap_close(rel, RowExclusiveLock);
    ReleaseSysCache(tuple);
    update_gs_encrypted_proc(func_id, p_info, proallargtypes_oids_orig, proallargtypes_size);
    return false;
}

/*
 * Store input parameters substitution info for insert statement in parser info
 */
void sql_fn_parser_replace_param_type_for_insert(struct ParseState* pstate, int param_no, Oid param_new_type, Oid relid,
    const char* col_name)
{
    SQLFunctionParseInfoPtr f_info = (SQLFunctionParseInfoPtr)pstate->p_cl_hook_state;
    if (param_no >= f_info->nargs) {
        return;
    }
    f_info->replaced_argtypes[param_no] = param_new_type;
    /* get the encrypted column (gs_encrypted_columns) */
        HeapTuple ce_tuple = search_sys_cache_copy_ce_col_name(relid, col_name);
        if (!HeapTupleIsValid(ce_tuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("failed to find encrypted column: \"%s\"", col_name)));
        }
        f_info->replaced_args_cl_oids[param_no] = DatumGetObjectId(HeapTupleGetOid(ce_tuple));
        heap_freetuple_ext(ce_tuple);

}
