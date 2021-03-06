/* -------------------------------------------------------------------------
 *
 * pl_comp.c		- Compiler part of the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_comp.c
 *
 * -------------------------------------------------------------------------
 */

#include "plpgsql.h"

#include <ctype.h>

#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/subselect.h"
#include "parser/parse_type.h"
#include "pgxc/locator.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/globalplancore.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "miscadmin.h"

/* functions reference other modules */
extern THR_LOCAL List* baseSearchPath;

#define FUNCS_PER_USER 128 /* initial table size */

/* ----------
 * Lookup table for EXCEPTION condition names
 * ----------
 */
typedef struct {
    const char* label;
    int sqlerrstate;
} ExceptionLabelMap;

static const ExceptionLabelMap exception_label_map[] = {
#include "plerrcodes.h" /* pgrminclude ignore */
    {NULL, 0}};

/* ----------
 * static prototypes
 * ----------
 */
static PLpgSQL_function* do_compile(FunctionCallInfo fcinfo, HeapTuple proc_tup, PLpgSQL_function* func,
    PLpgSQL_func_hashkey* hashkey, bool for_validator);
static void plpgsql_compile_error_callback(void* arg);
static void add_parameter_name(int item_type, int item_no, const char* name);
static void add_dummy_return(PLpgSQL_function* func);
static Node* plpgsql_pre_column_ref(ParseState* pstate, ColumnRef* cref);
static Node* plpgsql_post_column_ref(ParseState* pstate, ColumnRef* cref, Node* var);
static Node* plpgsql_param_ref(ParseState* pstate, ParamRef* pref);
static Node* resolve_column_ref(ParseState* pstate, PLpgSQL_expr* expr, ColumnRef* cref, bool error_if_no_field);
static Node* make_datum_param(PLpgSQL_expr* expr, int dno, int location);
static PLpgSQL_row* build_row_from_class(Oid class_oid);
static PLpgSQL_row* build_row_from_vars(PLpgSQL_variable** vars, int numvars);
static PLpgSQL_type* build_datatype(HeapTuple type_tup, int32 typmod, Oid collation);
static void compute_function_hashkey(
    FunctionCallInfo fcinfo, Form_pg_proc proc_struct, PLpgSQL_func_hashkey* hashkey, bool for_validator);
static void plpgsql_resolve_polymorphic_argtypes(
    int numargs, Oid* argtypes, const char* argmodes, Node* call_expr, bool for_validator, const char* proname);
static PLpgSQL_function* plpgsql_HashTableLookup(PLpgSQL_func_hashkey* func_key);
static void plpgsql_HashTableInsert(PLpgSQL_function* func, PLpgSQL_func_hashkey* func_key);
static void delete_function(PLpgSQL_function* func);
static void plpgsql_append_dlcell(plpgsql_HashEnt* entity);

static int plpgsql_getCustomErrorCode(void);
extern bool is_func_need_cache(Oid funcid, const char* func_name);

static bool plpgsql_check_search_path(PLpgSQL_function* func, HeapTuple procTup);
static bool plpgsql_check_insert_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func);
static bool plpgsql_check_updel_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func);
static bool plpgsql_check_opexpr_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func, List* opexpr_list);

/* ----------
 * plpgsql_compile		Make an execution tree for a PL/pgSQL function.
 *
 * If forValidator is true, we're only compiling for validation purposes,
 * and so some checks are skipped.
 *
 * Note: it's important for this to fall through quickly if the function
 * has already been compiled.
 * ----------
 */
PLpgSQL_function* plpgsql_compile(FunctionCallInfo fcinfo, bool for_validator)
{
    Oid func_oid = fcinfo->flinfo->fn_oid;
    HeapTuple proc_tup = NULL;
    Form_pg_proc proc_struct = NULL;
    PLpgSQL_function* func = NULL;
    PLpgSQL_func_hashkey hashkey;
    bool function_valid = false;
    bool hashkey_valid = false;

    /*
     * Lookup the pg_proc tuple by Oid; we'll need it in any case
     */
    proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u, while compile function", func_oid)));
    }
    proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);

    /*
     * See if there's already a cache entry for the current FmgrInfo. If not,
     * try to find one in the hash table.
     */
    func = (PLpgSQL_function*)fcinfo->flinfo->fn_extra;

recheck:
    if (func == NULL) {
        /* Compute hashkey using function signature and actual arg types */
        compute_function_hashkey(fcinfo, proc_struct, &hashkey, for_validator);
        hashkey_valid = true;

        /* And do the lookup */
        func = plpgsql_HashTableLookup(&hashkey);
    }

    if (func != NULL) {
        /* We have a compiled function, but is it still valid? */
        if (func->fn_xmin == HeapTupleGetRawXmin(proc_tup) &&
            ItemPointerEquals(&func->fn_tid, &proc_tup->t_self) && plpgsql_check_search_path(func, proc_tup)) {
            function_valid = true;
        } else {
            /*
             * Nope, so remove it from hashtable and try to drop associated
             * storage (if not done already).
             */
            delete_function(func);

            /*
             * If the function isn't in active use then we can overwrite the
             * func struct with new data, allowing any other existing fn_extra
             * pointers to make use of the new definition on their next use.
             * If it is in use then just leave it alone and make a new one.
             * (The active invocations will run to completion using the
             * previous definition, and then the cache entry will just be
             * leaked; doesn't seem worth adding code to clean it up, given
             * what a corner case this is.)
             *
             * If we found the function struct via fn_extra then it's possible
             * a replacement has already been made, so go back and recheck the
             * hashtable.
             */
            if (func->use_count != 0) {
                func = NULL;
                if (!hashkey_valid) {
                    goto recheck;
                }
            }
        }
    }

    /*
     * If the function wasn't found or was out-of-date, we have to compile it
     */
    if (!function_valid) {
        /*
         * Calculate hashkey if we didn't already; we'll need it to store the
         * completed function.
         */
        if (!hashkey_valid) {
            compute_function_hashkey(fcinfo, proc_struct, &hashkey, for_validator);
        }
        /*
         * Do the hard part.
         */
        func = do_compile(fcinfo, proc_tup, func, &hashkey, for_validator);
    }

    ReleaseSysCache(proc_tup);

    /*
     * Save pointer in FmgrInfo to avoid search on subsequent calls
     */
    fcinfo->flinfo->fn_extra = (void*)func;

    /*
     * Finally return the compiled function
     */
    return func;
}

/*
 * @Description : A function similar to plpgsql_compile, the difference is we use it for body
 *				 precompilation here, so no hashkey is needed here.
 * @in fcinfo : info of trigger ffunction which need be compiled.
 * @return : function information after compiled.
 */
PLpgSQL_function* plpgsql_compile_nohashkey(FunctionCallInfo fcinfo)
{
    Oid func_oid = fcinfo->flinfo->fn_oid;
    HeapTuple proc_tup = NULL;
    Form_pg_proc proc_struct = NULL;
    PLpgSQL_function* func = NULL;

    /* Lookup the pg_proc tuple by Oid, as we'll need it in any case */
    proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("cache lookup failed for function %u", func_oid)));
    }
    
	proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
    /*
     * See if there's already a cache entry for the current FmgrInfo. If not,
     * try to find one in the hash table.
     */
    func = (PLpgSQL_function*)fcinfo->flinfo->fn_extra;

    /*
     * We should set for_validator of do_compile to false here which only need be
     * set true by plpgsql_validator in CREATE FUNCTION or DO command.
     */
    if (func == NULL) {
        func = do_compile(fcinfo, proc_tup, func, NULL, false);
    }
    ReleaseSysCache(proc_tup);

    /* Save pointer in FmgrInfo to avoid search on subsequent calls. */
    fcinfo->flinfo->fn_extra = (void*)func;

    /* Finally return the compiled function */
    return func;
}

/*
 * @Description: whether add an explicit return statement or not.
 * @in function -	the function to check
 * @in num_out_args -out args number
 * @out - bool return true if id is need to add a dummy return
 */
static bool whether_add_return(PLpgSQL_function* func, int num_out_args)
{
    if (num_out_args == 1 || (num_out_args == 0 && func->fn_rettype == VOIDOID ) || func->fn_retset ||
        func->fn_rettype == RECORDOID) {
        return true;
    } else {
        return false;
    }
}
/*
 * This is the slow part of plpgsql_compile().
 *
 * The passed-in "function" pointer is either NULL or an already-allocated
 * function struct to overwrite.
 *
 * While compiling a function, the CurrentMemoryContext is the
 * per-function memory context of the function we are compiling. That
 * means a palloc() will allocate storage with the same lifetime as
 * the function itself.
 *
 * Because palloc()'d storage will not be immediately freed, temporary
 * allocations should either be performed in a short-lived memory
 * context or explicitly pfree'd. Since not all backend functions are
 * careful about pfree'ing their allocations, it is also wise to
 * switch into a short-term context before calling into the
 * backend. An appropriate context for performing short-term
 * allocations is the u_sess->plsql_cxt.compile_tmp_cxt.
 *
 * NB: this code is not re-entrant.  We assume that nothing we do here could
 * result in the invocation of another plpgsql function.
 */
static PLpgSQL_function* do_compile(FunctionCallInfo fcinfo, HeapTuple proc_tup, PLpgSQL_function* func,
    PLpgSQL_func_hashkey* hashkey, bool for_validator)
{
    Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
    bool is_trigger = CALLED_AS_TRIGGER(fcinfo);
    Datum prosrcdatum;
    bool isnull = false;
    char* proc_source = NULL;
    HeapTuple type_tup = NULL;
    Form_pg_type type_struct = NULL;
    PLpgSQL_variable* var = NULL;
    PLpgSQL_rec* rec = NULL;
    int i;
    ErrorContextCallback pl_err_context;
    int parse_rc;
    Oid rettypeid;
    int numargs;
    int num_in_args = 0;
    int num_out_args = 0;
    Oid* arg_types = NULL;
    char** argnames;
    char* arg_modes = NULL;
    int* in_arg_varnos = NULL;
    PLpgSQL_variable** out_arg_variables;

    Oid* saved_pseudo_current_userId = NULL;
    char* signature = NULL;

    List* current_searchpath = NIL;
    char* namespace_name = NULL;

    char context_name[NAMEDATALEN] = {0};
    int rc = 0;
    const int alloc_size = 128;

    /*
     * Setup the scanner input and error info.	We assume that this function
     * cannot be invoked recursively, so there's no need to save and restore
     * the static variables used here.
     */
    prosrcdatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_prosrc, &isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The definition of the function is null"),
                errhint("Check whether the definition of the function is complete in the pg_proc system table.")));
    }
    
    proc_source = TextDatumGetCString(prosrcdatum);
    plpgsql_scanner_init(proc_source);

    u_sess->plsql_cxt.plpgsql_error_funcname = pstrdup(NameStr(proc_struct->proname));

    /*
     * Setup error traceback support for ereport()
     */
    pl_err_context.callback = plpgsql_compile_error_callback;
    pl_err_context.arg = for_validator ? proc_source : NULL;
    pl_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pl_err_context;

    /*
     * Do extra syntax checks when validating the function definition. We skip
     * this when actually compiling functions for execution, for performance
     * reasons.
     */
    u_sess->plsql_cxt.plpgsql_check_syntax = for_validator;

    /*
     * Create the new function struct, if not done already.  The function
     * structs are never thrown away, so keep them in session memory context.
     */
    if (func == NULL) {
        func = (PLpgSQL_function*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(PLpgSQL_function));
    } else {
        /* re-using a previously existing struct, so clear it out */
        rc = memset_s(func, sizeof(PLpgSQL_function), 0, sizeof(PLpgSQL_function));
        securec_check(rc, "\0", "\0");
    }
    u_sess->plsql_cxt.plpgsql_curr_compile = func;
    u_sess->plsql_cxt.plpgsql_IndexErrorVariable = 0;

    signature = format_procedure(fcinfo->flinfo->fn_oid);
    /*
     * All the permanent output of compilation (e.g. parse tree) is kept in a
     * per-function memory context, so it can be reclaimed easily.
     */
    rc = snprintf_s(
        context_name, NAMEDATALEN, NAMEDATALEN - 1, "%s_%lu", "PL/pgSQL function context", u_sess->debug_query_id);
    securec_check_ss(rc, "", "");

    u_sess->plsql_cxt.plpgsql_func_cxt = AllocSetContextCreate(u_sess->top_mem_cxt,
        context_name,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * compile_tmp_cxt is a short temp context that will be detroyed after
     * function compile or execute.
     * func_cxt is a long term context that will stay until thread exit. So
     * malloc on func_cxt should be very careful.
     * signature is stored on a StringInfoData which is 1K byte at least, but
     * most signature will not be so long originally, so we should do a strdup.
     */
    u_sess->plsql_cxt.compile_tmp_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.plpgsql_func_cxt);
    func->fn_signature = pstrdup(signature);
    func->fn_searchpath = (OverrideSearchPath*)palloc0(sizeof(OverrideSearchPath));
    func->fn_owner = proc_struct->proowner;
    func->fn_oid = fcinfo->flinfo->fn_oid;
    func->fn_xmin = HeapTupleGetRawXmin(proc_tup);
    func->fn_tid = proc_tup->t_self;
    func->fn_is_trigger = is_trigger;
    func->fn_input_collation = fcinfo->fncollation;
    func->fn_cxt = u_sess->plsql_cxt.plpgsql_func_cxt;
    func->out_param_varno = -1; /* set up for no OUT param */
    func->resolve_option = (PLpgSQL_resolve_option)u_sess->plsql_cxt.plpgsql_variable_conflict;

    func->fn_searchpath->addCatalog = true;
    func->fn_searchpath->addTemp = true;
    if (proc_struct->pronamespace == PG_CATALOG_NAMESPACE) {
        current_searchpath = fetch_search_path(false);
        if (current_searchpath == NIL) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("the search_path is empty while the porc belongs to pg_catalog ")));
        }
        namespace_name = get_namespace_name(linitial_oid(current_searchpath));
        if (namespace_name == NULL) {
            list_free_ext(current_searchpath);
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("cannot find the namespace according to search_path")));
        }
        func->fn_searchpath->schemas = current_searchpath;
    } else {
        /* Assign namespace of current function to fn_searchpath */
        func->fn_searchpath->schemas = list_make1_oid(proc_struct->pronamespace);

        if (SUPPORT_BIND_SEARCHPATH) {
            /*
             * If SUPPORT_BIND_SEARCHPATH is true,
             * add system's search_path to fn_searchpath.
             * When the relation of other objects cannot be
             * found in the namespace of current function,
             * find them in search_path list.
             * Otherwise, we only find objects in the namespace
             * of current function.
             */

            ListCell* l = NULL;

            /* If u_sess->catalog_cxt.namespaceUser and roleid are not equeal,
             * then u_sess->catalog_cxt.baseSearchPath doesn't
             * contain currentUser schema.currenUser schema will be added in
             * PushOverrideSearchPath.
             * 
             * It can happen executing following statements.
             * 
             * create temp table t1(a int);
             * \d t1                     --(get schema pg_temp_xxx)
             * drop table t1;
             * drop schema pg_temp_xxx cascade;
             * call proc1()              --(proc1 contains create temp table statement)
             */
            Oid roleid = GetUserId();
            if (u_sess->catalog_cxt.namespaceUser != roleid) {
                func->fn_searchpath->addUser = true;
            }
            /* Use baseSearchPath not activeSearchPath. */
            foreach (l, u_sess->catalog_cxt.baseSearchPath) {
                Oid namespaceId = lfirst_oid(l);
                /*
                 * Append namespaceId to fn_searchpath directly.
                 */
                func->fn_searchpath->schemas = lappend_oid(func->fn_searchpath->schemas, namespaceId);
            }
        }
    }

    pfree_ext(signature);

    /*
     * Initialize the compiler, particularly the namespace stack.  The
     * outermost namespace contains function parameters and other special
     * variables (such as FOUND), and is named after the function itself.
     */
    plpgsql_ns_init();
    plpgsql_ns_push(NameStr(proc_struct->proname));
    u_sess->plsql_cxt.plpgsql_DumpExecTree = false;

    u_sess->plsql_cxt.datums_alloc = alloc_size;
    u_sess->plsql_cxt.plpgsql_nDatums = 0;
    /* This is short-lived, so needn't allocate in function's cxt */
    u_sess->plsql_cxt.plpgsql_Datums = (PLpgSQL_datum**)MemoryContextAlloc(
        u_sess->plsql_cxt.compile_tmp_cxt, sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.datums_alloc);
    u_sess->plsql_cxt.datums_last = 0;

    switch ((int)is_trigger) {
        case false:

            /*
             * Fetch info about the procedure's parameters. Allocations aren't
             * needed permanently, so make them in tmp cxt.
             *
             * We also need to resolve any polymorphic input or output
             * argument types.	In validation mode we won't be able to, so we
             * arbitrarily assume we are dealing with integers.
             */
            MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);

            numargs = get_func_arg_info(proc_tup, &arg_types, &argnames, &arg_modes);

            plpgsql_resolve_polymorphic_argtypes(numargs, arg_types, arg_modes,
                fcinfo->flinfo->fn_expr, for_validator, u_sess->plsql_cxt.plpgsql_error_funcname);

            in_arg_varnos = (int*)palloc0(numargs * sizeof(int));
            out_arg_variables = (PLpgSQL_variable**)palloc0(numargs * sizeof(PLpgSQL_variable*));

            MemoryContextSwitchTo(u_sess->plsql_cxt.plpgsql_func_cxt);

            /*
             * Create the variables for the procedure's parameters.
             */
            for (i = 0; i < numargs; i++) {
                const int buf_size = 32;
                char buf[buf_size];
                Oid arg_type_id = arg_types[i];
                char arg_mode = arg_modes ? arg_modes[i] : PROARGMODE_IN;
                PLpgSQL_type* argdtype = NULL;
                PLpgSQL_variable* argvariable = NULL;
                int arg_item_type;
                errno_t err = EOK;

                /* Create $n name for variable */
                err = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "$%d", i + 1);
                securec_check_ss(err, "", "");

                /* Create datatype info */
                argdtype = plpgsql_build_datatype(arg_type_id, -1, func->fn_input_collation);

                /* Disallow pseudotype argument */
                /* (note we already replaced polymorphic types) */
                /* (build_variable would do this, but wrong message) */
                if (argdtype->ttype != PLPGSQL_TTYPE_SCALAR && argdtype->ttype != PLPGSQL_TTYPE_ROW) {
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("PL/pgSQL functions cannot accept type %s", format_type_be(arg_type_id))));
                }
                /* Build variable and add to datum list */
                if (argnames != NULL) {
                    argvariable = plpgsql_build_variable(buf, 0, argdtype, false, argnames[i]);
                } else {
                    argvariable = plpgsql_build_variable(buf, 0, argdtype, false);
                }

                if (argvariable->dtype == PLPGSQL_DTYPE_VAR) {
                    arg_item_type = PLPGSQL_NSTYPE_VAR;
                } else {
                    AssertEreport(
                        argvariable->dtype == PLPGSQL_DTYPE_ROW, MOD_PLSQL, "variable type should be dtype row.");
                    arg_item_type = PLPGSQL_NSTYPE_ROW;
                }

                /* Remember arguments in appropriate arrays */
                if (arg_mode == PROARGMODE_IN || arg_mode == PROARGMODE_INOUT 
                    || arg_mode == PROARGMODE_VARIADIC) {
                    in_arg_varnos[num_in_args++] = argvariable->dno;
                }

                if (arg_mode == PROARGMODE_OUT || arg_mode == PROARGMODE_INOUT 
                    || arg_mode == PROARGMODE_TABLE) {
                    out_arg_variables[num_out_args++] = argvariable;
                }

                /* Add to namespace under the $n name */
                add_parameter_name(arg_item_type, argvariable->dno, buf);

                /* If there's a name for the argument, make an alias */
                if (argnames != NULL && argnames[i][0] != '\0') {
                    add_parameter_name(arg_item_type, argvariable->dno, argnames[i]);
                }
            }

            /*
             * If there's just one OUT parameter, out_param_varno points
             * directly to it.	If there's more than one, build a row that
             * holds all of them.
             */
            if (num_out_args == 1) {
                func->out_param_varno = out_arg_variables[0]->dno;
            } else if (num_out_args > 1) {
                PLpgSQL_row* row = build_row_from_vars(out_arg_variables, num_out_args);

                plpgsql_adddatum((PLpgSQL_datum*)row);
                func->out_param_varno = row->dno;
            }

            /*
             * Check for a polymorphic returntype. If found, use the actual
             * returntype type from the caller's FuncExpr node, if we have
             * one.  (In validation mode we arbitrarily assume we are dealing
             * with integers.)
             *
             * Note: errcode is FEATURE_NOT_SUPPORTED because it should always
             * work; if it doesn't we're in some context that fails to make
             * the info available.
             */
            rettypeid = proc_struct->prorettype;
            if (IsPolymorphicType(rettypeid)) {
                if (for_validator) {
                    if (rettypeid == ANYARRAYOID) {
                        rettypeid = INT4ARRAYOID;
                    } else if (rettypeid == ANYRANGEOID) {
                        rettypeid = INT4RANGEOID;
                    } else { /* ANYELEMENT or ANYNONARRAY */
                        rettypeid = INT4OID;
                    } /* XXX what could we use for ANYENUM? */
                } else {
                    rettypeid = get_fn_expr_rettype(fcinfo->flinfo);
                    if (!OidIsValid(rettypeid)) {
                        ereport(ERROR, (errmodule(MOD_PLSQL),  errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("could not determine actual return type for polymorphic function \"%s\"",
                                    u_sess->plsql_cxt.plpgsql_error_funcname)));
                    }
                }
            }

            /*
             * Normal function has a defined returntype
             */
            func->fn_rettype = rettypeid;
            func->fn_retset = proc_struct->proretset;

            /*
             * Lookup the function's return type
             */
            type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(rettypeid));
            if (!HeapTupleIsValid(type_tup)) {
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for type %u while compile function.", rettypeid)));
            }
            type_struct = (Form_pg_type)GETSTRUCT(type_tup);

            /* Disallow pseudotype result, except VOID or RECORD */
            /* (note we already replaced polymorphic types) */
            if (type_struct->typtype == TYPTYPE_PSEUDO) {
                if (rettypeid == VOIDOID || rettypeid == RECORDOID) {
                    /* okay */;
                } else if (rettypeid == TRIGGEROID) {
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("trigger functions can only be called as triggers")));
                } else {
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("PL/pgSQL functions cannot return type %s", format_type_be(rettypeid))));
                }
            }

            if (type_struct->typrelid != InvalidOid || rettypeid == RECORDOID) {
                func->fn_retistuple = true;
            } else {
                func->fn_retbyval = type_struct->typbyval;
                func->fn_rettyplen = type_struct->typlen;
                func->fn_rettypioparam = getTypeIOParam(type_tup);
                fmgr_info(type_struct->typinput, &(func->fn_retinput));

                /*
                 * install $0 reference, but only for polymorphic return
                 * types, and not when the return is specified through an
                 * output parameter.
                 */
                if (IsPolymorphicType(proc_struct->prorettype) && num_out_args == 0) {
                    (void)plpgsql_build_variable(
                        "$0", 0, build_datatype(type_tup, -1, func->fn_input_collation), true);
                }
            }
            ReleaseSysCache(type_tup);
            break;

        case true:
            /* Trigger procedure's return type is unknown yet */
            func->fn_rettype = InvalidOid;
            func->fn_retbyval = false;
            func->fn_retistuple = true;
            func->fn_retset = false;

            /* shouldn't be any declared arguments */
            if (proc_struct->pronargs != 0) {
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("trigger functions cannot have declared arguments"),
                        errhint("The arguments of the trigger can be accessed through TG_NARGS and TG_ARGV instead.")));
            }
            /* Add the record for referencing NEW */
            rec = plpgsql_build_record("new", 0, true);
            func->new_varno = rec->dno;

            /* Add the record for referencing OLD */
            rec = plpgsql_build_record("old", 0, true);
            func->old_varno = rec->dno;

            /* Add the variable tg_name */
            var = plpgsql_build_variable("tg_name", 0, plpgsql_build_datatype(NAMEOID, -1, InvalidOid), true);
            func->tg_name_varno = var->dno;

            /* Add the variable tg_when */
            var = plpgsql_build_variable(
                "tg_when", 0, plpgsql_build_datatype(TEXTOID, -1, func->fn_input_collation), true);
            func->tg_when_varno = var->dno;

            /* Add the variable tg_level */
            var = plpgsql_build_variable(
                "tg_level", 0, plpgsql_build_datatype(TEXTOID, -1, func->fn_input_collation), true);
            func->tg_level_varno = var->dno;

            /* Add the variable tg_op */
            var = plpgsql_build_variable(
                "tg_op", 0, plpgsql_build_datatype(TEXTOID, -1, func->fn_input_collation), true);
            func->tg_op_varno = var->dno;

            /* Add the variable tg_relid */
            var = plpgsql_build_variable("tg_relid", 0, plpgsql_build_datatype(OIDOID, -1, InvalidOid), true);
            func->tg_relid_varno = var->dno;

            /* Add the variable tg_relname */
            var = plpgsql_build_variable("tg_relname", 0, plpgsql_build_datatype(NAMEOID, -1, InvalidOid), true);
            func->tg_relname_varno = var->dno;

            /* tg_table_name is now preferred to tg_relname */
            var = plpgsql_build_variable("tg_table_name", 0, plpgsql_build_datatype(NAMEOID, -1, InvalidOid), true);
            func->tg_table_name_varno = var->dno;

            /* add the variable tg_table_schema */
            var = plpgsql_build_variable("tg_table_schema", 0, plpgsql_build_datatype(NAMEOID, -1, InvalidOid), true);
            func->tg_table_schema_varno = var->dno;

            /* Add the variable tg_nargs */
            var = plpgsql_build_variable("tg_nargs", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true);
            func->tg_nargs_varno = var->dno;

            /* Add the variable tg_argv */
            var = plpgsql_build_variable(
                "tg_argv", 0, plpgsql_build_datatype(TEXTARRAYOID, -1, func->fn_input_collation), true);
            func->tg_argv_varno = var->dno;

            break;

        default:
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized function typecode: %d", (int)is_trigger),
                    errhint("This node type is expected to be a function or trigger.")));
            break;
    }

    /* Remember if function is STABLE/IMMUTABLE */
    func->fn_readonly = (proc_struct->provolatile != PROVOLATILE_VOLATILE);

    /*
     * Create the magic FOUND variable.
     */
    var = plpgsql_build_variable("found", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->found_varno = var->dno;
    /* Create the magic FOUND variable for implicit cursor attribute %found. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_found__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->sql_cursor_found_varno = var->dno;

    /* Create the magic NOTFOUND variable for implicit cursor attribute %notfound. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_notfound__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->sql_notfound_varno = var->dno;

    /* Create the magic ISOPEN variable for implicit cursor attribute %isopen. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_isopen__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->sql_isopen_varno = var->dno;

    /* Create the magic ROWCOUNT variable for implicit cursor attribute %rowcount. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_rowcount__", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true);
    func->sql_rowcount_varno = var->dno;

    PushOverrideSearchPath(func->fn_searchpath);

    saved_pseudo_current_userId = u_sess->misc_cxt.Pseudo_CurrentUserId;
    u_sess->misc_cxt.Pseudo_CurrentUserId = &func->fn_owner;

    // Create the magic sqlcode variable marco
    var = plpgsql_build_variable("sqlcode", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true);
    func->sqlcode_varno = var->dno;

    /*
     * Now parse the function's text
     */
    parse_rc = plpgsql_yyparse();
    if (parse_rc != 0) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
    }
    func->action = u_sess->plsql_cxt.plpgsql_parse_result;
    func->goto_labels = u_sess->plsql_cxt.goto_labels;

    plpgsql_scanner_finish();
    pfree_ext(proc_source);

    PopOverrideSearchPath();

    u_sess->misc_cxt.Pseudo_CurrentUserId = saved_pseudo_current_userId;

    /*
     * If it has OUT parameters or returns VOID or returns a set, we allow
     * control to fall off the end without an explicit RETURN statement. The
     * easiest way to implement this is to add a RETURN statement to the end
     * of the statement list during parsing.
     * If it has more than one OUT parameters and returns not a set, we will not
     * allow to add a dummy return if there is not an explicit RETURN statement.
     */
    if (whether_add_return(func, num_out_args)) {
        add_dummy_return(func);
    }

    /*
     * Complete the function's info
     */
    func->fn_nargs = proc_struct->pronargs;
    for (i = 0; i < func->fn_nargs; i++) {
        func->fn_argvarnos[i] = in_arg_varnos[i];
    }
    func->ndatums = u_sess->plsql_cxt.plpgsql_nDatums;
    func->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.plpgsql_nDatums);
    for (i = 0; i < u_sess->plsql_cxt.plpgsql_nDatums; i++) {
        func->datums[i] = u_sess->plsql_cxt.plpgsql_Datums[i];
    }
    /* Debug dump for completed functions */
    if (u_sess->plsql_cxt.plpgsql_DumpExecTree) {
        plpgsql_dumptree(func);
    }
    /*
     * add it to the hash table except specified function.
     */
    if (hashkey && is_func_need_cache(func->fn_oid, NameStr(proc_struct->proname))) {
        plpgsql_HashTableInsert(func, hashkey);
        uint32 key = SPICacheHashFunc(func->fn_hashkey, sizeof(PLpgSQL_func_hashkey));
        SPICacheTableInsert(key, func->fn_oid);
    }

    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = pl_err_context.previous;
    u_sess->plsql_cxt.plpgsql_error_funcname = NULL;

    u_sess->plsql_cxt.plpgsql_check_syntax = false;

    MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    u_sess->plsql_cxt.compile_tmp_cxt = NULL;
    return func;
}

/* ----------
 * plpgsql_compile_inline	Make an execution tree for an anonymous code block.
 *
 * Note: this is generally parallel to do_compile(); is it worth trying to
 * merge the two?
 *
 * Note: we assume the block will be thrown away so there is no need to build
 * persistent data structures.
 * ----------
 */
PLpgSQL_function* plpgsql_compile_inline(char* proc_source)
{
    const char* func_name = "inline_code_block";
    PLpgSQL_function* func = NULL;
    ErrorContextCallback pl_err_context;
    Oid typinput;
    PLpgSQL_variable* var = NULL;
    int parse_rc;
    MemoryContext func_cxt;
    int i;
    const int alloc_size = 128;
    /*
     * Setup the scanner input and error info.	We assume that this function
     * cannot be invoked recursively, so there's no need to save and restore
     * the static variables used here.
     */
    plpgsql_scanner_init(proc_source);

    u_sess->plsql_cxt.plpgsql_error_funcname = pstrdup(func_name);

    /*
     * Setup error traceback support for ereport()
     */
    pl_err_context.callback = plpgsql_compile_error_callback;
    pl_err_context.arg = proc_source;
    pl_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pl_err_context;

    /* Do extra syntax checking if u_sess->attr.attr_sql.check_function_bodies is on */
    u_sess->plsql_cxt.plpgsql_check_syntax = u_sess->attr.attr_sql.check_function_bodies;

    /* Function struct does not live past current statement */
    func = (PLpgSQL_function*)palloc0(sizeof(PLpgSQL_function));

    u_sess->plsql_cxt.plpgsql_curr_compile = func;

    /*
     * All the rest of the compile-time storage (e.g. parse tree) is kept in
     * its own memory context, so it can be reclaimed easily.
     */
    func_cxt = AllocSetContextCreate(CurrentMemoryContext,
        "PL/pgSQL function context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    u_sess->plsql_cxt.compile_tmp_cxt = MemoryContextSwitchTo(func_cxt);

    func->fn_signature = pstrdup(func_name);
    func->fn_is_trigger = false;
    func->fn_input_collation = InvalidOid;
    func->fn_cxt = func_cxt;
    func->out_param_varno = -1; /* set up for no OUT param */
    func->resolve_option = (PLpgSQL_resolve_option)u_sess->plsql_cxt.plpgsql_variable_conflict;

    plpgsql_ns_init();
    plpgsql_ns_push(func_name);
    u_sess->plsql_cxt.plpgsql_DumpExecTree = false;

    u_sess->plsql_cxt.datums_alloc = alloc_size;
    u_sess->plsql_cxt.plpgsql_nDatums = 0;
    u_sess->plsql_cxt.plpgsql_Datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.datums_alloc);
    u_sess->plsql_cxt.datums_last = 0;

    /* Set up as though in a function returning VOID */
    func->fn_rettype = VOIDOID;
    func->fn_retset = false;
    func->fn_retistuple = false;
    /* a bit of hardwired knowledge about type VOID here */
    func->fn_retbyval = true;
    func->fn_rettyplen = sizeof(int32);
    getTypeInputInfo(VOIDOID, &typinput, &func->fn_rettypioparam);
    fmgr_info(typinput, &(func->fn_retinput));

    /*
     * Remember if function is STABLE/IMMUTABLE.  XXX would it be better to
     * set this TRUE inside a read-only transaction?  Not clear.
     */
    func->fn_readonly = false;

    /*
     * Create the magic FOUND variable.
     */
    var = plpgsql_build_variable("found", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->found_varno = var->dno;

    /* Create the magic FOUND variable for implicit cursor attribute %found. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_found__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->sql_cursor_found_varno = var->dno;

    /* Create the magic NOTFOUND variable for implicit cursor attribute %notfound. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_notfound__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->sql_notfound_varno = var->dno;

    /* Create the magic ISOPEN variable for implicit cursor attribute %isopen. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_isopen__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    func->sql_isopen_varno = var->dno;

    /* Create the magic ROWCOUNT variable for implicit cursor attribute %rowcount. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_rowcount__", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true);
    func->sql_rowcount_varno = var->dno;

    /*
     * Now parse the function's text
     */
    parse_rc = plpgsql_yyparse();
    if (parse_rc != 0) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
    }
    func->action = u_sess->plsql_cxt.plpgsql_parse_result;
    func->goto_labels = u_sess->plsql_cxt.goto_labels;

    plpgsql_scanner_finish();

    /*
     * If it returns VOID (always true at the moment), we allow control to
     * fall off the end without an explicit RETURN statement.
     */
    if (func->fn_rettype == VOIDOID) {
        add_dummy_return(func);
    }
    /*
     * Complete the function's info
     */
    func->fn_nargs = 0;
    func->ndatums = u_sess->plsql_cxt.plpgsql_nDatums;
    func->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.plpgsql_nDatums);
    for (i = 0; i < u_sess->plsql_cxt.plpgsql_nDatums; i++) {
        func->datums[i] = u_sess->plsql_cxt.plpgsql_Datums[i];
    }
    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = pl_err_context.previous;
    u_sess->plsql_cxt.plpgsql_error_funcname = NULL;

    u_sess->plsql_cxt.plpgsql_check_syntax = false;

    MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    u_sess->plsql_cxt.compile_tmp_cxt = NULL;
    return func;
}

/*
 * error context callback to let us supply a call-stack traceback.
 * If we are validating or executing an anonymous code block, the function
 * source text is passed as an argument.
 */
static void plpgsql_compile_error_callback(void* arg)
{
    if (arg != NULL) {
        /*
         * Try to convert syntax error position to reference text of original
         * CREATE FUNCTION or DO command.
         */
        if (function_parse_error_transpose((const char*)arg)) {
            return;
        }
        /*
         * Done if a syntax error position was reported; otherwise we have to
         * fall back to a "near line N" report.
         */
    }

    if (u_sess->plsql_cxt.plpgsql_error_funcname) {
        errcontext("compilation of PL/pgSQL function \"%s\" near line %d",
            u_sess->plsql_cxt.plpgsql_error_funcname, plpgsql_latest_lineno());
    }
}

/*
 * Add a name for a function parameter to the function's namespace
 */
static void add_parameter_name(int item_type, int item_no, const char* name)
{
    /*
     * Before adding the name, check for duplicates.  We need this even though
     * functioncmds.c has a similar check, because that code explicitly
     * doesn't complain about conflicting IN and OUT parameter names.  In
     * plpgsql, such names are in the same namespace, so there is no way to
     * disambiguate.
     */
    if (plpgsql_ns_lookup(plpgsql_ns_top(), true, name, NULL, NULL, NULL) != NULL) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("parameter name \"%s\" used more than once", name)));
    }
    /* OK, add the name */
    plpgsql_ns_additem(item_type, item_no, name);
}

/*
 * Add a dummy RETURN statement to the given function's body
 */
static void add_dummy_return(PLpgSQL_function* func)
{
    /*
     * If the outer block has an EXCEPTION clause, we need to make a new outer
     * block, since the added RETURN shouldn't act like it is inside the
     * EXCEPTION clause.
     */
    if (func->action->exceptions != NULL) {
        PLpgSQL_stmt_block* newm = NULL;

        newm = (PLpgSQL_stmt_block*)palloc0(sizeof(PLpgSQL_stmt_block));
        newm->cmd_type = PLPGSQL_STMT_BLOCK;
        newm->body = list_make1(func->action);

        func->action = newm;
    }
    if (func->action->body == NIL ||
        ((PLpgSQL_stmt*)llast(func->action->body))->cmd_type != PLPGSQL_STMT_RETURN) {
        PLpgSQL_stmt_return* newm = NULL;

        newm = (PLpgSQL_stmt_return*)palloc0(sizeof(PLpgSQL_stmt_return));
        newm->cmd_type = PLPGSQL_STMT_RETURN;
        newm->expr = NULL;
        newm->retvarno = func->out_param_varno;

        func->action->body = lappend(func->action->body, newm);
    }
}

/*
 * plpgsql_parser_setup		set up parser hooks for dynamic parameters
 *
 * Note: this routine, and the hook functions it prepares for, are logically
 * part of plpgsql parsing.  But they actually run during function execution,
 * when we are ready to evaluate a SQL query or expression that has not
 * previously been parsed and planned.
 */
void plpgsql_parser_setup(struct ParseState* pstate, PLpgSQL_expr* expr)
{
    pstate->p_pre_columnref_hook = plpgsql_pre_column_ref;
    pstate->p_post_columnref_hook = plpgsql_post_column_ref;
    pstate->p_paramref_hook = plpgsql_param_ref;
    /* no need to use p_coerce_param_hook */
    pstate->p_ref_hook_state = (void*)expr;
}

/*
 * plpgsql_pre_column_ref		parser callback before parsing a ColumnRef
 */
static Node* plpgsql_pre_column_ref(ParseState* pstate, ColumnRef* cref)
{
    PLpgSQL_expr* expr = (PLpgSQL_expr*)pstate->p_ref_hook_state;

    if (expr->func->resolve_option == PLPGSQL_RESOLVE_VARIABLE) {
        return resolve_column_ref(pstate, expr, cref, false);
    } else {
        return NULL;
    }
}

/*
 * plpgsql_post_column_ref		parser callback after parsing a ColumnRef
 */
static Node* plpgsql_post_column_ref(ParseState* pstate, ColumnRef* cref, Node* var)
{
    PLpgSQL_expr* expr = (PLpgSQL_expr*)pstate->p_ref_hook_state;
    Node* myvar = NULL;

    if (expr->func->resolve_option == PLPGSQL_RESOLVE_VARIABLE) {
        return NULL; /* we already found there's no match */
    }

    if (expr->func->resolve_option == PLPGSQL_RESOLVE_COLUMN && var != NULL) {
        return NULL; /* there's a table column, prefer that */
    }
    /*
     * If we find a record/row variable but can't match a field name, throw
     * error if there was no core resolution for the ColumnRef either.	In
     * that situation, the reference is inevitably going to fail, and
     * complaining about the record/row variable is likely to be more on-point
     * than the core parser's error message.  (It's too bad we don't have
     * access to transformColumnRef's internal crerr state here, as in case of
     * a conflict with a table name this could still be less than the most
     * helpful error message possible.)
     */
    myvar = resolve_column_ref(pstate, expr, cref, (var == NULL));

    if (myvar != NULL && var != NULL) {
        /*
         * We could leave it to the core parser to throw this error, but we
         * can add a more useful detail message than the core could.
         */
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_AMBIGUOUS_COLUMN),
                errmsg("column reference \"%s\" is ambiguous", NameListToString(cref->fields)),
                errdetail("It could refer to either a PL/pgSQL variable or a table column."),
                parser_errposition(pstate, cref->location)));
    }

    return myvar;
}

/*
 * plpgsql_param_ref		parser callback for ParamRefs ($n symbols)
 */
static Node* plpgsql_param_ref(ParseState* pstate, ParamRef* pref)
{
    PLpgSQL_expr* expr = (PLpgSQL_expr*)pstate->p_ref_hook_state;
    const int name_size = 32;
    char pname[name_size];
    PLpgSQL_nsitem* nse = NULL;
    errno_t rc = EOK;

    rc = snprintf_s(pname, sizeof(pname), sizeof(pname) - 1, "$%d", pref->number);
    securec_check_ss(rc, "\0", "\0");

    nse = plpgsql_ns_lookup(expr->ns, false, pname, NULL, NULL, NULL);

    if (nse == NULL) {
        return NULL; /* name not known to plpgsql */
    }
    return make_datum_param(expr, nse->itemno, pref->location);
}

/*
 * resolve_column_ref		attempt to resolve a ColumnRef as a plpgsql var
 *
 * Returns the translated node structure, or NULL if name not found
 *
 * error_if_no_field tells whether to throw error or quietly return NULL if
 * we are able to match a record/row name but don't find a field name match.
 */
static Node* resolve_column_ref(ParseState* pstate, PLpgSQL_expr* expr, ColumnRef* cref, bool error_if_no_field)
{
    PLpgSQL_execstate* estate = NULL;
    PLpgSQL_nsitem* nse = NULL;
    const char* name1 = NULL;
    const char* name2 = NULL;
    const char* name3 = NULL;
    const char* colname = NULL;
    int nnames;
    int nnames_scalar = 0;
    int nnames_wholerow = 0;
    int nnames_field = 0;

    bool pre_parse_trig = expr->func->pre_parse_trig;

    /*
     * We use the function's current estate to resolve parameter data types.
     * This is really pretty bogus because there is no provision for updating
     * plans when those types change ...
     * Notice: cur_estate is not valid at pre-parse trigger time.
     */
    if (!pre_parse_trig) {
        estate = expr->func->cur_estate;
    }
    /* ----------
     * The allowed syntaxes are:
     *
     * A		Scalar variable reference, or whole-row record reference.
     * A.B		Qualified scalar or whole-row reference, or field reference.
     * A.B.C	Qualified record field reference.
     * A.*		Whole-row record reference.
     * A.B.*	Qualified whole-row record reference.
     * ----------
     */
    switch (list_length(cref->fields)) {
        case 1: {
            Node* field1 = (Node*)linitial(cref->fields);

            AssertEreport(IsA(field1, String), MOD_PLSQL, "string type is required.");
            name1 = strVal(field1);
            nnames_scalar = 1;
            nnames_wholerow = 1;
            break;
        }
        case 2: {
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);

            AssertEreport(IsA(field1, String), MOD_PLSQL, "string type is required.");
            name1 = strVal(field1);

            /* Whole-row reference? */
            if (IsA(field2, A_Star)) {
                /* Set name2 to prevent matches to scalar variables */
                name2 = "*";
                nnames_wholerow = 1;
                break;
            }

            AssertEreport(IsA(field2, String), MOD_PLSQL, "string type is required.");
            name2 = strVal(field2);
            colname = name2;
            nnames_scalar = 2;
            nnames_wholerow = 2;
            nnames_field = 1;
            break;
        }
        case 3: {
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);

            AssertEreport(IsA(field1, String), MOD_PLSQL, "string type is required.");
            name1 = strVal(field1);
            AssertEreport(IsA(field2, String), MOD_PLSQL, "string type is required.");
            name2 = strVal(field2);

            /* Whole-row reference? */
            if (IsA(field3, A_Star)) {
                /* Set name3 to prevent matches to scalar variables */
                name3 = "*";
                nnames_wholerow = 2;
                break;
            }

            AssertEreport(IsA(field3, String), MOD_PLSQL, "string type is required.");
            name3 = strVal(field3);
            colname = name3;
            nnames_field = 2;
            break;
        }
        default:
            /* too many names, ignore */
            return NULL;
    }

    nse = plpgsql_ns_lookup(expr->ns, false, name1, name2, name3, &nnames);

    if (nse == NULL) {
        return NULL; /* name not known to plpgsql */
    }
    switch (nse->itemtype) {
        case PLPGSQL_NSTYPE_VAR:
            if (nnames == nnames_scalar) {
                return make_datum_param(expr, nse->itemno, cref->location);
            }
            break;
        case PLPGSQL_NSTYPE_REC:
            if (nnames == nnames_wholerow) {
                return make_datum_param(expr, nse->itemno, cref->location);
            }
            if (nnames == nnames_field) {
                /* colname could be a field in this record */
                int i;
                int ndatums;

                /* search for a datum referencing this field */
                if (pre_parse_trig) {
                    ndatums = expr->func->ndatums;
                } else {
                    ndatums = estate->ndatums;
                }
                for (i = 0; i < ndatums; i++) {
                    PLpgSQL_recfield* fld = NULL;

                    if (pre_parse_trig) {
                        fld = (PLpgSQL_recfield*)expr->func->datums[i];
                    } else {
                        fld = (PLpgSQL_recfield*)estate->datums[i];
                    }
                    if (fld->dtype == PLPGSQL_DTYPE_RECFIELD && fld->recparentno == nse->itemno &&
                        strcmp(fld->fieldname, colname) == 0) {
                        return make_datum_param(expr, i, cref->location);
                    }
                }

                /*
                 * We should not get here, because a RECFIELD datum should
                 * have been built at parse time for every possible qualified
                 * reference to fields of this record.	But if we do, handle
                 * it like field-not-found: throw error or return NULL.
                 */
                if (error_if_no_field) {
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("record \"%s\" has no field \"%s\"", (nnames_field == 1) ? name1 : name2, colname),
                            parser_errposition(pstate, cref->location)));
                }
            }
            break;
        case PLPGSQL_NSTYPE_ROW:
            if (nnames == nnames_wholerow) {
                return make_datum_param(expr, nse->itemno, cref->location);
            }
            if (nnames == nnames_field) {
                /* colname could be a field in this row */
                PLpgSQL_row* row = NULL;
                int i;

                if (pre_parse_trig) {
                    row = (PLpgSQL_row*)expr->func->datums[nse->itemno];
                } else {
                    row = (PLpgSQL_row*)estate->datums[nse->itemno];
                }
                for (i = 0; i < row->nfields; i++) {
                    if (row->fieldnames[i] && strcmp(row->fieldnames[i], colname) == 0) {
                        return make_datum_param(expr, row->varnos[i], cref->location);
                    }
                }
                /* Not found, so throw error or return NULL */
                if (error_if_no_field)
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("record \"%s\" has no field \"%s\"", (nnames_field == 1) ? name1 : name2, colname),
                            parser_errposition(pstate, cref->location)));
            }
            break;
        default:
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized plpgsql itemtype: %d, when resolve column reference", nse->itemtype)));
            break;
    }

    /* Name format doesn't match the plpgsql variable type */
    return NULL;
}

/*
 * Helper for columnref parsing: build a Param referencing a plpgsql datum,
 * and make sure that that datum is listed in the expression's paramnos.
 */
static Node* make_datum_param(PLpgSQL_expr* expr, int dno, int location)
{
    PLpgSQL_execstate* estate = NULL;
    PLpgSQL_datum* datum = NULL;
    Param* param = NULL;
    MemoryContext oldcontext = NULL;
    bool pre_parse_trig = expr->func->pre_parse_trig;

    /* see comment in resolve_column_ref */
    if (!pre_parse_trig) {
        estate = expr->func->cur_estate;
        AssertEreport(dno >= 0 && dno < estate->ndatums, MOD_PLSQL, "comment should be in resolve_column_ref");
        datum = estate->datums[dno];
    } else {
        estate = NULL;
        datum = expr->func->datums[dno];
    }

    /*
     * Bitmapset must be allocated in function's permanent memory context
     */
    oldcontext = MemoryContextSwitchTo(expr->func->fn_cxt);
    expr->paramnos = bms_add_member(expr->paramnos, dno);
    MemoryContextSwitchTo(oldcontext);

    param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = dno + 1;
    exec_get_datum_type_info(estate, datum, &param->paramtype, &param->paramtypmod, &param->paramcollid, expr->func);
    param->location = location;

    return (Node*)param;
}

/* ----------
 * plpgsql_parse_word		The scanner calls this to postparse
 *				any single word that is not a reserved keyword.
 *
 * word1 is the downcased/dequoted identifier; it must be palloc'd in the
 * function's long-term memory context.
 *
 * yytxt is the original token text; we need this to check for quoting,
 * so that later checks for unreserved keywords work properly.
 *
 * If recognized as a variable, fill in *wdatum and return TRUE;
 * if not recognized, fill in *word and return FALSE.
 * (Note: those two pointers actually point to members of the same union,
 * but for notational reasons we pass them separately.)
 * ----------
 */
/* param tok_flag is used to get self defined token flag */
bool plpgsql_parse_word(char* word1, const char* yytxt, PLwdatum* wdatum, PLword* word, int* tok_flag)
{
    PLpgSQL_nsitem* ns = NULL;
    PLpgSQL_var* var = NULL;

    ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);
    if (ns != NULL) {
        switch (ns->itemtype) {
            /* check cursor type variable, then get cursor type token */
            case PLPGSQL_NSTYPE_REFCURSOR:
                *tok_flag = PLPGSQL_TOK_REFCURSOR;
                return true;
            /* pass a tok_flag here to return T_VARRAY token outside plpgsql_parse_word() in yylex */
            case PLPGSQL_NSTYPE_VARRAY:
                *tok_flag = PLPGSQL_TOK_VARRAY;
                wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                wdatum->ident = word1;
                wdatum->quoted = false;
                wdatum->idents = NIL;
                return true;
            case PLPGSQL_NSTYPE_RECORD:
                *tok_flag = PLPGSQL_TOK_RECORD;
                wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                wdatum->ident = word1;
                wdatum->quoted = false;
                wdatum->idents = NIL;
                return true;
            case PLPGSQL_NSTYPE_VAR:
                var = (PLpgSQL_var*)u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                if (var != NULL && var->datatype != NULL && var->datatype->typinput.fn_oid == F_ARRAY_IN) {
                    *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
                    wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                    wdatum->ident = word1;
                    wdatum->quoted = false;
                    wdatum->idents = NIL;
                    return true;
                } else if (var != NULL && var->datatype && var->datatype->typinput.fn_oid == F_DOMAIN_IN) {
                    HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
                    if (HeapTupleIsValid(type_tuple)) {
                        Form_pg_type type_form = (Form_pg_type)GETSTRUCT(type_tuple);
                        if (F_ARRAY_OUT == type_form->typoutput) {
                            *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
                            wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                            wdatum->ident = word1;
                            wdatum->quoted = false;
                            wdatum->idents = NIL;
                            ReleaseSysCache(type_tuple);
                            return true;
                        }
                    }
                    ReleaseSysCache(type_tuple);
                }
                break;
            default:
                break;
        }
    }

    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, there's
     * no need to do anything either --- lookup will happen when the
     * expression is compiled.
     */
    if (u_sess->plsql_cxt.plpgsql_IdentifierLookup == IDENTIFIER_LOOKUP_NORMAL) {
        /*
         * Do a lookup in the current namespace stack
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);

        if (ns != NULL) {
            switch (ns->itemtype) {
                case PLPGSQL_NSTYPE_VAR:
                case PLPGSQL_NSTYPE_ROW:
                case PLPGSQL_NSTYPE_REC:
                case PLPGSQL_NSTYPE_RECORD:
                    wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                    wdatum->ident = word1;
                    wdatum->quoted = (yytxt[0] == '"');
                    wdatum->idents = NIL;
                    return true;

                default:
                    /* plpgsql_ns_lookup should never return anything else */
                    ereport(ERROR,
                        (errmodule(MOD_PLSQL),
                            errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized plpgsql itemtype: %d, the namespace linked list may be damaged.",
                                ns->itemtype)));
                    break;
            }
        }
    }

    /*
     * Nothing found - up to now it's a word without any special meaning for
     * us.
     */
    word->ident = word1;
    word->quoted = (yytxt[0] == '"');
    return false;
}

/* ----------
 * plpgsql_parse_dblword		Same lookup for two words
 *					separated by a dot.
 * ----------
 */
bool plpgsql_parse_dblword(char* word1, char* word2, PLwdatum* wdatum, PLcword* cword, int* nsflag)
{
    PLpgSQL_nsitem* ns = NULL;
    List* idents = NIL;
    int nnames;

    /*
     * String to compare with B in A.B,
     * compatible with A db's varray accesss method
     */
    const char* varray_first = "first";
    const char* varray_last = "last";
    const char* varray_count = "count";
    const char* varray_extend = "extend";
    if (word2 == NULL) {
        return false;
    }
    idents = list_make2(makeString(word1), makeString(word2));

    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (u_sess->plsql_cxt.plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE) {
        /*
         * Do a lookup in the current namespace stack
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, word2, NULL, &nnames);
        if (ns != NULL) {
            switch (ns->itemtype) {
                case PLPGSQL_NSTYPE_VAR:
                    /* Block-qualified reference to scalar variable. */
                    wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                    wdatum->ident = NULL;
                    wdatum->quoted = false; /* not used */
                    wdatum->idents = idents;
                    return true;

                case PLPGSQL_NSTYPE_REC:
                case PLPGSQL_NSTYPE_RECORD:
                    if (nnames == 1) {
                        /*
                         * First word is a record name, so second word could
                         * be a field in this record.  We build a RECFIELD
                         * datum whether it is or not --- any error will be
                         * detected later.
                         */
                        PLpgSQL_recfield* newm = NULL;

                        MemoryContext old_cxt = NULL;
                        if (u_sess->plsql_cxt.plpgsql_func_cxt != NULL)
                            old_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.plpgsql_func_cxt);
                        newm = (PLpgSQL_recfield*)palloc(sizeof(PLpgSQL_recfield));
                        newm->dtype = PLPGSQL_DTYPE_RECFIELD;
                        newm->fieldname = pstrdup(word2);
                        newm->recparentno = ns->itemno;

                        plpgsql_adddatum((PLpgSQL_datum*)newm);

                        if (old_cxt != NULL) {
                            MemoryContextSwitchTo(old_cxt);
                        }
                        wdatum->datum = (PLpgSQL_datum*)newm;
                    } else {
                        /* Block-qualified reference to record variable. */
                        wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                    }
                    wdatum->ident = NULL;
                    wdatum->quoted = false; /* not used */
                    wdatum->idents = idents;
                    return true;

                case PLPGSQL_NSTYPE_ROW:
                    if (nnames == 1) {
                        /*
                         * First word is a row name, so second word could be a
                         * field in this row.  Again, no error now if it
                         * isn't.
                         */
                        PLpgSQL_row* row = NULL;
                        int i;

                        row = (PLpgSQL_row*)(u_sess->plsql_cxt.plpgsql_Datums[ns->itemno]);
                        for (i = 0; i < row->nfields; i++) {
                            if (row->fieldnames[i] && strcmp(row->fieldnames[i], word2) == 0) {
                                wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[row->varnos[i]];
                                wdatum->ident = NULL;
                                wdatum->quoted = false; /* not used */
                                wdatum->idents = idents;
                                return true;
                            }
                        }
                        /* fall through to return CWORD */
                    } else {
                        /* Block-qualified reference to row variable. */
                        wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                        wdatum->ident = NULL;
                        wdatum->quoted = false; /* not used */
                        wdatum->idents = idents;
                        return true;
                    }
                    break;

                default:
                    break;
            }
        }
    }

    if (0 == pg_strcasecmp(varray_first, word2)) {
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);
        if (ns != NULL) {
            *nsflag = PLPGSQL_TOK_VARRAY_FIRST;
            wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
            wdatum->ident = NULL;
            wdatum->quoted = false; /* not used */
            wdatum->idents = idents;
            return true;
        }
    }
    if (0 == pg_strcasecmp(varray_last, word2)) {
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);
        if (ns != NULL) {
            *nsflag = PLPGSQL_TOK_VARRAY_LAST;
            wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
            wdatum->ident = NULL;
            wdatum->quoted = false; /* not used */
            wdatum->idents = idents;
            return true;
        }
    }
    if (0 == pg_strcasecmp(varray_count, word2)) {
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);
        if (ns != NULL) {
            *nsflag = PLPGSQL_TOK_VARRAY_COUNT;
            wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
            wdatum->ident = NULL;
            wdatum->quoted = false; /* not used */
            wdatum->idents = idents;
            return true;
        }
    }
    if (0 == pg_strcasecmp(varray_extend, word2)) {
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);
        if (ns != NULL) {
            *nsflag = PLPGSQL_TOK_VARRAY_EXTEND;
            wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
            wdatum->ident = NULL;
            wdatum->quoted = false; /* not used */
            wdatum->idents = idents;
            return true;
        }
    }

    /* Nothing found */
    cword->idents = idents;
    return false;
}

/* ----------
 * plpgsql_parse_tripword		Same lookup for three words
 *					separated by dots.
 * ----------
 */
bool plpgsql_parse_tripword(char* word1, char* word2, char* word3, PLwdatum* wdatum, PLcword* cword)
{
    PLpgSQL_nsitem* ns = NULL;
    List* idents = NULL;
    int nnames;

    idents = list_make3(makeString(word1), makeString(word2), makeString(word3));

    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (u_sess->plsql_cxt.plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE) {
        /*
         * Do a lookup in the current namespace stack. Must find a qualified
         * reference, else ignore.
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, word2, word3, &nnames);
        if (ns != NULL && nnames == 2) {
            switch (ns->itemtype) {
                case PLPGSQL_NSTYPE_REC:
                case PLPGSQL_NSTYPE_RECORD: {
                    /*
                     * words 1/2 are a record name, so third word could be
                     * a field in this record.
                     */
                    PLpgSQL_recfield* newm = NULL;

                    newm = (PLpgSQL_recfield*)palloc(sizeof(PLpgSQL_recfield));
                    newm->dtype = PLPGSQL_DTYPE_RECFIELD;
                    newm->fieldname = pstrdup(word3);
                    newm->recparentno = ns->itemno;

                    plpgsql_adddatum((PLpgSQL_datum*)newm);

                    wdatum->datum = (PLpgSQL_datum*)newm;
                    wdatum->ident = NULL;
                    wdatum->quoted = false; /* not used */
                    wdatum->idents = idents;
                    return true;
                }

                case PLPGSQL_NSTYPE_ROW: {
                    /*
                     * words 1/2 are a row name, so third word could be a
                     * field in this row.
                     */
                    PLpgSQL_row* row = NULL;
                    int i;

                    row = (PLpgSQL_row*)(u_sess->plsql_cxt.plpgsql_Datums[ns->itemno]);
                    for (i = 0; i < row->nfields; i++) {
                        if (row->fieldnames[i] && strcmp(row->fieldnames[i], word3) == 0) {
                            wdatum->datum = u_sess->plsql_cxt.plpgsql_Datums[row->varnos[i]];
                            wdatum->ident = NULL;
                            wdatum->quoted = false; /* not used */
                            wdatum->idents = idents;
                            return true;
                        }
                    }
                    /* fall through to return CWORD */
                    break;
                }

                default:
                    break;
            }
        }
    }

    /* Nothing found */
    cword->idents = idents;
    return false;
}

/* ----------
 * plpgsql_parse_wordtype	The scanner found word%TYPE. word can be
 *				a variable name or a basetype.
 *
 * Returns datatype struct, or NULL if no match found for word.
 * ----------
 */
PLpgSQL_type* plpgsql_parse_wordtype(char* ident)
{
    PLpgSQL_type* dtype = NULL;
    PLpgSQL_nsitem* nse = NULL;
    HeapTuple type_tup = NULL;

    /*
     * Do a lookup in the current namespace stack
     */
    nse = plpgsql_ns_lookup(plpgsql_ns_top(), false, ident, NULL, NULL, NULL);

    if (nse != NULL) {
        switch (nse->itemtype) {
            case PLPGSQL_NSTYPE_VAR:
                return ((PLpgSQL_var*)(u_sess->plsql_cxt.plpgsql_Datums[nse->itemno]))->datatype;

                /* XXX perhaps allow REC/ROW here? */

            default:
                return NULL;
        }
    }

    /*
     * Word wasn't found in the namespace stack. Try to find a data type with
     * that name, but ignore shell types and complex types.
     */
    type_tup = LookupTypeName(NULL, makeTypeName(ident), NULL);
    if (type_tup) {
        Form_pg_type type_struct = (Form_pg_type)GETSTRUCT(type_tup);

        if (!type_struct->typisdefined || type_struct->typrelid != InvalidOid) {
            ReleaseSysCache(type_tup);
            return NULL;
        }

        dtype = build_datatype(type_tup, -1, u_sess->plsql_cxt.plpgsql_curr_compile->fn_input_collation);

        ReleaseSysCache(type_tup);
        return dtype;
    }

    /*
     * Nothing found - up to now it's a word without any special meaning for
     * us.
     */
    return NULL;
}

/* ----------
 * plpgsql_parse_cwordtype		Same lookup for compositeword%TYPE
 * ----------
 */
PLpgSQL_type* plpgsql_parse_cwordtype(List* idents)
{
    PLpgSQL_type* dtype = NULL;
    PLpgSQL_nsitem* nse = NULL;
    const char* fldname = NULL;
    Oid class_oid;
    HeapTuple classtup = NULL;
    HeapTuple attrtup = NULL;
    HeapTuple type_tup = NULL;
    Form_pg_class class_struct = NULL;
    Form_pg_attribute attr_struct = NULL;
    MemoryContext old_cxt = NULL;
    bool flag = false;

    /* Avoid memory leaks in the long-term function context */
    old_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);

    if (list_length(idents) == 2) {
        /*
         * Do a lookup in the current namespace stack. We don't need to check
         * number of names matched, because we will only consider scalar
         * variables.
         */
        nse = plpgsql_ns_lookup(plpgsql_ns_top(), false, strVal(linitial(idents)), strVal(lsecond(idents)), NULL, NULL);

        if (nse != NULL && nse->itemtype == PLPGSQL_NSTYPE_VAR) {
            dtype = ((PLpgSQL_var*)(u_sess->plsql_cxt.plpgsql_Datums[nse->itemno]))->datatype;
            goto done;
        }

        /*
         * First word could also be a table name, which allowed to be synonym object.
         */
        (void)RelnameGetRelidExtended(strVal(linitial(idents)), &class_oid);
        if (!OidIsValid(class_oid)) {
            goto done;
        }
        fldname = strVal(lsecond(idents));
    } else if (list_length(idents) == 3) {
        RangeVar* relvar = NULL;

        relvar = makeRangeVar(strVal(linitial(idents)), strVal(lsecond(idents)), -1);
        /* Can't lock relation - we might not have privileges. */
        class_oid = RangeVarGetRelidExtended(relvar, NoLock, true, false, false, true, NULL, NULL);
        if (!OidIsValid(class_oid)) {
            goto done;
        }
        fldname = strVal(lthird(idents));
    } else {
        goto done;
    }
    classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(class_oid));
    if (!HeapTupleIsValid(classtup)) {
        goto done;
    }
    class_struct = (Form_pg_class)GETSTRUCT(classtup);

    /*
     * It must be a relation, sequence, view, composite type, materialized view, or foreign table
     */
    flag = class_struct->relkind != RELKIND_RELATION && class_struct->relkind != RELKIND_SEQUENCE &&
        class_struct->relkind != RELKIND_VIEW && class_struct->relkind != RELKIND_COMPOSITE_TYPE &&
        class_struct->relkind != RELKIND_FOREIGN_TABLE && class_struct->relkind != RELKIND_MATVIEW &&
        class_struct->relkind != RELKIND_STREAM && class_struct->relkind != RELKIND_CONTQUERY;
    if (flag) {
        goto done;
    }
    /*
     * Fetch the named table field and its type
     */
    attrtup = SearchSysCacheAttName(class_oid, fldname);
    if (!HeapTupleIsValid(attrtup)) {
        goto done;
    }
    attr_struct = (Form_pg_attribute)GETSTRUCT(attrtup);

    type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(attr_struct->atttypid));
    if (!HeapTupleIsValid(type_tup)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u when parse word in PLSQL.", attr_struct->atttypid)));
    }
    /*
     * Found that - build a compiler type struct in the caller's cxt and
     * return it
     */
    MemoryContextSwitchTo(old_cxt);
    dtype = build_datatype(type_tup, attr_struct->atttypmod, attr_struct->attcollation);
    MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);

done:
    if (HeapTupleIsValid(classtup)) {
        ReleaseSysCache(classtup);
    }
    if (HeapTupleIsValid(attrtup)) {
        ReleaseSysCache(attrtup);
    }
    if (HeapTupleIsValid(type_tup)) {
        ReleaseSysCache(type_tup);
    }

    MemoryContextSwitchTo(old_cxt);
    return dtype;
}

/* ----------
 * plpgsql_parse_wordrowtype		Scanner found word%ROWTYPE.
 *					So word must be a table name.
 * ----------
 */
PLpgSQL_type* plpgsql_parse_wordrowtype(char* ident)
{
    Oid class_oid;

    /*
     * Lookup the relation, which allows ident is a synonym object,
     * but no need to collect more errdetails.
     */
    (void)RelnameGetRelidExtended(ident, &class_oid);

    if (!OidIsValid(class_oid)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("relation \"%s\" does not exist when parse word.", ident)));
    }
    /* Build and return the row type struct */
    return plpgsql_build_datatype(get_rel_type_id(class_oid), -1, InvalidOid);
}

/* ----------
 * plpgsql_parse_cwordrowtype		Scanner found compositeword%ROWTYPE.
 *			So word must be a namespace qualified table name.
 * ----------
 */
PLpgSQL_type* plpgsql_parse_cwordrowtype(List* idents)
{
    Oid class_oid;
    RangeVar* relvar = NULL;
    MemoryContext old_cxt = NULL;

    if (list_length(idents) != 2) {
        return NULL;
    }

    /* Avoid memory leaks in long-term function context */
    old_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);

    /* Look up relation name.  Can't lock it - we might not have privileges. */
    relvar = makeRangeVar(strVal(linitial(idents)), strVal(lsecond(idents)), -1);

    /* Here relvar is allowed to be a synonym object. */
    class_oid = RangeVarGetRelidExtended(relvar, NoLock, false, false, false, true, NULL, NULL);

    MemoryContextSwitchTo(old_cxt);

    /* Build and return the row type struct */
    return plpgsql_build_datatype(get_rel_type_id(class_oid), -1, InvalidOid);
}

/*
 * @Description: build variable for refcursor, refcursor has four option: isopen found notfound rowcount.
 *                     these options only used to one cursor.  and cursor as an in or out parameter,
                        its options value should be changed or be returned by cursor returns.
 * @in varno - variable number in datums
 * @return -void
 */
static void build_cursor_variable(int varno)
{
    PLpgSQL_var* tmpvar = NULL;
    StringInfoData str;
    initStringInfo(&str);

    appendStringInfo(&str, "__gsdb_cursor_attri_%d_isopen__", varno);

    /* Create the magic ISOPEN variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    tmpvar->isnull = false;
    tmpvar->is_cursor_open = true;
    tmpvar->is_cursor_var = true;
    resetStringInfo(&str);
    appendStringInfo(&str, "__gsdb_cursor_attri_%d_found__", varno);

    /* Create the magic FOUND variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    tmpvar->is_cursor_var = true;
    resetStringInfo(&str);
    appendStringInfo(&str, "__gsdb_cursor_attri_%d_notfound__", varno);
    /* Create the magic NOTFOUND variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true);
    tmpvar->is_cursor_var = true;
    resetStringInfo(&str);
    appendStringInfo(&str, "__gsdb_cursor_attri_%d_rowcount__", varno);
    /* Create the magic ROWCOUNT variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true);
    tmpvar->is_cursor_var = true;
    pfree_ext(str.data);
}
/*
 * plpgsql_build_variable - build a datum-array entry of a given
 * datatype
 *
 * The returned struct may be a PLpgSQL_var, PLpgSQL_row, or
 * PLpgSQL_rec depending on the given datatype, and is allocated via
 * palloc.	The struct is automatically added to the current datum
 * array, and optionally to the current namespace.
 */
PLpgSQL_variable* plpgsql_build_variable(
    const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace, const char* varname)
{
    PLpgSQL_variable* result = NULL;

    switch (dtype->ttype) {
        case PLPGSQL_TTYPE_SCALAR: {
            /* Ordinary scalar datatype */
            PLpgSQL_var* var = NULL;

            var = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
            var->dtype = PLPGSQL_DTYPE_VAR;
            var->refname = pstrdup(refname);
            var->lineno = lineno;
            var->datatype = dtype;
            /* other fields might be filled by caller */

            /* preset to NULL */
            var->value = 0;
            var->isnull = true;
            var->freeval = false;

            plpgsql_adddatum((PLpgSQL_datum*)var);
            if (add2namespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, var->dno, refname);
            }

            if (dtype->typoid == REFCURSOROID) {
                build_cursor_variable(var->dno);
            }
            result = (PLpgSQL_variable*)var;
            break;
        }
        case PLPGSQL_TTYPE_ROW: {
            /* Composite type -- build a row variable */
            PLpgSQL_row* row = NULL;

            row = build_row_from_class(dtype->typrelid);

            row->customErrorCode = 0;
            if (0 == strcmp(format_type_be(row->rowtupdesc->tdtypeid), "exception")) {
                row->customErrorCode = plpgsql_getCustomErrorCode();
            }

            row->dtype = PLPGSQL_DTYPE_ROW;
            row->refname = pstrdup(refname);
            row->lineno = lineno;

            plpgsql_adddatum((PLpgSQL_datum*)row);
            if (add2namespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, row->dno, refname);
            }
            result = (PLpgSQL_variable*)row;
            break;
        }
        case PLPGSQL_TTYPE_REC: {
            /* "record" type -- build a record variable */
            PLpgSQL_rec* rec = NULL;

            rec = plpgsql_build_record(refname, lineno, add2namespace);
            result = (PLpgSQL_variable*)rec;
            break;
        }
        case PLPGSQL_TTYPE_PSEUDO:
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("variable \"%s\" has pseudo-type %s when build variable in PLSQL.",
                        refname,
                        format_type_be(dtype->typoid))));
            result = NULL; /* keep compiler quiet */
            break;
        case PLPGSQL_TTYPE_RECORD: {
            /* Composite type -- build a row variable */
            PLpgSQL_row* row = NULL;

            row = build_row_from_rec_type(refname, lineno, (PLpgSQL_rec_type*)dtype);

            plpgsql_adddatum((PLpgSQL_datum*)row);
            if (add2namespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, row->dno, refname);
            }
            result = (PLpgSQL_variable*)row;
            break;
        }
        default:
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized ttype: %d, when build variable in PLSQL, this situation should not occur.",
                        dtype->ttype)));
            result = NULL; /* keep compiler quiet */
            break;
    }

    return result;
}

/*
 * create a varray type
 */
PLpgSQL_variable* plpgsql_build_varrayType(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace)
{
    /* Ordinary scalar datatype */
    PLpgSQL_var* var = NULL;
    var = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
    var->refname = pstrdup(refname);
    var->lineno = lineno;
    var->datatype = dtype;
    /* other fields might be filled by caller */

    /* preset to NULL */
    var->value = 0;
    var->isnull = true;
    var->freeval = false;

    plpgsql_adddatum((PLpgSQL_datum*)var);
    if (add2namespace) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_VARRAY, var->dno, var->refname);
    }
    return (PLpgSQL_variable*)var;
}

/*
 * Build empty named record variable, and optionally add it to namespace
 */
PLpgSQL_rec* plpgsql_build_record(const char* refname, int lineno, bool add2namespace)
{
    PLpgSQL_rec* rec = NULL;

    rec = (PLpgSQL_rec*)palloc0(sizeof(PLpgSQL_rec));
    rec->dtype = PLPGSQL_DTYPE_REC;
    rec->refname = pstrdup(refname);
    rec->lineno = lineno;
    rec->tup = NULL;
    rec->tupdesc = NULL;
    rec->freetup = false;
    plpgsql_adddatum((PLpgSQL_datum*)rec);
    if (add2namespace) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_REC, rec->dno, rec->refname);
    }
    return rec;
}

/*
 * @Description: build a plpgsql record type.
 *
 *     Create a record type in the declaration phase
 *
 * @param[IN] typname: type name.
 * @param[IN] lineno: the lineno in plpgsql.
 * @param[IN] list: Information list for each record field.
 * @param[IN] add2namespace: if add  dno into namespace.
 * @return PLpgSQL_rec_type: Create the completed record type
 */
PLpgSQL_rec_type* plpgsql_build_rec_type(const char* typname, int lineno, List* list, bool add2namespace)
{
    int idx = 0;
    ListCell* cell = NULL;
    PLpgSQL_rec_attr* attr = NULL;
    PLpgSQL_rec_type* result = NULL;

    result = (PLpgSQL_rec_type*)palloc0(sizeof(PLpgSQL_rec_type));

    result->dtype = PLPGSQL_TTYPE_RECORD;
    result->ttype = PLPGSQL_TTYPE_RECORD;
    result->typoid = RECORDOID;
    result->typname = pstrdup(typname);

    result->attrnum = list->length;
    result->attrnames = (char**)palloc0(sizeof(char*) * list->length);
    result->types = (PLpgSQL_type**)palloc0(sizeof(PLpgSQL_type*) * list->length);
    result->notnulls = (bool*)palloc0(sizeof(bool) * list->length);
    result->defaultvalues = (PLpgSQL_expr**)palloc0(sizeof(PLpgSQL_expr*) * list->length);

    foreach (cell, list) {
        attr = (PLpgSQL_rec_attr*)lfirst(cell);

        result->attrnames[idx] = pstrdup(attr->attrname);
        result->types[idx] = attr->type;
        result->notnulls[idx] = attr->notnull;
        result->defaultvalues[idx] = attr->defaultvalue;
        idx++;
    }

    plpgsql_adddatum((PLpgSQL_datum*)result);

    if (add2namespace) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_RECORD, result->dno, typname);
    }
    return result;
}

/*
 * Build a row-variable data structure given the pg_class OID.
 */
static PLpgSQL_row* build_row_from_class(Oid class_oid)
{
    PLpgSQL_row* row = NULL;
    Relation rel;
    Form_pg_class class_struct;
    const char* relname = NULL;
    int i;

    /*
     * Open the relation to get info.
     */
    rel = relation_open(class_oid, AccessShareLock);
    class_struct = RelationGetForm(rel);
    relname = RelationGetRelationName(rel);

    /* accept relation, sequence, view, composite type, materialized view, or foreign table */
    if (class_struct->relkind != RELKIND_RELATION && class_struct->relkind != RELKIND_SEQUENCE &&
        class_struct->relkind != RELKIND_VIEW && class_struct->relkind != RELKIND_COMPOSITE_TYPE &&
        class_struct->relkind != RELKIND_FOREIGN_TABLE && class_struct->relkind != RELKIND_MATVIEW &&
        class_struct->relkind != RELKIND_STREAM && class_struct->relkind != RELKIND_CONTQUERY) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("relation \"%s\" is not a table, when build a row variable from relation in PLSQL.", relname)));
    }
    /*
     * Create a row datum entry and all the required variables that it will
     * point to.
     */
    row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->rowtupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
    row->nfields = class_struct->relnatts;
    row->fieldnames = (char**)palloc(sizeof(char*) * row->nfields);
    row->varnos = (int*)palloc(sizeof(int) * row->nfields);

    for (i = 0; i < row->nfields; i++) {
        Form_pg_attribute attr_struct;

        /*
         * Get the attribute and check for dropped column
         */
        attr_struct = row->rowtupdesc->attrs[i];

        if (!attr_struct->attisdropped) {
            char* attname = NULL;
            const int refname_size = (NAMEDATALEN * 2) + 100;
            char refname[refname_size];
            PLpgSQL_variable* var = NULL;
            errno_t rc = EOK;

            attname = NameStr(attr_struct->attname);
            rc = snprintf_s(refname, sizeof(refname), sizeof(refname) - 1, "%s.%s", relname, attname);
            securec_check_ss(rc, "\0", "\0");

            /*
             * Create the internal variable for the field
             *
             * We know if the table definitions contain a default value or if
             * the field is declared in the table as NOT NULL. But it's
             * possible to create a table field as NOT NULL without a default
             * value and that would lead to problems later when initializing
             * the variables due to entering a block at execution time. Thus
             * we ignore this information for now.
             */
            var = plpgsql_build_variable(refname, 0, plpgsql_build_datatype(attr_struct->atttypid, 
                                        attr_struct->atttypmod, attr_struct->attcollation), false);

            /* Add the variable to the row */
            row->fieldnames[i] = attname;
            row->varnos[i] = var->dno;
        } else {
            /* Leave a hole in the row structure for the dropped col */
            row->fieldnames[i] = NULL;
            row->varnos[i] = -1;
        }
    }

    relation_close(rel, AccessShareLock);

    return row;
}

/*
 * Build a row-variable data structure given the component variables.
 */
static PLpgSQL_row* build_row_from_vars(PLpgSQL_variable** vars, int numvars)
{
    PLpgSQL_row* row = NULL;
    int i;

    row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->rowtupdesc = CreateTemplateTupleDesc(numvars, false);
    row->nfields = numvars;
    row->fieldnames = (char**)palloc(numvars * sizeof(char*));
    row->varnos = (int*)palloc(numvars * sizeof(int));

    for (i = 0; i < numvars; i++) {
        PLpgSQL_variable* var = vars[i];
        Oid typoid = RECORDOID;
        int32 typmod = -1;
        Oid typcoll = InvalidOid;

        switch (var->dtype) {
            case PLPGSQL_DTYPE_VAR:
                typoid = ((PLpgSQL_var*)var)->datatype->typoid;
                typmod = ((PLpgSQL_var*)var)->datatype->atttypmod;
                typcoll = ((PLpgSQL_var*)var)->datatype->collation;
                break;

            case PLPGSQL_DTYPE_REC:
                break;

            case PLPGSQL_DTYPE_ROW:
                if (((PLpgSQL_row*)var)->rowtupdesc) {
                    typoid = ((PLpgSQL_row*)var)->rowtupdesc->tdtypeid;
                    typmod = ((PLpgSQL_row*)var)->rowtupdesc->tdtypmod;
                    /* composite types have no collation */
                }
                break;

            default:
                ereport(ERROR,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized dtype: %d when build row in PLSQL, this situation should not occur.",
                            var->dtype)));
                break;
        }

        row->fieldnames[i] = var->refname;
        row->varnos[i] = var->dno;

        TupleDescInitEntry(row->rowtupdesc, i + 1, var->refname, typoid, typmod, 0);
        TupleDescInitEntryCollation(row->rowtupdesc, i + 1, typcoll);
    }

    return row;
}

/*
 * @Description: build a row variable according to the record type.
 *
 *		Initialize the members of the row variable, build a variable
 *		for each record member, and initialize the row descriptor
 *
 * @param[IN] rowname: Variable name.
 * @param[IN] lineno: Line Numbers Recognized in Parsing.
 * @param[IN] type: The record types of previous declarations.
 * @return PLpgSQL_row: Returns the created row variable.
 */
PLpgSQL_row* build_row_from_rec_type(const char* rowname, int lineno, PLpgSQL_rec_type* type)
{
    int i;
    const int buf_size = (NAMEDATALEN * 2) + 100;
    char buf[buf_size];
    PLpgSQL_row* row = NULL;

    row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));

    row->dtype = PLPGSQL_DTYPE_RECORD;
    row->refname = pstrdup(rowname);
    row->nfields = type->attrnum;
    row->lineno = lineno;
    row->rowtupdesc = CreateTemplateTupleDesc(row->nfields, false);
    row->fieldnames = (char**)palloc(row->nfields * sizeof(char*));
    row->varnos = (int*)palloc(row->nfields * sizeof(int));

    for (i = 0; i < row->nfields; i++) {
        PLpgSQL_variable* var = NULL;
        Oid typoid;
        int32 typmod;
        int len = sizeof(buf);
        errno_t rc = EOK;

        rc = snprintf_s(buf, len, len - 1, "%s.%s", row->refname, type->attrnames[i]);
        securec_check_ss(rc, "", "");
        var = plpgsql_build_variable(buf, lineno, type->types[i], false);

        if (type->defaultvalues[i] != NULL)
            ((PLpgSQL_var*)var)->default_val = type->defaultvalues[i];

        row->fieldnames[i] = type->attrnames[i];
        row->varnos[i] = var->dno;

        typoid = type->types[i]->typoid;
        typmod = type->types[i]->atttypmod;

        TupleDescInitEntry(row->rowtupdesc, i + 1, type->attrnames[i], typoid, typmod, 0);
    }

    return row;
}

/*
 * plpgsql_build_datatype
 *		Build PLpgSQL_type struct given type OID, typmod, and collation.
 *
 * If collation is not InvalidOid then it overrides the type's default
 * collation.  But collation is ignored if the datatype is non-collatable.
 */
PLpgSQL_type* plpgsql_build_datatype(Oid typeOid, int32 typmod, Oid collation)
{
    HeapTuple type_tup;
    PLpgSQL_type* typ = NULL;

    type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(type_tup)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u, type Oid is invalid", typeOid)));
    }
    typ = build_datatype(type_tup, typmod, collation);

    ReleaseSysCache(type_tup);

    return typ;
}

/*
 * Utility subroutine to make a PLpgSQL_type struct given a pg_type entry
 */
static PLpgSQL_type* build_datatype(HeapTuple type_tup, int32 typmod, Oid collation)
{
    Form_pg_type type_struct = (Form_pg_type)GETSTRUCT(type_tup);
    PLpgSQL_type* typ = NULL;

    if (!type_struct->typisdefined) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" is only a shell when build data type in PLSQL.", NameStr(type_struct->typname))));
    }
    typ = (PLpgSQL_type*)palloc(sizeof(PLpgSQL_type));

    typ->typname = pstrdup(NameStr(type_struct->typname));
    typ->typoid = HeapTupleGetOid(type_tup);
    switch (type_struct->typtype) {
        case TYPTYPE_BASE:
        case TYPTYPE_DOMAIN:
        case TYPTYPE_ENUM:
        case TYPTYPE_RANGE:
            typ->ttype = PLPGSQL_TTYPE_SCALAR;
            break;
        case TYPTYPE_COMPOSITE:
            AssertEreport(OidIsValid(type_struct->typrelid), MOD_PLSQL, "It is invalid oid.");
            typ->ttype = PLPGSQL_TTYPE_ROW;
            break;
        case TYPTYPE_PSEUDO:
            if (typ->typoid == RECORDOID)
                typ->ttype = PLPGSQL_TTYPE_REC;
            else
                typ->ttype = PLPGSQL_TTYPE_PSEUDO;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized typtype: %d when build data type in PLSQL.", (int)type_struct->typtype)));
            break;
    }
    typ->typlen = type_struct->typlen;
    typ->typbyval = type_struct->typbyval;
    typ->typrelid = type_struct->typrelid;
    typ->typioparam = getTypeIOParam(type_tup);
    typ->collation = type_struct->typcollation;
    if (OidIsValid(collation) && OidIsValid(typ->collation)) {
        typ->collation = collation;
    }
    fmgr_info(type_struct->typinput, &(typ->typinput));
    typ->atttypmod = typmod;

    return typ;
}

/*
 *	plpgsql_recognize_err_condition
 *		Check condition name and translate it to SQLSTATE.
 *
 * Note: there are some cases where the same condition name has multiple
 * entries in the table.  We arbitrarily return the first match.
 */
int plpgsql_recognize_err_condition(const char* condname, bool allow_sqlstate)
{
    int i;

    if (allow_sqlstate) {
        if (strlen(condname) == 5 && strspn(condname, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") == 5) {
            return MAKE_SQLSTATE(condname[0], condname[1], condname[2], condname[3], condname[4]);
        }
    }

    for (i = 0; exception_label_map[i].label != NULL; i++) {
        if (strcmp(condname, exception_label_map[i].label) == 0) {
            return exception_label_map[i].sqlerrstate;
        }
    }

    ereport(ERROR,
        (errmodule(MOD_PLSQL),
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("unrecognized exception condition \"%s\"", condname)));
    return 0; /* keep compiler quiet */
}

/*
 * plpgsql_parse_err_condition
 *		Generate PLpgSQL_condition entry(s) for an exception condition name
 *
 * This has to be able to return a list because there are some duplicate
 * names in the table of error code names.
 */
PLpgSQL_condition* plpgsql_parse_err_condition(char* condname)
{
    int i;
    PLpgSQL_condition* newm = NULL;
    PLpgSQL_condition* prev = NULL;

    /*
     * XXX Eventually we will want to look for user-defined exception names
     * here.
     */

    /*
     * OTHERS is represented as code 0 (which would map to '00000', but we
     * have no need to represent that as an exception condition).
     */
    if (strcmp(condname, "others") == 0) {
        newm = (PLpgSQL_condition*)palloc(sizeof(PLpgSQL_condition));
        newm->sqlerrstate = 0;
        newm->condname = condname;
        newm->next = NULL;
        return newm;
    }

    prev = NULL;
    for (i = 0; exception_label_map[i].label != NULL; i++) {
        if (strcmp(condname, exception_label_map[i].label) == 0) {
            newm = (PLpgSQL_condition*)palloc(sizeof(PLpgSQL_condition));
            newm->sqlerrstate = exception_label_map[i].sqlerrstate;
            newm->condname = condname;
            newm->next = prev;
            prev = newm;
        }
    }

    if (prev == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("unrecognized exception condition \"%s\"", condname)));
    }
    return prev;
}

/* ----------
 * plpgsql_adddatum			Add a variable, record or row
 *					to the compiler's datum list.
 * ----------
 */
void plpgsql_adddatum(PLpgSQL_datum* newm)
{
    if (u_sess->plsql_cxt.plpgsql_nDatums == u_sess->plsql_cxt.datums_alloc) {
        u_sess->plsql_cxt.datums_alloc *= 2;
        u_sess->plsql_cxt.plpgsql_Datums = (PLpgSQL_datum**)repalloc(
            u_sess->plsql_cxt.plpgsql_Datums, sizeof(PLpgSQL_datum*) * u_sess->plsql_cxt.datums_alloc);
    }

    newm->dno = u_sess->plsql_cxt.plpgsql_nDatums;
    u_sess->plsql_cxt.plpgsql_Datums[u_sess->plsql_cxt.plpgsql_nDatums++] = newm;
}

/* ----------
 * plpgsql_add_initdatums		Make an array of the datum numbers of
 *					all the simple VAR datums created since the last call
 *					to this function.
 *
 * If varnos is NULL, we just forget any datum entries created since the
 * last call.
 *
 * This is used around a DECLARE section to create a list of the VARs
 * that have to be initialized at block entry.	Note that VARs can also
 * be created elsewhere than DECLARE, eg by a FOR-loop, but it is then
 * the responsibility of special-purpose code to initialize them.
 * ----------
 */
int plpgsql_add_initdatums(int** varnos)
{
    int i;
    int n = 0;

    for (i = u_sess->plsql_cxt.datums_last; i < u_sess->plsql_cxt.plpgsql_nDatums; i++) {
        switch (u_sess->plsql_cxt.plpgsql_Datums[i]->dtype) {
            case PLPGSQL_DTYPE_VAR:
                n++;
                break;

            default:
                break;
        }
    }

    if (varnos != NULL) {
        if (n > 0) {
            *varnos = (int*)palloc(sizeof(int) * n);

            n = 0;
            for (i = u_sess->plsql_cxt.datums_last; i < u_sess->plsql_cxt.plpgsql_nDatums; i++) {
                switch (u_sess->plsql_cxt.plpgsql_Datums[i]->dtype) {
                    case PLPGSQL_DTYPE_VAR:
                        (*varnos)[n++] = u_sess->plsql_cxt.plpgsql_Datums[i]->dno;
                    /* fall through */
                    
                    default:
                        break;
                }
            }
        } else {
            *varnos = NULL;
        }
    }

    u_sess->plsql_cxt.datums_last = u_sess->plsql_cxt.plpgsql_nDatums;
    return n;
}

/*
 * Compute the hashkey for a given function invocation
 *
 * The hashkey is returned into the caller-provided storage at *hashkey.
 */
static void compute_function_hashkey(
    FunctionCallInfo fcinfo, Form_pg_proc proc_struct, PLpgSQL_func_hashkey* hashkey, bool for_validator)
{
    /* Make sure any unused bytes of the struct are zero */
    errno_t rc = memset_s(hashkey, sizeof(PLpgSQL_func_hashkey), 0, sizeof(PLpgSQL_func_hashkey));
    securec_check(rc, "", "");

    /* get function OID */
    hashkey->funcOid = fcinfo->flinfo->fn_oid;

    /* get call context */
    hashkey->isTrigger = CALLED_AS_TRIGGER(fcinfo);

    /*
     * if trigger, get relation OID.  In validation mode we do not know what
     * relation is intended to be used, so we leave trigrelOid zero; the hash
     * entry built in this case will never really be used.
     */
    if (hashkey->isTrigger && !for_validator) {
        TriggerData* trigdata = (TriggerData*)fcinfo->context;

        hashkey->trigrelOid = RelationGetRelid(trigdata->tg_relation);
    }

    /* get input collation, if known */
    hashkey->inputCollation = fcinfo->fncollation;

    if (proc_struct->pronargs > 0) {
        errno_t err = EOK;
        /* get the argument types */
        err = memcpy_s(hashkey->argtypes,
            sizeof(hashkey->argtypes),
            proc_struct->proargtypes.values,
            proc_struct->pronargs * sizeof(Oid));
        securec_check(err, "\0", "\0");

        /* resolve any polymorphic argument types */
        plpgsql_resolve_polymorphic_argtypes(proc_struct->pronargs,
            hashkey->argtypes,
            NULL,
            fcinfo->flinfo->fn_expr,
            for_validator,
            NameStr(proc_struct->proname));
    }
}

/*
 * This is the same as the standard resolve_polymorphic_argtypes() function,
 * but with a special case for validation: assume that polymorphic arguments
 * are integer, integer-array or integer-range.  Also, we go ahead and report
 * the error if we can't resolve the types.
 */
static void plpgsql_resolve_polymorphic_argtypes(
    int numargs, Oid* arg_types, const char* arg_modes, Node* call_expr, bool for_validator, const char* pro_name)
{
    int i;

    if (!for_validator) {
        /* normal case, pass to standard routine */
        if (!resolve_polymorphic_argtypes(numargs, arg_types, arg_modes, call_expr))
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("could not determine actual argument type for polymorphic function \"%s\"", pro_name)));
    } else {
        /* special validation case */
        for (i = 0; i < numargs; i++) {
            switch (arg_types[i]) {
                case ANYELEMENTOID:
                case ANYNONARRAYOID:
                case ANYENUMOID: /* XXX dubious */
                    arg_types[i] = INT4OID;
                    break;
                case ANYARRAYOID:
                    arg_types[i] = INT4ARRAYOID;
                    break;
                case ANYRANGEOID:
                    arg_types[i] = INT4RANGEOID;
                    break;
                default:
                    break;
            }
        }
    }
}

/*
 * delete_function - clean up as much as possible of a stale function cache
 *
 * We can't release the PLpgSQL_function struct itself, because of the
 * possibility that there are fn_extra pointers to it.	We can release
 * the subsidiary storage, but only if there are no active evaluations
 * in progress.  Otherwise we'll just leak that storage.  Since the
 * case would only occur if a pg_proc update is detected during a nested
 * recursive call on the function, a leak seems acceptable.
 *
 * Note that this can be called more than once if there are multiple fn_extra
 * pointers to the same function cache.  Hence be careful not to do things
 * twice.
 */
static void delete_function(PLpgSQL_function* func)
{
    u_sess->plsql_cxt.is_delete_function = true;

    SPIPlanCacheTableDelete(SPICacheHashFunc(func->fn_hashkey, sizeof(PLpgSQL_func_hashkey)));

    /* remove function from hash table (might be done already) */
    plpgsql_HashTableDelete(func);
    /* release the function's storage if safe and not done already */
    if (func->use_count == 0) {
        plpgsql_free_function_memory(func);
    }

    u_sess->plsql_cxt.is_delete_function = false;
}

/* exported so we can call it from plpgsql_init() */
void plpgsql_HashTableInit(void)
{
    HASHCTL ctl;
    errno_t rc = EOK;

    /* don't allow double-initialization */
    AssertEreport(u_sess->plsql_cxt.plpgsql_HashTable == NULL, MOD_PLSQL, "don't allow double-initialization.");

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PLpgSQL_func_hashkey);
    ctl.entrysize = sizeof(plpgsql_HashEnt);
    ctl.hash = tag_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->plsql_cxt.plpgsql_HashTable =
        hash_create("PLpgSQL function cache", FUNCS_PER_USER, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

static PLpgSQL_function* plpgsql_HashTableLookup(PLpgSQL_func_hashkey* func_key)
{
    plpgsql_HashEnt* hentry = NULL;

    hentry = (plpgsql_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_HashTable, (void*)func_key, HASH_FIND, NULL);

    if (hentry != NULL) {
        /* add cell to the tail of the function list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsql_dlist_functions, hentry->cell);
        return hentry->function;
    } else {
        return NULL;
    }
}

static void plpgsql_HashTableInsert(PLpgSQL_function* func, PLpgSQL_func_hashkey* func_key)
{
    plpgsql_HashEnt* hentry = NULL;
    bool found = false;

    hentry = (plpgsql_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_HashTable, (void*)func_key, HASH_ENTER, &found);
    if (found) {
        /* move cell to the tail of the function list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsql_dlist_functions, hentry->cell);
        elog(WARNING, "trying to insert a function that already exists");
    } else {
        /* append the current compiling entity to the end of the compile results list. */
        plpgsql_append_dlcell(hentry);
    }

    hentry->function = func;
    /* prepare back link from function to hashtable key */
    func->fn_hashkey = &hentry->key;
}

void plpgsql_HashTableDelete(PLpgSQL_function* func)
{
    plpgsql_HashEnt* hentry = NULL;

    /* do nothing if not in table */
    if (func->fn_hashkey == NULL) {
        return;
    }

    hentry = (plpgsql_HashEnt*)hash_search(
        u_sess->plsql_cxt.plpgsql_HashTable, (void*)func->fn_hashkey, HASH_REMOVE, NULL);
    if (hentry == NULL) {
        elog(WARNING, "trying to delete function that does not exist");
    } else {
        /* delete the cell from the list. */
        u_sess->plsql_cxt.plpgsql_dlist_functions =
            dlist_delete_cell(u_sess->plsql_cxt.plpgsql_dlist_functions, hentry->cell, false);
    }
    /* remove back link, which no longer points to allocated storage */
    func->fn_hashkey = NULL;
}

/*
 * get a custom error code for a new user defined exceprtion.
 */
static int plpgsql_getCustomErrorCode(void)
{
    int curIndex = u_sess->plsql_cxt.plpgsql_IndexErrorVariable;
    int first = 0;
    int second = 0;
    int third = 0;

    if (u_sess->plsql_cxt.plpgsql_IndexErrorVariable >= u_sess->attr.attr_common.max_user_defined_exception) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("exception variable count %d exceed max value, max exception variable count is %d",
                    u_sess->plsql_cxt.plpgsql_IndexErrorVariable,
                    u_sess->attr.attr_common.max_user_defined_exception)));
    }
    u_sess->plsql_cxt.plpgsql_IndexErrorVariable++;

    first = (int)(curIndex / 100);
    second = (int)(curIndex / 10 - 10 * first);
    third = (int)(curIndex - 10 * second - 100 * first);

    /* curtom error code start with 'P1000' */
    return MAKE_SQLSTATE('P', '1', '0' + first, '0' + second, '0' + third);
}

/*
 * append the current compiling entity to the end of the compiled
 * function results list. If the stored results exceed the limit,
 * delete the head of the list, which is the last recently used
 * result.
 */
static void plpgsql_append_dlcell(plpgsql_HashEnt* entity)
{
    MemoryContext oldctx;
    PLpgSQL_function* func = NULL;
    oldctx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    u_sess->plsql_cxt.plpgsql_dlist_functions = dlappend(u_sess->plsql_cxt.plpgsql_dlist_functions, entity);
    (void)MemoryContextSwitchTo(oldctx);

    entity->cell = u_sess->plsql_cxt.plpgsql_dlist_functions->tail;

    while (dlength(u_sess->plsql_cxt.plpgsql_dlist_functions) > g_instance.attr.attr_sql.max_compile_functions) {
        DListCell* headcell = u_sess->plsql_cxt.plpgsql_dlist_functions->head;
        plpgsql_HashEnt* head_entity = (plpgsql_HashEnt*)lfirst(headcell);

        func = head_entity->function;
        if (func->use_count != 0) {
            break;
        }

        /* delete from the hash and delete the function's compile */
        delete_function(func);

        pfree_ext(func);
    }
}

/*
 * Check whether function's search path is same as system search_path.
 */
static bool plpgsql_check_search_path(PLpgSQL_function* func, HeapTuple proc_tup)
{
    /* If SUPPORT_BIND_SEARCHPATH is false, always return true. */
    if (!SUPPORT_BIND_SEARCHPATH) {
        return true;
    }
    bool isOidListSame = true;
    Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);

    if (proc_struct->pronamespace != PG_CATALOG_NAMESPACE) {
        /* Get lastest search_path if baseSearchPathValid is false */
        recomputeNamespacePath();

        int len1 = list_length(u_sess->catalog_cxt.baseSearchPath);
        /*
         * The first element of func->fn_searchpath->schemas is
         * namespace the current function belongs to.
         */
        int len2 = list_length(func->fn_searchpath->schemas) - 1;
        Assert(len2 >= 0);

        if (len1 == len2) {
            ListCell* lc1 = NULL;
            ListCell* lc2 = NULL;

            /* Check whether search_path has changed */
            lc1 = list_head(u_sess->catalog_cxt.baseSearchPath);
            lc2 = list_head(func->fn_searchpath->schemas);

            /* Check function schema from list second position to list tail */
            lc2 = lnext(lc2);
            for (; lc1 && lc2; lc1 = lnext(lc1), lc2 = lnext(lc2)) {
                if (lfirst_oid(lc1) != lfirst_oid(lc2)) {
                    isOidListSame = false;
                    break;
                }
            }
        } else {
            /* If length is different, two lists are different. */
            isOidListSame = false;
        }
    }

    return isOidListSame;
}

/*
 * @Description : Check co-location for INSERT/UPDATE/DELETE statement and the
 * the statment which it triggered.
 *
 * @in query : Query struct info about the IUD statement.
 * @in rte : range table entry for the insert/update/delete statement.
 * @in plpgsql_func : information for the insert/update/delete trigger function.
 * @return : true when statement and the triggered statement are co-location.
 */
bool plpgsql_check_colocate(Query* query, RangeTblEntry* rte, void* plpgsql_func)
{
    List* query_partAttrNum = NIL;
    List* trig_partAttrNum = NIL;
    Relation qe_relation = NULL; /* triggering query's rel */
    Relation tg_relation = NULL;
    RelationLocInfo* qe_rel_loc_info = NULL; /* triggering query's rel loc info */
    RelationLocInfo* tg_rel_loc_info = NULL; /* trigger body's rel loc info */
    int qe_rel_nodelist_len = 0;
    int tg_rel_nodelist_len = 0;

    Assert(query->commandType == CMD_INSERT || query->commandType == CMD_DELETE || query->commandType == CMD_UPDATE);

    PLpgSQL_function* func = (PLpgSQL_function*)plpgsql_func;

    /* Get event query relation and trigger body's relation. */
    qe_relation = func->tg_relation;
    tg_relation = relation_open(rte->relid, AccessShareLock);

    query_partAttrNum = qe_relation->rd_locator_info->partAttrNum;
    trig_partAttrNum = rte->partAttrNum;

    /* Check if trigger query table and trigger body table are on the same node list. */
    qe_rel_loc_info = qe_relation->rd_locator_info;
    tg_rel_loc_info = tg_relation->rd_locator_info;

    qe_rel_nodelist_len = list_length(qe_rel_loc_info->nodeList);
    tg_rel_nodelist_len = list_length(tg_rel_loc_info->nodeList);

    /* Cannot ship whenever target table is not row type. */
    if (!RelationIsRowFormat(tg_relation)) {
        relation_close(tg_relation, AccessShareLock);
        return false;
    }

    /* The query table and trigger table must in a same group. */
    if (0 != strcmp(qe_rel_loc_info->gname.data, tg_rel_loc_info->gname.data)) {
        relation_close(tg_relation, AccessShareLock);
        return false;
    }
    relation_close(tg_relation, AccessShareLock);

    /* If distribution key list lengths are different they both are not colocated. */
    if (list_length(trig_partAttrNum) != list_length(query_partAttrNum)) {
        return false;
    }

    /*
     * Used difference check function between INSERT and UPDATE/DELETE here because we use
     * targetlist to check INSERT and where clause to check UPDATE/DELETE.
     */
    if (query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE) {
        return plpgsql_check_updel_colocate(query, query_partAttrNum, trig_partAttrNum, func);
    } else {
        return plpgsql_check_insert_colocate(query, query_partAttrNum, trig_partAttrNum, func);
    }
}

/*
 * @Description : Check co-location for update or delete command. To check co-location of the
 * update or delete query in trigger and the triggering query, we walk through the lists of
 * of attribute no of distribution keys of both queries. For each pair, we check
 * if the distribute key of trigger table exist in its where clause and its expression is
 * from new/old and is the distribute key of the table the triggering query work on.
 *
 * For example,
 * tables:
 * create table t1(a1 int, b1 int, c1 int,d1 varchar(100)) distribute by hash(a1,b1);
 * create table t2(a2 int, b2 int, c2 int,d2 varchar(100)) distribute by hash(a2,b2);
 * update in trigger body:
 * update t2 set d2=new.d1 where a2=old.a1 and b2=old.b1;
 * triggering event:
 * an insert or update on t1
 *
 * we walk through two pairs: (a1, a2) and (b1, b2), and check if a2 is in the
 * where clause "a2=old.a1 and b2=old.b1", and if its expression is from new/old
 * and is a1; if b2 is in the where clause too and its expression is from new/old
 * and is b1. If the above two checks are satified, co-location is true.
 *
 * @in query : Query struct of the query with trigger.
 * @in qry_part_attr_num : list of attr no of the dist keys of triggering query.
 * @in trig_part_attr_num : list of attr no of the dist keys of query in trigger.
 * @in func : trigger function body information.
 * @return : when co-location return true.
 */
static bool plpgsql_check_updel_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func)
{
    Node* whereClause = NULL;
    List* opexpr_list = NIL;
    bool is_colocate = true;

    if (query->jointree == NULL || query->jointree->quals == NULL) {
        return false;
    }

    /* Recursively get a list of opexpr from quals. */
    opexpr_list = pull_opExpr((Node*)query->jointree->quals);
    if (opexpr_list == NIL) {
        return false;
    }
    /* Flatten AND/OR expression for checking or expr. */
    whereClause = eval_const_expressions(NULL, (Node*)query->jointree->quals);

    /* If it is or clause, we break the clause to check colocation of each expr */
    if (or_clause(whereClause)) {
        List* temp_list = NIL;
        ListCell* opexpr_cell = NULL;

        /* For or condtion, we can ship only when all opexpr are colocated. */
        foreach (opexpr_cell, opexpr_list) {
            temp_list = list_make1((Expr*)lfirst(opexpr_cell));
            is_colocate = plpgsql_check_opexpr_colocate(query, qry_part_attr_num, trig_part_attr_num, func, temp_list);

            if (temp_list != NIL) {
                list_free_ext(temp_list);
            }

            if (!is_colocate) {
                break;
        	}
		}
		
        if (opexpr_list != NIL) {
            list_free_ext(opexpr_list);
		}
        return is_colocate;
    } else {
        /* For and with no or condition, we can ship when any opexpr is colocated. */
        is_colocate = plpgsql_check_opexpr_colocate(query, qry_part_attr_num, trig_part_attr_num, func, opexpr_list);
    }

    if (opexpr_list != NIL) {
        list_free_ext(opexpr_list);
	}
    return is_colocate;
}

/*
 * @Description : Check co-location for opexpr. we walk through the lists of opexpr
 * to check where they are from new/old with all the distributeion keys of both queries.
 *
 * @in query : Query struct of the query with trigger.
 * @in qry_part_attr_num : list of attr no of the dist keys of triggering query.
 * @in trig_part_attr_num : list of attr no of the dist keys of query in trigger.
 * @in func : trigger function body information.
 * @return : when co-location return true.
 */
static bool plpgsql_check_opexpr_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func, List* opexpr_list)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    bool is_colocate = true;

    forboth(lc1, qry_part_attr_num, lc2, trig_part_attr_num)
    {
        Expr* expr = NULL;
        Param* param = NULL;
        PLpgSQL_recfield* recfield = NULL;
        PLpgSQL_rec* rec = NULL;
        int fno;
        AttrNumber attnum1 = lfirst_int(lc1);
        AttrNumber attnum2 = lfirst_int(lc2);
        ListCell* opexpr_cell = NULL;

        /* Only all distribute column can colocate, we can ship. */
        if (!is_colocate) {
            return false;
        }

        foreach (opexpr_cell, opexpr_list) {
            Expr* qual_expr = (Expr*)lfirst(opexpr_cell);

            /* Check all opexpr with distribute column */
            expr = pgxc_check_distcol_opexpr(query->resultRelation, attnum2, (OpExpr*)qual_expr);
            if (expr == NULL) {
                is_colocate = false;
                continue;
            }

            /* NEW/OLD is replaced with param by parser */
            if (!IsA(expr, Param)) {
                is_colocate = false;
                continue;
            }
            param = (Param*)expr;

            /* This param should point to datum in func */
            recfield = (PLpgSQL_recfield*)func->datums[param->paramid - 1];

            /* From there we get the new or old rec */
            rec = (PLpgSQL_rec*)func->datums[recfield->recparentno];
            if (strcmp(rec->refname, "new") != 0 && strcmp(rec->refname, "old") != 0) {
                is_colocate = false;
                continue;
            }

            /* We should already set tupdesc at the very beginning */
            if (rec->tupdesc == NULL) {
                is_colocate = false;
                continue;
            }

            /*
             * Find field index of new.a1 and only if it matches to
             * current distribution key of src table, we could call
             * both tables are DML colocated
             */
            fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
            if (fno != attnum1) {
                is_colocate = false;
                continue;
            }

            is_colocate = true;
            break;
        }
    }

    return is_colocate;
}

/*
 * @Description : check co-location for insert command. To check co-location of the
 * insert query in trigger and the triggering query, we walk through the lists of
 * attribute no of the distribution keys of both queries. For each pair, we check
 * if the trig table's dist key is in the target list, and if its expression is from
 * new/old and is the dist key from the table the triggering query is working on.
 *
 * For example,
 * tables:
 * create table t1(a1 int, b1 int) distribute by hash(a1,b1);
 * create table t2(a2 int, b2 int) distribute by hash(a2,b2);
 * insert in trig:
 * insert into t2 (a2, b2) values(NEW.a1, NEW.b1);
 * triggering event:
 * insert into t1 values(1,1);
 *
 * We walk through the dist key attr no pairs (a1, a2) and (b1, b2), check if
 * a2 exist in target list in the query in trigger, and if it's expression is
 * form new/old and is a1; check if b2 exist in target list in the query in
 * trigger, and if its expression is from new/old and is b1. If the above two
 * check are satisfied, the co-location is true.
 *
 * @in qry_part_attr_num : list of attr no of the dist keys of triggering query
 * @in trig_part_attr_num : list of attr no of the dist keys of query in trigger
 */
static bool plpgsql_check_insert_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ListCell* init_cell = list_head(query->targetList);

    forboth(lc1, qry_part_attr_num, lc2, trig_part_attr_num)
    {
        bool distribute_col_found = false;
        AttrNumber attnum1 = lfirst_int(lc1);
        AttrNumber attnum2 = lfirst_int(lc2);
        ListCell* cell = NULL;

        if (attnum1 != attnum2) {
            return false;
        }

        for_each_cell(cell, init_cell)
        {
            TargetEntry* tle = (TargetEntry*)lfirst(cell);
            Param* param = NULL;
            PLpgSQL_recfield* recfield = NULL;
            PLpgSQL_rec* rec = NULL;
            int fno;

            /* if current target entry is distribution key */
            if (tle->resno == attnum2) {
                /* new/old.a1 is replaced with param by parser */
                if (!IsA(tle->expr, Param)) {
                    return false;
                }

                param = (Param*)tle->expr;

                /* this param should point to datum in func */
                recfield = (PLpgSQL_recfield*)func->datums[param->paramid - 1];

                /* from there we get the new or old rec */
                rec = (PLpgSQL_rec*)func->datums[recfield->recparentno];
                if (strcmp(rec->refname, "new") != 0 && strcmp(rec->refname, "old") != 0) {
                    return false;
                }

                /*
                 * we should already set tupdesc at the very beginning like
                 * in plpgsql_is_trigger_shippable.
                 */
                if (rec->tupdesc == NULL) {
                    return false;
                }

                /*
                 * find field index of new.a1 and only if it matches to
                 * current distribution key of src table, we could call
                 * both tables are DML colocated
                 */
                fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
                if (fno != attnum1) {
                    return false;
                }

                distribute_col_found = true;

                /* break this loop to move on to check the next distribution. key */
                init_cell = lnext(init_cell);
                break;
            }
        }
        if (!distribute_col_found) {
            return false;
        }
    }
    return true;
}

