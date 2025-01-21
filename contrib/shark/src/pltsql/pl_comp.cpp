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
 *	  contrib/shark/src/pltsql/pl_comp.c
 *
 * -------------------------------------------------------------------------
 */

#include "pltsql.h"
#include "catalog/gs_package.h"
#include "catalog/pg_object.h"
#include "catalog/gs_dependencies.h"
#include "catalog/gs_dependencies_fn.h"
#include "catalog/pg_proc_fn.h"
#include "access/transam.h"
#include "parser/parse_coerce.h"
#include "utils/lsyscache.h"
#include "utils/pl_package.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include "commands/typecmds.h"

const int MAX_SUB_PROGRAM_LEVEL = 2;

static PLpgSQL_function* do_compile(FunctionCallInfo fcinfo, HeapTuple proc_tup, PLpgSQL_function* func,
    PLpgSQL_func_hashkey* hashkey, bool for_validator);
extern bool reload_proc_tuple_if_necessary(HeapTuple* proc_tup, Oid func_oid);
static void add_parameter_name(int item_type, int item_no, const char* name);
static void add_dummy_return(PLpgSQL_function* func);
static PLpgSQL_row* build_row_from_vars(PLpgSQL_variable** vars, int numvars);
static void plpgsql_resolve_polymorphic_argtypes(
    int numargs, Oid* arg_types, const char* arg_modes, Node* call_expr, bool for_validator, const char* pro_name);
static void plpgsql_HashTableInsert(PLpgSQL_function* func, PLpgSQL_func_hashkey* func_key);
static void plpgsql_append_dlcell(plpgsql_HashEnt* entity);
extern bool is_func_need_cache(Oid funcid, const char* func_name);
/* ----------
 * pltsql_compile		Make an execution tree for a PL/pgSQL function.
 *
 * If forValidator is true, we're only compiling for validation purposes,
 * and so some checks are skipped.
 *
 * Note: it's important for this to fall through quickly if the function
 * has already been compiled.
 * ----------
 */

/*
 * Check whether function's search path is same as system search_path.
 */
static bool plpgsql_check_search_path(PLpgSQL_function* func, HeapTuple proc_tup)
{
    /* If SUPPORT_BIND_SEARCHPATH is false, always return true. */
    return check_search_path_interface(func->fn_searchpath->schemas, proc_tup);
}

static void check_proc_args_type_match(FunctionCallInfo fcinfo, HeapTuple proc_tup, Form_pg_proc proc_struct)
{
    short nargs = Min(fcinfo->nargs, proc_struct->pronargs);
    if (nargs <= 0) {
        return;
    }
    oidvector* proargs = ProcedureGetArgTypes(proc_tup);

    for (short i = 0; i < nargs; i++) {
        if (IsPolymorphicType(proargs->values[i]) || !OidIsValid(fcinfo->argTypes[i])) {
            continue;
        }

        if (TypeCategory(proargs->values[i]) == TYPCATEGORY_STRING &&
            TypeCategory(fcinfo->argTypes[i]) == TYPCATEGORY_STRING) {
            continue;
        }

        if (OidIsValid(get_element_type(fcinfo->argTypes[i])) &&
            get_element_type(fcinfo->argTypes[i]) == get_element_type(proargs->values[i])) {
            continue;
        }

        if (fcinfo->argTypes[i] != proargs->values[i]) {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("type of function %u args have been changed, while the function maybe rebuilt",
                           fcinfo->flinfo->fn_oid)));
        }
    }
}

PLpgSQL_function* pltsql_compile(FunctionCallInfo fcinfo, bool for_validator, bool isRecompile)
{
    Oid func_oid = fcinfo->flinfo->fn_oid;
    PLpgSQL_func_hashkey hashkey;
    bool function_valid = false;
    bool hashkey_valid = false;
    bool isnull = false;
    bool func_valid = true;
    /*
     * Lookup the pg_proc tuple by Oid; we'll need it in any case
     */
    HeapTuple proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u, while compile function", func_oid)));
    }
    Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);

    /*
     * See if there's already a cache entry for the current FmgrInfo. If not,
     * try to find one in the hash table.
     */
    PLpgSQL_function* func = (PLpgSQL_function*)fcinfo->flinfo->fn_extra;
    bool ispackage = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_package, &isnull);
    Datum pkgoiddatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    Oid packageOid = ispackage ? DatumGetObjectId(pkgoiddatum) : InvalidOid;
    Oid old_value = saveCallFromPkgOid(packageOid);
    if (enable_plpgsql_gsdependency_guc()) {
        if (func == NULL) {
            /* Compute hashkey using function signature and actual arg types */
            compute_function_hashkey(proc_tup, fcinfo, proc_struct, &hashkey, for_validator);
            hashkey_valid = true;
            /* And do the lookup */
            func = plpgsql_HashTableLookup(&hashkey);
        }
        /**
         * only check for func need recompile or not,
        */
        if (func_oid >= FirstNormalObjectId) {
            func_valid = GetPgObjectValid(func_oid, OBJECT_TYPE_PROC);
        }
        if (!func_valid) {
            fcinfo->flinfo->fn_extra = NULL;
        }
        if (!func_valid && !u_sess->plsql_cxt.need_create_depend && !isRecompile) {
            if (!OidIsValid(packageOid)) {
            gsplsql_do_autonomous_compile(func_oid, false);
            }
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* Locking is performed before compilation. */
    gsplsql_lock_func_pkg_dependency_all(func_oid, PLSQL_FUNCTION_OBJ);
#endif

    if (reload_proc_tuple_if_necessary(&proc_tup, func_oid)) {
        proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
        func = NULL;
    }

    /*
     * perhaps function would be rebuilt, and args type maybe changed.
     * Coredump may occur if the input parameters are treated as other types,
     * so throw error directly.
     */
    check_proc_args_type_match(fcinfo, proc_tup, proc_struct);

recheck:
    if (func == NULL) {
        /* Compute hashkey using function signature and actual arg types */
        compute_function_hashkey(proc_tup, fcinfo, proc_struct, &hashkey, for_validator);
        hashkey_valid = true;

        /* And do the lookup */
        func = plpgsql_HashTableLookup(&hashkey);
    }

    if (!func_valid && func != NULL && !u_sess->plsql_cxt.need_create_depend &&
        !isRecompile && u_sess->SPI_cxt._connected >= 0 && !u_sess->plsql_cxt.during_compile) {
            func->is_need_recompile = true;
        }

    if (func != NULL) {
        /* We have a compiled function, but is it still valid? */
        if (func->fn_xmin == HeapTupleGetRawXmin(proc_tup) &&
            ItemPointerEquals(&func->fn_tid, &proc_tup->t_self) && plpgsql_check_search_path(func, proc_tup) &&
            !isRecompile && !func->is_need_recompile) {
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
    } else {
        if (OidIsValid(packageOid) && (u_sess->plsql_cxt.curr_compile_context == NULL ||
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL)) {
            PLpgSQL_package* pkg = PackageInstantiation(packageOid);
            ListCell* l = NULL;
            if (pkg == NULL) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), 
                    errmsg("not found package %u", packageOid)));
            }
            foreach(l, pkg->proc_compiled_list) {
                PLpgSQL_function* func = (PLpgSQL_function*)lfirst(l);
                if (func->fn_oid == fcinfo->flinfo->fn_oid) {
                    ReleaseSysCache(proc_tup);
                    return func;
                }
            }
            ereport(NOTICE, (errcode(ERRCODE_UNDEFINED_FUNCTION), 
                errmsg("not found function %u in package", fcinfo->flinfo->fn_oid)));
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
            compute_function_hashkey(proc_tup, fcinfo, proc_struct, &hashkey, for_validator);
        }
        /*
         * Do the hard part.
         */
        PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
        int save_compile_status = getCompileStatus();
        bool save_curr_status = GetCurrCompilePgObjStatus();
        PG_TRY();
        {
            List* ref_obj_list = gsplsql_prepare_recompile_func(func_oid, proc_struct->pronamespace, packageOid, isRecompile);
            SetCurrCompilePgObjStatus(true);
            bool isNull = false;
            /* Find method type from pg_object */
            char protypkind = OBJECTTYPE_NULL_PROC;
            HeapTuple objTuple = SearchSysCache2(PGOBJECTID,
                ObjectIdGetDatum(func_oid), CharGetDatum(OBJECT_TYPE_PROC));
            if (HeapTupleIsValid(objTuple)) {
                protypkind = GET_PROTYPEKIND(SysCacheGetAttr(PGOBJECTID, objTuple, Anum_pg_object_options, &isnull));
                ReleaseSysCache(objTuple);
            }
            if (!isNull && (protypkind == OBJECTTYPE_MEMBER_PROC || protypkind == OBJECTTYPE_CONSTRUCTOR_PROC))
                u_sess->plsql_cxt.typfunckind = protypkind;
            func = do_compile(fcinfo, proc_tup, func, &hashkey, for_validator);
            UpdateCurrCompilePgObjStatus(save_curr_status);
            gsplsql_complete_recompile_func(ref_obj_list);
            (void)CompileStatusSwtichTo(save_compile_status);
            u_sess->plsql_cxt.typfunckind = OBJECTTYPE_NULL_PROC;
        }
        PG_CATCH();
        {
#ifndef ENABLE_MULTIPLE_NODES
            bool insertError = (u_sess->attr.attr_common.plsql_show_all_error ||
                                    u_sess->attr.attr_sql.check_function_bodies) &&
                                    u_sess->plsql_cxt.isCreateFunction;
            if (insertError) {
                InsertError(func_oid);
            }
#endif
            u_sess->plsql_cxt.typfunckind = OBJECTTYPE_NULL_PROC;
            SetCurrCompilePgObjStatus(save_compile_status);
            popToOldCompileContext(save_compile_context);
            (void)CompileStatusSwtichTo(save_compile_status);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    ReleaseSysCache(proc_tup);

    /*
     * Save pointer in FmgrInfo to avoid search on subsequent calls
     */
    fcinfo->flinfo->fn_extra = (void*)func;
    restoreCallFromPkgOid(old_value);
    /*
     * Finally return the compiled function
     */
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
    /* pipelined function allow */
    if (num_out_args == 1 || (num_out_args == 0 && func->fn_rettype == VOIDOID ) || func->fn_retset ||
        func->fn_rettype == RECORDOID || func->is_pipelined) {
        return true;
    } else {
        return false;
    }
}

static bool IsNestAutonmous(List* stmts)
{
    for (int stmtid = 0; stmtid < list_length(stmts); stmtid++) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)list_nth(stmts, stmtid);
        if ((enum PLpgSQL_stmt_types)stmt->cmd_type == PLPGSQL_STMT_BLOCK) {
            PLpgSQL_stmt_block* block = (PLpgSQL_stmt_block*)stmt;
            if (block->isAutonomous == true || IsNestAutonmous(block->body)) {
                return true;
            }
        }
    }

    return false;
}

static bool CheckPipelinedResIsTuple(Form_pg_type type_struct) {
    bool retTypeIsValid = true;
    char typeCategory = type_struct->typcategory;
    if (type_struct->typtype == TYPTYPE_TABLEOF) {
        /**
         * table of index type is not supported as function return type. see
         * functioncmds.cpp#compute_return_type
         * the branch is reached when pipelined function is in target list
         * or nest table in nest table(unsupported feature)
         */
        retTypeIsValid = (typeCategory != TYPCATEGORY_TABLEOF_VARCHAR &&
                          typeCategory != TYPCATEGORY_TABLEOF_INTEGER);
    } else if (type_struct->typtype == TYPTYPE_BASE) {
        retTypeIsValid = typeCategory == TYPCATEGORY_ARRAY;
    } else {
        retTypeIsValid = false;
    }
    if (!retTypeIsValid) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("pipeLined function must have a supported collection return type"),
                        errcause("Wrong return type"),
                        erraction("Please check return type of the pipelined function")));
    }
    
    /* check res is tuple */
    bool pipelinedResIsTuple = false;
    Oid subType = SearchSubTypeByType(type_struct, NULL);
    HeapTuple typeElem = SearchSysCache1(TYPEOID, ObjectIdGetDatum(subType));
    if (!HeapTupleIsValid(typeElem)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for type %u", subType)));
    }
    Form_pg_type typeElemStruct = (Form_pg_type)GETSTRUCT(typeElem);
    if (typeElemStruct->typrelid != InvalidOid || type_struct->typelem == RECORDOID) {
        pipelinedResIsTuple = true;
    }
    ReleaseSysCache(typeElem);
    return pipelinedResIsTuple;
}

static void plpgsql_add_param_initdatums(int *newvarnos, int newnum, int** oldvarnos, int oldnum)
{
    int *varnos = NULL;
    errno_t rc = 0;
    varnos = (int*)palloc(sizeof(int) * (newnum + oldnum));

    if (oldnum > 0) {
        rc = memcpy_s(varnos, sizeof(int) * oldnum, (int*)(*oldvarnos), sizeof(int) * oldnum);
        securec_check(rc, "", "");
    }
    if (newnum > 0) {
        rc = memcpy_s(varnos + oldnum, sizeof(int) * newnum, newvarnos, sizeof(int) * newnum);
        securec_check(rc, "", "");
    }

    if (*oldvarnos)
        pfree(*oldvarnos);
    *oldvarnos = varnos;
}

/*
 * This is the slow part of pltsql_compile().
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
 * allocations is the u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt.
 *
 * NB: this code is not re-entrant.  We assume that nothing we do here could
 * result in the invocation of another plpgsql function.
 */
static PLpgSQL_function* do_compile(FunctionCallInfo fcinfo, HeapTuple proc_tup, PLpgSQL_function* func,
    PLpgSQL_func_hashkey* hashkey, bool for_validator)
{
    Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
    bool is_dml_trigger = CALLED_AS_TRIGGER(fcinfo);
    bool is_event_trigger = CALLED_AS_EVENT_TRIGGER(fcinfo);
    Datum proisprivatedatum;
    bool isnull = false;
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
    Oid pkgoid = InvalidOid;
    Oid namespaceOid = InvalidOid;
    int *allvarnos = NULL;
    int n_varnos = 0;

    // for subprogram.
    Oid parent_oid = InvalidOid;
    PLpgSQL_compile_context* parent_func_context = NULL;
    PLpgSQL_function* parent_func = NULL;
    
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
    Datum prosrcdatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_prosrc, &isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The definition of the function is null"),
                errhint("Check whether the definition of the function is complete in the pg_proc system table.")));
    }
    
    char* proc_source = TextDatumGetCString(prosrcdatum);
    bool ispackage = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_package, &isnull);
    Datum pkgoiddatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    if (!isnull && ispackage) {
        pkgoid = ObjectIdGetDatum(pkgoiddatum);
    }
    if (OidIsValid(Anum_pg_proc_proisprivate)) {
        proisprivatedatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_proisprivate, &isnull);
    } else {
        proisprivatedatum = BoolGetDatum(false);
    }

    /* get function's prokind */
    Datum prokindDatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_prokind, &isnull);
    bool isFunc = false;
    if (isnull || PROC_IS_FUNC(DatumGetChar(prokindDatum))) {
        /* Null prokind items are created when there is no procedure */
        isFunc = true;
    }
    bool is_pipelined = !isnull && PROC_IS_PIPELINED(DatumGetChar(prokindDatum));
    
    
    Datum pronamespaceDatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_pronamespace, &isnull);
    namespaceOid = DatumGetObjectId(pronamespaceDatum);
    /*
     * Setup error traceback support for ereport()
     */
    pl_err_context.callback = plpgsql_compile_error_callback;
    pl_err_context.arg = for_validator ? proc_source : NULL;
    pl_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pl_err_context;

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
    u_sess->plsql_cxt.plpgsql_IndexErrorVariable = 0;

    signature = format_procedure(fcinfo->flinfo->fn_oid);

    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
           u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        parent_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_oid;
        parent_func_context = u_sess->plsql_cxt.curr_compile_context;
        parent_func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
    }

    /*
     * All the permanent output of compilation (e.g. parse tree) is kept in a
     * per-function memory context, so it can be reclaimed easily.
     */
    rc = snprintf_s(
        context_name, NAMEDATALEN, NAMEDATALEN - 1, "%s_%lu", "PL/pgSQL function context", u_sess->debug_query_id);
    securec_check_ss(rc, "", "");
    int save_compile_status = getCompileStatus();
    PLpgSQL_compile_context* curr_compile = NULL;
    /* If has other function in package body header, should switch compile status to function,
       in case compile function in package's context. */
    if (u_sess->plsql_cxt.compile_status == COMPILIE_PKG_ANON_BLOCK) {
        if (!OidIsValid(pkgoid)) {
            /* has function in package body header */
            save_compile_status = CompileStatusSwtichTo(COMPILIE_PKG_ANON_BLOCK_FUNC);
        } else if (u_sess->plsql_cxt.curr_compile_context != NULL &&
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL &&
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid != pkgoid) {
            /* should not has other package's function in package body header compiled here */
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("current compile context's package %u not match current package %u",
                           u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid, pkgoid),
                    errdetail("N/A"), errcause("nested compilation swtich context error"),
                    erraction("check logic of compilation")));
        }
    }
    /*
     * if the function belong to a package,then the function memorycontext will use the package memorycontext,
     * because function may use package variable
     */
    curr_compile = createCompileContext(context_name);
    SPI_NESTCOMPILE_LOG(curr_compile->compile_cxt);

    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        checkCompileMemoryContext(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    u_sess->plsql_cxt.curr_compile_context = curr_compile;
    pushCompileContext();
    pltsql_scanner_init(proc_source);
    curr_compile->plpgsql_curr_compile = func;
    curr_compile->plpgsql_error_funcname = pstrdup(NameStr(proc_struct->proname));
    /*
     * Do extra syntax checks when validating the function definition. We skip
     * this when actually compiling functions for execution, for performance
     * reasons.
     */
    curr_compile->plpgsql_check_syntax = for_validator;

    /*
     * compile_tmp_cxt is a short temp context that will be detroyed after
     * function compile or execute.
     * func_cxt is a long term context that will stay until thread exit. So
     * malloc on func_cxt should be very careful.
     * signature is stored on a StringInfoData which is 1K byte at least, but
     * most signature will not be so long originally, so we should do a strdup.
     */
    curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(curr_compile->compile_cxt);
    func->fn_signature = pstrdup(signature);
    func->is_private = BoolGetDatum(proisprivatedatum);
    func->namespaceOid = namespaceOid;
    /*
     * if function belong to a package, it will use package search path.
     */
    if (curr_compile->plpgsql_curr_compile_package == NULL) {
        func->fn_searchpath = (OverrideSearchPath*)palloc0(sizeof(OverrideSearchPath));
    } else {
        func->fn_searchpath = curr_compile->plpgsql_curr_compile_package->pkg_searchpath;
    }
    func->fn_owner = proc_struct->proowner;
    func->fn_oid = fcinfo->flinfo->fn_oid;

    func->parent_oid = parent_oid;
    func->parent_func = parent_func;
    if (parent_func) {
        func->block_level = parent_func->block_level + 1;
        if (func->block_level > MAX_SUB_PROGRAM_LEVEL) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("The definition of the function is error."),
                    errhint("Subprogram nesting levels should be less than 2.")));
        }
        func->in_anonymous = parent_func->in_anonymous;
    }

    func->fn_xmin = HeapTupleGetRawXmin(proc_tup);
    func->fn_tid = proc_tup->t_self;
    func->fn_input_collation = fcinfo->fncollation;
    func->fn_cxt = curr_compile->compile_cxt;
    func->out_param_varno = -1; /* set up for no OUT param */
    func->resolve_option = GetResolveOption();
    func->invalItems = NIL;
    func->is_autonomous = false;
    func->is_insert_gs_source = false;

    func->pkg_oid = pkgoid;
    func->fn_searchpath->addCatalog = true;
    func->fn_searchpath->addTemp = true;
    func->ns_top = curr_compile->ns_top;
    func->guc_stat = u_sess->utils_cxt.behavior_compat_flags;
    func->is_pipelined = is_pipelined;

    if (is_dml_trigger)
        func->fn_is_trigger = PLPGSQL_DML_TRIGGER;
    else if (is_event_trigger)
        func->fn_is_trigger = PLPGSQL_EVENT_TRIGGER;
    else
        func->fn_is_trigger = PLPGSQL_NOT_TRIGGER;

    if (proc_struct->pronamespace == PG_CATALOG_NAMESPACE || proc_struct->pronamespace == PG_DB4AI_NAMESPACE) {
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
            if (module_logging_is_on(MOD_SCHEMA)) {
                char* str = nodeToString(func->fn_searchpath->schemas);
                ereport(DEBUG2, (errmodule(MOD_SCHEMA), errmsg("fn_searchpath:%s", str)));
                pfree(str);
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

    curr_compile->plpgsql_DumpExecTree = false;
    curr_compile->datums_alloc = alloc_size;
    curr_compile->plpgsql_nDatums = 0;
    /* This is short-lived, so needn't allocate in function's cxt */
    curr_compile->plpgsql_Datums = (PLpgSQL_datum**)MemoryContextAlloc(
        curr_compile->compile_tmp_cxt, sizeof(PLpgSQL_datum*) * curr_compile->datums_alloc);
    curr_compile->datum_need_free = (bool*)MemoryContextAlloc(
        curr_compile->compile_tmp_cxt, sizeof(bool) * curr_compile->datums_alloc);
    curr_compile->datums_last = 0;
    add_pkg_compile();
    add_parent_func_compile(parent_func_context);
    Oid base_oid = InvalidOid;
    bool isHaveTableOfIndexArgs = false;
    bool isHaveOutRefCursorArgs = false;
    switch (func->fn_is_trigger) {        
        case PLPGSQL_NOT_TRIGGER:

            /*
             * Fetch info about the procedure's parameters. Allocations aren't
             * needed permanently, so make them in tmp cxt.
             *
             * We also need to resolve any polymorphic input or output
             * argument types.	In validation mode we won't be able to, so we
             * arbitrarily assume we are dealing with integers.
             */
            MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);

            numargs = get_func_arg_info(proc_tup, &arg_types, &argnames, &arg_modes);

            plpgsql_resolve_polymorphic_argtypes(numargs, arg_types, arg_modes,
                fcinfo->flinfo->fn_expr, for_validator, curr_compile->plpgsql_error_funcname);

            in_arg_varnos = (int*)palloc0(numargs * sizeof(int));
            out_arg_variables = (PLpgSQL_variable**)palloc0(numargs * sizeof(PLpgSQL_variable*));

            MemoryContextSwitchTo(curr_compile->compile_cxt);

            /*
             * Create the variables for the procedure's parameters.
             */
            for (i = 0; i < numargs; i++) {
                const int buf_size = 32;
                char buf[buf_size];
                Oid arg_type_id = arg_types[i];
                int attrnum = 0;
                char arg_mode = arg_modes ? arg_modes[i] : PROARGMODE_IN;
                PLpgSQL_variable* argvariable = NULL;
                int arg_item_type;
                errno_t err = EOK;

                /* if procedure args have table of index by type */
                isHaveTableOfIndexArgs = isHaveTableOfIndexArgs || isTableofIndexbyType(arg_types[i]);

                /* Create $n name for variable */
                err = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "$%d", i + 1);
                securec_check_ss(err, "", "");

                /* Create datatype info */
                PLpgSQL_type* argdtype = plpgsql_build_datatype(arg_type_id, -1, func->fn_input_collation);
                if (arg_mode != PROARGMODE_IN && arg_mode != PROARGMODE_INOUT) {
                    argdtype->defaultvalues = get_default_plpgsql_expr_from_typeoid(arg_type_id, &attrnum);
                }

                /* Disallow pseudotype argument */
                /* (note we already replaced polymorphic types) */
                /* (build_variable would do this, but wrong message) */
                if (argdtype->ttype != PLPGSQL_TTYPE_SCALAR && argdtype->ttype != PLPGSQL_TTYPE_ROW) {
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("PL/pgSQL functions cannot accept type %s", format_type_be(arg_type_id))));
                }
                /* Build variable and add to datum list */
                if (argnames != NULL) {
                    argvariable = plpgsql_build_variable(buf, 0, argdtype, false, false, argnames[i]);
                } else {
                    argvariable = plpgsql_build_variable(buf, 0, argdtype, false);
                }
                if (argdtype->defaultvalues) {
                    PLpgSQL_row *row = (PLpgSQL_row *)argvariable;
                    plpgsql_add_param_initdatums(row->varnos, attrnum, &allvarnos, n_varnos);
                    n_varnos = n_varnos + attrnum;
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

                if (arg_type_id == REFCURSOROID
                    && (arg_mode == PROARGMODE_OUT || arg_mode == PROARGMODE_INOUT)) {
                    isHaveOutRefCursorArgs = true;
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
            func->is_plpgsql_func_with_outparam = is_function_with_plpgsql_language_and_outparam(func->fn_oid);
            if (num_out_args == 1 && !func->is_plpgsql_func_with_outparam) {
                func->out_param_varno = out_arg_variables[0]->dno;
            } else if (num_out_args > 1 || (num_out_args == 1 &&
                       func->is_plpgsql_func_with_outparam)) {
                PLpgSQL_row* row = build_row_from_vars(out_arg_variables, num_out_args);
                row->isImplicit = true;

                int varno = 0;
                varno = plpgsql_adddatum((PLpgSQL_datum*)row);
                func->out_param_varno = varno;
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
                                    u_sess->plsql_cxt.curr_compile_context->plpgsql_error_funcname)));
                    }
                }
            }

            /* do not rewrite fn_rettype if func is pipelined function */
            if (!func->is_pipelined && isTableofType(rettypeid, &base_oid, NULL)) {
                func->fn_rettype = base_oid;
            } else {
                func->fn_rettype = rettypeid;
            }

            if (rettypeid == REFCURSOROID) {
                isHaveOutRefCursorArgs = true;
            }

            /*
             * Normal function has a defined returntype
             */
            
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
                } else if (rettypeid == TRIGGEROID || rettypeid == EVTTRIGGEROID) {
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
                        "$0", 0, build_datatype(type_tup, -1, func->fn_input_collation), true, true);
                }
            }

            /* check pipelined result */
            func->pipelined_resistuple = func->is_pipelined && CheckPipelinedResIsTuple(type_struct);

            ReleaseSysCache(type_tup);
            break;

        case PLPGSQL_DML_TRIGGER:    
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
            rec = plpgsql_build_record("new", 0, true, NULL);
            func->new_varno = rec->dno;

            /* Add the record for referencing OLD */
            rec = plpgsql_build_record("old", 0, true, NULL);
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
        case PLPGSQL_EVENT_TRIGGER:
            func->fn_rettype = VOIDOID;
            func->fn_retbyval = false;
            func->fn_retistuple = true;
            func->fn_retset = false;
 
            /* shouldn't be any declared arguments */
            if (proc_struct->pronargs != 0)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                         errmsg("event trigger functions cannot have declared arguments")));
                                     
            /* Add the variable tg_event */
            var = plpgsql_build_variable("tg_event", 0,
                                         plpgsql_build_datatype(TEXTOID,
                                                                -1,
                                               func->fn_input_collation),
                                         true);
            func->tg_event_varno = var->dno;
 
            /* Add the variable tg_tag */
            var = plpgsql_build_variable("tg_tag", 0,
                                         plpgsql_build_datatype(TEXTOID,
                                                                -1,
                                               func->fn_input_collation),
                                         true);
            func->tg_tag_varno = var->dno;
 
            break;  

        default:
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized function typecode: %d", (int)func->fn_is_trigger),
                    errhint("This node type is expected to be a function or trigger.")));
            break;
    }
    /* Remember if function is STABLE/IMMUTABLE */
    func->fn_readonly = (proc_struct->provolatile != PROVOLATILE_VOLATILE);

    /*
     * Create the magic FOUND variable.
     */
    var = plpgsql_build_variable("found", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->found_varno = var->dno;
    /* Create the magic FOUND variable for implicit cursor attribute %found. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_found__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->sql_cursor_found_varno = var->dno;

    /* Create the magic NOTFOUND variable for implicit cursor attribute %notfound. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_notfound__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->sql_notfound_varno = var->dno;

    /* Create the magic ISOPEN variable for implicit cursor attribute %isopen. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_isopen__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->sql_isopen_varno = var->dno;

    /* Create the magic ROWCOUNT variable for implicit cursor attribute %rowcount. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_rowcount__", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true, true);
    func->sql_rowcount_varno = var->dno;

    PushOverrideSearchPath(func->fn_searchpath);

    saved_pseudo_current_userId = u_sess->misc_cxt.Pseudo_CurrentUserId;
    u_sess->misc_cxt.Pseudo_CurrentUserId = &func->fn_owner;

    // Create the magic sqlcode variable marco
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        var = plpgsql_build_variable("sqlcode", 0, plpgsql_build_datatype(TEXTOID, -1, InvalidOid), true, true);
    } else {
        var = plpgsql_build_variable("sqlcode", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true, true);
    }
    func->sqlcode_varno = var->dno;
    var = plpgsql_build_variable("sqlstate", 0, plpgsql_build_datatype(TEXTOID, -1, InvalidOid), true, true);
    func->sqlstate_varno = var->dno;
    var = plpgsql_build_variable("sqlerrm", 0, plpgsql_build_datatype(TEXTOID, -1, InvalidOid), true, true);
    func->sqlerrm_varno = var->dno;

#ifndef ENABLE_MULTIPLE_NODES
    /* Create bulk exception implicit variable for FORALL SAVE EXCEPTIONS */
    Oid typeOid = get_typeoid(PG_CATALOG_NAMESPACE, "bulk_exception");
    if (typeOid != InvalidOid) { /* version control */
        Oid arrTypOid = get_array_type(get_typeoid(PG_CATALOG_NAMESPACE, "bulk_exception"));
        var = plpgsql_build_variable(
            "bulk_exceptions", 0, plpgsql_build_datatype(arrTypOid, -1, InvalidOid), true, true);
        func->sql_bulk_exceptions_varno = var->dno;
    }
#endif
    /*
     * Now parse the function's text
     */
     /* 重置plpgsql块层次计数器 */
    u_sess->plsql_cxt.block_level = 0;
    bool saved_flag = u_sess->plsql_cxt.have_error;
    ResourceOwnerData* oldowner = NULL;
    int64 stackId = 0;
    MemoryContext oldcxt;
    volatile bool has_error = false;
    if (enable_plpgsql_gsdependency_guc() && u_sess->plsql_cxt.isCreateFunction && !IsInitdb) {
        oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
        oldcxt = CurrentMemoryContext;
        SPI_savepoint_create("createFunction");
        stackId = u_sess->plsql_cxt.nextStackEntryId;
        MemoryContextSwitchTo(oldcxt);
        bool save_isPerform = u_sess->parser_cxt.isPerform;
        PG_TRY();
        {
            u_sess->parser_cxt.isPerform = false;
            parse_rc = pltsql_yyparse();
            u_sess->parser_cxt.isPerform = save_isPerform;
            SPI_savepoint_release("createFunction");
            stp_cleanup_subxact_resource(stackId);
            MemoryContextSwitchTo(oldcxt);
            t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
        }
        PG_CATCH();
        {
            u_sess->parser_cxt.isPerform = save_isPerform;
            SPI_savepoint_rollbackAndRelease("createFunction", InvalidTransactionId);
            stp_cleanup_subxact_resource(stackId);
            t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
            MemoryContextSwitchTo(oldcxt);
            has_error = true;
            ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
            ereport(WARNING,
                    (errmodule(MOD_PLSQL),
                     errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("%s", edata->message),
                     errdetail("N/A"),
                     errcause("compile package or procedure error."),
                     erraction("check package or procedure error and redefine")));
            if (edata->sqlerrcode == ERRCODE_OUT_OF_LOGICAL_MEMORY) {
                PG_RE_THROW();
            }
            FlushErrorState();
        }
        PG_END_TRY();
    } else {
        bool save_isPerform = u_sess->parser_cxt.isPerform;
        u_sess->parser_cxt.isPerform = false;
        parse_rc = pltsql_yyparse();
        u_sess->parser_cxt.isPerform = save_isPerform;
    }
    if (enable_plpgsql_gsdependency_guc() && has_error) {
        pltsql_scanner_finish();
        pfree_ext(proc_source);
        PopOverrideSearchPath();
        u_sess->plsql_cxt.curr_compile_context = popCompileContext();
        clearCompileContext(curr_compile);
        return NULL;
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->plsql_cxt.have_error && u_sess->attr.attr_common.plsql_show_all_error) {
        u_sess->plsql_cxt.have_error = false;
        u_sess->plsql_cxt.create_func_error = true;
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Debug mod,create procedure has error."),
                errdetail("N/A"),
                errcause("compile procedure error."),
                erraction("check procedure error and redefine procedure")));  
    }
#endif
    u_sess->plsql_cxt.have_error = saved_flag;
    if (parse_rc != 0) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
    }
    func->action = curr_compile->plpgsql_parse_result;
    if (n_varnos > 0) {
        plpgsql_add_param_initdatums(allvarnos, n_varnos, &func->action->initvarnos, func->action->n_initvars);
        func->action->n_initvars = func->action->n_initvars + n_varnos;
    }

    if (is_dml_trigger && func->action->isAutonomous) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Triggers do not support autonomous transactions"),
                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                erraction("Modify SQL statement according to the manual.")));
    }

    if (func->fn_retset && func->action->isAutonomous) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Autonomous transactions do not support RETURN SETOF."),
                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                erraction("Modify SQL statement according to the manual.")));
    }

    if (isHaveTableOfIndexArgs && func->action->isAutonomous) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Autonomous do not support table of index Or record nested tabel of index as in, out args."),
                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                erraction("Modify SQL statement according to the manual.")));
    }

    if (isHaveTableOfIndexArgs && isFunc) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Function do not support table of index Or record nested tabel of index as in, out args."),
                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                erraction("Modify SQL statement according to the manual.")));
    }

    if (isHaveOutRefCursorArgs && isFunc && func->action->isAutonomous) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Autonomous function do not support ref cursor as return types or out, inout arguments."),
                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                erraction("Use procedure instead.")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (proc_struct->provolatile != PROVOLATILE_VOLATILE && func->action->isAutonomous) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Autonomous transactions do not support STABLE/IMMUTABLE."),
                errdetail("Please remove stable/immutable."),
                errcause("PL/SQL uses unsupported feature."),
                erraction("Modify SQL statement according to the manual.")));
    }
#endif

    if (IsNestAutonmous(func->action->body)) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Autonomous transactions do not support nested anonymous blocks."),
                errdetail("N/A"),
                errcause("PL/SQL uses unsupported feature."),
                erraction("You can delete AUTONOMOUS_TRANSACTION parameter.")));
    }

    func->goto_labels = curr_compile->goto_labels;

    pltsql_scanner_finish();
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

    /* pipelined function: readonly */
    if (func->is_pipelined && !func->is_autonomous) {
        func->fn_readonly = true;
    }
    
    /*
     * Complete the function's info
     */
    func->fn_nargs = proc_struct->pronargs;
    for (i = 0; i < func->fn_nargs; i++) {
        func->fn_argvarnos[i] = in_arg_varnos[i];
    }
    func->ndatums = curr_compile->plpgsql_nDatums;
    func->subprogram_ndatums = curr_compile->plpgsql_subprogram_nDatums;
    func->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * curr_compile->plpgsql_nDatums);
    func->datum_need_free = (bool*)palloc(sizeof(bool) * curr_compile->plpgsql_nDatums);
    for (i = 0; i < curr_compile->plpgsql_nDatums; i++) {
        func->datums[i] = curr_compile->plpgsql_Datums[i];
        func->datum_need_free[i] = curr_compile->datum_need_free[i];
    }
    /* Debug dump for completed functions */
    if (curr_compile->plpgsql_DumpExecTree) {
        plpgsql_dumptree(func);
    }
        
    if (enable_plpgsql_gsdependency_guc()) {
        bool curr_compile_status = GetCurrCompilePgObjStatus();
        if (curr_compile_status) {
            bool is_undefined = gsplsql_is_undefined_func(func->fn_oid);
            func->isValid = !is_undefined;

            if (!func->isValid && u_sess->plsql_cxt.createPlsqlType == CREATE_PLSQL_TYPE_RECOMPILE) {
                GsDependObjDesc obj = gsplsql_construct_func_head_obj(func->fn_oid, func->namespaceOid, func->pkg_oid);
                obj.type = GSDEPEND_OBJECT_TYPE_PROCHEAD;
                gsplsql_do_refresh_proc_header(&obj, &is_undefined);
            }

            if (is_undefined && !u_sess->plsql_cxt.compile_has_warning_info) {
                u_sess->plsql_cxt.compile_has_warning_info = true;
                ereport(WARNING, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                                  errmsg("The header information of function %s is not defined.", NameStr(proc_struct->proname))));
            }
            UpdateCurrCompilePgObjStatus(!is_undefined);
        } else {
            func->isValid = GetCurrCompilePgObjStatus();
        }
    } else {
        func->isValid = true;
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
    curr_compile->plpgsql_error_funcname = NULL;
    curr_compile->plpgsql_curr_compile = NULL;
    curr_compile->plpgsql_check_syntax = false;

    if (curr_compile->plpgsql_curr_compile_package != NULL &&
        func->fn_cxt == curr_compile->plpgsql_curr_compile_package->pkg_cxt) {
        List* proc_compiled_list = curr_compile->plpgsql_curr_compile_package->proc_compiled_list;
        curr_compile->plpgsql_curr_compile_package->proc_compiled_list = lappend(proc_compiled_list, func);
    }    
    MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
    CompileStatusSwtichTo(save_compile_status);
    if (curr_compile->plpgsql_curr_compile_package == NULL)
        curr_compile->compile_tmp_cxt = NULL;
    ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
        errmsg("%s finish compile, level: %d", __func__, list_length(u_sess->plsql_cxt.compile_context_list))));
    u_sess->plsql_cxt.curr_compile_context = popCompileContext();
    clearCompileContext(curr_compile);
    if (func->sub_type_oid_list != NULL) {
        ListCell* cell = NULL;
        foreach(cell, func->sub_type_oid_list) {
            Oid* oid = (Oid*)lfirst(cell);
            if (func->fn_oid == OID_MAX && OidIsValid(*oid)) {
                HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(*oid));
                if (HeapTupleIsValid(tup)) {
                    ReleaseSysCache(tup);
                    RemoveTypeById(*oid);
                }
            }
        }
        list_free_deep(func->sub_type_oid_list);
        func->sub_type_oid_list = NULL;
    }
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
    return func;
}

/* ----------
 * pltsql_compile_inline	Make an execution tree for an anonymous code block.
 *
 * Note: this is generally parallel to do_compile(); is it worth trying to
 * merge the two?
 *
 * Note: we assume the block will be thrown away so there is no need to build
 * persistent data structures.
 * ----------
 */
PLpgSQL_function* pltsql_compile_inline(char* proc_source)
{
    const char* func_name = "inline_code_block";
    ErrorContextCallback pl_err_context;
    Oid typinput;
    PLpgSQL_variable* var = NULL;
    int parse_rc;
    const int alloc_size = 128;
    int oldCompileStatus = getCompileStatus();
    /* if is anonymous Blocks */
    if (oldCompileStatus == NONE_STATUS) {
        CompileStatusSwtichTo(COMPILIE_ANON_BLOCK);
    }
    PLpgSQL_compile_context* curr_compile = createCompileContext("PL/pgSQL function context");
    SPI_NESTCOMPILE_LOG(curr_compile->compile_cxt);
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    u_sess->plsql_cxt.curr_compile_context = curr_compile;
    pushCompileContext();
    /*
     * Setup the scanner input and error info.	We assume that this function
     * cannot be invoked recursively, so there's no need to save and restore
     * the static variables used here.
     */
    pltsql_scanner_init(proc_source);

    curr_compile->plpgsql_error_funcname = pstrdup(func_name);

    /*
     * Setup error traceback support for ereport()
     */
    pl_err_context.callback = plpgsql_compile_error_callback;
    pl_err_context.arg = proc_source;
    pl_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pl_err_context;

    /* Do extra syntax checking if u_sess->attr.attr_sql.check_function_bodies is on */
    curr_compile->plpgsql_check_syntax = u_sess->attr.attr_sql.check_function_bodies;

    /* Function struct does not live past current statement */
    PLpgSQL_function* func = (PLpgSQL_function*)palloc0(sizeof(PLpgSQL_function));

    curr_compile->plpgsql_curr_compile = func;
    curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(curr_compile->compile_cxt);
    func->fn_oid = OID_MAX;
    func->in_anonymous = true;
    func->fn_signature = pstrdup(func_name);
    func->fn_is_trigger = PLPGSQL_NOT_TRIGGER;
    func->fn_input_collation = InvalidOid;
    func->fn_cxt = curr_compile->compile_cxt;
    func->out_param_varno = -1; /* set up for no OUT param */
    func->resolve_option = GetResolveOption();

    plpgsql_ns_init();
    plpgsql_ns_push(func_name);
    curr_compile->plpgsql_DumpExecTree = false;
    curr_compile->datums_alloc = alloc_size;
    curr_compile->plpgsql_nDatums = 0;
    curr_compile->plpgsql_Datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * curr_compile->datums_alloc);
    curr_compile->datum_need_free = (bool*)palloc(sizeof(bool) * curr_compile->datums_alloc);
    curr_compile->datums_last = 0;

    /* Set up as though in a function returning VOID */
    func->fn_rettype = VOIDOID;
    func->fn_retset = false;
    func->fn_retistuple = false;
    /* a bit of hardwired knowledge about type VOID here */
    func->fn_retbyval = true;
    func->fn_rettyplen = sizeof(int32);
    func->is_autonomous = false;
    func->is_insert_gs_source = false;
    getTypeInputInfo(VOIDOID, &typinput, &func->fn_rettypioparam);
    fmgr_info(typinput, &(func->fn_retinput));

    /*
     * Remember if function is STABLE/IMMUTABLE.  XXX would it be better to
     * set this TRUE inside a read-only transaction?  Not clear.
     */
    func->fn_readonly = false;
    add_pkg_compile();
    /*
     * Create the magic FOUND variable.
     */
    var = plpgsql_build_variable("found", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->found_varno = var->dno;

    /* Create the magic FOUND variable for implicit cursor attribute %found. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_found__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->sql_cursor_found_varno = var->dno;

    /* Create the magic NOTFOUND variable for implicit cursor attribute %notfound. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_notfound__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->sql_notfound_varno = var->dno;

    /* Create the magic ISOPEN variable for implicit cursor attribute %isopen. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_isopen__", 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true, true);
    func->sql_isopen_varno = var->dno;

    /* Create the magic ROWCOUNT variable for implicit cursor attribute %rowcount. */
    var = plpgsql_build_variable(
        "__gsdb_sql_cursor_attri_rowcount__", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true, true);
    func->sql_rowcount_varno = var->dno;

    /* Create the magic sqlcode variable marco */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        var = plpgsql_build_variable("sqlcode", 0, plpgsql_build_datatype(TEXTOID, -1, InvalidOid), true, true);
    } else {
    var = plpgsql_build_variable("sqlcode", 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true, true);
    }
    func->sqlcode_varno = var->dno;
    var = plpgsql_build_variable("sqlstate", 0, plpgsql_build_datatype(TEXTOID, -1, InvalidOid), true, true);
    func->sqlstate_varno = var->dno;
    var = plpgsql_build_variable("sqlerrm", 0, plpgsql_build_datatype(TEXTOID, -1, InvalidOid), true, true);
    func->sqlerrm_varno = var->dno;
#ifndef ENABLE_MULTIPLE_NODES
    /* Create bulk exception implicit variable for FORALL SAVE EXCEPTIONS */
    Oid typeOid = get_typeoid(PG_CATALOG_NAMESPACE, "bulk_exception");
    if (typeOid != InvalidOid) { /* version control */
        Oid arrTypOid = get_array_type(get_typeoid(PG_CATALOG_NAMESPACE, "bulk_exception"));
        var = plpgsql_build_variable(
            "bulk_exceptions", 0, plpgsql_build_datatype(arrTypOid, -1, InvalidOid), true, true);
        func->sql_bulk_exceptions_varno = var->dno;
    }
#endif
    /*
     * Now parse the function's text
     */
     /* 重置plpgsql块层次计数器 */
    u_sess->plsql_cxt.block_level = 0;
    parse_rc = pltsql_yyparse();
    if (parse_rc != 0) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
    }
    func->action = curr_compile->plpgsql_parse_result;
    func->goto_labels = curr_compile->goto_labels;

    pltsql_scanner_finish();

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
    func->ndatums = curr_compile->plpgsql_nDatums;
    func->datums = (PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * curr_compile->plpgsql_nDatums);
    func->datum_need_free = (bool*)palloc(sizeof(bool) * curr_compile->plpgsql_nDatums);
    for (int i = 0; i < curr_compile->plpgsql_nDatums; i++) {
        func->datums[i] = curr_compile->plpgsql_Datums[i];
        func->datum_need_free[i] = curr_compile->datum_need_free[i];
    }
    /*
     * Pop the error context stack
     */
    t_thrd.log_cxt.error_context_stack = pl_err_context.previous;
    curr_compile->plpgsql_error_funcname = NULL;
    curr_compile->plpgsql_curr_compile = NULL; 
    curr_compile->plpgsql_check_syntax = false;

    MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
    curr_compile->compile_tmp_cxt = NULL;
    ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
        errmsg("%s finish compile, level: %d", __func__, list_length(u_sess->plsql_cxt.compile_context_list))));
    u_sess->plsql_cxt.curr_compile_context = popCompileContext();
    clearCompileContext(curr_compile);
    CompileStatusSwtichTo(oldCompileStatus);
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
    return func;
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
    PLpgSQL_nsitem* item = plpgsql_ns_lookup(plpgsql_ns_top(), true, name, NULL, NULL, NULL);
    if (item != NULL && !item->inherit) {
        if (item->pkgname == NULL) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("parameter name \"%s\" used more than once", name)));
        }
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
        newm->isAutonomous = func->action->isAutonomous;

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
 * Build a row-variable data structure given the component variables.
 */
static PLpgSQL_row* build_row_from_vars(PLpgSQL_variable** vars, int numvars)
{
    PLpgSQL_row* row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->rowtupdesc = CreateTemplateTupleDesc(numvars, false);
    row->nfields = numvars;
    row->fieldnames = (char**)palloc(numvars * sizeof(char*));
    row->varnos = (int*)palloc(numvars * sizeof(int));
    row->default_val = NULL;
    row->atomically_null_object = false;
    
    for (int i = 0; i < numvars; i++) {
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
 * This is the same as the standard resolve_polymorphic_argtypes() function,
 * but with a special case for validation: assume that polymorphic arguments
 * are integer, integer-array or integer-range.  Also, we go ahead and report
 * the error if we can't resolve the types.
 */
static void plpgsql_resolve_polymorphic_argtypes(
    int numargs, Oid* arg_types, const char* arg_modes, Node* call_expr, bool for_validator, const char* pro_name)
{
    if (!for_validator) {
        /* normal case, pass to standard routine */
        if (!resolve_polymorphic_argtypes(numargs, arg_types, arg_modes, call_expr))
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("could not determine actual argument type for polymorphic function \"%s\"", pro_name)));
    } else {
        /* special validation case */
        for (int i = 0; i < numargs; i++) {
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

static void plpgsql_HashTableInsert(PLpgSQL_function* func, PLpgSQL_func_hashkey* func_key)
{
    plpgsql_HashEnt* hentry = NULL;
    bool found = false;

    hentry = (plpgsql_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_HashTable, (void*)func_key, HASH_ENTER, &found);
    if (found) {
        /* move cell to the tail of the function list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell);
        elog(WARNING, "trying to insert a function that already exists");
    } else {
        hentry->function = NULL;
        hentry->cell = NULL;
        /* append the current compiling entity to the end of the compile results list. */
        plpgsql_append_dlcell(hentry);
    }

    hentry->function = func;
    /* prepare back link from function to hashtable key */
    func->fn_hashkey = &hentry->key;
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
    u_sess->plsql_cxt.plpgsql_dlist_objects = dlappend(u_sess->plsql_cxt.plpgsql_dlist_objects , entity);
    (void)MemoryContextSwitchTo(oldctx);

    entity->cell = u_sess->plsql_cxt.plpgsql_dlist_objects->tail;

    while (dlength(u_sess->plsql_cxt.plpgsql_dlist_objects) > g_instance.attr.attr_sql.max_compile_functions) {
        DListCell* headcell = u_sess->plsql_cxt.plpgsql_dlist_objects->head;
        plpgsql_HashEnt* head_entity = (plpgsql_HashEnt*)lfirst(headcell);
        func = head_entity->function;
        /* package's PLpgSQL_function can only be freeed by package */
        while (func != NULL && OidIsValid(func->pkg_oid)) {
            headcell = (headcell == NULL) ? NULL : headcell->next;
            head_entity = (headcell == NULL) ? NULL : (plpgsql_HashEnt*)lfirst(headcell);
            func = (head_entity  == NULL) ? NULL : head_entity->function;
        }
        if (func == NULL || func->use_count != 0) {
            break;
        }

        /* delete from the hash and delete the function's compile */
        delete_function(func);

        pfree_ext(func);
    }
}
