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

#include "utils/pl_package.h"
#include "utils/plpgsql.h"

#include <ctype.h>

#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/gs_package.h"
#include "catalog/gs_package_fn.h"
#include "catalog/pg_type.h"
#include "commands/sqladvisor.h"
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
#include "tcop/tcopprot.h"

/* functions reference other modules */
extern THR_LOCAL List* baseSearchPath;

#define FUNCS_PER_USER 128 /* initial table size */
#define PKGS_PER_USER 256  /* initial table size */
#define MAXSTRLEN ((1 << 11) - 1)
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
extern void plpgsql_compile_error_callback(void* arg);
static void add_parameter_name(int item_type, int item_no, const char* name);
static void add_dummy_return(PLpgSQL_function* func);
static Node* plpgsql_pre_column_ref(ParseState* pstate, ColumnRef* cref);
static Node* plpgsql_post_column_ref(ParseState* pstate, ColumnRef* cref, Node* var);
static Node* plpgsql_param_ref(ParseState* pstate, ParamRef* pref);
static Node* resolve_column_ref(ParseState* pstate, PLpgSQL_expr* expr, ColumnRef* cref, bool error_if_no_field);
static Node* make_datum_param(PLpgSQL_expr* expr, int dno, int location);
extern PLpgSQL_row* build_row_from_class(Oid class_oid);
static PLpgSQL_row* build_row_from_vars(PLpgSQL_variable** vars, int numvars);
static void compute_function_hashkey(HeapTuple proc_tup, FunctionCallInfo fcinfo, Form_pg_proc proc_struct,
    PLpgSQL_func_hashkey* hashkey, bool for_validator);
static void plpgsql_resolve_polymorphic_argtypes(
    int numargs, Oid* argtypes, const char* argmodes, Node* call_expr, bool for_validator, const char* proname);
static PLpgSQL_function* plpgsql_HashTableLookup(PLpgSQL_func_hashkey* func_key);
static void plpgsql_HashTableInsert(PLpgSQL_function* func, PLpgSQL_func_hashkey* func_key);
static void plpgsql_append_dlcell(plpgsql_HashEnt* entity);
static bool plpgsql_lookup_tripword_datum(int itemno, const char* word2, const char* word3,
    PLwdatum* wdatum, int* tok_flag, bool isPkgVar);
static void get_datum_tok_type(PLpgSQL_datum* target, int* tok_flag);
static PLpgSQL_type* plpgsql_get_cursor_type_relid(const char* cursorname, const char* colname, MemoryContext oldCxt);
static int find_package_rowfield(PLpgSQL_datum* datum, const char* pkgName, const char* schemaName = NULL);

extern int plpgsql_getCustomErrorCode(void);
extern bool is_func_need_cache(Oid funcid, const char* func_name);

extern bool plpgsql_check_insert_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func);

typedef int (*plsql_parser)(void);
static inline plsql_parser PlsqlParser()
{
    int (*plsql_parser_hook)(void) = plpgsql_yyparse;
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_sql.enable_custom_parser) {
        int id = GetCustomParserId();
        if (id >= 0 && g_instance.plsql_parser_hook[id] != NULL) {
            plsql_parser_hook = (int(*)(void))g_instance.plsql_parser_hook[id];
        }
    }
#endif
    return plsql_parser_hook;
}

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

/*
 * Check whether function's search path is same as system search_path.
 */
static bool plpgsql_check_search_path(PLpgSQL_function* func, HeapTuple proc_tup)
{
    /* If SUPPORT_BIND_SEARCHPATH is false, always return true. */
    return check_search_path_interface(func->fn_searchpath->schemas, proc_tup);
}

PLpgSQL_function* plpgsql_compile(FunctionCallInfo fcinfo, bool for_validator)
{
    Oid func_oid = fcinfo->flinfo->fn_oid;
    PLpgSQL_func_hashkey hashkey;
    bool function_valid = false;
    bool hashkey_valid = false;
    bool isnull = false;
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
    Datum pkgoiddatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    Oid packageOid = DatumGetObjectId(pkgoiddatum);
    Oid old_value = saveCallFromPkgOid(packageOid);
    
recheck:
    if (func == NULL) {
        /* Compute hashkey using function signature and actual arg types */
        compute_function_hashkey(proc_tup, fcinfo, proc_struct, &hashkey, for_validator);
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
        PG_TRY();
        {
            func = do_compile(fcinfo, proc_tup, func, &hashkey, for_validator);
            (void)CompileStatusSwtichTo(save_compile_status);
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
 * @Description : A function similar to plpgsql_compile, the difference is we use it for body
 *				 precompilation here, so no hashkey is needed here.
 * @in fcinfo : info of trigger ffunction which need be compiled.
 * @return : function information after compiled.
 */
PLpgSQL_function* plpgsql_compile_nohashkey(FunctionCallInfo fcinfo)
{
    Oid func_oid = fcinfo->flinfo->fn_oid;

    /* Lookup the pg_proc tuple by Oid, as we'll need it in any case */
    HeapTuple proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("cache lookup failed for function %u", func_oid)));
    }

    /*
     * See if there's already a cache entry for the current FmgrInfo. If not,
     * try to find one in the hash table.
     */
    PLpgSQL_function* func = (PLpgSQL_function*)fcinfo->flinfo->fn_extra;

    /*
     * We should set for_validator of do_compile to false here which only need be
     * set true by plpgsql_validator in CREATE FUNCTION or DO command.
     */
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    int save_compile_status = getCompileStatus();
    PG_TRY();
    {
        if (func == NULL) {
            func = do_compile(fcinfo, proc_tup, func, NULL, false);
        }
        (void)CompileStatusSwtichTo(save_compile_status);
    }
    PG_CATCH();
    {
        popToOldCompileContext(save_compile_context);
        (void)CompileStatusSwtichTo(save_compile_status);
        PG_RE_THROW();
    }
    PG_END_TRY();
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

void checkCompileMemoryContext(const MemoryContext cxt)
{
    if (cxt == NULL) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("current compile memory context is NULL"),
                        errdetail("N/A"), errcause("nested compilation switch context error"),
                        erraction("check logic of compilation")));
    }
}

/*
 * get compile context if NULL, create new context.
 */
MemoryContext getCompileContext(char* const context_name)
{
    switch (u_sess->plsql_cxt.compile_status) {
        case COMPILIE_ANON_BLOCK: {
            return AllocSetContextCreate(CurrentMemoryContext, context_name, 
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        } break;
        case COMPILIE_PKG: {
            return AllocSetContextCreate(u_sess->top_mem_cxt, context_name, 
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        } break;
        case COMPILIE_PKG_ANON_BLOCK: 
        case COMPILIE_PKG_FUNC: {
            if (u_sess->plsql_cxt.curr_compile_context == NULL ||
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL) {
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("current compile context is NULL"),
                        errdetail("N/A"), errcause("nested compilation swtich context error"),
                        erraction("check logic of compilation")));
            }
            checkCompileMemoryContext(u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_cxt);
            return u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_cxt;
        } break;
        case NONE_STATUS: {
            if (u_sess->plsql_cxt.curr_compile_context != NULL) {
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("current compile context should be NULL"),
                        errdetail("N/A"), errcause("nested compilation swtich context error"),
                        erraction("check logic of compilation")));
            }
            return AllocSetContextCreate(u_sess->top_mem_cxt, context_name, 
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        } break;
        case COMPILIE_PKG_ANON_BLOCK_FUNC: {
            return AllocSetContextCreate(u_sess->top_mem_cxt, context_name, 
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        } break;
        default: {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized compile type: %d", u_sess->plsql_cxt.compile_status)));
            break;
        }
    }
    return NULL;
}

static void initCompileContext(PLpgSQL_compile_context* compile_cxt, MemoryContext cxt)
{
    compile_cxt->datums_alloc = 0;
    compile_cxt->datums_pkg_alloc = 0;
    compile_cxt->plpgsql_nDatums = 0;
    compile_cxt->plpgsql_pkg_nDatums = 0;
    compile_cxt->datums_last = 0;
    compile_cxt->datums_pkg_last = 0;
    compile_cxt->plpgsql_error_funcname = NULL;
    compile_cxt->plpgsql_error_pkgname = NULL;
    compile_cxt->plpgsql_parse_result = NULL;
    compile_cxt->plpgsql_Datums = NULL;
    compile_cxt->datum_need_free = NULL;
    compile_cxt->plpgsql_curr_compile = NULL;
    compile_cxt->plpgsql_DumpExecTree = false;
    compile_cxt->plpgsql_pkg_DumpExecTree = false;
    compile_cxt->ns_top = NULL;
    compile_cxt->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
    compile_cxt->yyscanner = NULL;
    /* ??? should palloc every time? */
    compile_cxt->core_yy = (core_yy_extra_type*)palloc0(sizeof(core_yy_extra_type));
    compile_cxt->scanorig = NULL;
    compile_cxt->plpgsql_yyleng = 0;
    compile_cxt->num_pushbacks = 0;
    compile_cxt->cur_line_start = 0;
    compile_cxt->cur_line_end = 0;
    compile_cxt->cur_line_num = 0;
    compile_cxt->goto_labels = NIL;
    compile_cxt->plpgsql_check_syntax = false;
    compile_cxt->compile_tmp_cxt = NULL;
    compile_cxt->compile_cxt = cxt;

    switch (u_sess->plsql_cxt.compile_status) {
        case COMPILIE_PKG_ANON_BLOCK: 
        case COMPILIE_PKG_FUNC:
            compile_cxt->plpgsql_curr_compile_package = 
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            break;
        default:
            compile_cxt->plpgsql_curr_compile_package = NULL;
            break;
    }
}

void clearCompileContext(PLpgSQL_compile_context* compile_cxt)
{
    if (compile_cxt == NULL) {
        return;
    }
    pfree_ext(compile_cxt->core_yy);
    pfree_ext(compile_cxt->plpgsql_error_funcname);
    pfree_ext(compile_cxt->plpgsql_error_pkgname);
    pfree_ext(compile_cxt);
}

void clearCompileContextList(const int releaseLength)
{
    if (u_sess->plsql_cxt.compile_context_list == NULL) {
        return ;
    }
    ListCell* lc = list_head(u_sess->plsql_cxt.compile_context_list);
    PLpgSQL_compile_context* compile_cxt = NULL;
    int len = list_length(u_sess->plsql_cxt.compile_context_list);
    int i = len;
    while (lc != NULL) {
        i--;
        ListCell* newlc = lnext(lc);
        if (i < len - releaseLength) {
            lc = newlc;
            continue;
        }
        compile_cxt = (PLpgSQL_compile_context*)lfirst(lc);
        u_sess->plsql_cxt.compile_context_list = list_delete(u_sess->plsql_cxt.compile_context_list, compile_cxt);
        clearCompileContext(compile_cxt);
        lc = newlc;
    }
}

PLpgSQL_compile_context* createCompileContext(char* const context_name)
{
    MemoryContext cxt = getCompileContext(context_name);
    
    PLpgSQL_compile_context* compile_cxt = (PLpgSQL_compile_context*)MemoryContextAllocZero(
        cxt, sizeof(PLpgSQL_compile_context));
    initCompileContext(compile_cxt, cxt);

    return compile_cxt;
}

void pushCompileContext()
{
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        u_sess->plsql_cxt.compile_context_list = lcons(u_sess->plsql_cxt.curr_compile_context, 
            u_sess->plsql_cxt.compile_context_list);
        MemoryContextSwitchTo(oldcontext);
    }
}

PLpgSQL_compile_context* popCompileContext()
{
    if (u_sess->plsql_cxt.compile_context_list != NIL) {
        u_sess->plsql_cxt.compile_context_list = list_delete_first(u_sess->plsql_cxt.compile_context_list);
        if (list_length(u_sess->plsql_cxt.compile_context_list) == 0) {
            u_sess->plsql_cxt.compile_context_list = NIL;
            return NULL;
        }
        return (PLpgSQL_compile_context*)linitial(u_sess->plsql_cxt.compile_context_list);
    }
    return NULL;
}

void popToOldCompileContext(PLpgSQL_compile_context* save)
{
    while (u_sess->plsql_cxt.compile_context_list != NIL) {
        PLpgSQL_compile_context* curr = (PLpgSQL_compile_context*)linitial(u_sess->plsql_cxt.compile_context_list);
        if (save == curr) {
            break;
        }
        popCompileContext();
    }
    u_sess->plsql_cxt.curr_compile_context = save;
}

int CompileStatusSwtichTo(int newCompileStatus)
{
    int oldCompileStatus = u_sess->plsql_cxt.compile_status;
    u_sess->plsql_cxt.compile_status = newCompileStatus;
    return oldCompileStatus;
}

int getCompileStatus()
{
    return u_sess->plsql_cxt.compile_status;
}

PLpgSQL_resolve_option GetResolveOption()
{
    return (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) ?
        PLPGSQL_RESOLVE_COLUMN : PLPGSQL_RESOLVE_ERROR;
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
 * allocations is the u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt.
 *
 * NB: this code is not re-entrant.  We assume that nothing we do here could
 * result in the invocation of another plpgsql function.
 */
static PLpgSQL_function* do_compile(FunctionCallInfo fcinfo, HeapTuple proc_tup, PLpgSQL_function* func,
    PLpgSQL_func_hashkey* hashkey, bool for_validator)
{
    Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
    bool is_trigger = CALLED_AS_TRIGGER(fcinfo);
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
    Datum pkgoiddatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    if (!isnull) {
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
    plpgsql_scanner_init(proc_source);
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
    func->fn_xmin = HeapTupleGetRawXmin(proc_tup);
    func->fn_tid = proc_tup->t_self;
    func->fn_is_trigger = is_trigger;
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
    Oid base_oid = InvalidOid;
    bool isHaveTableOfIndexArgs = false;
    bool isHaveOutRefCursorArgs = false;
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
            if (num_out_args == 1) {
                func->out_param_varno = out_arg_variables[0]->dno;
            } else if (num_out_args > 1) {
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

            if (isTableofType(rettypeid, &base_oid, NULL)) {
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
                        "$0", 0, build_datatype(type_tup, -1, func->fn_input_collation), true, true);
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
    bool saved_flag = u_sess->plsql_cxt.have_error;
    u_sess->plsql_cxt.have_error = false;
    parse_rc = (*PlsqlParser())();
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

    if (is_trigger && func->action->isAutonomous) {
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
    func->ndatums = curr_compile->plpgsql_nDatums;
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
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
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
    plpgsql_scanner_init(proc_source);

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

    func->fn_signature = pstrdup(func_name);
    func->fn_is_trigger = false;
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
    parse_rc = (*PlsqlParser())();
    if (parse_rc != 0) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Syntax parsing error, plpgsql parser returned %d", parse_rc)));
    }
    func->action = curr_compile->plpgsql_parse_result;
    func->goto_labels = curr_compile->goto_labels;

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
    if (item != NULL) {
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
    Node* myvar = resolve_column_ref(pstate, expr, cref, (var == NULL));

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
    errno_t rc = EOK;

    rc = snprintf_s(pname, sizeof(pname), sizeof(pname) - 1, "$%d", pref->number);
    securec_check_ss(rc, "\0", "\0");

    PLpgSQL_nsitem* nse = plpgsql_ns_lookup(expr->ns, false, pname, NULL, NULL, NULL);

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
    const char* name4 = NULL;
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
     * A.B.C.D  Qualified schema.pkg.record.field reference.
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
            nnames_scalar = 3;
            nnames_wholerow = 3;
            nnames_field = 2;
            break;
        }
        case 4: {
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);
            Node* field4 = (Node*)lfourth(cref->fields);

            AssertEreport(IsA(field1, String), MOD_PLSQL, "string type is required.");
            name1 = strVal(field1);
            AssertEreport(IsA(field2, String), MOD_PLSQL, "string type is required.");
            name2 = strVal(field2);
            AssertEreport(IsA(field3, String), MOD_PLSQL, "string type is required.");
            name3 = strVal(field3);
            AssertEreport(IsA(field4, String), MOD_PLSQL, "string type is required.");
            name4 = strVal(field4);
            colname = name4;
            nnames_scalar = 4;
            nnames_wholerow = 4;
            nnames_field = 3;
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
    if (nse->itemtype == PLPGSQL_NSTYPE_UNKNOWN) {
        if (nse->pkgname == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_PACKAGE),
                    (errmsg("not found package"))));
        }
        Oid pkgOid = PackageNameGetOid(nse->pkgname);
        PLpgSQL_pkg_hashkey hashkey;
        hashkey.pkgOid = pkgOid;
        PLpgSQL_package* pkg = plpgsql_pkg_HashTableLookup(&hashkey);
        if (pkg == NULL) {
            pkg = PackageInstantiation(pkgOid);
        }
        if (pkg != NULL) {
            PLpgSQL_nsitem* pkgPublicNs = plpgsql_ns_lookup(pkg->public_ns, false, nse->name, NULL, NULL, NULL);
            nse->itemtype = pkgPublicNs->itemtype;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_PACKAGE),
                    (errmsg("not found package"))));
        }
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
                        switch (nnames_wholerow) {
                            case 2: /* row.col */
                                return make_datum_param(expr, row->varnos[i], cref->location);
                            case 3: { /* pkg.row.col */
                                PLpgSQL_nsitem* tempns = NULL;
                                PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                                char* refname = ((PLpgSQL_variable*)tempdatum)->refname;
                                tempns = plpgsql_ns_lookup(expr->ns, false, name1, refname, NULL, NULL);
                                if (tempns != NULL) {
                                    return make_datum_param(expr, tempns->itemno, cref->location);
                                }
                                break;
                            }
                            case 4: { /* schema.pkg.row.col */
                                PLpgSQL_nsitem* tempns = NULL;
                                PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                                char* refname = ((PLpgSQL_variable*)tempdatum)->refname;
                                tempns = plpgsql_ns_lookup(expr->ns, false, name1, name2, refname, NULL);
                                if (tempns != NULL) {
                                    return make_datum_param(expr, tempns->itemno, cref->location);
                                }
                                break;
                            }
                            default :
                                break;
                        }
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
    Oid tableOfIndexType = InvalidOid;
    exec_get_datum_type_info(estate, datum, &param->paramtype, &param->paramtypmod, &param->paramcollid,
        &tableOfIndexType, expr->func);
    param->location = location;
    param->tableOfIndexType = tableOfIndexType;
    if (datum->dtype == PLPGSQL_DTYPE_RECORD) {
        param->recordVarTypOid = ((PLpgSQL_row*)datum)->recordVarTypOid;
    } else {
        param->recordVarTypOid = InvalidOid;
    }

    return (Node*)param;
}

HeapTuple getPLpgsqlVarTypeTup(char* word)
{
    if (u_sess->plsql_cxt.curr_compile_context == NULL) {
        return NULL;
    }
    PLpgSQL_nsitem* ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word, NULL, NULL, NULL);
    if (ns == NULL) {
        /* find from pkg */
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
            PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            ns = plpgsql_ns_lookup(pkg->public_ns, false, word, NULL, NULL, NULL);
            if (ns == NULL) {
                ns = plpgsql_ns_lookup(pkg->private_ns, false, word, NULL, NULL, NULL);
            }
            if (ns != NULL && ns->itemtype == PLPGSQL_NSTYPE_VAR) {
                PLpgSQL_var* var = (PLpgSQL_var*)pkg->datums[ns->itemno];
                if (OidIsValid(var->datatype->typoid)) {
                    HeapTuple typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
                    return typeTup;
                }
            }
        }
    } else {
        if (ns != NULL && ns->itemtype == PLPGSQL_NSTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
            if (OidIsValid(var->datatype->typoid)) {
                HeapTuple typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
                return typeTup;
            }
        }
    }
    
    return NULL;
}

HeapTuple FindRowVarColType(List* nameList)
{
    if (u_sess->plsql_cxt.curr_compile_context == NULL) {
        return NULL;
    }

    PLpgSQL_datum* datum = NULL;
    char* field = NULL;

    /* find row var and field first */
    switch (list_length(nameList)) {
        case 2: {
            datum = plpgsql_lookup_datum(false, strVal(linitial(nameList)), NULL, NULL, NULL);
            field = strVal(lsecond(nameList));
            break;
        }
        case 3: {
            char* word1 = strVal(linitial(nameList));
            char* word2 = strVal(lsecond(nameList));
            List *names2 = list_make2(makeString(word1), makeString(word2));
            datum = GetPackageDatum(names2);
            list_free_ext(names2);
            field = strVal(lthird(nameList));
            break;
        }
        case 4: {
            char* word1 = strVal(linitial(nameList));
            char* word2 = strVal(lsecond(nameList));
            char* word3 = strVal(lthird(nameList));
            List *names3 = list_make3(makeString(word1), makeString(word2), makeString(word3));
            datum = GetPackageDatum(names3);
            list_free_ext(names3);
            field = strVal(lfourth(nameList));
            break;
        }
        default:
            return NULL;
    }

    if (datum == NULL) {
        return NULL;
    }

    if (datum->dtype != PLPGSQL_DTYPE_RECORD && datum->dtype != PLPGSQL_DTYPE_ROW) {
        return NULL;
    }

    PLpgSQL_row* row = (PLpgSQL_row*)datum;
    int i;
    Oid typOid = InvalidOid;
    for (i = 0; i < row->nfields; i++) {
        if (row->fieldnames[i] && (strcmp(row->fieldnames[i], field) == 0)) {
            if (row->ispkg) {
                datum = row->pkg->datums[row->varnos[i]];
            } else {
                datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[row->varnos[i]];
            }
            if (datum->dtype == PLPGSQL_DTYPE_VAR) {
                /* scalar variable, just return the datatype */
                typOid = ((PLpgSQL_var*)datum)->datatype->typoid;
            } else if (datum->dtype == PLPGSQL_DTYPE_ROW) {
                /* row variable, need to build a new one */
                typOid = ((PLpgSQL_row*)datum)->rowtupdesc->tdtypeid;
            }
            break;
        }
    }

    if (!OidIsValid(typOid)) {
        return NULL;
    }

    HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typOid));
    /* should not happen */
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for type %u", typOid)));
    }

    return tup;
}

HeapTuple getCursorTypeTup(const char* word)
{
    if (u_sess->plsql_cxt.curr_compile_context == NULL) {
        return NULL;
    }
    Oid typeOid = InvalidOid;
    PLpgSQL_nsitem* ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word, NULL, NULL, NULL);
    if (ns == NULL) {
        /* find from pkg*/
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
            PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            ns = plpgsql_ns_lookup(pkg->public_ns, false, word, NULL, NULL, NULL);
            if (ns == NULL) {
                ns = plpgsql_ns_lookup(pkg->private_ns, false, word, NULL, NULL, NULL);
            }
            if (ns != NULL && ns->itemtype == PLPGSQL_NSTYPE_VAR) {
                PLpgSQL_var* var = (PLpgSQL_var*)pkg->datums[ns->itemno];
                if (var->datatype->typoid == REFCURSOROID && OidIsValid(var->datatype->cursorCompositeOid)) {
                    typeOid = var->datatype->cursorCompositeOid;
                }
            }
        }
    } else {
        if (ns->itemtype == PLPGSQL_NSTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
            if (var->datatype->typoid == REFCURSOROID && OidIsValid(var->datatype->cursorCompositeOid)) {
                typeOid = var->datatype->cursorCompositeOid;
            }
        }
    }

    if (OidIsValid(typeOid)) {
        HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));
        }
        return tuple;
    } else {
        return NULL;
    }
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
    PLpgSQL_var* var = NULL;

    PLpgSQL_nsitem* ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, NULL, NULL, NULL);
    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (ns == NULL) {
        if (curr_compile->plpgsql_curr_compile_package != NULL) {
            PLpgSQL_package* pkg = curr_compile->plpgsql_curr_compile_package;
            ns = plpgsql_ns_lookup(pkg->public_ns, false, word1, NULL, NULL, NULL);
            if (ns == NULL) {
                ns = plpgsql_ns_lookup(pkg->private_ns, false, word1, NULL, NULL, NULL);
            }
        }
    }
    if (ns != NULL) {
        switch (ns->itemtype) {
            /* check cursor type variable, then get cursor type token */
            case PLPGSQL_NSTYPE_REFCURSOR:
                *tok_flag = PLPGSQL_TOK_REFCURSOR;
                wdatum->datum = NULL;
                wdatum->ident = word1;
                wdatum->quoted = false;
                wdatum->idents = NIL;
                wdatum->dno = ns->itemno;
                return true;
            /* pass a tok_flag here to return T_VARRAY token outside plpgsql_parse_word() in yylex */
            case PLPGSQL_NSTYPE_VARRAY:
                *tok_flag = PLPGSQL_TOK_VARRAY;
                wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                wdatum->ident = word1;
                wdatum->quoted = false;
                wdatum->idents = NIL;
                wdatum->dno = ns->itemno;
                return true;
            case PLPGSQL_NSTYPE_TABLE:
                *tok_flag = PLPGSQL_TOK_TABLE;
                wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                wdatum->ident = word1;
                wdatum->quoted = false;
                wdatum->idents = NIL;
                wdatum->dno = ns->itemno;
                return true;
            case PLPGSQL_NSTYPE_RECORD:
                *tok_flag = PLPGSQL_TOK_RECORD;
                wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                wdatum->ident = word1;
                wdatum->quoted = false;
                wdatum->idents = NIL;
                wdatum->dno = ns->itemno;
                return true;
            case PLPGSQL_NSTYPE_VAR:
                var = (PLpgSQL_var*)curr_compile->plpgsql_Datums[ns->itemno];
                if (var != NULL && var->datatype != NULL && var->datatype->typinput.fn_oid == F_ARRAY_IN) {
                    if (var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE) {
                        *tok_flag = PLPGSQL_TOK_TABLE_VAR;
                        wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                        wdatum->ident = word1;
                        wdatum->quoted = false;
                        wdatum->idents = NIL;
                        wdatum->dno = ns->itemno;
                        return true;
                    } else {
                        /* table of for the moment */
                        *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
                        wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                        wdatum->ident = word1;
                        wdatum->quoted = false;
                        wdatum->idents = NIL;
                        wdatum->dno = ns->itemno;
                        return true;
                    } 
                } else if (var != NULL && var->datatype && var->datatype->typinput.fn_oid == F_DOMAIN_IN) {
                    HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
                    if (HeapTupleIsValid(type_tuple)) {
                        Form_pg_type type_form = (Form_pg_type)GETSTRUCT(type_tuple);
                        if (F_ARRAY_OUT == type_form->typoutput) {
                            *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
                            wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                            wdatum->ident = word1;
                            wdatum->quoted = false;
                            wdatum->idents = NIL;
                            wdatum->dno = ns->itemno;
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
    if (curr_compile->plpgsql_IdentifierLookup == IDENTIFIER_LOOKUP_NORMAL) {
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
                case PLPGSQL_NSTYPE_UNKNOWN:
                    wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                    wdatum->ident = word1;
                    wdatum->quoted = (yytxt[0] == '"');
                    wdatum->idents = NIL;
                    wdatum->dno = ns->itemno;
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
static bool isVarrayVar(PLpgSQL_var *var)
{
    if (var == NULL || var->datatype == NULL) {
        return false;
    }
    if (var->datatype->typinput.fn_oid != F_ARRAY_IN && var->datatype->typinput.fn_oid != F_DOMAIN_IN) {
        return false;
    }
    if (var->datatype->typinput.fn_oid == F_DOMAIN_IN) {
        HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
        if (!HeapTupleIsValid(type_tuple)) {
            return false;
        }
        Form_pg_type type_form = (Form_pg_type)GETSTRUCT(type_tuple);
        if (type_form->typoutput != F_ARRAY_OUT) {
            return false;
        }
    }
    return true;
}

static bool isVarrayWord(const char *compWord, const char *firstWord, const char *secondWord, const char *thirdWord,
    PLwdatum* wdatum, int* nsflag)
{
    typedef struct {
        const char *varrayWord;
        unsigned int wordValue;
    } VarrayWord;
    VarrayWord hash[] = {
        {"first", PLPGSQL_TOK_VARRAY_FIRST},
        {"last", PLPGSQL_TOK_VARRAY_LAST},
        {"count", PLPGSQL_TOK_VARRAY_COUNT},
        {"exists", PLPGSQL_TOK_VARRAY_EXISTS},
        {"prior", PLPGSQL_TOK_VARRAY_PRIOR},
        {"next", PLPGSQL_TOK_VARRAY_NEXT},
        {"trim", PLPGSQL_TOK_VARRAY_TRIM},
        {"delete", PLPGSQL_TOK_VARRAY_DELETE},
        {"extend", PLPGSQL_TOK_VARRAY_EXTEND},
        {NULL, 0}
    };
    PLpgSQL_nsitem *ns = NULL;
    PLpgSQL_datum *datum = NULL;
    PLpgSQL_var *var = NULL;
    int dno;

    for (unsigned int i = 0; hash[i].varrayWord != NULL; i++) {
        if (pg_strcasecmp(hash[i].varrayWord, compWord) == 0) {
            int nnames = 0;
            ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, firstWord, secondWord, thirdWord, &nnames);
            if (ns == NULL) {
                return false;
            }

            bool isRow = false; /* row.col */
            bool isTriRow = false; /* row.col1.col2 */
            const char* field = NULL;
            if (ns->itemtype == PLPGSQL_NSTYPE_ROW) {
                if (nnames == 1) {
                    if (secondWord != NULL && thirdWord != NULL) {
                        isTriRow = true;
                    } else if (secondWord != NULL) {
                        isRow = true;
                        field = secondWord;
                    }
                } else if (nnames == 2) {
                    if (thirdWord != NULL) {
                        isRow = true;
                        field = thirdWord;
                    }
                }
            }

            if (isRow) {
                PLpgSQL_row* row = NULL;
                int j = 0;
                bool matched = false;

                row = (PLpgSQL_row*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno]);
                for (j = 0; j < row->nfields; j++) {
                    if (row->fieldnames[j] && strcmp(row->fieldnames[j], field) == 0) {
                        if (row->ispkg) {
                            PLpgSQL_nsitem* tempns = NULL;
                            PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[j]];
                            char* refname = ((PLpgSQL_variable*)tempdatum)->refname;
                            tempns = plpgsql_ns_lookup(plpgsql_ns_top(), false, firstWord, refname, NULL, NULL);
                            if (tempns != NULL) {
                                dno = tempns->itemno;
                            } else {
                                dno = plpgsql_adddatum(tempdatum, false);
                                if (tempdatum->dtype == PLPGSQL_DTYPE_VAR) {
                                    plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, dno, refname, firstWord);
                                } else {
                                    plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, dno, refname, firstWord);
                                }
                            }
                            datum = tempdatum;
                        } else {
                            datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[row->varnos[j]];
                            dno = row->varnos[j];
                        }
                        matched = true;
                        break;
                    }
                }

                if (!matched) {
                    return false;
                }
            } else if (isTriRow) {
                int tok_flag = -1;
                PLwdatum wdatum;
                wdatum.datum = NULL;
                plpgsql_lookup_tripword_datum(ns->itemno, secondWord, thirdWord, &wdatum, &tok_flag, false);
                if (wdatum.datum == NULL) {
                    return false;
                }
                datum = wdatum.datum;
                dno = wdatum.dno;
            } else {
                datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
                dno = ns->itemno;
            }

            if (datum == NULL || datum->dtype != PLPGSQL_DTYPE_VAR) {
                return false;
            }
            var = (PLpgSQL_var*)datum;
            if (!isVarrayVar(var)) {
                return false;
            }
            *nsflag = hash[i].wordValue;
            wdatum->datum = datum;
            wdatum->dno = dno;
            return true;
        }
    }
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
    bool isPkgVar = false;

    if (word2 == NULL) {
        return false;
    }
    idents = list_make2(makeString(word1), makeString(word2));
    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;

    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (curr_compile->plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE) {
        /*
         * Do a lookup in the current namespace stack
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, word2, NULL, &nnames);
        if (ns != NULL) {
            switch (ns->itemtype) {
                case PLPGSQL_NSTYPE_VAR:
                    /* Block-qualified reference to scalar variable. */
                    wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                    wdatum->ident = pstrdup(NameListToString(idents));
                    wdatum->quoted = false; /* not used */
                    wdatum->idents = idents;
                    wdatum->dno = ns->itemno;
                    isPkgVar = (nnames == 2) && (ns->pkgname != NULL) && (strcmp(ns->pkgname, word1) == 0);
                    if (isPkgVar) {
                        /* package variable, need to get var type */
                        get_datum_tok_type(wdatum->datum, nsflag);
                    }
                    return true;
                case PLPGSQL_NSTYPE_UNKNOWN:
                    /* it's a package variable. */
                    wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                    wdatum->ident = pstrdup(NameListToString(idents));
                    wdatum->quoted = false; /* not used */
                    wdatum->idents = idents;
                    wdatum->dno = ns->itemno;
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
                        if (u_sess->plsql_cxt.curr_compile_context->compile_cxt != NULL)
                            old_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_cxt);
                        newm = (PLpgSQL_recfield*)palloc(sizeof(PLpgSQL_recfield));
                        newm->dtype = PLPGSQL_DTYPE_RECFIELD;
                        newm->fieldname = pstrdup(word2);
                        newm->recparentno = ns->itemno;

                        (void)plpgsql_adddatum((PLpgSQL_datum*)newm);

                        if (old_cxt != NULL) {
                            MemoryContextSwitchTo(old_cxt);
                        }
                        wdatum->datum = (PLpgSQL_datum*)newm;
                        wdatum->dno = newm->dno;
                    } else {
                        /* Block-qualified reference to record variable. */
                        wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                        wdatum->dno = ns->itemno;
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

                        row = (PLpgSQL_row*)(curr_compile->plpgsql_Datums[ns->itemno]);
                        for (i = 0; i < row->nfields; i++) {
                            if (row->fieldnames[i] && strcmp(row->fieldnames[i], word2) == 0) {
                                wdatum->datum = curr_compile->plpgsql_Datums[row->varnos[i]];
                                wdatum->ident = pstrdup(NameListToString(idents));
                                wdatum->quoted = false; /* not used */
                                wdatum->idents = idents;
                                wdatum->dno = row->varnos[i];
                                get_datum_tok_type(wdatum->datum, nsflag);
                                return true;
                            }
                        }
                        /* fall through to return CWORD */
                    } else {
                        /* Block-qualified reference to row variable. */
                        wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
                        wdatum->ident = NULL;
                        wdatum->quoted = false; /* not used */
                        wdatum->idents = idents;
                        wdatum->dno = ns->itemno;
                        return true;
                    }
                    break;

                default:
                    break;
            }
        } else {
            int dno = plpgsql_pkg_add_unknown_var_to_namespace(idents);
            if (dno != -1) {
                wdatum->datum = curr_compile->plpgsql_Datums[dno];
                wdatum->ident = pstrdup(NameListToString(idents));
                wdatum->quoted = false; /* not used */
                wdatum->idents = idents;
                wdatum->dno = dno;
                *nsflag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                get_datum_tok_type(wdatum->datum, nsflag);
                return true;
            }
        }
    }
    if (isVarrayWord(word2, word1, NULL, NULL, wdatum, nsflag)) {
        wdatum->ident = NULL;
        wdatum->quoted = false;
        wdatum->idents = idents;
        return true;
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
bool plpgsql_parse_tripword(char* word1, char* word2, char* word3, PLwdatum* wdatum, PLcword* cword, int* tok_flag)
{
    PLpgSQL_nsitem* ns = NULL;
    List* idents = NULL;
    int nnames;
    int dno = -1;
    int i = 0;

    idents = list_make3(makeString(word1), makeString(word2), makeString(word3));
    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (curr_compile->plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE) {
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

                    (void)plpgsql_adddatum((PLpgSQL_datum*)newm);

                    wdatum->datum = (PLpgSQL_datum*)newm;
                    wdatum->ident = NULL;
                    wdatum->quoted = false; /* not used */
                    wdatum->idents = idents;
                    wdatum->dno = newm->dno;
                    return true;
                }

                case PLPGSQL_NSTYPE_ROW: {
                    /*
                     * words 1/2 are a row name, so third word could be a
                     * field in this row.
                     */
                    PLpgSQL_row* row = NULL;

                    row = (PLpgSQL_row*)(curr_compile->plpgsql_Datums[ns->itemno]);
                    for (i = 0; i < row->nfields; i++) {
                        if (row->fieldnames[i] && strcmp(row->fieldnames[i], word3) == 0) {
                            if (row->ispkg) {
                                PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                                dno = find_package_rowfield(tempdatum, word1);
                                wdatum->datum = tempdatum;
                                wdatum->dno = dno;
                                wdatum->ident = pstrdup(NameListToString(idents));
                                *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                            } else {
                                wdatum->datum = curr_compile->plpgsql_Datums[row->varnos[i]];
                                wdatum->dno = row->varnos[i];
                                wdatum->ident = NULL;
                            }
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

        if (ns != NULL && nnames == 1 && ns->itemtype == PLPGSQL_NSTYPE_ROW) {
            if (plpgsql_lookup_tripword_datum(ns->itemno, word2, word3, wdatum, tok_flag, false)) {
                wdatum->idents = idents;
                wdatum->ident = pstrdup(NameListToString(idents));
                return true;
            }
        }

        if (ns != NULL && nnames == 3) {
            /* schema.pkg.val form */
            wdatum->datum = curr_compile->plpgsql_Datums[ns->itemno];
            wdatum->ident = pstrdup(NameListToString(idents));
            wdatum->quoted = false; /* not used */
            wdatum->idents = idents;
            wdatum->dno = ns->itemno;
            *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
            return true;
        }

        if (ns == NULL) {
            /* find if a package variable */
            dno = plpgsql_pkg_add_unknown_var_to_namespace(idents);
            if (dno != -1) {
                wdatum->datum = curr_compile->plpgsql_Datums[dno];
                wdatum->ident = pstrdup(NameListToString(idents));
                wdatum->quoted = false; /* not used */
                wdatum->idents = idents;
                wdatum->dno = dno;
                *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                return true;
            }

            /* word1/word2 may be a package variable */
            List *idents2 = list_make2(makeString(word1), makeString(word2));
            dno = plpgsql_pkg_add_unknown_var_to_namespace(idents2);
            if (dno != -1) {
                PLpgSQL_datum* tempdatum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
                if (tempdatum->dtype == PLPGSQL_DTYPE_ROW || tempdatum->dtype == PLPGSQL_DTYPE_RECORD) {
                    PLpgSQL_row* row = (PLpgSQL_row*)tempdatum;
                    for (i = 0; i < row->nfields; i++) {
                        if (row->fieldnames[i] && strcmp(row->fieldnames[i], word3) == 0) {
                            PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                            dno = find_package_rowfield(tempdatum, word1);
                            wdatum->datum = tempdatum;
                            wdatum->ident = pstrdup(NameListToString(idents));
                            wdatum->quoted = false; /* not used */
                            wdatum->idents = idents;
                            wdatum->dno = dno;
                            *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                            return true;
                        }
                    }
                }
            }
            list_free(idents2);
        }
    }
    if (isVarrayWord(word3, word1, word2, NULL, wdatum, tok_flag)) {
        wdatum->ident = NULL;
        wdatum->quoted = false;
        wdatum->idents = idents;
        return true;
    }

    /* Nothing found */
    cword->idents = idents;
    return false;
}

/* ----------
 * plpgsql_parse_quadword		Same lookup for four words
 *					separated by dots.
 * ----------
 */
bool plpgsql_parse_quadword(char* word1, char* word2, char* word3, char* word4,
    PLwdatum* wdatum, PLcword* cword, int* tok_flag)
{
    PLpgSQL_nsitem* ns = NULL;
    List* idents = NULL;
    int nnames;
    int dno = -1;
    int i = 0;

    idents = list_make4(makeString(word1), makeString(word2), makeString(word3), makeString(word4));
    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (curr_compile->plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE) {
        /*
         * Do a lookup in the current namespace stack. Must find a qualified
         * reference, else ignore.
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, word2, word3, &nnames);

        /* pkg.row.col1.col2 or label.row.col1.col2 */
        if (ns != NULL && nnames == 2) {
            switch (ns->itemtype) {
                case PLPGSQL_NSTYPE_ROW: {
                    /*
                     * words 1/2 are a row name, so word 3/4 word could be
                     * fields in this row.
                     */
                    if (plpgsql_lookup_tripword_datum(ns->itemno, word3, word4, wdatum, tok_flag, true)) {
                        wdatum->idents = idents;
                        wdatum->ident = pstrdup(NameListToString(idents));
                        return true;
                    }
                    /* fall through to return CWORD */
                    break;
                }
                default:
                    break;
            }
        }

        /* row.col1.col2.col3 */
        if (ns != NULL && nnames == 1 && ns->itemtype == PLPGSQL_NSTYPE_ROW) {
            PLpgSQL_row* row = NULL;
            row = (PLpgSQL_row*)(curr_compile->plpgsql_Datums[ns->itemno]);
            for (i = 0; i < row->nfields; i++) {
                if (row->fieldnames[i] && strcmp(row->fieldnames[i], word2) == 0) {
                    if (plpgsql_lookup_tripword_datum(row->varnos[i], word3, word4, wdatum, tok_flag, false)) {
                        wdatum->idents = idents;
                        wdatum->ident = pstrdup(NameListToString(idents));
                        return true;
                    }
                }
            }
        }

        /* schema.pkg.row.col */
        if (ns != NULL && nnames == 3) {
            /* schema.pkg.val form */
            if (ns->itemtype == PLPGSQL_NSTYPE_ROW) {
                PLpgSQL_row* row = NULL;
                row = (PLpgSQL_row*)(curr_compile->plpgsql_Datums[ns->itemno]);
                for (i = 0; i < row->nfields; i++) {
                    if (row->fieldnames[i] && strcmp(row->fieldnames[i], word4) == 0) {
                        PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                        dno = find_package_rowfield(tempdatum, word2, word1);
                        wdatum->datum = tempdatum;
                        wdatum->dno = dno;
                        wdatum->ident = pstrdup(NameListToString(idents));
                        *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                        wdatum->quoted = false; /* not used */
                        wdatum->idents = idents;
                        return true;
                    }
                }
            }
        }

        /* maybe a unkown package var, try to find it */
        if (ns == NULL) {
            /* find if a package variable */
            List *idents3 = list_make3(makeString(word1), makeString(word2), makeString(word3));
            dno = plpgsql_pkg_add_unknown_var_to_namespace(idents3);
            if (dno != -1) {
                PLpgSQL_datum* tempdatum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
                if (tempdatum->dtype == PLPGSQL_DTYPE_ROW || tempdatum->dtype == PLPGSQL_DTYPE_RECORD) {
                    PLpgSQL_row* row = NULL;
                    row = (PLpgSQL_row*)(curr_compile->plpgsql_Datums[dno]);
                    for (i = 0; i < row->nfields; i++) {
                        if (row->fieldnames[i] && strcmp(row->fieldnames[i], word4) == 0) {
                            PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                            dno = find_package_rowfield(tempdatum, word2, word1);
                            wdatum->datum = tempdatum;
                            wdatum->dno = dno;
                            wdatum->ident = pstrdup(NameListToString(idents));
                            *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                            wdatum->quoted = false; /* not used */
                            wdatum->idents = idents;
                            return true;
                        }
                    }
                }
            }
            list_free(idents3);

            /* word1/word2 may be a package variable */
            List *idents2 = list_make2(makeString(word1), makeString(word2));
            dno = plpgsql_pkg_add_unknown_var_to_namespace(idents2);
            if (dno != -1) {
                if (plpgsql_lookup_tripword_datum(dno, word3, word4, wdatum, tok_flag, true)) {
                    wdatum->idents = idents;
                    wdatum->ident = pstrdup(NameListToString(idents));
                    return true;
                }
            }
            list_free(idents2);
        }
    }
    if (isVarrayWord(word4, word1, word2, word3, wdatum, tok_flag)) {
        wdatum->ident = NULL;
        wdatum->quoted = false;
        wdatum->idents = idents;
        return true;
    }

    /* Nothing found */
    cword->idents = idents;
    return false;
}

static int find_package_rowfield(PLpgSQL_datum* datum, const char* pkgName, const char* schemaName)
{
    char* refname = ((PLpgSQL_variable*)datum)->refname;
    PLpgSQL_nsitem* tempns = NULL;
    int dno = -1;
    if (schemaName != NULL) {
        tempns = plpgsql_ns_lookup(plpgsql_ns_top(), false, schemaName, pkgName, refname, NULL);
    } else {
        tempns = plpgsql_ns_lookup(plpgsql_ns_top(), false, pkgName, refname, NULL, NULL);
    }
    if (tempns != NULL) {
        dno = tempns->itemno;
    } else {
        dno = plpgsql_adddatum(datum, false);
        if (datum->dtype == PLPGSQL_DTYPE_VAR) {
            plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, dno, refname, pkgName, schemaName);
        } else {
            plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, dno, refname, pkgName, schemaName);
        }
    }

    return dno;
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
                return ((PLpgSQL_var*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[nse->itemno]))->datatype;

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

        dtype = build_datatype(type_tup, -1, 
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_input_collation);

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
    Oid class_oid = InvalidOid;
    HeapTuple classtup = NULL;
    HeapTuple attrtup = NULL;
    HeapTuple type_tup = NULL;
    Form_pg_class class_struct = NULL;
    Form_pg_attribute attr_struct = NULL;
    MemoryContext old_cxt = NULL;
    bool flag = false;

    /* Avoid memory leaks in the long-term function context */
    old_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);

    if (list_length(idents) == 2) {
        /*
         * Do a lookup in the current namespace stack.
         */
        int nnames;
        nse = plpgsql_ns_lookup(plpgsql_ns_top(), false, strVal(linitial(idents)),
            strVal(lsecond(idents)), NULL, &nnames);

        if (nse != NULL && nse->itemtype == PLPGSQL_NSTYPE_VAR) {
            dtype = ((PLpgSQL_var*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[nse->itemno]))->datatype;
            goto done;
        }

        /*
         * word1 is a row, word2 may a field of word1.
         * find it, and return the field's type
         */
        bool isRow = (nse != NULL) && (nse->itemtype == PLPGSQL_NSTYPE_ROW) && (nnames == 1);
        if (isRow) {
            dtype = plpgsql_get_row_field_type(nse->itemno, strVal(lsecond(idents)), old_cxt);
            if (dtype != NULL) {
                goto done;
            }
        }
        /*
         * word1 is a cursor, word2 may a col of word1.
         * find it, and return the col type.
         */
        dtype = plpgsql_get_cursor_type_relid(strVal(linitial(idents)), strVal(lsecond(idents)), old_cxt);
        if (dtype != NULL) {
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
        char* castPkgName = NULL;
        Oid pkgOid = InvalidOid;

        /* first word is package name, cast type name. */
        pkgOid = PackageNameGetOid(strVal(linitial(idents)), InvalidOid);
        if (OidIsValid(pkgOid)) {
            castPkgName = CastPackageTypeName(strVal(lsecond(idents)), pkgOid, true);
            relvar = makeRangeVar(NULL, castPkgName, -1);
        } else {
            relvar = makeRangeVar(strVal(linitial(idents)), strVal(lsecond(idents)), -1);
        }

        /* Can't lock relation - we might not have privileges. */
        class_oid = RangeVarGetRelidExtended(relvar, NoLock, true, false, false, true, NULL, NULL);
        pfree_ext(relvar);
        pfree_ext(castPkgName);
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
    flag = class_struct->relkind != RELKIND_RELATION && !RELKIND_IS_SEQUENCE(class_struct->relkind) &&
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
    MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);

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
        char message[MAXSTRLEN]; 
        errno_t rc = 0;
        rc = sprintf_s(message, MAXSTRLEN, "relation \"%s\" does not exist when parse word.", ident);
        securec_check_ss_c(rc, "", "");
        InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc, true);
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
    old_cxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);

    /* Look up relation name.  Can't lock it - we might not have privileges. */
    relvar = makeRangeVar(strVal(linitial(idents)), strVal(lsecond(idents)), -1);

    /* Here relvar is allowed to be a synonym object. */
    class_oid = RangeVarGetRelidExtended(relvar, NoLock, false, false, false, true, NULL, NULL);

    MemoryContextSwitchTo(old_cxt);

    /* Build and return the row type struct */
    return plpgsql_build_datatype(get_rel_type_id(class_oid), -1, InvalidOid);
}

/* cursor generate a composite type, find its col type */
static PLpgSQL_type* plpgsql_get_cursor_type_relid(const char* cursorname, const char* colname, MemoryContext oldCxt)
{
    int nnames;
    PLpgSQL_nsitem* nse = plpgsql_ns_lookup(plpgsql_ns_top(), false, cursorname, NULL, NULL, &nnames);
    if (nse == NULL || nse->itemtype != PLPGSQL_NSTYPE_VAR) {
        return NULL;
    }
    PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[nse->itemno];
    if (!OidIsValid(var->datatype->cursorCompositeOid)) {
        return NULL;
    }
    HeapTuple typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->cursorCompositeOid));
    if (!HeapTupleIsValid(typeTup)) {
        return NULL;
    }
    /* find composite's Oid in pgclass */
    Form_pg_type typeStruct = (Form_pg_type)GETSTRUCT(typeTup);
    Oid classOid = typeStruct->typrelid;
    ReleaseSysCache(typeTup);
    /*
     * Fetch the named table field and its type
     */
    HeapTuple attrTup = SearchSysCacheAttName(classOid, colname);
    if (!HeapTupleIsValid(attrTup)) {
        return NULL;
    }
    Form_pg_attribute attrStruct = (Form_pg_attribute)GETSTRUCT(attrTup);
    Oid attrTypeOid = attrStruct->atttypid;
    ReleaseSysCache(attrTup);
    HeapTuple attrTypeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(attrTypeOid));
    if (!HeapTupleIsValid(attrTypeTup)) {
        return NULL;
    }
    /* caller may in a temp ctx, need switch to a long term cxt. */
    MemoryContext saveCxt = MemoryContextSwitchTo(oldCxt);
    PLpgSQL_type* dtype = build_datatype(attrTypeTup, attrStruct->atttypmod, attrStruct->attcollation);
    ReleaseSysCache(attrTypeTup);
    MemoryContextSwitchTo(saveCxt);
    return dtype;
}

/*
 * get the type of row variable's field, if not found, return null
 * we may need to build a PLpgSQL_type, old_cxt is the MemoryContext which
 * you want to palloc memory
 */
PLpgSQL_type* plpgsql_get_row_field_type(int dno, const char* fieldname, MemoryContext old_cxt)
{
    PLpgSQL_datum* datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
    if (datum->dtype != PLPGSQL_DTYPE_RECORD && datum->dtype != PLPGSQL_DTYPE_ROW) {
        return NULL;
    }

    PLpgSQL_type* dtype = NULL;
    PLpgSQL_row* row = (PLpgSQL_row*)datum;
    int i;
    bool fieldMatched = FALSE;
    for (i = 0; i < row->nfields; i++) {
        fieldMatched = (row->fieldnames[i]) && (strcmp(row->fieldnames[i], fieldname) == 0);
        if (fieldMatched) {
            if (row->ispkg) {
                datum = row->pkg->datums[row->varnos[i]];
            } else {
                datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[row->varnos[i]];
            }
            if (datum->dtype == PLPGSQL_DTYPE_VAR) {
                /* scalar variable, just return the datatype */
                dtype = ((PLpgSQL_var*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[datum->dno]))->datatype;
            }
            if (datum->dtype == PLPGSQL_DTYPE_ROW) {
                /* row variable, need to build a new one */
                Oid typoid =
                    ((PLpgSQL_row*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[datum->dno]))->rowtupdesc->tdtypeid;
                int32 typmod =
                    ((PLpgSQL_row*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[datum->dno]))->rowtupdesc->tdtypmod;
                /* caller may in a temp ctx, need switch to a long term cxt. */
                MemoryContext save_cxt = MemoryContextSwitchTo(old_cxt);
                dtype = plpgsql_build_datatype(typoid, typmod, InvalidOid);
                MemoryContextSwitchTo(save_cxt);
            }
            break;
        }
    }
    return dtype;
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
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true,
        true);
    tmpvar->isnull = false;
    tmpvar->is_cursor_open = true;
    tmpvar->is_cursor_var = true;
    resetStringInfo(&str);
    appendStringInfo(&str, "__gsdb_cursor_attri_%d_found__", varno);

    /* Create the magic FOUND variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true,
        true);
    tmpvar->is_cursor_var = true;
    resetStringInfo(&str);
    appendStringInfo(&str, "__gsdb_cursor_attri_%d_notfound__", varno);
    /* Create the magic NOTFOUND variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(BOOLOID, -1, InvalidOid), true,
        true);
    tmpvar->is_cursor_var = true;
    resetStringInfo(&str);
    appendStringInfo(&str, "__gsdb_cursor_attri_%d_rowcount__", varno);
    /* Create the magic ROWCOUNT variable for explicit cursor attribute %isopen. */
    tmpvar = (PLpgSQL_var*)plpgsql_build_variable(str.data, 0, plpgsql_build_datatype(INT4OID, -1, InvalidOid), true,
        true);
    tmpvar->is_cursor_var = true;
    pfree_ext(str.data);
}

const char *plpgsql_code_int2cstring(int sqlcode)
{
    if (sqlcode >= 0) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("SQLCODE in EXCEPTION_INIT should be less than 0")));
    }

    char* buf = t_thrd.buf_cxt.unpack_sql_state_buf;
    errno_t rc = EOK;

    rc = snprintf_s(buf, SQL_STATE_BUF_LEN, SQL_STATE_BUF_LEN - 1, "%d", sqlcode);
    securec_check_ss(rc, "\0", "\0");
    return buf;
}

const int plpgsql_code_cstring2int(const char *codename)
{
    Assert(codename != NULL);
    int sqlcode = atoi(codename);

    if (sqlcode == 0) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("Invalid SQLstate, maybe no EXCEPTION_INIT for this SQLstate")));
    } else if (sqlcode > 0) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("SQLCODE in EXCEPTION_INIT should be less than 0")));
    }
    return sqlcode;
}

void plpgsql_set_variable(const char* varname, int value)
{
#ifdef ENABLE_MULTIPLE_NODES
    return;
#else
    if (value >= 0) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("SQLCODE in EXCEPTION_INIT should be less than 0")));
    }

    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;

    int varno;
    PLpgSQL_datum* var = NULL;
    for (varno = 0; varno < curr_compile->plpgsql_nDatums; varno++) {
        var = curr_compile->plpgsql_Datums[varno];
        if (var->dtype != PLPGSQL_TTYPE_ROW) {
            continue;
        }

        PLpgSQL_row* rowvar = (PLpgSQL_row*)var;
        if (rowvar->refname && pg_strcasecmp(rowvar->refname, varname) == 0) {
            rowvar->customErrorCode = value;
            rowvar->hasExceptionInit = true;
            return;
        }
    }

    ereport(ERROR,
        (errmodule(MOD_PLSQL),
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("Undefined exception name '%s' in EXCEPTION_INIT", varname)));
    return;
#endif
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
PLpgSQL_variable* plpgsql_build_variable(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace,
    bool isImplicit, const char* varname, knl_pl_body_type plType)
{
    PLpgSQL_variable* result = NULL;
    int varno;
    char* pkgname = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        pkgname = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature;
    }
    switch (dtype->ttype) {
        case PLPGSQL_TTYPE_SCALAR: {
            /* Ordinary scalar datatype */
            PLpgSQL_var* var = NULL;

            var = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
            var->dtype = PLPGSQL_DTYPE_VAR;
            var->refname = pstrdup(refname);
            var->lineno = lineno;
            var->datatype = dtype;
            var->pkg = NULL;
            /* other fields might be filled by caller */

            /* preset to NULL */
            var->value = 0;
            var->isnull = true;
            var->freeval = false;
            var->nest_table = NULL;
            var->nest_layers = 0;
            var->tableOfIndexType = dtype->tableOfIndexType;
            var->isIndexByTblOf = (OidIsValid(var->tableOfIndexType));
            var->addNamespace = add2namespace;
            var->varname = varname == NULL ? NULL : pstrdup(varname);
            varno = plpgsql_adddatum((PLpgSQL_datum*)var);
            if (add2namespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, varno, refname, pkgname);
            }
            if (u_sess->plsql_cxt.curr_compile_context != NULL &&
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL &&
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
                var->pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            }
            if (dtype->typoid == REFCURSOROID) {
                build_cursor_variable(varno);
                var->cursor_closed = false;
            }
            result = (PLpgSQL_variable*)var;
            break;
        }
        case PLPGSQL_TTYPE_ROW: {
            /* Composite type -- build a row variable */
            PLpgSQL_row* row = NULL;

            row = build_row_from_class(dtype->typrelid);
            row->addNamespace = add2namespace;
            row->customErrorCode = 0;
            if (0 == strcmp(format_type_be(row->rowtupdesc->tdtypeid), "exception")) {
                row->customErrorCode = plpgsql_getCustomErrorCode();
            }
            row->hasExceptionInit = false;

            row->dtype = PLPGSQL_DTYPE_ROW;
            row->refname = pstrdup(refname);
            row->lineno = lineno;
            row->addNamespace = add2namespace;
            row->varname = varname == NULL ? NULL : pstrdup(varname);
            row->default_val = NULL;
            varno = plpgsql_adddatum((PLpgSQL_datum*)row);
            if (add2namespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, refname, pkgname);
            }
            if (u_sess->plsql_cxt.curr_compile_context != NULL &&
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL &&
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
                row->pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            }
            result = (PLpgSQL_variable*)row;
            break;
        }
        case PLPGSQL_TTYPE_REC: {
            /* "record" type -- build a record variable */
            PLpgSQL_rec* rec = NULL;

            rec = plpgsql_build_record(refname, lineno, add2namespace);
            rec->addNamespace = add2namespace;
            rec->varname = varname == NULL ? NULL : pstrdup(varname);
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
            row->addNamespace = add2namespace;
            row->varname = varname == NULL ? NULL : pstrdup(varname);
            row->default_val = NULL;
            varno = plpgsql_adddatum((PLpgSQL_datum*)row);
            if (add2namespace) {
                plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, refname, pkgname);
            }
            if (u_sess->plsql_cxt.curr_compile_context != NULL &&
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL &&
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
                row->pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
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
    result->isImplicit = isImplicit;
    return result;
}

/*
 * create a varray type
 */
PLpgSQL_variable* plpgsql_build_varrayType(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace)
{
    /* Ordinary scalar datatype */
    PLpgSQL_var* var = NULL;
    int varno;
    var = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
    var->refname = pstrdup(refname);
    var->lineno = lineno;
    var->datatype = dtype;
    /* other fields might be filled by caller */

    /* preset to NULL */
    var->value = 0;
    var->isnull = true;
    var->freeval = false;
    var->isImplicit = true;
    var->nest_table = NULL;

    varno = plpgsql_adddatum((PLpgSQL_datum*)var);
    if (add2namespace) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_VARRAY, varno, var->refname);
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        var->dtype = PLPGSQL_DTYPE_VARRAY;
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        var->pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
    }
    return (PLpgSQL_variable*)var;
}

/*
 * create a table of type
 */
PLpgSQL_variable* plpgsql_build_tableType(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace)
{
    /* Ordinary scalar datatype */
    PLpgSQL_var* var = NULL;
    int varno;
    var = (PLpgSQL_var*)palloc0(sizeof(PLpgSQL_var));
    var->refname = pstrdup(refname);
    var->lineno = lineno;
    var->datatype = dtype;
    /* other fields might be filled by caller */

    /* preset to NULL */
    var->value = 0;
    var->isnull = true;
    var->freeval = false;
    var->isImplicit = true;
    var->isIndexByTblOf = false;

    varno = plpgsql_adddatum((PLpgSQL_datum*)var);
    if (add2namespace) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_TABLE, varno, var->refname);
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        var->dtype = PLPGSQL_DTYPE_TABLE;
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        var->pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
    }
    return (PLpgSQL_variable*)var;
}
/*
 * Build empty named record variable, and optionally add it to namespace
 */
PLpgSQL_rec* plpgsql_build_record(const char* refname, int lineno, bool add2namespace)
{
    PLpgSQL_rec* rec = NULL;
    int varno;

    rec = (PLpgSQL_rec*)palloc0(sizeof(PLpgSQL_rec));
    rec->dtype = PLPGSQL_DTYPE_REC;
    rec->refname = pstrdup(refname);
    rec->lineno = lineno;
    rec->tup = NULL;
    rec->tupdesc = NULL;
    rec->freetup = false;
    varno = plpgsql_adddatum((PLpgSQL_datum*)rec);
    char* pkgname = NULL;
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        pkgname = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature;
    }
    if (add2namespace) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_REC, varno, rec->refname, pkgname);
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
    int varno;
    ListCell* cell = NULL;
    PLpgSQL_rec_attr* attr = NULL;
    PLpgSQL_rec_type* result = NULL;

    result = (PLpgSQL_rec_type*)palloc0(sizeof(PLpgSQL_rec_type));

    result->dtype = PLPGSQL_DTYPE_RECORD_TYPE;
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

    varno = plpgsql_adddatum((PLpgSQL_datum*)result);

    if (add2namespace) {
       plpgsql_ns_additem(PLPGSQL_NSTYPE_RECORD, varno, typname);
    }
    return result;
}

/*
 * Build a row-variable data structure given the pg_class OID.
 */
PLpgSQL_row* build_row_from_class(Oid class_oid)
{
    /*
     * Open the relation to get info.
     */
    Relation rel = relation_open(class_oid, AccessShareLock);
    Form_pg_class class_struct = RelationGetForm(rel);
    const char* relname = RelationGetRelationName(rel);

    /* accept relation, sequence, view, composite type, materialized view, or foreign table */
    if (class_struct->relkind != RELKIND_RELATION && !RELKIND_IS_SEQUENCE(class_struct->relkind) &&
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
    PLpgSQL_row* row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->rowtupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
    row->nfields = class_struct->relnatts;
    row->fieldnames = (char**)palloc(sizeof(char*) * row->nfields);
    row->varnos = (int*)palloc(sizeof(int) * row->nfields);
    row->default_val = NULL;

    for (int i = 0; i < row->nfields; i++) {
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
    PLpgSQL_row* row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->rowtupdesc = CreateTemplateTupleDesc(numvars, false);
    row->nfields = numvars;
    row->fieldnames = (char**)palloc(numvars * sizeof(char*));
    row->varnos = (int*)palloc(numvars * sizeof(int));
    row->default_val = NULL;

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

PLpgSQL_row* build_row_from_tuple_desc(const char* rowname, int lineno, TupleDesc desc) 
{
    PLpgSQL_row* row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_RECORD;
    row->refname = pstrdup(rowname);
    row->nfields = desc->natts;
    row->lineno = lineno;
    row->rowtupdesc = CreateTemplateTupleDesc(row->nfields, false);
    row->fieldnames = (char**)palloc(row->nfields * sizeof(char*));
    row->varnos = (int*)palloc(row->nfields * sizeof(int));
    row->default_val = NULL;

    for (int i = 0; i < desc->natts; i++) {
        Form_pg_attribute pg_att_form = desc->attrs[i];
        char* att_name = NameStr(pg_att_form->attname);
        if (att_name == NULL || strcmp(att_name, "?column?") == 0) {
            ereport(ERROR,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("The %dth column in %s record variable does't have alias.", (i + 1), rowname)));
        }
		
        StringInfoData info;
        initStringInfo(&info);
        appendStringInfo(&info, "%s.%s", rowname, att_name);

        PLpgSQL_type* type = plpgsql_build_datatype(pg_att_form->atttypid, pg_att_form->atttypmod, pg_att_form->attcollation);
        PLpgSQL_variable* var = plpgsql_build_variable(info.data, lineno, type, false);
        pfree(info.data);

        row->fieldnames[i] = pstrdup(att_name);
        row->varnos[i] = var->dno;

        TupleDescInitEntry(row->rowtupdesc, i + 1, att_name, pg_att_form->atttypid, pg_att_form->atttypmod, 0);
    }

    row->addNamespace = true;
    int varno = plpgsql_adddatum((PLpgSQL_datum*)row);
    plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, rowname);
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
    const int buf_size = (NAMEDATALEN * 2) + 100;
    char buf[buf_size];

    PLpgSQL_row* row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));

    row->dtype = PLPGSQL_DTYPE_RECORD;
    row->refname = pstrdup(rowname);
    row->nfields = type->attrnum;
    row->lineno = lineno;
    row->rowtupdesc = CreateTemplateTupleDesc(row->nfields, false);
    row->fieldnames = (char**)palloc(row->nfields * sizeof(char*));
    row->varnos = (int*)palloc(row->nfields * sizeof(int));
    row->default_val = NULL;
    row->recordVarTypOid = type->typoid;

    for (int i = 0; i < row->nfields; i++) {
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
    HeapTuple type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(type_tup)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u, type Oid is invalid", typeOid)));
    }

    PLpgSQL_type* typ = NULL;
    if (((Form_pg_type)GETSTRUCT(type_tup))->typtype == TYPTYPE_TABLEOF) {
        /* if type is table of, find the base type tuple */
        Oid base_oid = ((Form_pg_type)GETSTRUCT(type_tup))->typelem;
        HeapTuple base_type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(base_oid));
        if (!HeapTupleIsValid(base_type_tup)) {
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for type %u, type Oid is invalid", base_oid)));
        }
        typ = build_datatype(base_type_tup, typmod, collation);
        typ->collectionType = PLPGSQL_COLLECTION_TABLE;
        if (((Form_pg_type)GETSTRUCT(type_tup))->typcategory == TYPCATEGORY_TABLEOF_VARCHAR) {
            typ->tableOfIndexType = VARCHAROID;
        } else if (((Form_pg_type)GETSTRUCT(type_tup))->typcategory == TYPCATEGORY_TABLEOF_INTEGER) {
            typ->tableOfIndexType = INT4OID;
        }
        ReleaseSysCache(base_type_tup);
    } else {
        typ = build_datatype(type_tup, typmod, collation);
    }
    ReleaseSysCache(type_tup);

    return typ;
}

PLpgSQL_type* plpgsql_build_nested_datatype()
{
    PLpgSQL_type *typ = NULL;
    HeapTuple type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(INT4OID));
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        typ = build_datatype(type_tup, -1, 0);
    } else {
        typ = build_datatype(type_tup, -1,
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_input_collation);
    }
    ReleaseSysCache(type_tup);
    return typ;
}

/*
 * Utility subroutine to make a PLpgSQL_type struct given a pg_type entry
 */
PLpgSQL_type* build_datatype(HeapTuple type_tup, int32 typmod, Oid collation)
{
    Form_pg_type type_struct = (Form_pg_type)GETSTRUCT(type_tup);

    if (!type_struct->typisdefined) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" is only a shell when build data type in PLSQL.", NameStr(type_struct->typname))));
    }
    PLpgSQL_type* typ = (PLpgSQL_type*)palloc(sizeof(PLpgSQL_type));

    typ->typname = pstrdup(NameStr(type_struct->typname));
    typ->typoid = HeapTupleGetOid(type_tup);
    typ->collectionType = PLPGSQL_COLLECTION_NONE;
    typ->tableOfIndexType = InvalidOid;
    typ->cursorCompositeOid = InvalidOid;
    typ->typnamespace = get_namespace_name((type_struct->typnamespace));
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
        case TYPTYPE_TABLEOF:
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("don't support nest table of PLSQL.")));
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


/* ----------
 * plpgsql_adddatum			Add a variable, record or row
 *					to the compiler's datum list.
 * ----------
 */
int plpgsql_adddatum(PLpgSQL_datum* newm, bool isChange)
{
    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (curr_compile->plpgsql_curr_compile == NULL) {
        if (curr_compile->plpgsql_pkg_nDatums == curr_compile->datums_pkg_alloc) {
            curr_compile->datums_pkg_alloc *= 2;
            curr_compile->plpgsql_Datums = (PLpgSQL_datum**)repalloc(
                curr_compile->plpgsql_Datums, sizeof(PLpgSQL_datum*) * curr_compile->datums_pkg_alloc);
            curr_compile->datum_need_free = (bool*)repalloc(
                curr_compile->datum_need_free, sizeof(bool) * curr_compile->datums_pkg_alloc);
        }
        if (isChange) {
            newm->dno = curr_compile->plpgsql_pkg_nDatums;
        }
        curr_compile->plpgsql_Datums[curr_compile->plpgsql_pkg_nDatums] = newm;
        curr_compile->datum_need_free[curr_compile->plpgsql_pkg_nDatums] = isChange;
        curr_compile->plpgsql_pkg_nDatums++;
        return curr_compile->plpgsql_pkg_nDatums - 1;
    } else {
        if (curr_compile->plpgsql_nDatums == curr_compile->datums_alloc) {
            curr_compile->datums_alloc *= 2;
            curr_compile->plpgsql_Datums = (PLpgSQL_datum**)repalloc(
                curr_compile->plpgsql_Datums, sizeof(PLpgSQL_datum*) * curr_compile->datums_alloc);
            curr_compile->datum_need_free = (bool*)repalloc(
                curr_compile->datum_need_free, sizeof(bool) * curr_compile->datums_alloc);
        }
        if (isChange) {
            newm->dno = curr_compile->plpgsql_nDatums;
        }
        curr_compile->plpgsql_Datums[curr_compile->plpgsql_nDatums] = newm;
        curr_compile->datum_need_free[curr_compile->plpgsql_nDatums] = isChange;
        curr_compile->plpgsql_nDatums++;
        return curr_compile->plpgsql_nDatums - 1;     
    }
}
#ifndef ENABLE_MULTIPLE_NODES
/* return true if input variable is cross packages. */
static bool plpgsql_check_cross_pkg_val(PLpgSQL_datum* val)
{
    if (!val->ispkg) {
        return false;
    }
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    Oid curPkgOid = InvalidOid;
    if (curr_compile->plpgsql_curr_compile_package != NULL) {
        curPkgOid = curr_compile->plpgsql_curr_compile_package->pkg_oid;
    }
    if (curPkgOid == InvalidOid) {
        return true;
    }

    switch (val->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* var = (PLpgSQL_var*)val;
            if (var->pkg && var->pkg->pkg_oid != curPkgOid)
                return true;
            break;
        }
        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_RECORD: {
            PLpgSQL_row* row = (PLpgSQL_row*)val;
            if (row->pkg && row->pkg->pkg_oid != curPkgOid)
                return true;
            break;
        }
        /* keep compiler quite */
        default:
            break;
    }
    return false;
}
#endif
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
    int plpgsql_nDatums;
    Assert(u_sess->plsql_cxt.curr_compile_context != NULL);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (curr_compile->plpgsql_curr_compile_package != NULL && curr_compile->plpgsql_curr_compile == NULL) {
        plpgsql_nDatums = curr_compile->plpgsql_pkg_nDatums;
    } else {
        plpgsql_nDatums = curr_compile->plpgsql_nDatums;
    }
    for (i = curr_compile->datums_last; i < plpgsql_nDatums; i++) {
        switch (curr_compile->plpgsql_Datums[i]->dtype) {
            case PLPGSQL_DTYPE_VAR:
            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD:
#ifndef ENABLE_MULTIPLE_NODES
                if (!plpgsql_check_cross_pkg_val(curr_compile->plpgsql_Datums[i]))
#endif
                    n++;
            /* fall through */
            default:
                break;
        }
    }

    if (varnos != NULL) {
        if (n > 0) {
            *varnos = (int*)palloc(sizeof(int) * n);

            n = 0;
            for (i = curr_compile->datums_last; i < plpgsql_nDatums; i++) {
                switch (curr_compile->plpgsql_Datums[i]->dtype) {
                    case PLPGSQL_DTYPE_VAR:
                    case PLPGSQL_DTYPE_ROW:
                    case PLPGSQL_DTYPE_RECORD:
#ifndef ENABLE_MULTIPLE_NODES
                        if (!plpgsql_check_cross_pkg_val(curr_compile->plpgsql_Datums[i]))
#endif
                            (*varnos)[n++] = curr_compile->plpgsql_Datums[i]->dno;
                    /* fall through */
                    default:
                        break;
                }
            }
        } else {
            *varnos = NULL;
        }
    }

    curr_compile->datums_last = plpgsql_nDatums;
    return n;
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
    if (allow_sqlstate) {
        if (strlen(condname) == 5 && strspn(condname, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") == 5) {
            return MAKE_SQLSTATE(condname[0], condname[1], condname[2], condname[3], condname[4]);
        }
    }

    for (int i = 0; exception_label_map[i].label != NULL; i++) {
        if (strcmp(condname, exception_label_map[i].label) == 0) {
            return exception_label_map[i].sqlerrstate;
        }
    }
    char message[MAXSTRLEN]; 
    errno_t rc = 0;
    rc = sprintf_s(message, MAXSTRLEN, "unrecognized exception condition \"%s\"", condname);
    securec_check_ss_c(rc, "", "");
    InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
    ereport(ERROR,
        (errmodule(MOD_PLSQL),
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("unrecognized exception condition \"%s\"", condname)));
    return 0; /* keep compiler quiet */
}

/*
 * plpgsql_parse_err_condition
 *              Generate PLpgSQL_condition entry(s) for an exception condition name
 *
 * This has to be able to return a list because there are some duplicate
 * names in the table of error code names.
 */
PLpgSQL_condition* plpgsql_parse_err_condition(char* condname)
{
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
    for (int i = 0; exception_label_map[i].label != NULL; i++) {
        if (strcmp(condname, exception_label_map[i].label) == 0) {
            newm = (PLpgSQL_condition*)palloc(sizeof(PLpgSQL_condition));
            newm->sqlerrstate = exception_label_map[i].sqlerrstate;
            newm->condname = condname;
            newm->next = prev;
            prev = newm;
        }
    }

    if (prev == NULL) {
        char message[MAXSTRLEN]; 
        errno_t rc = 0;
        rc = sprintf_s(message, MAXSTRLEN, "unrecognized exception condition \"%s\"", condname);
        securec_check_ss_c(rc, "", "");
        InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("unrecognized exception condition \"%s\"", condname)));
    }
    return prev;
}


/*
 * Compute the hashkey for a given function invocation
 *
 * The hashkey is returned into the caller-provided storage at *hashkey.
 */
static void compute_function_hashkey(HeapTuple proc_tup, FunctionCallInfo fcinfo, Form_pg_proc proc_struct,
    PLpgSQL_func_hashkey* hashkey, bool for_validator)
{
    /* Make sure any unused bytes of the struct are zero */
    errno_t rc = memset_s(hashkey, sizeof(PLpgSQL_func_hashkey), 0, sizeof(PLpgSQL_func_hashkey));
    securec_check(rc, "", "");

    /* get function OID */
    hashkey->funcOid = fcinfo->flinfo->fn_oid;

    bool isnull = false;
    Datum packageOid_datum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    Oid packageOid = DatumGetObjectId(packageOid_datum);
    hashkey->packageOid = packageOid;
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
        oidvector* proargs = ProcedureGetArgTypes(proc_tup);

        /* get the argument types */
        errno_t err = memcpy_s(hashkey->argtypes,
            sizeof(hashkey->argtypes),
            proargs->values,
            proc_struct->pronargs * sizeof(Oid));
        securec_check(err, "\0", "\0");

        /* resolve any polymorphic argument types */
        plpgsql_resolve_polymorphic_argtypes(proc_struct->pronargs,
            hashkey->argtypes,
            NULL,
            fcinfo->flinfo->fn_expr, for_validator, NameStr(proc_struct->proname));
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
void delete_function(PLpgSQL_function* func, bool fromPackage)
{
    u_sess->plsql_cxt.is_delete_function = true;

    SPIPlanCacheTableDelete(SPICacheHashFunc(func->fn_hashkey, sizeof(PLpgSQL_func_hashkey)));

    /* remove function from hash table (might be done already) */
    plpgsql_HashTableDelete(func);
    /* release the function's storage if safe and not done already */
    if (func->use_count == 0) {
        plpgsql_free_function_memory(func, fromPackage);
    }

    u_sess->plsql_cxt.is_delete_function = false;
}

/* exported so we can call it from plpgsql_init() */
void plpgsql_HashTableInit(void)
{
    HASHCTL ctl;

    /* don't allow double-initialization */
    AssertEreport(u_sess->plsql_cxt.plpgsql_HashTable == NULL, MOD_PLSQL, "don't allow double-initialization.");

    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PLpgSQL_func_hashkey);
    ctl.entrysize = sizeof(plpgsql_HashEnt);
    ctl.hash = tag_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->plsql_cxt.plpgsql_HashTable =
        hash_create("PLpgSQL function cache", FUNCS_PER_USER, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    /* init package hash table */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PLpgSQL_pkg_hashkey);
    ctl.entrysize = sizeof(plpgsql_pkg_HashEnt);
    ctl.hash = tag_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->plsql_cxt.plpgsql_pkg_HashTable =
        hash_create("PLpgSQL package cache", PKGS_PER_USER, &ctl, HASH_ELEM | HASH_PACKAGE | HASH_CONTEXT);
}

static PLpgSQL_function* plpgsql_HashTableLookup(PLpgSQL_func_hashkey* func_key)
{
    plpgsql_HashEnt* hentry = NULL;

    hentry = (plpgsql_HashEnt*)hash_search(u_sess->plsql_cxt.plpgsql_HashTable, (void*)func_key, HASH_FIND, NULL);

    if (hentry != NULL) {
        /* add cell to the tail of the function list. */
        dlist_add_tail_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell);
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

void plpgsql_HashTableDelete(PLpgSQL_function* func)
{
    /* do nothing if not in table */
    if (func->fn_hashkey == NULL) {
        return;
    }

    plpgsql_HashEnt* hentry = (plpgsql_HashEnt*)hash_search(
        u_sess->plsql_cxt.plpgsql_HashTable, (void*)func->fn_hashkey, HASH_REMOVE, NULL);
    if (hentry == NULL) {
        elog(WARNING, "trying to delete function that does not exist");
    } else {
        /* delete the cell from the list. */
        u_sess->plsql_cxt.plpgsql_dlist_objects =
            dlist_delete_cell(u_sess->plsql_cxt.plpgsql_dlist_objects, hentry->cell, false);
    }
    /* remove back link, which no longer points to allocated storage */
    func->fn_hashkey = NULL;
}

/*
 * get a custom error code for a new user defined exceprtion.
 */
int plpgsql_getCustomErrorCode(void)
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
bool plpgsql_check_insert_colocate(
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

/* Try to find field word2 and word3 of a row variable, if find, return ture */
static bool plpgsql_lookup_tripword_datum(int itemno, const char* word2, const char* word3,
    PLwdatum* wdatum, int* tok_flag, bool isPkgVar)
{
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;

    if (curr_compile->plpgsql_Datums[itemno]->dtype != PLPGSQL_DTYPE_RECORD
        && curr_compile->plpgsql_Datums[itemno]->dtype != PLPGSQL_DTYPE_ROW) {
        return false;
    }

    PLpgSQL_row* row = NULL;
    PLpgSQL_datum* tempDatum = NULL;
    int i;
    bool isPkg = false;
    row = (PLpgSQL_row*)(curr_compile->plpgsql_Datums[itemno]);
    isPkg = row->ispkg && isPkgVar;

    /* find field word2 first */
    for (i = 0; i < row->nfields; i++) {
        if (row->fieldnames[i] && strcmp(row->fieldnames[i], word2) == 0) {
            if (isPkg) {
                tempDatum = row->pkg->datums[row->varnos[i]];
            } else {
                tempDatum = curr_compile->plpgsql_Datums[row->varnos[i]];
            }
            break;
        }
    }

    /* then find field word3 */
    if (tempDatum != NULL &&
        (tempDatum->dtype == PLPGSQL_DTYPE_ROW || tempDatum->dtype == PLPGSQL_DTYPE_RECORD)) {
        row = (PLpgSQL_row*)tempDatum;
        for (i = 0; i < row->nfields; i++) {
            if (row->fieldnames[i] && strcmp(row->fieldnames[i], word3) == 0) {
                if (isPkg) {
                    int dno = -1;
                    PLpgSQL_nsitem* tempns = NULL;
                    PLpgSQL_datum* tempdatum = row->pkg->datums[row->varnos[i]];
                    char* refname = ((PLpgSQL_variable*)tempdatum)->refname;
                    tempns = plpgsql_ns_lookup(plpgsql_ns_top(), false, row->pkg->pkg_signature, refname, NULL, NULL);
                    if (tempns != NULL) {
                        dno =  tempns->itemno;
                    } else {
                        dno = plpgsql_adddatum(tempdatum, false);
                        if (tempdatum->dtype == PLPGSQL_DTYPE_VAR) {
                            plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, dno, refname, row->pkg->pkg_signature);
                        } else {
                            plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, dno, refname, row->pkg->pkg_signature);
                        }
                    }
                    wdatum->datum = tempdatum;
                    wdatum->dno = dno;
                    *tok_flag = PLPGSQL_TOK_PACKAGE_VARIABLE;
                } else {
                    wdatum->datum = curr_compile->plpgsql_Datums[row->varnos[i]];
                    wdatum->dno = row->varnos[i];
                    get_datum_tok_type(wdatum->datum, tok_flag);
                }
                wdatum->ident = NULL;
                wdatum->quoted = false; /* not used */
                return true;
            }
        }
    }
    return false;
}

static void get_datum_tok_type(PLpgSQL_datum* target, int* tok_flag)
{
    if (target->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* var = (PLpgSQL_var*)(target);
        if (var != NULL && var->datatype != NULL &&
            var->datatype->typinput.fn_oid == F_ARRAY_IN) {
            if (var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE) {
                *tok_flag = PLPGSQL_TOK_TABLE_VAR;
            } else {
                *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
            }
        } else if (var != NULL && var->datatype &&
            var->datatype->typinput.fn_oid == F_DOMAIN_IN) {
            HeapTuple type_tuple =
                SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
            if (HeapTupleIsValid(type_tuple)) {
                Form_pg_type type_form = (Form_pg_type)GETSTRUCT(type_tuple);
                if (F_ARRAY_OUT == type_form->typoutput) {
                    *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
                }
            }
            ReleaseSysCache(type_tuple);
        }
    }
}

TupleDesc getCursorTupleDesc(PLpgSQL_expr* expr, bool isOnlySelect, bool isOnlyParse)
{
    if (expr == NULL || !ALLOW_PROCEDURE_COMPILE_CHECK) {
        return NULL;
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        /* 
         * because while compile a package, don't have struct function yet.
         * but make_datum_param needs it, so we should palloc a func.
         */
        expr->func = (PLpgSQL_function*)palloc0(sizeof(PLpgSQL_function));
        expr->func->fn_cxt = CurrentMemoryContext;
    } else {
        expr->func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
        if (expr->func->fn_is_trigger) {
            return NULL;
        }
    }
    MemoryContext current_context = CurrentMemoryContext;
    bool temp_pre_parse_trig = expr->func->pre_parse_trig ;
    expr->func->pre_parse_trig = true;
    expr->func->datums = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums;
    expr->func->ndatums = u_sess->plsql_cxt.curr_compile_context->plpgsql_nDatums;
    TupleDesc tupleDesc = NULL;
    PG_TRY();
    {
        List* parsetreeList = pg_parse_query(expr->query);
        ListCell* cell = NULL;
        List* queryList = NIL;
        foreach(cell, parsetreeList) {
            Node *parsetree = (Node *)lfirst(cell);
            if (nodeTag(parsetree) == T_SelectStmt) {
                if (checkSelectIntoParse((SelectStmt*)parsetree)) {
                    list_free_deep(parsetreeList);
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("select into clause is not supported in cursor or for..in loop condition yet."),
                    errdetail("query \"%s\" is not supported in cursor or for..in loop condition yet.", expr->query),
                    errcause("feature not supported"),
                    erraction("modify the query")));
                }
            } else {
                if (isOnlySelect) {
                    expr->func->pre_parse_trig = temp_pre_parse_trig;
                    expr->func->datums = NULL;
                    expr->func->ndatums = 0;
                    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
                        pfree(expr->func);
                    }
                    expr->func = NULL;
                    list_free_deep(parsetreeList);
                    return NULL;
                }
            }
            queryList = pg_analyze_and_rewrite_params(parsetree, expr->query, 
                (ParserSetupHook)plpgsql_parser_setup, (void*)expr);
        }
        Query* query = (Query*)linitial(queryList);
        Assert(IsA(query, Query));
        if (!isOnlyParse) {
            tupleDesc =  ExecCleanTypeFromTL(query->targetList, false);
        }
        expr->func->pre_parse_trig = temp_pre_parse_trig;
        expr->func->datums = NULL;
        expr->func->ndatums = 0;
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
            pfree(expr->func);
        }
        expr->func = NULL;
        list_free_deep(parsetreeList);
        list_free_deep(queryList);
    }
    PG_CATCH();
    {
        /* Save error info */
        MemoryContext ecxt = MemoryContextSwitchTo(current_context);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        (void)MemoryContextSwitchTo(ecxt);
        expr->func->pre_parse_trig = temp_pre_parse_trig;
        expr->func->datums = NULL;
        expr->func->ndatums = 0;
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
            pfree(expr->func);
        }
        expr->func = NULL;
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("compile failed when parse the query: %s", expr->query),
                        errdetail("%s", edata->message),
                        errcause("The syntax of the query is wrong"),
                        erraction("modify the query")));
    }
    PG_END_TRY();

    return tupleDesc;
}
static int get_inner_type_ind(Oid typeoid)
{
    if (typeoid == InvalidOid) {
        return 0;
    }
    Form_pg_type typtup;
    Oid typelem;
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeoid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for type %u", typeoid)));
    }
    typtup = (Form_pg_type)GETSTRUCT(tp);
    typelem = typtup->typelem;
    ReleaseSysCache(tp);
    return 1 + get_inner_type_ind(typelem);
}

/* Simply check whether the ColumnRef's type meets the subscript requirements. */
Node* plpgsql_check_match_var(Node* node, ParseState* pstate, ColumnRef* cref)
{
    Node* ans = NULL;
    if (node != NULL && IsA(node, Var) &&
        pstate->p_pre_columnref_hook == plpgsql_pre_column_ref &&
        cref->indnum > 0) {
        Var* colvar = (Var*)node;
        if (get_inner_type_ind(colvar->vartype) -1 != cref->indnum) {
            ans = resolve_column_ref(pstate, (PLpgSQL_expr*)pstate->p_ref_hook_state, cref, false);
        }
    }
    return ans;
}
