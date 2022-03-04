/* -------------------------------------------------------------------------
 *
 * fmgr.c
 *	  The openGauss function manager.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/fmgr/fmgr.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tuptoaster.h"
#include "access/ustore/knl_utuptoaster.h"
#include "bulkload/dist_fdw.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/functions.h"
#include "executor/spi.h"
#include "gc_fdw.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/plpgsql.h"
#include "log_fdw.h"

/*
 * Hooks for function calls
 */
THR_LOCAL PGDLLIMPORT needs_fmgr_hook_type needs_fmgr_hook = NULL;
THR_LOCAL PGDLLIMPORT fmgr_hook_type fmgr_hook = NULL;
extern void InitFuncCallUDFInfo(FunctionCallInfoData* fcinfo, int argN, bool setFuncPtr);

/*
 * Declaration for old-style function pointer type.  This is now used only
 * in fmgr_oldstyle() and is no longer exported.
 *
 * The m68k SVR4 ABI defines that pointers are returned in %a0 instead of
 * %d0. So if a function pointer is declared to return a pointer, the
 * compiler may look only into %a0, but if the called function was declared
 * to return an integer type, it puts its value only into %d0. So the
 * caller doesn't pick up the correct return value. The solution is to
 * declare the function pointer to return int, so the compiler picks up the
 * return value from %d0. (Functions returning pointers put their value
 * *additionally* into %d0 for compatibility.) The price is that there are
 * some warnings about int->pointer conversions ... which we can suppress
 * with suitably ugly casts in fmgr_oldstyle().
 */
#if (defined(__mc68000__) || (defined(__m68k__))) && defined(__ELF__)
typedef int32 (*func_ptr)();
typedef int32 (*func_ptr_p1)(void*);
typedef int32 (*func_ptr_pp)(Datum, void*);
typedef int32 (*func_ptr_p2)(Datum, void*);
typedef int32 (*func_ptr_p3)(Datum, Datum, Datum);
typedef int32 (*func_ptr_p4)(Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p5)(Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p6)(Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p7)(Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p8)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p9)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p10)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p11)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p12)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p13)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p14)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p15)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef int32 (*func_ptr_p16)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
#else
typedef char* (*func_ptr)();
typedef char* (*func_ptr_p1)(void*);
typedef char* (*func_ptr_pp)(Datum, void*);
typedef char* (*func_ptr_p2)(Datum, Datum);
typedef char* (*func_ptr_p3)(Datum, Datum, Datum);
typedef char* (*func_ptr_p4)(Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p5)(Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p6)(Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p7)(Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p8)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p9)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p10)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p11)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p12)(Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p13)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p14)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p15)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
typedef char* (*func_ptr_p16)(
    Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum, Datum);
#endif

/*
 * For an oldstyle function, fn_extra points to a record like this:
 */
typedef struct {
    func_ptr func;                     /* Address of the oldstyle function */
    bool arg_toastable[FUNC_MAX_ARGS]; /* is n'th arg of a toastable
                                        * datatype? */
} Oldstyle_fnextra;

/*
 * Hashtable for fast lookup of external C functions
 */
typedef struct {
    /* fn_oid is the hash key and so must be first! */
    Oid fn_oid;            /* OID of an external C function */
    TransactionId fn_xmin; /* for checking up-to-dateness */
    ItemPointerData fn_tid;
    PGFunction user_fn;             /* the function's address */
    const Pg_finfo_record* inforec; /* address of its info record */
} CFuncHashTabEntry;

/*
 * For loading plpgsql function.
 */
typedef struct {
    char* func_name;
    PGFunction func_addr;
} RegExternFunc;

/* notice: order by ascii code */
static RegExternFunc plpgsql_function_table[] = {
    {"intervaltonum", intervaltonum},
    {"plpgsql_call_handler", plpgsql_call_handler},
    {"plpgsql_inline_handler", plpgsql_inline_handler},
    {"plpgsql_validator", plpgsql_validator},
    {"rawtohex", rawtohex},
    {"regexp_substr", regexp_substr},
    {"report_application_error", report_application_error},
};

static HTAB* CFuncHash = NULL;

static void fmgr_info_cxt_security(Oid functionId, FmgrInfo* finfo, MemoryContext mcxt, bool ignore_security);
static void fmgr_info_C_lang(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple);
static void fmgr_info_other_lang(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple);
static CFuncHashTabEntry* lookup_C_func(HeapTuple procedureTuple);
static void record_C_func(HeapTuple procedureTuple, PGFunction user_fn, const Pg_finfo_record* inforec);
static Datum fmgr_oldstyle(PG_FUNCTION_ARGS);
static Datum fmgr_security_definer(PG_FUNCTION_ARGS);

extern bool RPCInitFencedUDFIfNeed(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple);
extern char* get_language_name(Oid languageOid);

/*
 * Lookup routines for builtin-function table.	We can search by either Oid
 * or name, but search by Oid is much faster.
 */

const FmgrBuiltin* fmgr_isbuiltin(Oid id)
{
    const Builtin_func*  func = SearchBuiltinFuncByOid(id);
    if (func == NULL)
        return NULL;
    else
        return (func->prolang != INTERNALlanguageId) ? NULL : (const FmgrBuiltin*)func;
}

/*
 * Lookup a builtin by name.  Note there can be more than one entry in
 * the array with the same name, but they should all point to the same
 * routine.
 */
static const FmgrBuiltin* fmgr_lookupByName(const char* name)
{
    int low = 0;
    int high = fmgr_nbuiltins - 1;
    int ret;
    while (low <= high) {
        int i = (high + low) / 2;
        ret = strcmp(name, fmgr_builtins[i].funcName);
        if (ret == 0) {
            return fmgr_builtins + i;
        } else if (ret > 0) {
            low = i + 1;
        } else {
            high = i - 1;
        }
    }

    return NULL;
}

/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.
 *
 * The caller's CurrentMemoryContext is used as the fn_mcxt of the info
 * struct; this means that any subsidiary data attached to the info struct
 * (either by fmgr_info itself, or later on by a function call handler)
 * will be allocated in that context.  The caller must ensure that this
 * context is at least as long-lived as the info struct itself.  This is
 * not a problem in typical cases where the info struct is on the stack or
 * in freshly-palloc'd space.  However, if one intends to store an info
 * struct in a long-lived table, it's better to use fmgr_info_cxt.
 */
void fmgr_info(Oid functionId, FmgrInfo* finfo)
{
    fmgr_info_cxt_security(functionId, finfo, CurrentMemoryContext, false);
}

/*
 * Fill a FmgrInfo struct, specifying a memory context in which its
 * subsidiary data should go.
 */
void fmgr_info_cxt(Oid functionId, FmgrInfo* finfo, MemoryContext mcxt)
{
    fmgr_info_cxt_security(functionId, finfo, mcxt, false);
}

/*
 * This one does the actual work.  ignore_security is ordinarily false
 * but is set to true when we need to avoid recursion.
 */
static void fmgr_info_cxt_security(Oid functionId, FmgrInfo* finfo, MemoryContext mcxt, bool ignore_security)
{
    const FmgrBuiltin* fbp = NULL;
    HeapTuple procedureTuple;
    Form_pg_proc procedureStruct;
    Datum prosrcdatum;
    bool isnull = false;
    char* prosrc = NULL;

    /*
     * fn_oid *must* be filled in last.  Some code assumes that if fn_oid is
     * valid, the whole struct is valid.  Some FmgrInfo struct's do survive
     * elogs.
     */
    finfo->fn_oid = InvalidOid;
    finfo->fn_extra = NULL;
    finfo->fn_mcxt = mcxt;
    finfo->fn_expr = NULL; /* caller may set this later */
    finfo->fn_fenced = false;
    finfo->fnLibPath = NULL;

    if ((fbp = fmgr_isbuiltin(functionId)) != NULL) {
        /*
         * Fast path for builtin functions: don't bother consulting pg_proc
         */
        finfo->fn_nargs = fbp->nargs;
        finfo->fn_strict = fbp->strict;
        finfo->fn_retset = fbp->retset;
        finfo->fn_stats = TRACK_FUNC_ALL; /* ie, never track */
        finfo->fn_addr = fbp->func;
        finfo->fn_oid = functionId;
        finfo->fn_rettype = fbp->rettype;
        return;
    }

    /* Otherwise we need the pg_proc entry */
    procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));

    if (!HeapTupleIsValid(procedureTuple))
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u", functionId)));

    procedureStruct = (Form_pg_proc)GETSTRUCT(procedureTuple);

    finfo->fn_nargs = procedureStruct->pronargs;
    finfo->fn_strict = procedureStruct->proisstrict;
    finfo->fn_retset = procedureStruct->proretset;
    finfo->fn_rettype = procedureStruct->prorettype;
    if (IsClientLogicType(finfo->fn_rettype)) {
        HeapTuple gstup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(functionId));
        if (!HeapTupleIsValid(gstup)) /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", functionId)));
        Form_gs_encrypted_proc gsform = (Form_gs_encrypted_proc)GETSTRUCT(gstup);
        finfo->fn_rettypemod = gsform->prorettype_orig;
        ReleaseSysCache(gstup);
    }
    finfo->fn_languageId = procedureStruct->prolang;
    finfo->fn_volatile = procedureStruct->provolatile;
    /*
     * If it has prosecdef set, non-null proconfig, or if a plugin wants to
     * hook function entry/exit, use fmgr_security_definer call handler ---
     * unless we are being called again by fmgr_security_definer or
     * fmgr_info_other_lang.
     *
     * When using fmgr_security_definer, function stats tracking is always
     * disabled at the outer level, and instead we set the flag properly in
     * fmgr_security_definer's private flinfo and implement the tracking
     * inside fmgr_security_definer.  This loses the ability to charge the
     * overhead of fmgr_security_definer to the function, but gains the
     * ability to set the track_functions GUC as a local GUC parameter of an
     * interesting function and have the right things happen.
     */
    if (!ignore_security && (procedureStruct->prosecdef || !heap_attisnull(procedureTuple, Anum_pg_proc_proconfig, NULL) ||
                                FmgrHookIsNeeded(functionId))) {
        Datum procFenced = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_fenced, &isnull);
        /* fmgr_security_definer invoke fmgr_info_cxt_security to initialize again */
        finfo->fn_fenced = !isnull && DatumGetBool(procFenced);
        finfo->fn_addr = fmgr_security_definer;
        finfo->fn_stats = TRACK_FUNC_ALL; /* ie, never track */
        finfo->fn_oid = functionId;
        ReleaseSysCache(procedureTuple);
        return;
    }

    switch (procedureStruct->prolang) {
        case INTERNALlanguageId:

            /*
             * For an ordinary builtin function, we should never get here
             * because the isbuiltin() search above will have succeeded.
             * However, if the user has done a CREATE FUNCTION to create an
             * alias for a builtin function, we can end up here.  In that case  * we have to look up the function by
             * name.  The name of the internal function is stored in prosrc (it doesn't have to be the same as the name
             * of the alias!)
             */
            prosrcdatum = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_prosrc, &isnull);

            if (isnull)
                ereport(
                    ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null prosrc")));

            prosrc = TextDatumGetCString(prosrcdatum);
            fbp = fmgr_lookupByName(prosrc);

            if (fbp == NULL)
                ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("internal function \"%s\" is not in internal lookup table", prosrc)));

            pfree_ext(prosrc);
            /* Should we check that nargs, strict, retset match the table? */
            finfo->fn_addr = fbp->func;
            /* note this policy is also assumed in fast path above */
            finfo->fn_stats = TRACK_FUNC_ALL; /* ie, never track */
            break;

        case ClanguageId:
            fmgr_info_C_lang(functionId, finfo, procedureTuple);
            finfo->fn_stats = TRACK_FUNC_PL; /* ie, track if ALL */
            break;

        case SQLlanguageId:
            finfo->fn_addr = fmgr_sql;
            finfo->fn_stats = TRACK_FUNC_PL; /* ie, track if ALL */
            break;

        default:
            fmgr_info_other_lang(functionId, finfo, procedureTuple);
            finfo->fn_stats = TRACK_FUNC_OFF; /* ie, track if not OFF */
            break;
    }

    finfo->fn_oid = functionId;
    ReleaseSysCache(procedureTuple);
}

static int ExternFuncComp(const void* m1, const void* m2)
{
    RegExternFunc* mi1 = (RegExternFunc*)m1;
    RegExternFunc* mi2 = (RegExternFunc*)m2;

    return strncmp(mi1->func_name, mi2->func_name, 64);
}

/*
 * @Description: Get function address, plpgsql has been static linked now
 * instead of dynamically loaded, use this to avoid load plpgsql lib.
 * @in funcname: Function name.
 * @return: Function address.
 */
static PGFunction load_plpgsql_function(char* funcname)
{
    PGFunction retval = NULL;
    RegExternFunc tmp_key;
    RegExternFunc* search_result = NULL;

    tmp_key.func_name = funcname;
    search_result = (RegExternFunc*)bsearch(&tmp_key,
        plpgsql_function_table,
        sizeof(plpgsql_function_table) / sizeof(plpgsql_function_table[0]),
        sizeof(RegExternFunc),
        ExternFuncComp);
    if (search_result != NULL) {
        retval = search_result->func_addr;
    } else if (!strcmp(funcname, "dist_fdw_validator")) {
        retval = &dist_fdw_validator;
    } else if (!strcmp(funcname, "dist_fdw_handler")) {
        retval = &dist_fdw_handler;
    } else if (!strcmp(funcname, "file_fdw_validator")) {
        retval = &file_fdw_validator;
    } else if (!strcmp(funcname, "file_fdw_handler")) {
        retval = &file_fdw_handler;
    } else if (!strcmp(funcname, "hdfs_fdw_validator")) {
        retval = &hdfs_fdw_validator;
    } else if (!strcmp(funcname, "hdfs_fdw_handler")) {
        retval = &hdfs_fdw_handler;
#ifdef ENABLE_MOT
    } else if (!strcmp(funcname, "mot_fdw_validator")) {
        retval = &mot_fdw_validator;
    } else if (!strcmp(funcname, "mot_fdw_handler")) {
        retval = &mot_fdw_handler;
#endif
    } else if (!strcmp(funcname, "log_fdw_handler")) {
        retval = &log_fdw_handler;
    } else if (!strcmp(funcname, "log_fdw_validator")) {
        retval = &log_fdw_validator;
    } else if (!strcmp(funcname, "gc_fdw_handler")) {
        retval = &gc_fdw_handler;
    } else if (!strcmp(funcname, "gc_fdw_validator")) {
        retval = &gc_fdw_validator;
    }

    return retval;
}

/*
 * Special fmgr_info processing for C-language functions.  Note that
 * finfo->fn_oid is not valid yet.
 */
static void fmgr_info_C_lang(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple)
{
    CFuncHashTabEntry* hashentry = NULL;
    PGFunction user_fn;
    const Pg_finfo_record* inforec = NULL;
    bool isnull = false;
    CFunInfo funInfo;

    Datum prosrcattr, probinattr;
    char* prosrcstring = NULL;
    char* probinstring = NULL;

    Pg_finfo_record default_inforec_1 = {1};
    inforec = &default_inforec_1;

    bool user_defined_func = false;

    /*
     * Get prosrc and probin strings (link symbol and library filename).
     * While in general these columns might be null, that's not allowed
     * for C-language functions.
     */
    prosrcattr = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_prosrc, &isnull);

    if (isnull)
        ereport(ERROR,  (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null prosrc for C function %u", functionId)));

    prosrcstring = TextDatumGetCString(prosrcattr);
    check_backend_env(prosrcstring);

    probinattr = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_probin, &isnull);

    if (isnull)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null probin for C function %u", functionId)));

    probinstring = TextDatumGetCString(probinattr);

    if (strncmp(probinstring, "$libdir/pg_plugin/", strlen("$libdir/pg_plugin/")) == 0) {
        user_defined_func = true;

        /* Lock to this function, ensure is is not droped.*/
        LockDatabaseObject(ProcedureRelationId, functionId, 0, AccessShareLock);
    }

    AutoMutexLock libraryLock(&dlerror_lock);

    libraryLock.lock();

    /*
     * We use function RunUdFFencedMode to replace if this UDF
     * is defined in fencedMode
     */
    if (RPCInitFencedUDFIfNeed(functionId, finfo, procedureTuple))
        return;

    /*
     * See if we have the function address cached already
     */
    hashentry = lookup_C_func(procedureTuple);

    if (hashentry != NULL) {
        user_fn = hashentry->user_fn;
        inforec = hashentry->inforec;
    } else {
        /* Look up the function itself */
        if (strcmp(probinstring, "$libdir/plpgsql") && strcmp(probinstring, "$libdir/dist_fdw") &&
            strcmp(probinstring, "$libdir/file_fdw") && strcmp(probinstring, "$libdir/log_fdw") &&
            strcmp(probinstring, "$libdir/hdfs_fdw") &&
#ifdef ENABLE_MULTIPLE_NODES
            strcmp(probinstring, "$libdir/postgres_fdw")
#else
            strcmp(probinstring, "$libdir/mot_fdw")
#endif
            ) {
            funInfo = load_external_function(probinstring, prosrcstring, true, false);
            user_fn = funInfo.user_fn;
            inforec = funInfo.inforec;

            /* Cache the addresses for later calls */
            record_C_func(procedureTuple, user_fn, inforec);
        } else {
            /* These function define in system codes, their version must be 1.*/
            user_fn = load_plpgsql_function(prosrcstring);
        }
    }

    switch (inforec->api_version) {
        case 0:
        case 1:
            /* New style: call directly */
            finfo->fn_addr = user_fn;
            break;

        default:
            /* Shouldn't get here if fetch_finfo_record did its job */
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized function API version: %d", inforec->api_version)));
            break;
    }

    pfree_ext(probinstring);
    pfree_ext(prosrcstring);

    libraryLock.unLock();
}

/*
 * Special fmgr_info processing for other-language functions.  Note
 * that finfo->fn_oid is not valid yet.
 */
static void fmgr_info_other_lang(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple)
{
    Form_pg_proc procedureStruct = (Form_pg_proc)GETSTRUCT(procedureTuple);
    Oid language = procedureStruct->prolang;
    HeapTuple languageTuple;
    Form_pg_language languageStruct;
    FmgrInfo plfinfo;

    /*
     * We only support fenced java-udf now. For java-udf, finfo->fn_addr will be replaced by
     * RPCFencedUDF in RPCInitFencedUDFIfNeed, and return.
     */
    char* lname = get_language_name(language);
    if ((language == JavalanguageId || strncmp(lname, "plpython", strlen("plpython")) == 0) 
                    && RPCInitFencedUDFIfNeed(functionId, finfo, procedureTuple)) {
        return;
    }

    languageTuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(language));

    if (!HeapTupleIsValid(languageTuple))
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for language %u", functionId)));

    languageStruct = (Form_pg_language)GETSTRUCT(languageTuple);

    /*
     * Look up the language's call handler function, ignoring any attributes
     * that would normally cause insertion of fmgr_security_definer.  We need
     * to get back a bare pointer to the actual C-language function.
     */
    fmgr_info_cxt_security(languageStruct->lanplcallfoid, &plfinfo, CurrentMemoryContext, true);
    finfo->fn_addr = plfinfo.fn_addr;

    /*
     * If lookup of the PL handler function produced nonnull fn_extra,
     * complain --- it must be an oldstyle function! We no longer support
     * oldstyle PL handlers.
     */
    if (plfinfo.fn_extra != NULL)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("language %u has old-style handler", language)));

    ReleaseSysCache(languageTuple);
}

/*
 * Fetch and validate the information record for the given external function.
 * The function is specified by a handle for the containing library
 * (obtained from load_external_function) as well as the function name.
 *
 * If no info function exists for the given name, it is not an error.
 * Instead we return a default info record for a version-0 function.
 * We want to raise an error here only if the info function returns
 * something bogus.
 *
 * This function is broken out of fmgr_info_C_lang so that fmgr_c_validator
 * can validate the information record for a function not yet entered into
 * pg_proc.
 */
const Pg_finfo_record* fetch_finfo_record(void* filehandle, char* funcname, bool isValidate)
{
    char* infofuncname = NULL;
    PGFInfoFunction infofunc;
    const Pg_finfo_record* inforec = NULL;
    Pg_finfo_record* default_inforec = (Pg_finfo_record*)palloc0(sizeof(Pg_finfo_record));
    errno_t rc;

    /* Compute name of info func */
    infofuncname = (char*)palloc(strlen(funcname) + 10);
    rc = strcpy_s(infofuncname, strlen(funcname) + 10, "pg_finfo_");
    securec_check(rc, "\0", "\0");

    rc = strcat_s(infofuncname, strlen(funcname) + 10, funcname);
    securec_check(rc, "\0", "\0");

    /* Try to look up the info function */
    infofunc = (PGFInfoFunction)lookup_external_function(filehandle, infofuncname);

    if (infofunc == NULL) {
        /* Not found --- assume version 0 */

        /*isValidate is TRUE only if calling comes from function fmgr_c_validator, and it indicates the SQL is DDL (such
         * as create) operation.*/
        /*In version 0, DDL will cause warning, while DML(such as select) will not.*/
        if (isValidate) {
            ereport(WARNING, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("Function \"%s\" is not declared as PG_FUNCTION_INFO_V1()", funcname),
                    errhint("SQL-callable C-functions recommends accompanying PG_FUNCTION_INFO_V1(%s).", funcname)));
        }
        pfree_ext(infofuncname);
        return default_inforec;
    }

    /* Found, so call it */
    inforec = (*infofunc)();

    /* Validate result as best we can */
    if (inforec == NULL) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null result from info function \"%s\"", infofuncname)));
    }

    switch (inforec->api_version) {
        case 1:
            /* OK, no additional fields to validate */
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized API version %d reported by info function \"%s\"",
                        inforec->api_version, infofuncname)));
            break;
    }

    pfree_ext(infofuncname);
    return inforec;
}

/* -------------------------------------------------------------------------
 *		Routines for caching lookup information for external C functions.
 *
 * The routines in dfmgr.c are relatively slow, so we try to avoid running
 * them more than once per external function per session.  We use a hash table
 * with the function OID as the lookup key.
 * -------------------------------------------------------------------------
 */

/*
 * lookup_C_func: try to find a C function in the hash table
 *
 * If an entry exists and is up to date, return it; else return NULL
 */
static CFuncHashTabEntry* lookup_C_func(HeapTuple procedureTuple)
{
    Oid fn_oid = HeapTupleGetOid(procedureTuple);
    CFuncHashTabEntry* entry = NULL;

    if (CFuncHash == NULL) {
        return NULL; /* no table yet */
    }

    entry = (CFuncHashTabEntry*)hash_search(CFuncHash, &fn_oid, HASH_FIND, NULL);
    if (entry == NULL) {
        return NULL; /* no such entry */
    }

    if (entry->fn_xmin == HeapTupleGetRawXmin(procedureTuple) &&
        ItemPointerEquals(&entry->fn_tid, &procedureTuple->t_self)) {
        return entry; /* OK */
    }

    return NULL; /* entry is out of date */
}

/*
 * record_C_func: enter (or update) info about a C function in the hash table
 */
static void record_C_func(HeapTuple procedureTuple, PGFunction user_fn, const Pg_finfo_record* inforec)
{
    Oid fn_oid = HeapTupleGetOid(procedureTuple);
    CFuncHashTabEntry* entry = NULL;
    bool found = false;

    /* Create the hash table if it doesn't exist yet */
    if (CFuncHash == NULL) {
        HASHCTL hash_ctl;

        errno_t rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "\0", "\0");
        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(CFuncHashTabEntry);
        hash_ctl.hash = oid_hash;

        /*
         * This hash table should be process visible.
         * Use process MemoryContext.
         */
        hash_ctl.hcxt = g_instance.instance_context;
        CFuncHash = hash_create("CFuncHash", 100, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
    }

    entry = (CFuncHashTabEntry*)hash_search(CFuncHash, &fn_oid, HASH_ENTER, &found);
    /* OID is already filled in */
    entry->fn_xmin = HeapTupleGetRawXmin(procedureTuple);
    entry->fn_tid = procedureTuple->t_self;
    entry->user_fn = user_fn;
    entry->inforec = inforec;
}

/*
 * clear_external_function_hash: remove entries for a library being closed
 *
 * Presently we just zap the entire hash table, but later it might be worth
 * the effort to remove only the entries associated with the given handle.
 */
void clear_external_function_hash(void* filehandle)
{
    if (CFuncHash != NULL) {
        hash_destroy(CFuncHash);
    }

    CFuncHash = NULL;
}

/*
 * Copy an FmgrInfo struct
 *
 * This is inherently somewhat bogus since we can't reliably duplicate
 * language-dependent subsidiary info.	We cheat by zeroing fn_extra,
 * instead, meaning that subsidiary info will have to be recomputed.
 */
void fmgr_info_copy(FmgrInfo* dstinfo, FmgrInfo* srcinfo, MemoryContext destcxt)
{
    if (dstinfo == NULL || srcinfo == NULL) {
        return;
    }

    errno_t rc = memcpy_s(dstinfo, sizeof(FmgrInfo), srcinfo, sizeof(FmgrInfo));
    securec_check(rc, "\0", "\0");

    dstinfo->fn_mcxt = destcxt;

    if (dstinfo->fn_addr == fmgr_oldstyle) {
        /* For oldstyle functions we must copy fn_extra */
        Oldstyle_fnextra* fnextra = NULL;

        fnextra = (Oldstyle_fnextra*)MemoryContextAlloc(destcxt, sizeof(Oldstyle_fnextra));
        rc = memcpy_s(fnextra, sizeof(Oldstyle_fnextra), srcinfo->fn_extra, sizeof(Oldstyle_fnextra));
        securec_check(rc, "\0", "\0");
        dstinfo->fn_extra = (void*)fnextra;
    } else {
        dstinfo->fn_extra = NULL;
    }
}

/*
 * Specialized lookup routine for fmgr_internal_validator: given the alleged
 * name of an internal function, return the OID of the function.
 * If the name is not recognized, return InvalidOid.
 */
Oid fmgr_internal_function(const char* proname)
{
    const FmgrBuiltin* fbp = fmgr_lookupByName(proname);

    if (fbp == NULL) {
        return InvalidOid;
    }

    return fbp->foid;
}

/*
 * Handler for old-style "C" language functions
 */
static Datum fmgr_oldstyle(PG_FUNCTION_ARGS)
{
    Oldstyle_fnextra* fnextra = NULL;
    int n_arguments = fcinfo->nargs;
    int i;
    bool isnull = false;
    func_ptr user_fn;
    char* returnValue = NULL;

    if (fcinfo->flinfo == NULL || fcinfo->flinfo->fn_extra == NULL) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("fmgr_oldstyle received NULL pointer")));
    }

    fnextra = (Oldstyle_fnextra*)fcinfo->flinfo->fn_extra;

    /*
     * Result is NULL if any argument is NULL, but we still call the function
     * (peculiar, but that's the way it worked before, and after all this is a
     * backwards-compatibility wrapper).  Note, however, that we'll never get
     * here with NULL arguments if the function is marked strict.
     *
     * We also need to detoast any TOAST-ed inputs, since it's unlikely that
     * an old-style function knows about TOASTing.
     */
    isnull = false;

    for (i = 0; i < n_arguments; i++) {
        if (PG_ARGISNULL(i)) {
            isnull = true;
        } else if (fnextra->arg_toastable[i]) {
            fcinfo->arg[i] = PointerGetDatum(PG_DETOAST_DATUM(fcinfo->arg[i]));
        }
    }

    fcinfo->isnull = isnull;

    user_fn = fnextra->func;

    switch (n_arguments) {
        case 0:
            returnValue = (char*)(*user_fn)();
            break;

        case 1:

            /*
             * nullvalue() used to use isNull to check if arg is NULL; perhaps
             * there are other functions still out there that also rely on
             * this undocumented hack?
             */
            {
                func_ptr_pp fn_p = (func_ptr_pp)user_fn;
                returnValue = (char*)(*fn_p)(fcinfo->arg[0], &fcinfo->isnull);
            }
            break;

        case 2: {
            func_ptr_p2 fn_p = (func_ptr_p2)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0], fcinfo->arg[1]);
        } break;

        case 3: {
            func_ptr_p3 fn_p = (func_ptr_p3)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0], fcinfo->arg[1], fcinfo->arg[2]);
        } break;

        case 4: {
            func_ptr_p4 fn_p = (func_ptr_p4)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0], fcinfo->arg[1], fcinfo->arg[2], fcinfo->arg[3]);
        } break;

        case 5: {
            func_ptr_p5 fn_p = (func_ptr_p5)user_fn;
            returnValue =
                (char*)(*fn_p)(fcinfo->arg[0], fcinfo->arg[1], fcinfo->arg[2], fcinfo->arg[3], fcinfo->arg[4]);
        } break;

        case 6: {
            func_ptr_p6 fn_p = (func_ptr_p6)user_fn;
            returnValue = (char*)(*fn_p)(
                fcinfo->arg[0], fcinfo->arg[1], fcinfo->arg[2], fcinfo->arg[3], fcinfo->arg[4], fcinfo->arg[5]);
        } break;

        case 7: {
            func_ptr_p7 fn_p = (func_ptr_p7)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6]);
        } break;

        case 8: {
            func_ptr_p8 fn_p = (func_ptr_p8)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7]);
        } break;

        case 9: {
            func_ptr_p9 fn_p = (func_ptr_p9)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8]);
        } break;

        case 10: {
            func_ptr_p10 fn_p = (func_ptr_p10)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9]);
        } break;

        case 11: {
            func_ptr_p11 fn_p = (func_ptr_p11)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9],
                fcinfo->arg[10]);
        } break;

        case 12: {
            func_ptr_p12 fn_p = (func_ptr_p12)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9],
                fcinfo->arg[10],
                fcinfo->arg[11]);
        } break;

        case 13: {
            func_ptr_p13 fn_p = (func_ptr_p13)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9],
                fcinfo->arg[10],
                fcinfo->arg[11],
                fcinfo->arg[12]);
        } break;

        case 14: {
            func_ptr_p14 fn_p = (func_ptr_p14)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9],
                fcinfo->arg[10],
                fcinfo->arg[11],
                fcinfo->arg[12],
                fcinfo->arg[13]);
        } break;

        case 15: {
            func_ptr_p15 fn_p = (func_ptr_p15)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9],
                fcinfo->arg[10],
                fcinfo->arg[11],
                fcinfo->arg[12],
                fcinfo->arg[13],
                fcinfo->arg[14]);
        } break;

        case 16: {
            func_ptr_p16 fn_p = (func_ptr_p16)user_fn;
            returnValue = (char*)(*fn_p)(fcinfo->arg[0],
                fcinfo->arg[1],
                fcinfo->arg[2],
                fcinfo->arg[3],
                fcinfo->arg[4],
                fcinfo->arg[5],
                fcinfo->arg[6],
                fcinfo->arg[7],
                fcinfo->arg[8],
                fcinfo->arg[9],
                fcinfo->arg[10],
                fcinfo->arg[11],
                fcinfo->arg[12],
                fcinfo->arg[13],
                fcinfo->arg[14],
                fcinfo->arg[15]);
        } break;

        default:

            /*
             * Increasing FUNC_MAX_ARGS doesn't automatically add cases to the
             * above code, so mention the actual value in this error not
             * FUNC_MAX_ARGS.  You could add cases to the above if you needed
             * to support old-style functions with many arguments, but making
             * 'em be new-style is probably a better idea.
             */
            ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                    errmsg("function %u has too many arguments (%d, maximum is %d)",
                        fcinfo->flinfo->fn_oid, n_arguments, 16)));
            returnValue = NULL; /* keep compiler quiet */
            break;
    }

    return PointerGetDatum(returnValue);
}

/*
 * Support for security-definer and proconfig-using functions.	We support
 * both of these features using the same call handler, because they are
 * often used together and it would be inefficient (as well as notationally
 * messy) to have two levels of call handler involved.
 */
struct fmgr_security_definer_cache {
    FmgrInfo flinfo;      /* lookup info for target function */
    Oid userid;           /* userid to set, or InvalidOid */
    ArrayType* proconfig; /* GUC values to set, or NULL */
    Datum arg;            /* passthrough argument for plugin modules */
};

/*
 * Function handler for security-definer/proconfig/plugin-hooked functions.
 * We extract the OID of the actual function and do a fmgr lookup again.
 * Then we fetch the pg_proc row and copy the owner ID and proconfig fields.
 * (All this info is cached for the duration of the current query.)
 * To execute a call, we temporarily replace the flinfo with the cached
 * and looked-up one, while keeping the outer fcinfo (which contains all
 * the actual arguments, etc.) intact.	This is not re-entrant, but then
 * the fcinfo itself can't be used re-entrantly anyway.
 */
static Datum fmgr_security_definer(PG_FUNCTION_ARGS)
{
    Datum result = (Datum)0;
    struct fmgr_security_definer_cache* volatile fcache = NULL;
    FmgrInfo* save_flinfo = NULL;
    Oid save_userid;
    int save_sec_context;
    volatile int save_nestlevel;
    PgStat_FunctionCallUsage fcusage;

    /* Does not allow commit in pre setting scenario */
    bool savedisAllowCommitRollback = false;

    if (!fcinfo->flinfo->fn_extra) {
        HeapTuple tuple;
        Form_pg_proc procedureStruct;
        Datum datum;
        bool isnull = false;
        MemoryContext oldcxt;

        fcache = (fmgr_security_definer_cache*)MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*fcache));

        fmgr_info_cxt_security(fcinfo->flinfo->fn_oid, &fcache->flinfo, fcinfo->flinfo->fn_mcxt, true);
        fcache->flinfo.fn_expr = fcinfo->flinfo->fn_expr;

        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for function %u", fcinfo->flinfo->fn_oid)));
        }

        procedureStruct = (Form_pg_proc)GETSTRUCT(tuple);
        if (procedureStruct->prosecdef) {
            fcache->userid = procedureStruct->proowner;
        }

        datum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proconfig, &isnull);
        if (!isnull) {
            oldcxt = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
            fcache->proconfig = DatumGetArrayTypePCopy(datum);
            MemoryContextSwitchTo(oldcxt);
        }

        ReleaseSysCache(tuple);

        fcinfo->flinfo->fn_extra = fcache;
    } else {
        fcache = (fmgr_security_definer_cache*)fcinfo->flinfo->fn_extra;
    }

    /* GetUserIdAndSecContext is cheap enough that no harm in a wasted call */
    GetUserIdAndSecContext(&save_userid, &save_sec_context);

    if (fcache->proconfig != NULL) { /* Need a new GUC nesting level */
        save_nestlevel = NewGUCNestLevel();
    } else {
        save_nestlevel = 0; /* keep compiler quiet */
    }

    if (OidIsValid(fcache->userid) && !u_sess->exec_cxt.is_exec_trigger_func) {
        SetUserIdAndSecContext(fcache->userid, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
    }

    if (fcache->proconfig != NULL) {
        ProcessGUCArray(fcache->proconfig, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, GUC_ACTION_SAVE);
    }

    /* function manager hook */
    if (fmgr_hook) {
        (*fmgr_hook)(FHET_START, &(fcache->flinfo), &(fcache->arg));
    }

    /*
     * We don't need to restore GUC or userid settings on error, because the
     * ensuing xact or subxact abort will do that.	The PG_TRY block is only
     * needed to clean up the flinfo link.
     */
    save_flinfo = fcinfo->flinfo;

    PG_TRY();
    {
        fcinfo->flinfo = &fcache->flinfo;

        /* See notes in fmgr_info_cxt_security */
        pgstat_init_function_usage(fcinfo, &fcusage);

        result = FunctionCallInvoke(fcinfo);

        /*
         * We could be calling either a regular or a set-returning function,
         * so we have to test to see what finalize flag to use.
         */
        pgstat_end_function_usage(&fcusage,
            (fcinfo->resultinfo == NULL || !IsA(fcinfo->resultinfo, ReturnSetInfo) ||
                ((ReturnSetInfo*)fcinfo->resultinfo)->isDone != ExprMultipleResult));
    }
    PG_CATCH();
    {
        fcinfo->flinfo = save_flinfo;

        if (fmgr_hook) {
            (*fmgr_hook)(FHET_ABORT, &(fcache->flinfo), &(fcache->arg));
        }

        /* restore is_allow_commit_rollback */
        stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->flinfo = save_flinfo;

    if (fcache->proconfig != NULL) {
        AtEOXact_GUC(true, save_nestlevel);
    }

    if (OidIsValid(fcache->userid) && !u_sess->exec_cxt.is_exec_trigger_func) {
        SetUserIdAndSecContext(save_userid, save_sec_context);
    }

    if (fmgr_hook) {
        (*fmgr_hook)(FHET_END, &(fcache->flinfo), &(fcache->arg));
    }

    /* restore is_allow_commit_rollback */
    return result;
}

/* -------------------------------------------------------------------------
 *		Support routines for callers of fmgr-compatible functions
 * -------------------------------------------------------------------------
 */

/*
 * These are for invocation of a specifically named function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.	Also, the function cannot be one that needs to
 * look at FmgrInfo, since there won't be any.
 */
Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 1, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 2, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 3, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall4Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 4, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall5Coll(
    PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 5, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall6Coll(
    PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 6, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall7Coll(
    PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6, Datum arg7)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 7, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall8Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 8, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.arg[7] = arg8;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;
    fcinfo.argnull[7] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

Datum DirectFunctionCall9Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8, Datum arg9)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 9, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.arg[7] = arg8;
    fcinfo.arg[8] = arg9;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;
    fcinfo.argnull[7] = false;
    fcinfo.argnull[8] = false;

    result = (*func)(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function returned NULL")));
    }

    return result;
}

/*
 * These are for invocation of a previously-looked-up function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.
 */
Datum FunctionCall1Coll(FmgrInfo* flinfo, Oid collation, Datum arg1)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 1, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }
    return result;
}

Datum FunctionCall2Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2)
{
    /*
     * XXX if you change this routine, see also the inlined version in
     * utils/sort/tuplesort.c!
     */
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 2, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall3Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 3, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall4Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 4, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall5Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 5, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall6Coll(
    FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 6, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall7Coll(
    FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6, Datum arg7)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 7, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall8Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5,
    Datum arg6, Datum arg7, Datum arg8)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 8, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.arg[7] = arg8;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;
    fcinfo.argnull[7] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

Datum FunctionCall9Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5,
    Datum arg6, Datum arg7, Datum arg8, Datum arg9)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 9, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.arg[7] = arg8;
    fcinfo.arg[8] = arg9;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;
    fcinfo.argnull[7] = false;
    fcinfo.argnull[8] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    return result;
}

/*
 * These are for invocation of a function identified by OID with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.	These are essentially fmgr_info() followed
 * by FunctionCallN().	If the same function is to be invoked repeatedly,
 * do the fmgr_info() once and then use FunctionCallN().
 */
Datum OidFunctionCall0Coll(Oid functionId, Oid collation)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 0, collation, NULL, NULL);

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall1Coll(Oid functionId, Oid collation, Datum arg1)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 1, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 2, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 3, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall4Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 4, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall5Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 5, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall6Coll(
    Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 6, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall7Coll(
    Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6, Datum arg7)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 7, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall8Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5,
    Datum arg6, Datum arg7, Datum arg8)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 8, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.arg[7] = arg8;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;
    fcinfo.argnull[7] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

Datum OidFunctionCall9Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5,
    Datum arg6, Datum arg7, Datum arg8, Datum arg9)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    fmgr_info(functionId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 9, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.arg[3] = arg4;
    fcinfo.arg[4] = arg5;
    fcinfo.arg[5] = arg6;
    fcinfo.arg[6] = arg7;
    fcinfo.arg[7] = arg8;
    fcinfo.arg[8] = arg9;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;
    fcinfo.argnull[4] = false;
    fcinfo.argnull[5] = false;
    fcinfo.argnull[6] = false;
    fcinfo.argnull[7] = false;
    fcinfo.argnull[8] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return result;
}

void CheckNullResult(Oid oid, bool isnull, char* str)
{
    if (str == NULL) {
        if (!isnull) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("input function %u returned non-NULL", oid)));
        }
    } else {
        if (isnull) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("input function %u returned NULL", oid)));
        }
    }
}

/*
 * Special cases for convenient invocation of datatype I/O functions.
 */

/*
 * Call a previously-looked-up datatype input function.
 *
 * "str" may be NULL to indicate we are reading a NULL.  In this case
 * the caller should assume the result is NULL, but we'll call the input
 * function anyway if it's not strict.  So this is almost but not quite
 * the same as FunctionCall3.
 *
 * One important difference from the bare function call is that we will
 * push any active SPI context, allowing SPI-using I/O functions to be
 * called from other SPI functions without extra notation.	This is a hack,
 * but the alternative of expecting all SPI functions to do SPI_push/SPI_pop
 * around I/O calls seems worse.
 */
Datum InputFunctionCall(FmgrInfo* flinfo, char* str, Oid typioparam, int32 typmod)
{
    FunctionCallInfoData fcinfo;
    Datum result;
    bool pushed = false;

    if (str == NULL && flinfo->fn_strict) {
        return (Datum)0; /* just return null result */
    }

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    InitFunctionCallInfoData(fcinfo, flinfo, 3, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = CStringGetDatum(str);
    fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
    fcinfo.arg[2] = Int32GetDatum(typmod);
    fcinfo.argnull[0] = (str == NULL);
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Should get null result if and only if str is NULL */
    CheckNullResult(fcinfo.flinfo->fn_oid, fcinfo.isnull, str);

    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return result;
}

/*
 * @Description: input function call for date type
 * @IN/OUT flinfo: function call info
 * @IN/OUT str: input string
 * @IN/OUT typioparam: type oid
 * @IN/OUT typmod: type mode
 * @IN/OUT date_time_fmt: date time format
 * @Return: data type datum
 * @See also:
 */
Datum InputFunctionCallForDateType(FmgrInfo* flinfo, char* str, Oid typioparam, int32 typmod, char* date_time_fmt)
{
    FunctionCallInfoData fcinfo;
    Datum result;
    bool pushed = false;

    if (str == NULL && flinfo->fn_strict) {
        return (Datum)0; /* just return null result */
    }

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    InitFunctionCallInfoData(fcinfo, flinfo, 4, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = CStringGetDatum(str);
    fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
    fcinfo.arg[2] = Int32GetDatum(typmod);
    fcinfo.arg[3] = CStringGetDatum(date_time_fmt);

    fcinfo.argnull[0] = (str == NULL);
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;
    fcinfo.argnull[3] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Should get null result if and only if str is NULL */
    CheckNullResult(fcinfo.flinfo->fn_oid, fcinfo.isnull, str);
    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return result;
}

/*
 * Call a previously-looked-up datatype output function.
 *
 * Do not call this on NULL datums.
 *
 * This is almost just window dressing for FunctionCall1, but it includes
 * SPI context pushing for the same reasons as InputFunctionCall.
 */
char* OutputFunctionCall(FmgrInfo* flinfo, Datum val)
{
    char* result = NULL;
    bool pushed = false;

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    result = DatumGetCString(FunctionCall1(flinfo, val));

    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return result;
}

/*
 * Call a previously-looked-up datatype binary-input function.
 *
 * "buf" may be NULL to indicate we are reading a NULL.  In this case
 * the caller should assume the result is NULL, but we'll call the receive
 * function anyway if it's not strict.  So this is almost but not quite
 * the same as FunctionCall3.  Also, this includes SPI context pushing for
 * the same reasons as InputFunctionCall.
 */
Datum ReceiveFunctionCall(FmgrInfo* flinfo, StringInfo buf, Oid typioparam, int32 typmod)
{
    FunctionCallInfoData fcinfo;
    Datum result;
    bool pushed = false;

    if (buf == NULL && flinfo->fn_strict) {
        return (Datum)0; /* just return null result */
    }

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    InitFunctionCallInfoData(fcinfo, flinfo, 3, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = PointerGetDatum(buf);
    fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
    fcinfo.arg[2] = Int32GetDatum(typmod);
    fcinfo.argnull[0] = (buf == NULL);
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Should get null result if and only if buf is NULL */
    if (buf == NULL) {
        if (!fcinfo.isnull) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("receive function %u returned non-NULL", fcinfo.flinfo->fn_oid)));
        }
    } else {
        if (fcinfo.isnull) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("receive function %u returned NULL", fcinfo.flinfo->fn_oid)));
        }
    }

    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return result;
}

/*
 * Call a previously-looked-up datatype binary-output function.
 *
 * Do not call this on NULL datums.
 *
 * This is little more than window dressing for FunctionCall1, but it does
 * guarantee a non-toasted result, which strictly speaking the underlying
 * function doesn't.  Also, this includes SPI context pushing for the same
 * reasons as InputFunctionCall.
 */
bytea* SendFunctionCall(FmgrInfo* flinfo, Datum val)
{
    bytea* result = NULL;
    bool pushed = false;

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    result = DatumGetByteaP(FunctionCall1(flinfo, val));

    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return result;
}

/*
 * As above, for I/O functions identified by OID.  These are only to be used
 * in seldom-executed code paths.  They are not only slow but leak memory.
 */
Datum OidInputFunctionCall(Oid functionId, char* str, Oid typioparam, int32 typmod)
{
    FmgrInfo flinfo;

    fmgr_info(functionId, &flinfo);
    return InputFunctionCall(&flinfo, str, typioparam, typmod);
}

char* OidOutputFunctionCall(Oid functionId, Datum val)
{
    FmgrInfo flinfo;

    fmgr_info(functionId, &flinfo);
    return OutputFunctionCall(&flinfo, val);
}

Datum OidReceiveFunctionCall(Oid functionId, StringInfo buf, Oid typioparam, int32 typmod)
{
    FmgrInfo flinfo;

    fmgr_info(functionId, &flinfo);
    return ReceiveFunctionCall(&flinfo, buf, typioparam, typmod);
}

bytea* OidSendFunctionCall(Oid functionId, Datum val)
{
    FmgrInfo flinfo;

    fmgr_info(functionId, &flinfo);
    return SendFunctionCall(&flinfo, val);
}

/*
 * !!! OLD INTERFACE !!!
 *
 * fmgr() is the only remaining vestige of the old-style caller support
 * functions.  It's no longer used anywhere in the openGauss distribution,
 * but we should leave it around for a release or two to ease the transition
 * for user-supplied C functions.  OidFunctionCallN() replaces it for new
 * code.
 *
 * DEPRECATED, DO NOT USE IN NEW CODE
 */
char* fmgr(Oid procedureId, ...)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    int n_arguments;
    Datum result;

    fmgr_info(procedureId, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, flinfo.fn_nargs, InvalidOid, NULL, NULL);
    n_arguments = fcinfo.nargs;

    if (n_arguments > 0) {
        va_list pvar;
        int i;

        if (n_arguments > FUNC_MAX_ARGS) {
            /* free args memory as soon as possible */
            FreeFunctionCallInfoData(fcinfo);
            ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                    errmsg("function %u has too many arguments (%d, maximum is %d)",
                        flinfo.fn_oid, n_arguments, FUNC_MAX_ARGS)));
        }
        va_start(pvar, procedureId);

        for (i = 0; i < n_arguments; i++)
            fcinfo.arg[i] = PointerGetDatum(va_arg(pvar, char*));

        va_end(pvar);
    }

    result = FunctionCallInvoke(&fcinfo);

    /* free args memory as soon as possible */
    FreeFunctionCallInfoData(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", flinfo.fn_oid)));
    }

    return DatumGetPointer(result);
}

/* -------------------------------------------------------------------------
 *		Support routines for standard maybe-pass-by-reference datatypes
 *
 * int8, float4, and float8 can be passed by value if Datum is wide enough.
 * (For backwards-compatibility reasons, we allow pass-by-ref to be chosen
 * at compile time even if pass-by-val is possible.)  For the float types,
 * we need a support routine even if we are passing by value, because many
 * machines pass int and float function parameters/results differently;
 * so we need to play weird games with unions.
 *
 * Note: there is only one switch controlling the pass-by-value option for
 * both int8 and float8; this is to avoid making things unduly complicated
 * for the timestamp types, which might have either representation.
 * -------------------------------------------------------------------------
 */
#ifndef USE_FLOAT8_BYVAL /* controls int8 too */

Datum Int64GetDatum(int64 X)
{
    int64* retval = (int64*)palloc(sizeof(int64));

    *retval = X;
    return PointerGetDatum(retval);
}
#endif /* USE_FLOAT8_BYVAL */

Datum Int128GetDatum(int128 X)
{
    int128* retval = (int128*)palloc(sizeof(int128));
    *retval = X;
    return PointerGetDatum(retval);
}

Datum Float4GetDatum(float4 X)
{
#ifdef USE_FLOAT4_BYVAL
    union {
        float4 value;
        int32 retval;
    } myunion;

    myunion.value = X;
    return SET_4_BYTES(myunion.retval);
#else
    float4* retval = (float4*)palloc(sizeof(float4));

    *retval = X;
    return PointerGetDatum(retval);
#endif
}

#ifdef USE_FLOAT4_BYVAL

float4 DatumGetFloat4(Datum X)
{
    union {
        int32 value;
        float4 retval;
    } myunion;

    myunion.value = GET_4_BYTES(X);
    return myunion.retval;
}
#endif /* USE_FLOAT4_BYVAL */

Datum Float8GetDatum(float8 X)
{
#ifdef USE_FLOAT8_BYVAL
    union {
        float8 value;
        int64 retval;
    } myunion;

    myunion.value = X;
    return SET_8_BYTES(myunion.retval);
#else
    float8* retval = (float8*)palloc(sizeof(float8));

    *retval = X;
    return PointerGetDatum(retval);
#endif
}

#ifdef USE_FLOAT8_BYVAL

float8 DatumGetFloat8(Datum X)
{
    union {
        int64 value;
        float8 retval;
    } myunion;

    myunion.value = GET_8_BYTES(X);
    return myunion.retval;
}
#endif /* USE_FLOAT8_BYVAL */

/* -------------------------------------------------------------------------
 *		Support routines for toastable datatypes
 * -------------------------------------------------------------------------
 */
struct varlena* pg_detoast_datum(struct varlena* datum)
{
    if (unlikely(datum == NULL)) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("NULL input for detoast datum")));
    }
	
    if (VARATT_IS_EXTENDED(datum)) {
        return heap_tuple_untoast_attr(datum);
    } else {
        return datum;
    }
}

struct varlena* pg_detoast_datum_copy(struct varlena* datum)
{
    if (VARATT_IS_EXTENDED(datum)) {
        return heap_tuple_untoast_attr(datum);
    } else {
        /* Make a modifiable copy of the varlena object */
        Size len = VARSIZE(datum);
        struct varlena* result = (struct varlena*)palloc(len);

        errno_t rc = memcpy_s(result, len, datum, len);
        securec_check(rc, "\0", "\0");

        return result;
    }
}

struct varlena* pg_detoast_datum_slice(struct varlena* datum, int64 first, int32 count)
{
    /* Only get the specified portion from the toast rel */
    return heap_tuple_untoast_attr_slice(datum, first, count);
}

struct varlena* pg_detoast_datum_packed(struct varlena* datum)
{
	if (unlikely(datum == NULL)) {
		ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("NULL input for detoast datum packed")));
	}

    if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum)) {
        return heap_tuple_untoast_attr(datum);
    } else {
        return datum;
    }
}

/* -------------------------------------------------------------------------
 *		Support routines for extracting info from fn_expr parse tree
 *
 * These are needed by polymorphic functions, which accept multiple possible
 * input types and need help from the parser to know what they've got.
 * Also, some functions might be interested in whether a parameter is constant.
 * -------------------------------------------------------------------------
 */

/*
 * Get the actual type OID of the function return type
 *
 * Returns InvalidOid if information is not available
 */
Oid get_fn_expr_rettype(FmgrInfo* flinfo)
{
    Node* expr = NULL;

    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if (flinfo == NULL || !flinfo->fn_expr) {
        return InvalidOid;
    }

    expr = flinfo->fn_expr;

    return exprType(expr);
}

/*
 * Get the actual type OID of a specific function argument (counting from 0)
 *
 * Returns InvalidOid if information is not available
 */
Oid get_fn_expr_argtype(FmgrInfo* flinfo, int argnum)
{
    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if (flinfo == NULL || !flinfo->fn_expr) {
        return InvalidOid;
    }

    return get_call_expr_argtype(flinfo->fn_expr, argnum);
}

bool setArg(List** args, Node* expr)
{
    if (IsA(expr, FuncExpr)) {
        *args = ((FuncExpr*)expr)->args;
    } else if (IsA(expr, OpExpr)) {
        *args = ((OpExpr*)expr)->args;
    } else if (IsA(expr, DistinctExpr)) {
        *args = ((DistinctExpr*)expr)->args;
    } else if (IsA(expr, ScalarArrayOpExpr)) {
        *args = ((ScalarArrayOpExpr*)expr)->args;
    } else if (IsA(expr, ArrayCoerceExpr)) {
        *args = list_make1(((ArrayCoerceExpr*)expr)->arg);
    } else if (IsA(expr, NullIfExpr)) {
        *args = ((NullIfExpr*)expr)->args;
    } else if (IsA(expr, WindowFunc)) {
        *args = ((WindowFunc*)expr)->args;
    } else {
        return false;
    }
    return true;
}
/*
 * Get the actual type OID of a specific function argument (counting from 0),
 * but working from the calling expression tree instead of FmgrInfo
 *
 * Returns InvalidOid if information is not available
 */
Oid get_call_expr_argtype(Node* expr, int argnum)
{
    List* args = NIL;
    Oid argtype;

    if (expr == NULL) {
        return InvalidOid;
    }

    if (!setArg(&args, expr)) {
        return InvalidOid;
    }

    if (argnum < 0 || argnum >= list_length(args)) {
        return InvalidOid;
    }

    argtype = exprType((Node*)list_nth(args, argnum));

    /*
     * special hack for ScalarArrayOpExpr and ArrayCoerceExpr: what the
     * underlying function will actually get passed is the element type of the
     * array.
     */
    if (IsA(expr, ScalarArrayOpExpr) && argnum == 1) {
        argtype = get_base_element_type(argtype);
    } else if (IsA(expr, ArrayCoerceExpr) && argnum == 0) {
        argtype = get_base_element_type(argtype);
    }

    return argtype;
}

/*
 * Find out whether a specific function argument is constant for the
 * duration of a query
 *
 * Returns false if information is not available
 */
bool get_fn_expr_arg_stable(FmgrInfo* flinfo, int argnum)
{
    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if (flinfo == NULL || !flinfo->fn_expr) {
        return false;
    }

    return get_call_expr_arg_stable(flinfo->fn_expr, argnum);
}

/*
 * Find out whether a specific function argument is constant for the
 * duration of a query, but working from the calling expression tree
 *
 * Returns false if information is not available
 */
bool get_call_expr_arg_stable(Node* expr, int argnum)
{
    List* args = NIL;
    Node* arg = NULL;

    if (expr == NULL) {
        return false;
    }

    if (!setArg(&args, expr)) {
        return false;
    }

    if (argnum < 0 || argnum >= list_length(args)) {
        return false;
    }

    arg = (Node*)list_nth(args, argnum);
    /*
     * Either a true Const or an external Param will have a value that doesn't
     * change during the execution of the query.  In future we might want to
     * consider other cases too, e.g. now().
     */
    if (IsA(arg, Const)) {
        return true;
    }

    if (IsA(arg, Param) && ((Param*)arg)->paramkind == PARAM_EXTERN) {
        return true;
    }

    return false;
}

/*
 * Get the VARIADIC flag from the function invocation
 *
 * Returns false (the default assumption) if information is not available
 *
 * Note this is generally only of interest to VARIADIC ANY functions
 */
bool get_fn_expr_variadic(FmgrInfo* flinfo)
{
    Node* expr = NULL;

    /*
     * can't return anything useful if we have no FmgrInfo or if its fn_expr
     * node has not been initialized
     */
    if (flinfo == NULL || flinfo->fn_expr == NULL) {
        return false;
    }

    expr = flinfo->fn_expr;

    if (IsA(expr, FuncExpr)) {
        return ((FuncExpr*)expr)->funcvariadic;
    } else {
        return false;
    }
}

/* -------------------------------------------------------------------------
 *     Support routines for procedural language implementations
 * -------------------------------------------------------------------------
 */

/*
 * Verify that a validator is actually associated with the language of a
 * particular function and that the user has access to both the language and
 * the function.  All validators should call this before doing anything
 * substantial.  Doing so ensures a user cannot achieve anything with explicit
 * calls to validators that he could not achieve with CREATE FUNCTION or by
 * simply calling an existing function.
 *
 * When this function returns false, callers should skip all validation work
 * and call PG_RETURN_VOID().  This never happens at present; it is reserved
 * for future expansion.
 *
 * In particular, checking that the validator corresponds to the function's
 * language allows untrusted language validators to assume they process only
 * superuser-chosen source code.  (Untrusted language call handlers, by
 * definition, do assume that.)  A user lacking the USAGE language privilege
 * would be unable to reach the validator through CREATE FUNCTION, so we check
 * that to block explicit calls as well.  Checking the EXECUTE privilege on
 * the function is often superfluous, because most users can clone the
 * function to get an executable copy.  It is meaningful against users with no
 * database TEMP right and no permanent schema CREATE right, thereby unable to
 * create any function.  Also, if the function tracks persistent state by
 * function OID or name, validating the original function might permit more
 * mischief than creating and validating a clone thereof.
 */
bool CheckFunctionValidatorAccess(Oid validatorOid, Oid functionOid)
{
    HeapTuple procTup;
    HeapTuple langTup;
    Form_pg_proc procStruct;
    Form_pg_language langStruct;
    AclResult aclresult;

    /* Get the function's pg_proc entry */
    procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
    if (!HeapTupleIsValid(procTup)) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u", functionOid)));
    }

    procStruct = (Form_pg_proc)GETSTRUCT(procTup);

    /*
     * Fetch pg_language entry to know if this is the correct validation
     * function for that pg_proc entry.
     */
    langTup = SearchSysCache1(LANGOID, ObjectIdGetDatum(procStruct->prolang));
    if (!HeapTupleIsValid(langTup)) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for language %u", procStruct->prolang)));
    }

    langStruct = (Form_pg_language)GETSTRUCT(langTup);
    if (langStruct->lanvalidator != validatorOid) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("language validation function %u called for language %u instead of %u",
                    validatorOid, procStruct->prolang, langStruct->lanvalidator)));
    }

    /* first validate that we have permissions to use the language */
    aclresult = pg_language_aclcheck(procStruct->prolang, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, ACL_KIND_LANGUAGE, NameStr(langStruct->lanname));
    }

    /*
     * Check whether we are allowed to execute the function itself. If we can
     * execute it, there should be no possible side-effect of
     * compiling/validation that execution can't have.
     */
    aclresult = pg_proc_aclcheck(functionOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, ACL_KIND_PROC, NameStr(procStruct->proname));
    }

    ReleaseSysCache(procTup);
    ReleaseSysCache(langTup);

    return true;
}

// Support routines for callers of fmgr-compatible functions
//
// These are for invocation of a specifically named function with a
// directly-computed parameter list.  Note that neither arguments nor result
// are allowed to be NULL.	Also, the function cannot be one that needs to
// look at FmgrInfo, since there won't be any.
Datum DirectCall0(bool* isRetNull, PGFunction func, Oid collation)
{
    FunctionCallInfoData fcinfo;
    Datum result = 0;

    if (*isRetNull) {
        PG_RETURN_VOID();
    }

    InitFunctionCallInfoData(fcinfo, NULL, 0, collation, NULL, NULL);

    result = (*func)(&fcinfo);

    if (fcinfo.isnull) {
        *isRetNull = true;
    } else {
        *isRetNull = false;
    }

    return result;
}

Datum DirectCall1(bool* isRetNull, PGFunction func, Oid collation, Datum arg1)
{
    FunctionCallInfoData fcinfo;
    Datum result = 0;

    if (*isRetNull) {
        PG_RETURN_VOID();
    }

    InitFunctionCallInfoData(fcinfo, NULL, 1, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = (*func)(&fcinfo);

    if (fcinfo.isnull) {
        *isRetNull = true;
    } else {
        *isRetNull = false;
    }

    return result;
}

Datum DirectCall2(bool* isRetNull, PGFunction func, Oid collation, Datum arg1, Datum arg2)
{
    FunctionCallInfoData fcinfo;
    Datum result = 0;

    if (*isRetNull) {
        PG_RETURN_VOID();
    }

    InitFunctionCallInfoData(fcinfo, NULL, 2, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = (*func)(&fcinfo);

    if (fcinfo.isnull) {
        *isRetNull = true;
    } else {
        *isRetNull = false;
    }

    return result;
}

Datum DirectCall3(bool* isRetNull, PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3)
{
    FunctionCallInfoData fcinfo;
    Datum result = 0;

    if (*isRetNull) {
        PG_RETURN_VOID();
    }

    InitFunctionCallInfoData(fcinfo, NULL, 3, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.arg[2] = arg3;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    result = (*func)(&fcinfo);

    if (fcinfo.isnull) {
        *isRetNull = true;
    } else {
        *isRetNull = false;
    }

    return result;
}

void InitVecFunctionCallInfoData(
    FunctionCallInfoData* Fcinfo, FmgrInfo* Flinfo, int Nargs, Oid Collation, fmNodePtr Context, fmNodePtr Resultinfo)
{
    Fcinfo->flinfo = Flinfo;
    Fcinfo->context = Context;
    Fcinfo->resultinfo = Resultinfo;
    Fcinfo->fncollation = Collation;
    Fcinfo->isnull = false;
    Fcinfo->nargs = Nargs;
    Fcinfo->udfInfo.valid_UDFArgsHandlerPtr = false;
    Fcinfo->udfInfo.UDFResultHandlerPtr = NULL;
    Fcinfo->udfInfo.udfMsgBuf = makeStringInfo();
    if ((Nargs + EXTRA_NARGS) > FUNC_PREALLOCED_ARGS) {
        Fcinfo->arg = (Datum*)palloc0((Nargs + EXTRA_NARGS) * sizeof(Datum));
        Fcinfo->argnull = (bool*)palloc0((Nargs + EXTRA_NARGS) * sizeof(bool));
        Fcinfo->argTypes = (Oid*)palloc0((Nargs) * sizeof(Oid));
    } else {
        Fcinfo->arg = Fcinfo->prealloc_arg;
        Fcinfo->argnull = Fcinfo->prealloc_argnull;
        Fcinfo->argTypes = Fcinfo->prealloc_argTypes;
    }
    InitFuncCallUDFInfo(Fcinfo, Nargs, true);
}

void FreeFuncCallUDFInfo(FunctionCallInfoData* Fcinfo)
{
    if (Fcinfo->flinfo && Fcinfo->flinfo->fn_fenced) {
        for (int i = 0; i < BatchMaxSize; ++i) {
            pfree_ext(Fcinfo->udfInfo.UDFArgsHandlerPtr);
            Fcinfo->udfInfo.UDFArgsHandlerPtr = NULL;

            if (Fcinfo->udfInfo.udfMsgBuf->len > 0) {
                pfree_ext(Fcinfo->udfInfo.udfMsgBuf->data);
                resetStringInfo(Fcinfo->udfInfo.udfMsgBuf);
            }
            pfree_ext(Fcinfo->udfInfo.arg[i]);
            pfree_ext(Fcinfo->udfInfo.null[i]);
        }
        pfree_ext(Fcinfo->udfInfo.arg);
        pfree_ext(Fcinfo->udfInfo.null);
        pfree_ext(Fcinfo->udfInfo.result);
        pfree_ext(Fcinfo->udfInfo.resultIsNull);

        Fcinfo->udfInfo.arg = NULL;
        Fcinfo->udfInfo.null = NULL;
        Fcinfo->udfInfo.result = NULL;
        Fcinfo->udfInfo.resultIsNull = NULL;
        Fcinfo->udfInfo.valid_UDFArgsHandlerPtr = false;
        Fcinfo->flinfo->fn_fenced = false;
    }
}

/*
 * @Description: copy cursor data from Source_cursor to target_cursor
 * @in target_cursor - target cursor data
 * @in Source_cursor - source cursor data
 * @return -void
 */
void CopyCursorInfoData(Cursor_Data* target_cursor, Cursor_Data* Source_cursor)
{
    target_cursor->is_open = Source_cursor->is_open;
    target_cursor->found = Source_cursor->found;
    target_cursor->not_found = Source_cursor->not_found;
    target_cursor->row_count = Source_cursor->row_count;
    target_cursor->null_open = Source_cursor->null_open;
    target_cursor->null_fetch = Source_cursor->null_fetch;
    target_cursor->cur_dno = Source_cursor->cur_dno;
}
