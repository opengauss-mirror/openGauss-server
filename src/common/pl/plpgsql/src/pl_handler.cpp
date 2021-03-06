/* -------------------------------------------------------------------------
 *
 * pl_handler.c		- Handler for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_handler.c
 *
 * -------------------------------------------------------------------------
 */

#include "plpgsql.h"

#include "auditfuncs.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "pgstat.h"
#include "utils/timestamp.h"
#include "executor/spi_priv.h"

#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#endif

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Custom GUC variable */
static const struct config_enum_entry variable_conflict_options[] = {{"error", PLPGSQL_RESOLVE_ERROR, false},
    {"use_variable", PLPGSQL_RESOLVE_VARIABLE, false},
    {"use_column", PLPGSQL_RESOLVE_COLUMN, false},
    {NULL, 0, false}};

static void auditExecPLpgSQLFunction(PLpgSQL_function* func, AuditResult result)
{
    char details[PGAUDIT_MAXLENGTH];
    errno_t rcs = EOK;

    if (unlikely(func == NULL)) {
        ereport(ERROR, 
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function should not be null")));
    }

    rcs = snprintf_s(
        details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1, "Execute PLpgSQL function(oid = %u). ", func->fn_oid);
    securec_check_ss(rcs, "\0", "\0");

    audit_report(AUDIT_FUNCTION_EXEC, result, func->fn_signature, details);
}

/*
 * @Description: Exec anonymous block.
 * @in func: Plpg function.
 */
static void auditExecAnonymousBlock(const PLpgSQL_function* func)
{
    char details[PGAUDIT_MAXLENGTH];
    errno_t rc = EOK;

    if (unlikely(func == NULL)) {
        ereport(ERROR, 
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("function should not be null")));
    }

    rc = snprintf_s(details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1,
                    "Execute PLpgSQL anonymous code block(oid = %u). ", func->fn_oid);
    securec_check_ss(rc, "", "");

    audit_report(AUDIT_FUNCTION_EXEC, AUDIT_OK, func->fn_signature, details);
}

/*
 * _PG_init()			- library load-time initialization
 *
 * DO NOT make this static nor change its name!
 */
void _PG_init(void)
{
    /* Be sure we do initialization only once (should be redundant now) */
    if (u_sess->plsql_cxt.inited == false) {
        plpgsql_HashTableInit();
        SPICacheTableInit();
        PG_TRY();
        {
            pg_bindtextdomain(TEXTDOMAIN);

            DefineCustomEnumVariable("plpgsql.variable_conflict",
                gettext_noop("Sets handling of conflicts between PL/pgSQL variable names and table column names."),
                NULL,
                &u_sess->plsql_cxt.plpgsql_variable_conflict,
                PLPGSQL_RESOLVE_ERROR,
                variable_conflict_options,
                PGC_SUSET,
                0,
                NULL,
                NULL,
                NULL);

            EmitWarningsOnPlaceholders("plpgsql");
            RegisterXactCallback(plpgsql_xact_cb, NULL);
            RegisterSubXactCallback(plpgsql_subxact_cb, NULL);
            /* Set up a rendezvous point with optional instrumentation plugin */
            u_sess->plsql_cxt.plugin_ptr = (PLpgSQL_plugin**)find_rendezvous_variable("PLpgSQL_plugin");
        }
        PG_CATCH();
        {
            hash_destroy(u_sess->plsql_cxt.plpgsql_HashTable);
            u_sess->plsql_cxt.plpgsql_HashTable = NULL;
            UnregisterXactCallback(plpgsql_xact_cb, NULL);
            UnregisterSubXactCallback(plpgsql_subxact_cb, NULL);
            PG_RE_THROW();
        }
        PG_END_TRY();
        u_sess->plsql_cxt.inited = true;
    }
}

static void validate_search_path(PLpgSQL_function* func)
{
    ListCell *cell = NULL;
    List* oidlist = NIL;
    foreach (cell, func->fn_searchpath->schemas) {
        Oid namespaceId = lfirst_oid(cell);
        HeapTuple tuple = NULL;

        tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(namespaceId));
        if (!HeapTupleIsValid(tuple)) {
            oidlist = lcons_oid(namespaceId, oidlist);
        } else {
            ReleaseSysCache(tuple);
        }

    }
    foreach (cell, oidlist) {
        func->fn_searchpath->schemas = list_delete_oid(func->fn_searchpath->schemas, lfirst_oid(cell));
    }
}

/* ----------
 * plpgsql_call_handler
 *
 * The PostgreSQL function manager and trigger manager
 * call this function for execution of PL/pgSQL procedures.
 * ----------
 */
PG_FUNCTION_INFO_V1(plpgsql_call_handler);

Datum plpgsql_call_handler(PG_FUNCTION_ARGS)
{
    bool nonatomic;
    PLpgSQL_function* func = NULL;
    PLpgSQL_execstate* save_cur_estate = NULL;
    Datum retval;
    int rc;
    Oid func_oid = fcinfo->flinfo->fn_oid;
    Oid* saved_Pseudo_CurrentUserId = NULL;
    // PGSTAT_INIT_PLSQL_TIME_RECORD
    int64 startTime = 0;
    bool needRecord = false;
    bool saved_current_stp_with_exception = false;
    /* 
     * if the atomic stored in fcinfo is false means allow 
     * commit/rollback within stored procedure. 
     * set the nonatomic and will be reused within function.
     */
    nonatomic = fcinfo->context && 
                IsA(fcinfo->context, FunctionScanState) &&
                !castNode(FunctionScanState, fcinfo->context)->atomic;

    bool outer_is_stream = false;
    bool outer_is_stream_support = false;
    int fun_arg = fcinfo->nargs;
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        outer_is_stream = u_sess->opt_cxt.is_stream;
        outer_is_stream_support = u_sess->opt_cxt.is_stream_support;

        u_sess->opt_cxt.is_stream = true;
        u_sess->opt_cxt.is_stream_support = true;
    }
#else
    outer_is_stream = u_sess->opt_cxt.is_stream;
    outer_is_stream_support = u_sess->opt_cxt.is_stream_support;

    u_sess->opt_cxt.is_stream = false;
    u_sess->opt_cxt.is_stream_support = false;
#endif

    _PG_init();
    /*
     * Connect to SPI manager
     */
    if ((rc =  SPI_connect_ext(DestSPI, NULL, NULL, nonatomic ? SPI_OPT_NONATOMIC : 0, func_oid)) != SPI_OK_CONNECT) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("SPI_connect failed: %s when execute PLSQL function.", SPI_result_code_string(rc))));
    }

    PGSTAT_START_PLSQL_TIME_RECORD();
    saved_current_stp_with_exception = plpgsql_get_current_value_stp_with_exception();
    /* Find or compile the function */
    func = plpgsql_compile(fcinfo, false);

    PGSTAT_END_PLSQL_TIME_RECORD(PL_COMPILATION_TIME);

    int cursor_step = 0;
    /* copy cursor option on in-parameter to function body's cursor */
    for (int i = 0; i < fun_arg; i++) {
        int dno = i + cursor_step;
        if (fcinfo->argTypes[i] == REFCURSOROID) {
            Cursor_Data* arg_cursor = &fcinfo->refcursor_data.argCursor[i];
            ExecCopyDataToDatum(func->datums, dno, arg_cursor);
            cursor_step += 4;
        }
    }

    /* Must save and restore prior value of cur_estate */
    save_cur_estate = func->cur_estate;

    // set the procedure's search_path as the current search_path
    validate_search_path(func);
    PushOverrideSearchPath(func->fn_searchpath, true);

    saved_Pseudo_CurrentUserId = u_sess->misc_cxt.Pseudo_CurrentUserId;
    u_sess->misc_cxt.Pseudo_CurrentUserId = &func->fn_owner;

    /* Mark the function as busy, so it can't be deleted from under us */
    func->use_count++;

    PGSTAT_START_PLSQL_TIME_RECORD();

    PG_TRY();
    {
        /*
         * Determine if called as function or trigger and call appropriate
         * subhandler
         */
        if (CALLED_AS_TRIGGER(fcinfo)) {
            retval = PointerGetDatum(plpgsql_exec_trigger(func, (TriggerData*)fcinfo->context));
        }
        else {
            retval = plpgsql_exec_function(func, fcinfo, false);
        }
    }
    PG_CATCH();
    {
        /* Decrement use-count, restore cur_estate, and propagate error */
        func->use_count--;
        func->cur_estate = save_cur_estate;
        plpgsql_restore_current_value_stp_with_exception(saved_current_stp_with_exception);
        // resume the search_path when there is an error
        PopOverrideSearchPath();
        u_sess->misc_cxt.Pseudo_CurrentUserId = saved_Pseudo_CurrentUserId;

        PG_RE_THROW();
    }
    PG_END_TRY();

    PGSTAT_END_PLSQL_TIME_RECORD(PL_EXECUTION_TIME);

    cursor_step = 0;
    /* cursor as an in parameter, its option shoule be return to the caller */
    for (int i = 0; i < fun_arg; i++) {
        int dno = i + cursor_step;
        if (fcinfo->argTypes[i] == REFCURSOROID && fcinfo->refcursor_data.argCursor != NULL) {
            Cursor_Data* arg_cursor = &fcinfo->refcursor_data.argCursor[i];
            ExecCopyDataFromDatum(func->datums, dno, arg_cursor);
            cursor_step += 4;
        }
    }

    /* set cursor optin to null which opened in this procedure */
    ResetPortalCursor(GetCurrentSubTransactionId(), func->fn_oid, func->use_count);

    func->use_count--;

    func->cur_estate = save_cur_estate;

    // resume the search_path when the procedure has executed
    PopOverrideSearchPath();

    if (AUDIT_EXEC_ENABLED) {
        auditExecPLpgSQLFunction(func, AUDIT_OK);
    }

    u_sess->misc_cxt.Pseudo_CurrentUserId = saved_Pseudo_CurrentUserId;

    /*
     * Disconnect from SPI manager
     */
    if ((rc = SPI_finish()) != SPI_OK_FINISH) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_finish failed: %s when execute PLSQL function.", SPI_result_code_string(rc))));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        u_sess->opt_cxt.is_stream = outer_is_stream;
        u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
    }
#else
    u_sess->opt_cxt.is_stream = outer_is_stream;
    u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
#endif

    return retval;
}

/* ----------
 * plpgsql_inline_handler
 *
 * Called by PostgreSQL to execute an anonymous code block
 * ----------
 */
PG_FUNCTION_INFO_V1(plpgsql_inline_handler);

Datum plpgsql_inline_handler(PG_FUNCTION_ARGS)
{
    InlineCodeBlock* codeblock = (InlineCodeBlock*)DatumGetPointer(PG_GETARG_DATUM(0));
    PLpgSQL_function* func = NULL;
    FunctionCallInfoData fake_fcinfo;
    FmgrInfo flinfo;
    Datum retval;
    int rc;
    // PGSTAT_INIT_PLSQL_TIME_RECORD
    int64 startTime = 0;
    bool needRecord = false;

#ifndef ENABLE_MULTIPLE_NODES
    bool outerIsStream = u_sess->opt_cxt.is_stream;
    bool outerIsStreamSupport = u_sess->opt_cxt.is_stream_support;
#endif

    _PG_init();

    AssertEreport(IsA(codeblock, InlineCodeBlock), MOD_PLSQL, "Inline code block is required.");

    /*
     * Connect to SPI manager
     */
    if ((rc = SPI_connect_ext(DestSPI, NULL, NULL, codeblock->atomic ? 0 : SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_connect failed: %s when execute anonymous block.", SPI_result_code_string(rc))));
    }

    PGSTAT_START_PLSQL_TIME_RECORD();

    /* Compile the anonymous code block */
    func = plpgsql_compile_inline(codeblock->source_text);

    PGSTAT_END_PLSQL_TIME_RECORD(PL_COMPILATION_TIME);

    /* Mark the function as busy, just pro forma */
    func->use_count++;

    /*
     * Set up a fake fcinfo with just enough info to satisfy
     * plpgsql_exec_function().  In particular note that this sets things up
     * with no arguments passed.
     */
    rc = memset_s(&fake_fcinfo, sizeof(fake_fcinfo), 0, sizeof(fake_fcinfo));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
    securec_check(rc, "\0", "\0");

    fake_fcinfo.flinfo = &flinfo;
    flinfo.fn_oid = InvalidOid;
    flinfo.fn_mcxt = CurrentMemoryContext;

    PGSTAT_START_PLSQL_TIME_RECORD();

    retval = plpgsql_exec_function(func, &fake_fcinfo, false);

    PGSTAT_END_PLSQL_TIME_RECORD(PL_EXECUTION_TIME);
    if (AUDIT_EXEC_ENABLED) {
        auditExecAnonymousBlock(func);
    }


    /* set cursor optin to null which opened in this block */
    ResetPortalCursor(GetCurrentSubTransactionId(), func->fn_oid, func->use_count);

    /* Function should now have no remaining use-counts ... */
    func->use_count--;
    AssertEreport(func->use_count == 0, MOD_PLSQL, "Function should now have no remaining use-counts");

    /* ... so we can free subsidiary storage */
    plpgsql_free_function_memory(func);

    /*
     * Disconnect from SPI manager
     */
    if ((rc = SPI_finish()) != SPI_OK_FINISH) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_finish failed: %s when execute anonymous block.", SPI_result_code_string(rc))));
    }

#ifndef ENABLE_MULTIPLE_NODES
    u_sess->opt_cxt.is_stream = outerIsStream;
    u_sess->opt_cxt.is_stream_support = outerIsStreamSupport;
#endif

    return retval;
}

/* ----------
 * plpgsql_validator
 *
 * This function attempts to validate a PL/pgSQL function at
 * CREATE FUNCTION time.
 * ----------
 */
PG_FUNCTION_INFO_V1(plpgsql_validator);

Datum plpgsql_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    Form_pg_proc proc = NULL;
    char functyptype;
    int numargs;
    Oid* argtypes = NULL;
    char** argnames;
    char* argmodes = NULL;
    bool istrigger = false;
    int i;

#ifndef ENABLE_MULTIPLE_NODES
    bool outerIsStream = u_sess->opt_cxt.is_stream;
    bool outerIsStreamSupport = u_sess->opt_cxt.is_stream_support;
#endif

    _PG_init();

    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid)) {
        PG_RETURN_VOID();
    }
        

    /* Get the new function's pg_proc entry */
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u when validate function.", funcoid)));
    }

    proc = (Form_pg_proc)GETSTRUCT(tuple);

    functyptype = get_typtype(proc->prorettype);

    /* Disallow pseudotype result */
    /* except for TRIGGER, RECORD, VOID, or polymorphic */
    if (functyptype == TYPTYPE_PSEUDO) {
        /* we assume OPAQUE with no arguments means a trigger */
        if (proc->prorettype == TRIGGEROID || (proc->prorettype == OPAQUEOID && proc->pronargs == 0)) {
            istrigger = true;
        } else if (proc->prorettype != RECORDOID && proc->prorettype != VOIDOID && !IsPolymorphicType(proc->prorettype)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("PL/pgSQL functions cannot return type %s", format_type_be(proc->prorettype))));
        }
    }

    /* Disallow pseudotypes in arguments (either IN or OUT) */
    /* except for polymorphic */
    numargs = get_func_arg_info(tuple, &argtypes, &argnames, &argmodes);
    for (i = 0; i < numargs; i++) {
        if (get_typtype(argtypes[i]) == TYPTYPE_PSEUDO) {
            if (!IsPolymorphicType(argtypes[i])) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("PL/pgSQL functions cannot accept type %s", format_type_be(argtypes[i]))));
            }
        }
    }

    /* Postpone body checks if !u_sess->attr.attr_sql.check_function_bodies */
    if (u_sess->attr.attr_sql.check_function_bodies) {
        FunctionCallInfoData fake_fcinfo;
        FmgrInfo flinfo;
        TriggerData trigdata;
        int rc;

        /*
         * Connect to SPI manager (is this needed for compilation?)
         */
        if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
            ereport(ERROR, (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_connect failed: %s when validate function.", SPI_result_code_string(rc))));
        }
        /*
         * Set up a fake fcinfo with just enough info to satisfy
         * plpgsql_compile().
         */
        errno_t errorno = memset_s(&fake_fcinfo, sizeof(fake_fcinfo), 0, sizeof(fake_fcinfo));
        securec_check(errorno, "", "");
        errorno = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
        securec_check(errorno, "", "");
        fake_fcinfo.flinfo = &flinfo;
        flinfo.fn_oid = funcoid;
        flinfo.fn_mcxt = CurrentMemoryContext;
        if (istrigger) {
            errorno = memset_s(&trigdata, sizeof(trigdata), 0, sizeof(trigdata));
            securec_check(errorno, "", "");
            trigdata.type = T_TriggerData;
            fake_fcinfo.context = (Node*)&trigdata;
        }

        /* Test-compile the function */
        plpgsql_compile(&fake_fcinfo, true);

        /*
         * Disconnect from SPI manager
         */
        if ((rc = SPI_finish()) != SPI_OK_FINISH) {
            ereport(ERROR, (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_connect failed: %s when validate function.", SPI_result_code_string(rc))));
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    u_sess->opt_cxt.is_stream = outerIsStream;
    u_sess->opt_cxt.is_stream_support = outerIsStreamSupport;
#endif

    ReleaseSysCache(tuple);

    PG_RETURN_VOID();
}

