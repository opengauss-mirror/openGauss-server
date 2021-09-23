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

#include "utils/plpgsql.h"
#include "utils/fmgroids.h"
#include "auditfuncs.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/gs_package.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/indexing.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/autonomoustransaction.h"
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
 * record which package calling the function,it use in private function.
 * because the private function can only called by the function which in the same
 * package
 */
extern Oid saveCallFromPkgOid(Oid pkgOid) 
{
    Oid oldValue = u_sess->plsql_cxt.running_pkg_oid;
    u_sess->plsql_cxt.running_pkg_oid = pkgOid;
    return oldValue;
}

/*
 * restore the runing_pkg_oid value to the old value
 */

extern void restoreCallFromPkgOid(Oid pkgOid) 
{
    u_sess->plsql_cxt.running_pkg_oid = pkgOid;
}

static void processError(List* funcname) 
{
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmodule(MOD_PLSQL),
            errmsg("package declare error function: %s", NameListToString(funcname))));
}

/*
 * process different situation in package,for example,duplicate declartion,duplicate definition,
 * and only declartion no definition.
 * the recordArr record the proc list which has been declared and which has been defined,
 * for example
 * recordArr[0] = 1; it means the location 0 is pairing location 1, so the location 1 value must be 0;
 * so recordArr[record[1]] = 1;
 */
void processPackageProcList(PLpgSQL_package* pkg)
{
    ListCell* cell = NULL;
    CreateFunctionStmt* stmtArray[list_length(pkg->proc_list)] = {NULL};
    int recordArr[list_length(pkg->proc_list)] = {0};
    int i = 0;
    int funcStmtLength = 0;
    foreach(cell, pkg->proc_list) {
        if (IsA(lfirst(cell), CreateFunctionStmt)) {
            CreateFunctionStmt* funcStmt = (CreateFunctionStmt*)lfirst(cell);
            stmtArray[i] = funcStmt;
            i = i + 1;
        }
    }
    funcStmtLength = i;
    if(funcStmtLength == 0) {
        return;
    }
    /*
     * process the situation that only 1 declartion or only 1 definition.
     */
    if (funcStmtLength <= 1) {
        if (stmtArray[0]->isFunctionDeclare && pkg->is_bodydefined) {
            processError(stmtArray[0]->funcname);
        } else if (stmtArray[0]->isFunctionDeclare && !pkg->is_bodydefined) {
            return;
        }
    } 
    for (int j = 0; j < funcStmtLength - 1; j++) {
        for (int k = j + 1; k < funcStmtLength; k++) {

            char* funcname1 = NULL;
            char* funcname2 = NULL;
            CreateFunctionStmt* funcStmt1 = stmtArray[j];
            CreateFunctionStmt* funcStmt2 = stmtArray[k];
            List* funcnameList1 = funcStmt1->funcname;
            List* funcnameList2 = funcStmt2->funcname;
            List* argList1 = funcStmt1->parameters;
            List* argList2 = funcStmt2->parameters;
            funcname1 = getFuncName(funcnameList1);
            funcname2 = getFuncName(funcnameList2);

            if (strcmp(funcname1, funcname2)) {
                continue;
            }

            if (!isSameArgList(argList1, argList2)) {
                continue;
            }
            if (funcStmt1->isProcedure != funcStmt2->isProcedure) {
                processError(funcnameList1);
            }
            if (funcStmt1->isFunctionDeclare && funcStmt2->isFunctionDeclare) {
                processError(funcnameList1);
            }
            if (!(funcStmt1->isFunctionDeclare || funcStmt2->isFunctionDeclare)) {
                processError(funcnameList1);
            }
            if (recordArr[j] == 0) {
                if (recordArr[k]!=0) {
                    processError(funcnameList1);
                }
                recordArr[j] = k;
                recordArr[k] = j;
            } else if(recordArr[recordArr[j]] != j) {
                processError(funcnameList1);
            }
            if (!funcStmt1->isPrivate || !funcStmt2->isPrivate) {
                funcStmt1->isPrivate = false;
                funcStmt2->isPrivate = false;
            }
        }
    }

    i = 0;

    for(i = 0; i < funcStmtLength; i++) {
        if (stmtArray[i]->isFunctionDeclare && pkg->is_bodydefined) {
            if (recordArr[i] == 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmodule(MOD_PLSQL),
                        errmsg("not found function definition: %s", NameListToString(stmtArray[i]->funcname))));
            }
        }
    }
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
        PG_TRY();
        {
            plpgsql_HashTableInit();
            SPICacheTableInit();

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
            hash_destroy(u_sess->plsql_cxt.plpgsql_pkg_HashTable);
            hash_destroy(u_sess->SPI_cxt.SPICacheTable);
            u_sess->plsql_cxt.plpgsql_HashTable = NULL;
            u_sess->plsql_cxt.plpgsql_pkg_HashTable = NULL;
            u_sess->SPI_cxt.SPICacheTable = NULL;
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
    DebugInfo* save_debug_info = NULL;
    Datum retval;
    int rc;
    Oid func_oid = fcinfo->flinfo->fn_oid;
    Oid* saved_Pseudo_CurrentUserId = NULL;
    Oid old_user = InvalidOid;
    int save_sec_context = 0;
    Oid cast_owner = InvalidOid;
    bool has_switch = false;
    // PGSTAT_INIT_PLSQL_TIME_RECORD
    int64 startTime = 0;
    bool needRecord = false;
    bool saved_current_stp_with_exception = false;
    HeapTuple proc_tup = NULL;
    Oid package_oid = InvalidOid;
    PLpgSQL_package* pkg = NULL;
    Form_pg_proc proc_struct = NULL;
    Datum package_oid_datum;
    bool isnull = false;
    Datum pkgbody_src;
    HeapTuple pkgtup;
    /* 
     * if the atomic stored in fcinfo is false means allow 
     * commit/rollback within stored procedure. 
     * set the nonatomic and will be reused within function.
     */
    nonatomic = fcinfo->context && IsA(fcinfo->context, FunctionScanState) &&
        !castNode(FunctionScanState, fcinfo->context)->atomic;
    /*
     * if the function in a package, then compile the package,and find the compiled function in
     * pkg->proc_compiled_list
     */            
    proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u, while compile function", func_oid)));
    }
    proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
    package_oid_datum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    package_oid = DatumGetObjectId(package_oid_datum);

    if (OidIsValid(package_oid)) {
        pkgtup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(package_oid));
        if (!HeapTupleIsValid(pkgtup)) {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for package %u, while compile package", package_oid)));
        }
        pkgbody_src = SysCacheGetAttr(PACKAGEOID, pkgtup, Anum_gs_package_pkgbodydeclsrc, &isnull);
        if (isnull) {
            ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("package body not defined")));
        }
        ReleaseSysCache(pkgtup);
    }
    ReleaseSysCache(proc_tup);
    if (OidIsValid(package_oid)) {
        if (u_sess->plsql_cxt.plpgsql_curr_compile_package == NULL) {
            pkg = PackageInstantiation(package_oid);
        }
    }
    if (pkg != NULL) {
        ListCell* l;
        foreach(l, pkg->proc_compiled_list) {
            PLpgSQL_function* package_func = (PLpgSQL_function*)lfirst(l);
            if (package_func->fn_oid == func_oid) {
                func = package_func;
            }
        }
    }

    /* get cast owner and make sure current user is cast owner when execute cast-func */
    GetUserIdAndSecContext(&old_user, &save_sec_context);
    cast_owner = u_sess->exec_cxt.cast_owner;
    if (InvalidCastOwnerId == cast_owner || !OidIsValid(cast_owner)) {
        ereport(LOG, (errmsg("old system table pg_cast does not have castowner column, use old default permission")));
    } else {
        SetUserIdAndSecContext(cast_owner, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
        has_switch = true;
    }

    int fun_arg = fcinfo->nargs;
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

    _PG_init();
    /*
     * Connect to SPI manager
     */
    if ((rc =  SPI_connect_ext(DestSPI, NULL, NULL, nonatomic ? SPI_OPT_NONATOMIC : 0, func_oid)) != SPI_OK_CONNECT) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("SPI_connect failed: %s when execute PLSQL function.", SPI_result_code_string(rc))));
    }

    PGSTAT_START_PLSQL_TIME_RECORD();
    /*
     * save running package value
     */
    Oid old_value = saveCallFromPkgOid(package_oid);
    saved_current_stp_with_exception = plpgsql_get_current_value_stp_with_exception();
    /* Find or compile the function */
    if (func == NULL) {
        func = plpgsql_compile(fcinfo, false);
    }

    restoreCallFromPkgOid(old_value);

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

    /* Must save and restore prior value of cur_estate and debug_info */
    save_cur_estate = func->cur_estate;
    save_debug_info = func->debug;

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
        Oid old_value = saveCallFromPkgOid(func->pkg_oid);
        if (CALLED_AS_TRIGGER(fcinfo)) {
            /* Trigger is not supported for package */
            if (OidIsValid(package_oid)) {
                ereport(ERROR,
                    (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Trigger call function in package is not supported."),
                        errdetail("trigger not support for package"),
                        errcause("System not supported."),
                        erraction("Do not use package with trigger.")));
            }
            retval = PointerGetDatum(plpgsql_exec_trigger(func, (TriggerData*)fcinfo->context));
        }
        else {
            if (func->is_private && !u_sess->is_autonomous_session) {
                if (OidIsValid(old_value)) {
                    if (func->pkg_oid != old_value) {
                        ereport(ERROR, (errcode(ERRCODE_NO_FUNCTION_PROVIDED),
                                (errmsg("not support call package private function or procedure"))));
                    }
                } else {
                    ereport(ERROR, (errcode(ERRCODE_NO_FUNCTION_PROVIDED),
                            (errmsg("not support call package private function or procedure"))));
                }
            }
            if (func->action->isAutonomous && u_sess->plsql_cxt.is_package_instantiation) {
                ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                        errmsg("package instantiation can not have autonomous function"),
                        errdetail("this package have autonmous function"),
                        errcause("package have autonmous function"),
                        erraction("redefine package")));
            }

            if (IsAutonomousTransaction(func->action->isAutonomous)) {
                retval = plpgsql_exec_autonm_function(func, fcinfo, NULL);
            } else {
                retval = plpgsql_exec_function(func, fcinfo, false);
            }
            /* Disconnecting and releasing resources */
            DestoryAutonomousSession(false);
        }
        restoreCallFromPkgOid(old_value);
    }
    PG_CATCH();
    {
        /* Decrement use-count, restore cur_estate, and propagate error */
        func->use_count--;
        func->cur_estate = save_cur_estate;
        if (save_debug_info != NULL)
            func->debug = save_debug_info;
        plpgsql_restore_current_value_stp_with_exception(saved_current_stp_with_exception);
        // resume the search_path when there is an error
        PopOverrideSearchPath();

        u_sess->plsql_cxt.running_pkg_oid = InvalidOid;
        u_sess->plsql_cxt.plpgsql_curr_compile = NULL;
        u_sess->plsql_cxt.plpgsql_curr_compile_package = NULL;
        u_sess->misc_cxt.Pseudo_CurrentUserId = saved_Pseudo_CurrentUserId;
        /* AutonomousSession Disconnecting and releasing resources */
        DestoryAutonomousSession(true);

        PG_RE_THROW();
    }
    PG_END_TRY();

    PGSTAT_END_PLSQL_TIME_RECORD(PL_EXECUTION_TIME);
#ifndef ENABLE_MULTIPLE_NODES
    /* debug finished, close debug resource */
    if (func->debug) {
        /* if debuger is waiting for end msg, send end */
        server_send_end_msg(func->debug);
        /* pass opt to upper debug function */
        server_pass_upper_debug_opt(func->debug);
        clean_up_debug_server(func->debug, false, true);
    }
#endif
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
    func->debug = save_debug_info;

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
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI_finish failed: %s when execute PLSQL function.", SPI_result_code_string(rc))));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        u_sess->opt_cxt.is_stream = outer_is_stream;
        u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
    }
#else
    u_sess->opt_cxt.query_dop = outerDop;
#endif
    if (has_switch) {
        SetUserIdAndSecContext(old_user, save_sec_context);
        u_sess->exec_cxt.cast_owner = InvalidOid;
    }
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
    int outerDop = u_sess->opt_cxt.query_dop;
    u_sess->opt_cxt.query_dop = 1;
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
    if (u_sess->plsql_cxt.plpgsql_curr_compile_package != NULL) {
        func->pkg_oid = u_sess->plsql_cxt.plpgsql_curr_compile_package->pkg_oid;
    }

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

    PG_TRY();
    {
        if (IsAutonomousTransaction(func->action->isAutonomous)) {
            retval = plpgsql_exec_autonm_function(func, &fake_fcinfo, codeblock->source_text);
        } else {
            Oid old_value = saveCallFromPkgOid(func->pkg_oid);
            retval = plpgsql_exec_function(func, &fake_fcinfo, false);
            restoreCallFromPkgOid(old_value);
        }
    }
    PG_CATCH();
    {
        /* AutonomousSession Disconnecting and releasing resources */
        DestoryAutonomousSession(true);
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Disconnecting and releasing resources */
    DestoryAutonomousSession(false);

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
    if (u_sess->plsql_cxt.plpgsql_curr_compile_package != NULL) {
        func->pkg_oid = u_sess->plsql_cxt.plpgsql_curr_compile_package->pkg_oid;
        plpgsql_free_function_memory(func);
    } else {
        plpgsql_free_function_memory(func);
    }

    /*
     * Disconnect from SPI manager
     */
    if ((rc = SPI_finish()) != SPI_OK_FINISH) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_finish failed: %s when execute anonymous block.", SPI_result_code_string(rc))));
    }

#ifndef ENABLE_MULTIPLE_NODES
    u_sess->opt_cxt.query_dop = outerDop;
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
    /*
     * 3 means the number of arguments of function plpgsql_validator, while 'is_place' is the third one,
     * and 2 is the postion of 'is_place' in PG_FUNCTION_ARGS
     */
    bool replace = false;
    if (PG_NARGS() >= 3) {
        replace = PG_GETARG_BOOL(2);
    }

#ifndef ENABLE_MULTIPLE_NODES
    int outerDop = u_sess->opt_cxt.query_dop;
    u_sess->opt_cxt.query_dop = 1;
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

    ReleaseSysCache(tuple);
    /* Postpone body checks if !u_sess->attr.attr_sql.check_function_bodies */
    if (u_sess->attr.attr_sql.check_function_bodies) {
        FunctionCallInfoData fake_fcinfo;
        FmgrInfo flinfo;
        TriggerData trigdata;
        int rc;
        PLpgSQL_function* func = NULL;
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
        fake_fcinfo.arg = (Datum*)palloc0(sizeof(Datum));
        fake_fcinfo.arg[0] = fcinfo->arg[1];
        flinfo.fn_oid = funcoid;
        flinfo.fn_mcxt = CurrentMemoryContext;
        if (istrigger) {
            errorno = memset_s(&trigdata, sizeof(trigdata), 0, sizeof(trigdata));
            securec_check(errorno, "", "");
            trigdata.type = T_TriggerData;
            fake_fcinfo.context = (Node*)&trigdata;
        }

        /* Test-compile the function */
        PG_TRY();
        {
            func = plpgsql_compile(&fake_fcinfo, true);
        }
        PG_CATCH();
        {
            u_sess->plsql_cxt.plpgsql_curr_compile = NULL;
            u_sess->plsql_cxt.plpgsql_curr_compile_package = NULL;
            PG_RE_THROW();
        }
        PG_END_TRY();
        /* Skip validation on Initdb */
        /*
         * Disconnect from SPI manager
         */
        if ((rc = SPI_finish()) != SPI_OK_FINISH) {
            ereport(ERROR, (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_connect failed: %s when validate function.", SPI_result_code_string(rc))));
        }
        if (!IsInitdb && u_sess->attr.attr_common.enable_full_encryption && 
            u_sess->plsql_cxt.plpgsql_curr_compile_package == NULL) {
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_MAIN_COORDINATOR) {
#endif
                /*
                 * set ClientAuthInProgress to prevent warnings from the parser
                 * to be sent to client
                 */
                bool saved_client_auth = u_sess->ClientAuthInProgress;
                u_sess->ClientAuthInProgress = true;
                pl_validate_function_sql(func, replace);
                u_sess->ClientAuthInProgress = saved_client_auth;
#ifdef ENABLE_MULTIPLE_NODES
            }
#endif
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    u_sess->opt_cxt.query_dop = outerDop;
#endif

    PG_RETURN_VOID();
}

/* ----------
 * plpgsql_package_validator
 *
 * This package attempts to validate a PL/pgSQL package at
 * CREATE PACKAGE time.
 * ----------
 */
PLpgSQL_package* plpgsql_package_validator(Oid packageOid, bool isSpec, bool isCreate)
{
    PLpgSQL_package* pkg = NULL;
    _PG_init();
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
    /* Postpone body checks if !u_sess->attr.attr_sql.check_function_bodies */
    PG_TRY();
    {
        pkg = plpgsql_pkg_compile(packageOid, true, isSpec, isCreate);
        u_sess->plsql_cxt.plpgsql_curr_compile_package = NULL;
    }
    PG_CATCH();
    {
        u_sess->plsql_cxt.plpgsql_curr_compile = NULL;
        u_sess->plsql_cxt.plpgsql_curr_compile_package = NULL;
        u_sess->plsql_cxt.running_pkg_oid = InvalidOid;
        u_sess->parser_cxt.in_package_function_compile = false;
        PG_RE_THROW();
    }
    PG_END_TRY();
    return pkg;
}

/* ----------
 * FunctionInPackageCompile
 *
 * compile the package function, and record the result in compiled function list
 * ----------
 */
void FunctionInPackageCompile(PLpgSQL_package* pkg) 
{
    HeapTuple oldtup;

    ScanKeyData entry;
    ScanKeyInit(&entry, Anum_pg_proc_packageid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(pkg->pkg_oid));
    Relation pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(pg_proc_rel, InvalidOid, false, NULL, 1, &entry);
    /*
     * do we need job status not run, we can change the owner, 
     * now only check if have the permission to change the owner, if after change, the running job may 
     * failed in check permission
     */
    while ((oldtup = systable_getnext(scan)) != NULL) {
        HeapTuple proctup = heap_copytuple(oldtup);
        if (HeapTupleIsValid(proctup)) {
            Oid funcOid = InvalidOid;
            HeapTuple languageTuple;
            Form_pg_language languageStruct;
            funcOid = HeapTupleGetOid(proctup);
            char* language = "plpgsql";
            languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));

            if (!HeapTupleIsValid(languageTuple)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("language \"%s\" does not exist", language)));
            }

            languageStruct = (Form_pg_language)GETSTRUCT(languageTuple);
            Oid languageValidator = languageStruct->lanvalidator;
            OidFunctionCall1Coll(languageValidator, InvalidOid, ObjectIdGetDatum(funcOid));
            ReleaseSysCache(languageTuple);

            if (!OidIsValid(funcOid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_PLSQL),
                        errmsg("cache lookup failed for relid %u", funcOid)));
            }
        } else {
            ereport(ERROR, 
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_PLSQL),
                    errmsg("cache lookup failed")));
        }
        heap_freetuple(proctup);
    }    
    systable_endscan(scan);
    heap_close(pg_proc_rel, RowExclusiveLock);
}

/* ----------
 * PackageInit
 *
 * init the package.including package variable.
 * ----------
 */

void PackageInit(PLpgSQL_package* pkg, bool isCreate) 
{
    ListCell* cell = NULL;
    if (isCreate) {
        processPackageProcList(pkg);
        foreach(cell, pkg->proc_list)
        {
            if (IsA(lfirst(cell), CreateFunctionStmt)) {
                MemoryContext temp = MemoryContextSwitchTo(pkg->pkg_cxt);
                CreateFunctionStmt* funcStmt = (CreateFunctionStmt*)lfirst(cell);
                if (!funcStmt->isExecuted) {
                    char* funcStr = funcStmt->queryStr;
                    CreateFunction(funcStmt, funcStr, pkg->pkg_oid);
                    funcStmt->isExecuted = true;
                }
                (void*)MemoryContextSwitchTo(temp);
            }
        }
    } else {
        if (pkg->is_bodydefined) {
            FunctionInPackageCompile(pkg);
        }
    }
    cell = NULL;
    bool oldStatus = false;
    bool needResetErrMsg = stp_disable_xact_and_set_err_msg(&oldStatus, STP_XACT_PACKAGE_INSTANTIATION);
    PG_TRY();
    {
        u_sess->plsql_cxt.is_package_instantiation = true;
        foreach(cell, pkg->proc_list) {
            if (IsA(lfirst(cell), DoStmt)) {
                MemoryContext temp = MemoryContextSwitchTo(pkg->pkg_cxt);
                DoStmt* doStmt = (DoStmt*)lfirst(cell);
                if (!isCreate) {
                    if (!doStmt->isExecuted) {
                        ExecuteDoStmt(doStmt, true);
                        doStmt->isExecuted = true;
                    }
                } else {
                    if (doStmt->isSpec) {
                        ExecuteDoStmt(doStmt, true);
                        doStmt->isExecuted = true;
                    }
                }
                (void*)MemoryContextSwitchTo(temp);
            }
        }
        stp_reset_xact_state_and_err_msg(oldStatus, needResetErrMsg);
        u_sess->plsql_cxt.is_package_instantiation = false;
    }
    PG_CATCH();
    {
        
        stp_reset_xact_state_and_err_msg(oldStatus, needResetErrMsg);
        u_sess->plsql_cxt.is_package_instantiation = false;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* ----------
 * ConnectSPI
 *
 * connect the plpgsql 
 * ----------
 */

void ConnectSPI() 
{
    int rc;
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_connect failed: %s when validate function.", SPI_result_code_string(rc))));
    }
}

/* ----------
 * DisconnectSPI
 *
 * disconnect the plpgsql 
 * ----------
 */

void DisconnectSPI() 
{
    int rt;
    if ((rt = SPI_finish()) != SPI_OK_FINISH) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_connect failed: %s when validate function.", SPI_result_code_string(rt))));
    }
}
