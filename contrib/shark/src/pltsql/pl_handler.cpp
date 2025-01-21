/* -------------------------------------------------------------------------
 *
 * pl_handler.c		- Handler for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  contrib/shark/src/pltsql/pl_handler.c
 *
 * -------------------------------------------------------------------------
 */

#include "fmgr.h"
#include "pltsql.h"
#include "pgstat.h"
#include "catalog/pg_object.h"
#include "catalog/pg_object_type.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_proc_ext.h"
#include "catalog/gs_package.h"
#include "executor/executor.h"
#include "tcop/autonomoustransaction.h"
#include "pgaudit.h"
#include "auditfuncs.h"
#include "utils/builtins.h"
#include "utils/pl_package.h"
#include "funcapi.h"
#include "commands/proclang.h"

extern void saveCallFromFuncOid(Oid funcOid);
extern Oid getCurrCallerFuncOid();
extern void CheckPipelinedWithOutParam(const PLpgSQL_function *func);
#define MAXSTRLEN ((1 << 11) - 1)

static void get_func_actual_rows(Oid funcid, uint32* rows);
static void get_proc_coverage(Oid func_oid, int* coverage);

static void auditExecPLpgSQLFunction(PLpgSQL_function* func, AuditResult result)
{
    char details[PGAUDIT_MAXLENGTH];
    errno_t rcs = EOK;

    if (unlikely(func == NULL)) {
        char message[MAXSTRLEN]; 
        sprintf_s(message, MAXSTRLEN, "function should not be null");
        InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc, true);
        ereport(ERROR, 
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function should not be null")));
    }

    rcs = snprintf_s(
        details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1, "Execute PLpgSQL function(oid = %u). ", func->fn_oid);
    securec_check_ss(rcs, "\0", "\0");

    audit_report(AUDIT_FUNCTION_EXEC, result, func->fn_signature, details);
}


static void ProcInsertGsSource(Oid funcOid, bool status)
{
    HeapTuple procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(procTup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u, while collecting gs_source log", funcOid)));
    }
    Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(procTup);
    bool isnull = false;

    /* Skip nested create function stmt within package body */
    bool ispackage = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_package, &isnull);
    Datum packageIdDatum = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_packageid, &isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The prokind of the function is null"),
                errhint("Check whether the definition of the function is complete in the pg_proc system table.")));
    }
    Oid packageOid = ispackage ? DatumGetObjectId(packageIdDatum) : InvalidOid;
    if (OidIsValid(packageOid)) {
        ReleaseSysCache(procTup);
        return;
    }

    /* Skip trigger function for now */
    if (procStruct->prorettype == TRIGGEROID) {
        ReleaseSysCache(procTup);
        return;
    }

    Datum prokindDatum = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prokind, &isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The prokind of the function is null"),
                errhint("Check whether the definition of the function is complete in the pg_proc system table.")));
    }
    char prokind = CharGetDatum(prokindDatum);

    if (PROC_IS_AGG(prokind) || PROC_IS_WIN(prokind)) {
        ReleaseSysCache(procTup);
        return;
    }

    char* name = NameStr(procStruct->proname);
    Oid nspid = procStruct->pronamespace;
    const char* type = PROC_IS_PRO(prokind) ? ("procedure") : ("function");
    if (strcasecmp(get_language_name((Oid)procStruct->prolang), "plpgsql") != 0) {
        ReleaseSysCache(procTup);
        return;
    }
    InsertGsSource(funcOid, nspid, name, type, status);

    ReleaseSysCache(procTup);

    return;
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
 * pltsql_call_handler
 *
 * The PostgreSQL function manager and trigger manager
 * call this function for execution of PL/pgSQL procedures.
 * ----------
 */
PG_FUNCTION_INFO_V1(pltsql_call_handler);

static Oid get_package_id(Oid func_oid)
{
    HeapTuple proc_tup = NULL;
    Oid package_oid = InvalidOid;
    Form_pg_proc proc_struct = NULL;
    Datum package_oid_datum;
    bool isnull = false;
    HeapTuple pkgtup;
    Datum pkgbody_src;

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
    bool ispackage = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_package, &isnull);
    package_oid_datum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull);
    package_oid = ispackage ? DatumGetObjectId(package_oid_datum) : InvalidOid;

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

    return package_oid;
}

static PGconn* LoginDatabase(char* host, int port, char* user, char* password,
    char* dbname, const char* progname, char* encoding)
{
    PGconn* conn = NULL;
    char portValue[32];
#define PARAMS_ARRAY_SIZE 10
    const char* keywords[PARAMS_ARRAY_SIZE];
    const char* values[PARAMS_ARRAY_SIZE];
    int count = 0;
    int retryNum = 10;
    int rc;

    rc = sprintf_s(portValue, sizeof(portValue), "%d", port);
    securec_check_ss(rc, "\0", "\0");

    keywords[0] = "host";
    values[0] = host;
    keywords[1] = "port";
    values[1] = portValue;
    keywords[2] = "user";
    values[2] = user;
    keywords[3] = "password";
    values[3] = password;
    keywords[4] = "dbname";
    values[4] = dbname;
    keywords[5] = "fallback_application_name";
    values[5] = progname;
    keywords[6] = "client_encoding";
    values[6] = encoding;
    keywords[7] = "connect_timeout";
    values[7] = "5";
    keywords[8] = "options";
    /* this mode: remove timeout */
    values[8] = "-c xc_maintenance_mode=on";
    keywords[9] = NULL;
    values[9] = NULL;

retry:
    /* try to connect to database */
    conn = PQconnectdbParams(keywords, values, true);
    if (PQstatus(conn) != CONNECTION_OK) {
        if (++count < retryNum) {
            ereport(LOG, (errmsg("Could not connect to the %s, the connection info : %s",
                                 dbname, PQerrorMessage(conn))));
            PQfinish(conn);
            conn = NULL;

            /* sleep 0.1 s */
            pg_usleep(100000L);
            goto retry;
        }

        char connErrorMsg[1024] = {0};
        errno_t rc;
        rc = snprintf_s(connErrorMsg, 1024, 1023,
                        "%s", PQerrorMessage(conn));
        securec_check_ss(rc, "\0", "\0");

        PQfinish(conn);
        conn = NULL;
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                       (errmsg("Could not connect to the %s, "
                               "we have tried %d times, the connection info: %s",
                               dbname, count, connErrorMsg))));
    }

    return (conn);
}

Datum pltsql_call_handler(PG_FUNCTION_ARGS)
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
    PGSTAT_INIT_TIME_RECORD();
    bool needRecord = false;
    PLpgSQL_package* pkg = NULL;
    MemoryContext oldContext = CurrentMemoryContext;
    int pkgDatumsNumber = 0;
    bool savedisAllowCommitRollback = true;
    bool enableProcCoverage = u_sess->attr.attr_common.enable_proc_coverage;
    Oid saveCallerOid = InvalidOid;
    Oid savaCallerParentOid = InvalidOid;
    bool is_pkg_func = false;
    
    /* Check if type body exists if using type method */
    HeapTuple proc_tup = NULL;
    bool isnull = false;
    /*
     * If the function in a package, thne compile the package, and find the compiled function in
     * pkg->proc_compiled_list
     */
    proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for function %u, while compile function.", func_oid)));
    }
    bool ispackage = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_package, &isnull);
    Oid protypoid = ispackage ? InvalidOid : DatumGetObjectId(
        SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &isnull));
    /* Find method type from pg_object */
    char typmethodkind = OBJECTTYPE_NULL_PROC;
    HeapTuple objTuple = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(func_oid), CharGetDatum(OBJECT_TYPE_PROC));
    if (HeapTupleIsValid(objTuple)) {
        typmethodkind = GET_PROTYPEKIND(SysCacheGetAttr(PGOBJECTID, objTuple, Anum_pg_object_options, &isnull));
        ReleaseSysCache(objTuple);
    }
    if (OidIsValid(protypoid) && (typmethodkind != OBJECTTYPE_DEFAULT_CONSTRUCTOR_PROC) &&
        (typmethodkind != TABLE_VARRAY_CONSTRUCTOR_PROC)) {
        HeapTuple tuple = SearchSysCache1(OBJECTTYPE, ObjectIdGetDatum(protypoid));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for object type %u, while compile function.", protypoid)));
        }
        Form_pg_object_type pg_object_type_struct = (Form_pg_object_type)GETSTRUCT(tuple);
        if (!pg_object_type_struct->isbodydefined)
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_OBJECT_TYPE_BODY_DEFINE_ERROR),
                errmsg("type body \"%s\" not define", get_typename(protypoid))));
        ReleaseSysCache(tuple);
    }
    ReleaseSysCache(proc_tup);
    /*
     * if the atomic stored in fcinfo is false means allow
     * commit/rollback within stored procedure.
     * set the nonatomic and will be reused within function.
     */
    if (fcinfo->context) {
        nonatomic = IsA(fcinfo->context, FunctionScanState) && !castNode(FunctionScanState, fcinfo->context)->atomic;
    } else {
        nonatomic = u_sess->SPI_cxt.is_allow_commit_rollback;
    }

    /* get cast owner and make sure current user is cast owner when execute cast-func */
    GetUserIdAndSecContext(&old_user, &save_sec_context);
    cast_owner = u_sess->exec_cxt.cast_owner;
    if (cast_owner != InvalidCastOwnerId && OidIsValid(cast_owner)) {
        SetUserIdAndSecContext(cast_owner, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
        has_switch = true;
    }

    bool save_need_create_depend = u_sess->plsql_cxt.need_create_depend;
    u_sess->plsql_cxt.need_create_depend = false;
    if (u_sess->SPI_cxt._connected == -1) {
        plpgsql_hashtable_clear_invalid_obj();
    }

    _PG_init();
    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    rc =  SPI_connect_ext(DestSPI, NULL, NULL, nonatomic ? SPI_OPT_NONATOMIC : 0, func_oid);
    if (rc  != SPI_OK_CONNECT) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("SPI_connect failed: %s when execute PLSQL function.", SPI_result_code_string(rc))));
    }

    Oid package_oid = get_package_id(func_oid);
    if (OidIsValid(package_oid)) {
        if (u_sess->plsql_cxt.curr_compile_context == NULL ||
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL ||
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid != package_oid) {
            pkg = PackageInstantiation(package_oid);
        }
    }

    proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u, while compile function", func_oid)));
    }

    Oid caller_oid = GetProprocoidByOid(HeapTupleGetOid(proc_tup));
    if (OidIsValid(caller_oid) && caller_oid == getCurrCallerFuncOid()) {
        PLpgSQL_func_hashkey hashkey;
        Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tup);
        fcinfo->fncollation = InvalidOid;
        /* Compute hashkey using function signature and actual arg types */
        compute_function_hashkey(proc_tup, fcinfo, proc_struct, &hashkey, false);
            /* And do the lookup */
        func = plpgsql_HashTableLookup(&hashkey);
    }

    ReleaseSysCache(proc_tup);

    if (pkg != NULL) {
        ListCell* l;
        foreach(l, pkg->proc_compiled_list) {
            PLpgSQL_function* package_func = (PLpgSQL_function*)lfirst(l);
            if (package_func->fn_oid == func_oid) {
                func = package_func;
            }
        }
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
    /* Saves the status of whether to send commandId. */
    bool saveSetSendCommandId = IsSendCommandId();
#else
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif
    int connect = SPI_connectid();
    Oid firstLevelPkgOid = InvalidOid;
    Oid firstLevelfuncOid = InvalidOid;
    bool save_curr_status = GetCurrCompilePgObjStatus();
    bool save_is_exec_autonomous = u_sess->plsql_cxt.is_exec_autonomous;
    bool save_is_pipelined = u_sess->plsql_cxt.is_pipelined;
    PG_TRY();
    {
        PGSTAT_START_PLSQL_TIME_RECORD();
        /*
         * save running package value
         */
        firstLevelPkgOid = saveCallFromPkgOid(package_oid);
        firstLevelfuncOid = getCurrCallerFuncOid();
        bool saved_current_stp_with_exception = plpgsql_get_current_value_stp_with_exception();
        int *coverage = NULL;
        if (enableProcCoverage) {
            uint32 rows;
            int ret;
            get_func_actual_rows(func_oid, &rows);
            coverage = (int*)palloc(sizeof(int) * rows);
            ret = memset_s(coverage, sizeof(int) * rows, 0, sizeof(int) * rows);
            securec_check(ret, "\0", "\0");
        }
        /* Find or compile the function */
        if (func == NULL) {
            u_sess->plsql_cxt.compile_has_warning_info = false;
            SetCurrCompilePgObjStatus(true);
            u_sess->plsql_cxt.isCreateFuncSubprogramBody = false;
            func = pltsql_compile(fcinfo, false);
            if (enable_plpgsql_gsdependency_guc() && func != NULL) {
                SetPgObjectValid(func_oid, OBJECT_TYPE_PROC, GetCurrCompilePgObjStatus());
                if (!GetCurrCompilePgObjStatus()) {
                    ereport(WARNING, (errmodule(MOD_PLSQL),
                        errmsg("Function %s recompile with compilation errors, please use ALTER COMPILE to recompile.",
                               get_func_name(func_oid))));
                }
            }
            u_sess->plsql_cxt.isCreateFuncSubprogramBody = true;
            u_sess->plsql_cxt.cur_func_oid = func->fn_oid;
        }

        if (!is_pkg_func && u_sess->plsql_cxt.running_func_oid != func->fn_oid) {
            if (func->proc_list != NULL) {
                saveCallFromFuncOid(func->fn_oid);
                savaCallerParentOid = func->parent_oid;
            } else if (func->parent_func) {
                saveCallFromFuncOid(func->parent_oid);
            }
        }
        saveCallerOid = func->fn_oid;
        
        if (func->fn_readonly) {
            stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_IMMUTABLE);
        }
        
        bool guc_changed = ((u_sess->utils_cxt.behavior_compat_flags & OPT_PROC_OUTPARAM_OVERRIDE) !=
                           (func->guc_stat & OPT_PROC_OUTPARAM_OVERRIDE));
        if (guc_changed) {
            ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmodule(MOD_PLSQL),
                           errmsg("Cannot change the PROC_OUTPARAM_OVERRIDE guc status while in the same session.")));
        }

        restoreCallFromPkgOid(firstLevelPkgOid);

        PGSTAT_END_PLSQL_TIME_RECORD(PL_COMPILATION_TIME);

        int cursor_step = 0;
        if (pkg != NULL) {
            pkgDatumsNumber = pkg->ndatums;
        }
        /* copy cursor option on in-parameter to function body's cursor */
        for (int i = 0; i < fun_arg; i++) {
            int dno = i + cursor_step + pkgDatumsNumber;
            if (fcinfo->argTypes[i] == REFCURSOROID && fcinfo->refcursor_data.argCursor != NULL && 
                func->datums[dno]->dtype == PLPGSQL_DTYPE_VAR) {
                Cursor_Data* arg_cursor = &fcinfo->refcursor_data.argCursor[i];
                ExecCopyDataToDatum(func->datums, dno, arg_cursor);
                cursor_step += 4;
            }
        }

        /* Must save and restore prior value of cur_estate and debug_info */
        save_cur_estate = func->cur_estate;
        save_debug_info = func->debug;
        NodeTag old_node_tag = t_thrd.postgres_cxt.cur_command_tag;

        // set the procedure's search_path as the current search_path
        validate_search_path(func);
        PushOverrideSearchPath(func->fn_searchpath, true);

        saved_Pseudo_CurrentUserId = u_sess->misc_cxt.Pseudo_CurrentUserId;
        u_sess->misc_cxt.Pseudo_CurrentUserId = &func->fn_owner;

        /* Mark packages the function use, so them can't be deleted from under us */
        AddPackageUseCount(func);
        /* Mark the function as busy, so it can't be deleted from under us */
        func->use_count++;

        PGSTAT_START_PLSQL_TIME_RECORD();
        Oid secondLevelPkgOid = saveCallFromPkgOid(func->pkg_oid);
        /* save flag for nest plpgsql compile */
        PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
        int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
        int save_compile_status = u_sess->plsql_cxt.compile_status;
        FormatCallStack* saveplcallstack = t_thrd.log_cxt.call_stack;
        PG_TRY();
        {
            /*
             * Determine if called as function or trigger and call appropriate
             * subhandler
             */
            if (CALLED_AS_TRIGGER(fcinfo)) {
                retval = PointerGetDatum(plpgsql_exec_trigger(func, (TriggerData *)fcinfo->context));
            } else if (CALLED_AS_EVENT_TRIGGER(fcinfo)) {
                plpgsql_exec_event_trigger(func, (EventTriggerData *)fcinfo->context);

            } else {
                if (func->is_private && !u_sess->is_autonomous_session) {
                    if (OidIsValid(secondLevelPkgOid)) {
                        if (func->pkg_oid != secondLevelPkgOid) {
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
                                    errcause("package have autonmous function"), erraction("redefine package")));
                }

                CheckPipelinedWithOutParam(func);

                if (IsAutonomousTransaction(func->action->isAutonomous)) {
                    retval = plpgsql_exec_autonm_function(func, fcinfo, NULL);
                } else {
                    retval = plpgsql_exec_function(func, fcinfo, false, coverage);
                }
                /* Disconnecting and releasing resources */
                DestoryAutonomousSession(false);
            }
            restoreCallFromPkgOid(secondLevelPkgOid);
            if (func->fn_readonly) {
                stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
            }
        }
        PG_CATCH();
        {
            /* reset cur_exception_cxt */
            u_sess->plsql_cxt.cur_exception_cxt = NULL;

            t_thrd.log_cxt.call_stack = saveplcallstack;
            t_thrd.postgres_cxt.cur_command_tag = old_node_tag;

#ifndef ENABLE_MULTIPLE_NODES
            /* for restore parent session and automn session package var values */
            processAutonmSessionPkgsInException(func);
#endif
            /* Decrement use-count, restore cur_estate, and propagate error */
            func->use_count--;
            func->cur_estate = save_cur_estate;
            DecreasePackageUseCount(func);
#ifndef ENABLE_MULTIPLE_NODES
            /* debug finished, close debug resource */
            if (func->debug) {
                /* if debuger is waiting for end msg, send end */
                PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[func->debug->comm->comm_idx];
                if (debug_comm->isGmsRunning()) {
                    server_send_gms_end_msg(func->debug);
                } else {
                    server_send_end_msg(func->debug);
                }
                /* pass opt to upper debug function */
                server_pass_upper_debug_opt(func->debug);
                clean_up_debug_server(func->debug, false, true);
            }
            if (save_debug_info != NULL)
                func->debug = save_debug_info;
#endif /* ENABLE_MULTIPLE_NODES */
            plpgsql_restore_current_value_stp_with_exception(saved_current_stp_with_exception);
            // resume the search_path when there is an error
            PopOverrideSearchPath();

            saveCallFromFuncOid(InvalidOid);
            restoreCallFromPkgOid(secondLevelPkgOid);
            ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
                errmsg("%s clear curr_compile_context because of error.", __func__)));
            /* reset nest plpgsql compile */
            u_sess->plsql_cxt.curr_compile_context = save_compile_context;
            u_sess->plsql_cxt.compile_status = save_compile_status;
            u_sess->plsql_cxt.cur_func_oid = InvalidOid;
            clearCompileContextList(save_compile_list_length);

            u_sess->misc_cxt.Pseudo_CurrentUserId = saved_Pseudo_CurrentUserId;
            /* AutonomousSession Disconnecting and releasing resources */
            DestoryAutonomousSession(true);
            pfree_ext(u_sess->plsql_cxt.pass_func_tupdesc);
            if (func->fn_readonly) {
                stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
            }
            PG_RE_THROW();
        }
        PG_END_TRY();

        PGSTAT_END_PLSQL_TIME_RECORD(PL_EXECUTION_TIME);
#ifndef ENABLE_MULTIPLE_NODES
        /* debug finished, close debug resource */
        if (func->debug) {
            /* if debuger is waiting for end msg, send end */
            PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[func->debug->comm->comm_idx];
            /* if debuger is waiting for end msg, send end */
            if (debug_comm->isGmsRunning()) {
                server_send_gms_end_msg(func->debug);
            } else {
                server_send_end_msg(func->debug);
            }
            /* pass opt to upper debug function */
            server_pass_upper_debug_opt(func->debug);
            clean_up_debug_server(func->debug, false, true);
        }
#endif
        if (enableProcCoverage && func->namespaceOid != PG_CATALOG_NAMESPACE) {
            get_proc_coverage(func_oid, coverage);
            pfree(coverage);
        }

        cursor_step = 0;
        /* cursor as an in parameter, its option shoule be return to the caller */
        if (pkg != NULL) {
            pkgDatumsNumber = pkg->ndatums;
        }
        for (int i = 0; i < fun_arg; i++) {
            int dno = i + cursor_step + pkgDatumsNumber;
            if (fcinfo->argTypes[i] == REFCURSOROID && fcinfo->refcursor_data.argCursor != NULL 
                && func->datums[dno]->dtype == PLPGSQL_DTYPE_VAR) {
                Cursor_Data* arg_cursor = &fcinfo->refcursor_data.argCursor[i];
                ExecCopyDataFromDatum(func->datums, dno, arg_cursor);
                cursor_step += 4;
            }
        }

        /* set cursor optin to null which opened in this procedure */
        ResetPortalCursor(GetCurrentSubTransactionId(), func->fn_oid, func->use_count, false);

        func->use_count--;
        DecreasePackageUseCount(func);
        func->cur_estate = save_cur_estate;
        func->debug = save_debug_info;
        t_thrd.postgres_cxt.cur_command_tag = old_node_tag;

        // resume the search_path when the procedure has executed
        PopOverrideSearchPath();

        if (AUDIT_EXEC_ENABLED) {
            auditExecPLpgSQLFunction(func, AUDIT_OK);
        }

        u_sess->misc_cxt.Pseudo_CurrentUserId = saved_Pseudo_CurrentUserId;
    }
    PG_CATCH();
    {
        u_sess->plsql_cxt.need_create_depend = save_need_create_depend;
        SetCurrCompilePgObjStatus(save_curr_status);
        u_sess->plsql_cxt.is_exec_autonomous = save_is_exec_autonomous;
        u_sess->plsql_cxt.is_pipelined = save_is_pipelined;
        saveCallFromFuncOid(firstLevelfuncOid);
        u_sess->plsql_cxt.cur_func_oid = InvalidOid;
        /* clean stp save pointer if the outermost function is end. */
        if (u_sess->SPI_cxt._connected == 0) {
            t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
        }
#ifdef ENABLE_MULTIPLE_NODES
        SetSendCommandId(saveSetSendCommandId);
#else
        dopControl.ResetSmp();
        gsplsql_unlock_func_pkg_dependency_all();
#endif
        /* ErrorData could be allocted in SPI's MemoryContext, copy it. */
        oldContext = MemoryContextSwitchTo(oldContext);
        ErrorData *edata = CopyErrorData();
        (void)MemoryContextSwitchTo(oldContext);
        /*
         * Reset error stack to empty since plpgsql_call_handler's recursive depth
         * is more bigger than error stack's.
         */
        FlushErrorState();

        /* destory all the SPI connect created in this PL function. */
        SPI_disconnect(connect);
        /* re-throw the original error messages */
        ReThrowError(edata);
    }
    PG_END_TRY();
    u_sess->plsql_cxt.need_create_depend = save_need_create_depend;
    u_sess->plsql_cxt.is_exec_autonomous = save_is_exec_autonomous;
    u_sess->plsql_cxt.is_pipelined = save_is_pipelined;
    /* clean stp save pointer if the outermost function is end. */
    if (u_sess->SPI_cxt._connected == 0) {
        t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
    }
#ifdef ENABLE_MULTIPLE_NODES
    SetSendCommandId(saveSetSendCommandId);
#else
    dopControl.ResetSmp();
    gsplsql_unlock_func_pkg_dependency_all();
#endif

    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
    if ((rc = SPI_finish()) != SPI_OK_FINISH) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI_finish failed: %s when execute PLSQL function.", SPI_result_code_string(rc))));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        u_sess->opt_cxt.is_stream = outer_is_stream;
        u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
    }
#endif
    UpdateCurrCompilePgObjStatus(save_curr_status);
    if (has_switch) {
        SetUserIdAndSecContext(old_user, save_sec_context);
        u_sess->exec_cxt.cast_owner = InvalidOid;
    }
    
    u_sess->plsql_cxt.cur_func_oid = InvalidOid;
    if (!is_pkg_func && u_sess->plsql_cxt.running_func_oid == saveCallerOid) {
        if (!OidIsValid(savaCallerParentOid) && OidIsValid(firstLevelfuncOid))
            saveCallFromFuncOid(firstLevelfuncOid);
        else
            saveCallFromFuncOid(savaCallerParentOid);
    }
    return retval;
}

/* ----------
 * pltsql_inline_handler
 *
 * Called by PostgreSQL to execute an anonymous code block
 * ----------
 */
PG_FUNCTION_INFO_V1(pltsql_inline_handler);

Datum pltsql_inline_handler(PG_FUNCTION_ARGS)
{
    InlineCodeBlock* codeblock = (InlineCodeBlock*)DatumGetPointer(PG_GETARG_DATUM(0));
    PLpgSQL_function* func = NULL;
    FunctionCallInfoData fake_fcinfo;
    FmgrInfo flinfo;
    Datum retval;
    int rc;
    PGSTAT_INIT_TIME_RECORD();
    bool needRecord = false;
    Oid package_oid = InvalidOid;

    if (u_sess->SPI_cxt._connected == -1) {
        plpgsql_hashtable_clear_invalid_obj();
    }
    _PG_init();

    AssertEreport(IsA(codeblock, InlineCodeBlock), MOD_PLSQL, "Inline code block is required.");

    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        package_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
#ifndef ENABLE_MULTIPLE_NODES
        gsplsql_lock_func_pkg_dependency_all(package_oid, PLSQL_PACKAGE_OBJ);
#endif
    }
    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
#ifdef ENABLE_MOT
    bool needSPIFinish = true;
#endif
    rc = SPI_connect_ext(DestSPI, NULL, NULL, codeblock->atomic ? 0 : SPI_OPT_NONATOMIC);
    if (rc != SPI_OK_CONNECT) {
#ifdef ENABLE_MOT
        if (rc != SPI_ERROR_CONNECT) {
#endif
            ereport(ERROR, (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_connect failed: %s when execute anonymous block.", SPI_result_code_string(rc))));
#ifdef ENABLE_MOT
        } else {
            needSPIFinish = false;
        }
#endif
    }
    PGSTAT_START_PLSQL_TIME_RECORD();

#ifndef ENABLE_MULTIPLE_NODES
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#else
    /* Saves the status of whether to send commandId. */
    bool saveSetSendCommandId = IsSendCommandId();
#endif

    /* Compile the anonymous code block */
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    bool save_is_exec_autonomous = u_sess->plsql_cxt.is_exec_autonomous;
    int save_compile_status = getCompileStatus();
    PG_TRY();
    {
        func = pltsql_compile_inline(codeblock->source_text);
        if (u_sess->plsql_cxt.curr_compile_context != NULL &&
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
            func->pkg_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
        }
    }
    PG_CATCH();
    {
#ifndef ENABLE_MULTIPLE_NODES
        gsplsql_unlock_func_pkg_dependency_all();
        dopControl.ResetSmp();
#endif
        popToOldCompileContext(save_compile_context);
        CompileStatusSwtichTo(save_compile_status);

#ifdef ENABLE_MOT
        if (needSPIFinish) {
#endif
            (void)SPI_finish();
#ifdef ENABLE_MOT
        }
#endif
        PG_RE_THROW();
    }
    PG_END_TRY();
    PGSTAT_END_PLSQL_TIME_RECORD(PL_COMPILATION_TIME);

    func->is_insert_gs_source = u_sess->plsql_cxt.is_insert_gs_source;
    /* Mark packages the function use, so them can't be deleted from under us */
    AddPackageUseCount(func);
    /* Mark the function as busy, just pro forma */
    func->use_count++;

#ifndef ENABLE_MULTIPLE_NODES
        gsplsql_lock_depend_pkg_on_session(func);
#endif
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
    /* save flag for nest plpgsql compile */
    save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    save_compile_status = u_sess->plsql_cxt.compile_status;
    DebugInfo* save_debug_info = func->debug;
    NodeTag old_node_tag = t_thrd.postgres_cxt.cur_command_tag;
    FormatCallStack* saveplcallstack = t_thrd.log_cxt.call_stack;
    PG_TRY();
    {
        if (IsAutonomousTransaction(func->action->isAutonomous)) {
            retval = plpgsql_exec_autonm_function(func, &fake_fcinfo, codeblock->source_text);
        } else {
            Oid old_value = saveCallFromPkgOid(func->pkg_oid);
            u_sess->plsql_cxt.need_init = true;
            retval = plpgsql_exec_function(func, &fake_fcinfo, false);
            restoreCallFromPkgOid(old_value);
            u_sess->plsql_cxt.need_init = true;
        }
    }
    PG_CATCH();
    {
        if (u_sess->SPI_cxt._connected == 0) {
            t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
        }
        t_thrd.log_cxt.call_stack = saveplcallstack;
#ifndef ENABLE_MULTIPLE_NODES
        gsplsql_unlock_func_pkg_dependency_all();
#endif
        /* Decrement package use-count */
        DecreasePackageUseCount(func);

#ifndef ENABLE_MULTIPLE_NODES
        /* debug finished, close debug resource */
        if (func->debug) {
            PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[func->debug->comm->comm_idx];
            /* if debuger is waiting for end msg, send end */
            if (debug_comm->isGmsRunning()) {
                server_send_gms_end_msg(func->debug);
            } else {
                server_send_end_msg(func->debug);
            }
            /* pass opt to upper debug function */
            server_pass_upper_debug_opt(func->debug);
            clean_up_debug_server(func->debug, false, true);
            delete_debug_func(InvalidOid);
        }
        func->debug = save_debug_info;
        /* for restore parent session and automn session package var values */
        (void)processAutonmSessionPkgsInException(func);

        dopControl.ResetSmp();
#endif
        t_thrd.postgres_cxt.cur_command_tag = old_node_tag;
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s clear curr_compile_context because of error.", __func__)));
        /* reset nest plpgsql compile */
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.compile_status = save_compile_status;
        u_sess->plsql_cxt.is_exec_autonomous = save_is_exec_autonomous;
        clearCompileContextList(save_compile_list_length);
        /* AutonomousSession Disconnecting and releasing resources */
        DestoryAutonomousSession(true);
#ifdef ENABLE_MULTIPLE_NODES
        SetSendCommandId(saveSetSendCommandId);
#endif

        if (u_sess->SPI_cxt._connected == 0) {
            plpgsql_hashtable_clear_invalid_obj();
        }

#ifdef ENABLE_MOT
        if (needSPIFinish) {
#endif
            (void)SPI_finish();
#ifdef ENABLE_MOT
        }
#endif
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->plsql_cxt.is_exec_autonomous = save_is_exec_autonomous;
#ifndef ENABLE_MULTIPLE_NODES
    /* debug finished, close debug resource */
    if (func->debug) {
        /* if debuger is waiting for end msg, send end */
        PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[func->debug->comm->comm_idx];
        /* if debuger is waiting for end msg, send end */
        if (debug_comm->isGmsRunning()) {
            server_send_gms_end_msg(func->debug);
        } else {
            server_send_end_msg(func->debug);
        }
        /* pass opt to upper debug function */
        server_pass_upper_debug_opt(func->debug);
        clean_up_debug_server(func->debug, false, true);
        delete_debug_func(InvalidOid);
    }
    func->debug = save_debug_info;
#endif
    t_thrd.postgres_cxt.cur_command_tag = old_node_tag;
    if (u_sess->SPI_cxt._connected == 0) {
        t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
    }

#ifdef ENABLE_MULTIPLE_NODES
    SetSendCommandId(saveSetSendCommandId);
#endif

#ifdef ENABLE_MULTIPLE_NODES
    SetSendCommandId(saveSetSendCommandId);
#else
    gsplsql_unlock_func_pkg_dependency_all();
    dopControl.ResetSmp();
#endif

    /* Disconnecting and releasing resources */
    DestoryAutonomousSession(false);

    PGSTAT_END_PLSQL_TIME_RECORD(PL_EXECUTION_TIME);
    if (AUDIT_EXEC_ENABLED) {
        auditExecAnonymousBlock(func);
    }


    /* set cursor optin to null which opened in this block */
    ResetPortalCursor(GetCurrentSubTransactionId(), func->fn_oid, func->use_count, false);

    /* Function should now have no remaining use-counts ... */
    func->use_count--;
    DecreasePackageUseCount(func);
    AssertEreport(func->use_count == 0, MOD_PLSQL, "Function should now have no remaining use-counts");

    /* ... so we can free subsidiary storage */
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        func->pkg_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
        plpgsql_free_function_memory(func);
    } else {
        plpgsql_free_function_memory(func);
    }
    if (u_sess->SPI_cxt._connected == 0) {
        plpgsql_hashtable_clear_invalid_obj();
    }

    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
#ifdef ENABLE_MOT
    if (needSPIFinish) {
#endif
        if (((rc = SPI_finish()) != SPI_OK_FINISH)) {
            ereport(ERROR, (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_finish failed: %s when execute anonymous block.", SPI_result_code_string(rc))));
        }
#ifdef ENABLE_MOT
    }
#endif

    return retval;
}

/* ----------
 * pltsql_validator
 *
 * This function attempts to validate a PL/pgSQL function at
 * CREATE FUNCTION time.
 * ----------
 */
PG_FUNCTION_INFO_V1(pltsql_validator);

Datum pltsql_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    Form_pg_proc proc = NULL;
    char functyptype;
    int numargs;
    Oid* argtypes = NULL;
    char** argnames;
    char* argmodes = NULL;
    bool is_dml_trigger = false;
    bool is_event_trigger = false;
    Oid saveCallerOid = InvalidOid;
    Oid savaCallerParentOid = InvalidOid;
    bool is_pkg_func = false;
    
    int i;
    /*
     * 3 means the number of arguments of function plpgsql_validator, while 'is_place' is the third one,
     * and 2 is the postion of 'is_place' in PG_FUNCTION_ARGS
     */
    bool replace = false;
    if (PG_NARGS() >= 3) {
        replace = PG_GETARG_BOOL(2);
    }

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
            is_dml_trigger = true;                
        } else if (proc->prorettype == EVTTRIGGEROID) { 
            is_event_trigger = true;    
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
    bool save_curr_status = GetCurrCompilePgObjStatus();
    /* Postpone body checks if !u_sess->attr.attr_sql.check_function_bodies */
    if (u_sess->attr.attr_sql.check_function_bodies) {
        FunctionCallInfoData fake_fcinfo;
        FmgrInfo flinfo;
        TriggerData dml_trigdata;
        EventTriggerData event_trigdata;
        PLpgSQL_function* func = NULL;
        /*
         * Set up a fake fcinfo with just enough info to satisfy
         * pltsql_compile().
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
        if (is_dml_trigger) {
            errorno = memset_s(&dml_trigdata, sizeof(dml_trigdata), 0, sizeof(dml_trigdata));
            securec_check(errorno, "", "");
            dml_trigdata.type = T_TriggerData;
            fake_fcinfo.context = (Node*)&dml_trigdata;
        } else if (is_event_trigger) {
            MemSet(&event_trigdata, 0, sizeof(event_trigdata));
            event_trigdata.type = T_EventTriggerData;
            fake_fcinfo.context = (Node *) &event_trigdata;
        }
        /* save flag for nest plpgsql compile */
        PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
        int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
        int save_compile_status = u_sess->plsql_cxt.compile_status;
        /* Test-compile the function */
        PG_TRY();
        {
            SetCurrCompilePgObjStatus(true);
            u_sess->parser_cxt.isCreateFuncOrProc = true;
            func = pltsql_compile(&fake_fcinfo, true);
            u_sess->parser_cxt.isCreateFuncOrProc = false;
            if (func != NULL) {
                if (OidIsValid(func->pkg_oid)) {
                    is_pkg_func = true;
                }
                // ignore in case of function has subprogram to update the running_func_oid.
                if (!is_pkg_func && u_sess->plsql_cxt.running_func_oid != func->fn_oid) {
                    saveCallFromFuncOid(func->parent_oid);
                }
                saveCallerOid = func->fn_oid;
                savaCallerParentOid = func->parent_oid;
            }
        }
        PG_CATCH();
        {
            SetCurrCompilePgObjStatus(save_curr_status);
#ifndef ENABLE_MULTIPLE_NODES
            u_sess->parser_cxt.isPerform = false;
            bool insertError = (u_sess->attr.attr_common.plsql_show_all_error ||
                                    !u_sess->attr.attr_sql.check_function_bodies) &&
                                    u_sess->plsql_cxt.isCreateFunction;
            if (!SKIP_GS_SOURCE && !IsInitdb && u_sess->plsql_cxt.isCreateFunction) {
                ProcInsertGsSource(funcoid, false);
            }
            if (insertError) {
                if (!IsInitdb) {
                    int rc = CompileWhich();
                    if (rc == PLPGSQL_COMPILE_PACKAGE || rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
                        InsertError(u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid);
                    }
                    u_sess->plsql_cxt.isCreateFunction = false;
                    u_sess->plsql_cxt.isCreateTypeBody = false;
                    u_sess->plsql_cxt.createFunctionOid = InvalidOid;
                    saveCallFromFuncOid(InvalidOid);
                    u_sess->plsql_cxt.isCreateFuncSubprogramBody = false;
                }
            }
#endif
            u_sess->plsql_cxt.errorList = NULL;
            u_sess->plsql_cxt.procedure_first_line = 0;
            u_sess->plsql_cxt.procedure_start_line = 0;
            u_sess->plsql_cxt.package_as_line = 0;
            u_sess->plsql_cxt.package_first_line = 0;
            u_sess->plsql_cxt.isCreateFunction = false;
            u_sess->plsql_cxt.isCreateTypeBody = false;
            u_sess->plsql_cxt.typfunckind = OBJECTTYPE_NULL_PROC;
            u_sess->plsql_cxt.createFunctionOid = InvalidOid;
            u_sess->plsql_cxt.isCreateFuncSubprogramBody = false;
            ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
                errmsg("%s clear curr_compile_context because of error.", __func__)));
            /* reset nest plpgsql compile */
            u_sess->plsql_cxt.curr_compile_context = save_compile_context;
            u_sess->plsql_cxt.compile_status = save_compile_status;
#ifndef ENABLE_MULTIPLE_NODES
            if (!u_sess->attr.attr_common.plsql_show_all_error) {
                clearCompileContextList(save_compile_list_length);
            }
#else
            clearCompileContextList(save_compile_list_length);
#endif
            u_sess->parser_cxt.isCreateFuncOrProc = false;
            PG_RE_THROW();
        }
        PG_END_TRY();
        bool isNotComipilePkg = u_sess->plsql_cxt.curr_compile_context == NULL ||
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL;
        bool isNotCompileSubprogram = (func && !OidIsValid(func->parent_oid));
        if (!IsInitdb && (
#ifdef ENABLE_MULTIPLE_NODES
            !IS_MAIN_COORDINATOR ||
#endif
            u_sess->attr.attr_common.enable_full_encryption) &&
            isNotComipilePkg && isNotCompileSubprogram) {
                /*
                 * set ClientAuthInProgress to prevent warnings from the parser
                 * to be sent to client
                 */
                bool saved_client_auth = u_sess->ClientAuthInProgress;
                u_sess->ClientAuthInProgress = true;
                pl_validate_function_sql(func, replace);
                u_sess->ClientAuthInProgress = saved_client_auth;
        }
        UpdateCurrCompilePgObjStatus(save_curr_status);
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (!IsInitdb && u_sess->plsql_cxt.isCreateFunction && !u_sess->plsql_cxt.running_func_oid) {
        ProcInsertGsSource(funcoid, true);
    }
#endif
    if (!is_pkg_func && u_sess->plsql_cxt.running_func_oid == saveCallerOid) {
        saveCallFromFuncOid(savaCallerParentOid);
    }

    PG_RETURN_VOID();
}

static void get_func_actual_rows(Oid funcid, uint32* rows)
{
    int headerlines = 0;
    char* funcdef = pg_get_functiondef_worker(funcid, &headerlines);
    if (funcdef == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("Unexpected NULL value for function definition"),
                errdetail("N/A"),
                errcause("Function definition is NULL"),
                erraction("Re-create the function and retry")));
    }
    int nLine = 0;
    for (unsigned int i = 0; i < strlen(funcdef); i++) {
        if (funcdef[i] == '\n') {
            nLine++;
        }
    }
    *rows = nLine;
    pfree(funcdef);
}

static void get_proc_coverage(Oid func_oid, int* coverage)
{
    PGconn* conn = NULL;
    char* dbName = u_sess->proc_cxt.MyProcPort->database_name;
    conn = LoginDatabase("localhost", g_instance.attr.attr_network.PostPortNumber,
    NULL, NULL, "postgres", "gs_clean", "auto");

    if (PQstatus(conn) == CONNECTION_OK) {
        char proId[MAX_INT32_LEN];
        char* proNname = get_func_name(func_oid);
        StringInfoData pro_querys;
        StringInfoData pro_canbreak;
        StringInfoData pro_coverage;
        int rc = sprintf_s(proId, MAX_INT32_LEN, "%u", func_oid);
        securec_check_ss(rc, "\0", "\0");
        const char* infoCodeValues[1];
        infoCodeValues[0] = proId;
        uint32 rows;
        int headerLines;
        CodeLine* infoCode = debug_show_code_worker(func_oid, &rows, &headerLines);
        
        initStringInfo(&pro_querys);
        initStringInfo(&pro_canbreak);
        initStringInfo(&pro_coverage);
        appendStringInfo(&pro_querys, "%s", infoCode[0].code);
        appendStringInfo(&pro_canbreak, "{%s", infoCode[0].canBreak ? "true" : "false");
        appendStringInfo(&pro_coverage, "{%d", coverage[0]);
        /* debug_show_code_worker set int to rows */
        for (int i = 1; i < (int)rows; ++i) {
            appendStringInfo(&pro_querys, "\n %s", infoCode[i].code);
            appendStringInfo(&pro_canbreak, ",%s", infoCode[i].canBreak ? "true" : "false");
            appendStringInfo(&pro_coverage, ",%d", coverage[i]);
        }
        appendStringInfoString(&pro_canbreak, "}");
        appendStringInfoString(&pro_coverage, "}");

        const char *insertQuery = "INSERT INTO coverage.proc_coverage"
                                  "(pro_oid, pro_name, db_name, pro_querys, pro_canbreak, coverage) "
                                  "VALUES ($1, $2, $3, $4, $5, $6)";
        const Oid paramTypes[6] = {OIDOID, TEXTOID, TEXTOID, TEXTOID, BOOLARRAYOID, INT4ARRAYOID};
        const int paramFormats[6] = {0};
        const char *paramValues[6];
        paramValues[0] = proId;
        paramValues[1] = proNname;
        paramValues[2] = dbName;
        paramValues[3] = pro_querys.data;
        paramValues[4] = pro_canbreak.data;
        paramValues[5] = pro_coverage.data;

        PGresult *res = PQexecParams(conn, insertQuery, 6, paramTypes, paramValues, NULL, paramFormats, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            ereport(WARNING,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("Insert failed: %s", PQerrorMessage(conn))));
        }
        FreeStringInfo(&pro_querys);
        FreeStringInfo(&pro_canbreak);
        FreeStringInfo(&pro_coverage);
        pfree(infoCode);
        PQclear(res);
        PQfinish(conn);
    } else {
        ereport(WARNING,
            (errcode(ERRCODE_INVALID_STATUS), errmsg("Database connect failed: %s", PQerrorMessage(conn))));
    }
}
