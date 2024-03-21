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
 *	  src/pl/plpgsql/src/pl_handler.c
 *
 * -------------------------------------------------------------------------
 */

#include "utils/plpgsql_domain.h"
#include "utils/plpgsql.h"
#include "utils/fmgroids.h"
#include "utils/pl_package.h"
#include "auditfuncs.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/gs_package.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/indexing.h"
#include "catalog/catalog.h"
#include "commands/proclang.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/autonomoustransaction.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/acl.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "utils/timestamp.h"
#include "executor/spi_priv.h"
#include "distributelayer/streamMain.h"
#include "commands/event_trigger.h"
#include "catalog/pg_object.h"
#include "catalog/gs_dependencies_fn.h"

#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#endif

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
#define MAXSTRLEN ((1 << 11) - 1)
#define HTML_LT_LEN 4
#define HTML_AMP_LEN 5
#define HTML_QUOT_LEN 6
static void init_do_stmt(PLpgSQL_package *pkg, bool isCreate, ListCell *cell, int oldCompileStatus,
                  PLpgSQL_compile_context *curr_compile, List *temp_tableof_index, MemoryContext oldcxt);
static void get_func_actual_rows(Oid funcid, uint32* rows);
static void get_proc_coverage(Oid func_oid, int* coverage);
static void generate_procoverage_html(int beginId, int endId, StringInfoData* result, bool isDefault);
static void generate_procoverage_table(char* value, StringInfoData* result, int index,
                  List** coverage_array, List** pro_querys);
static void generate_procoverage_rows(StringInfoData* result, List* coverage_array, List* pro_querys);
static void deconstruct_coverage_array(List** coverage_array, char* array_string);
static void deconstruct_querys_array(List** pro_querys, const char* querys_string);
static char* replace_html_entity(const char* input);

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

#ifndef ENABLE_MULTIPLE_NODES
static void processError(CreateFunctionStmt* stmt, enum FunctionErrorType ErrorType)
{
    int lines = stmt->startLineNumber + u_sess->plsql_cxt.package_first_line - 1;
    InsertErrorMessage("function declared in package specification "
                       "and package body must be the same", 0, false, lines);
    if (ErrorType == FuncitonDefineError) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmodule(MOD_PLSQL),
                errmsg("function declared in package specification and "
                "package body must be the same, function: %s",
                NameListToString(stmt->funcname))));
    } else if (ErrorType == FunctionDuplicate) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmodule(MOD_PLSQL),
                errmsg("function declared duplicate: %s",
                NameListToString(stmt->funcname))));
    } else if (ErrorType == FunctionUndefined) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmodule(MOD_PLSQL),
                errmsg("function undefined: %s",
                NameListToString(stmt->funcname))));
    } else if (ErrorType == FunctionReturnTypeError) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmodule(MOD_PLSQL),
                errmsg("function return type must be consistent: %s",
                NameListToString(stmt->funcname))));
    }
}

void InsertGsSource(Oid objId, Oid nspid, const char* name, const char* type, bool status)
{
    bool notInsert = u_sess->attr.attr_common.upgrade_mode != 0 || IsSystemNamespace(nspid) || 
        IsToastNamespace(nspid) || IsCStoreNamespace(nspid) || 
        IsPackageSchemaOid(nspid) || SKIP_GS_SOURCE;
    if (notInsert || t_thrd.log_cxt.errordata_stack_depth > ERRORDATA_STACK_SIZE - 2) {
        return;
    }
    if (g_instance.attr.attr_storage.max_concurrent_autonomous_transactions <= 0) {
        ereport(WARNING, (errcode(ERRCODE_PLPGSQL_ERROR),
            errmsg("The stored procedure creation information cannot be inserted into the gs source."),
            errdetail("The value of MAX_CONCURRENT_AUTONOMOUS_TRANSACTIONS must be greater than 0."),
            errcause("gs_source insertion depends on autonomous transaction."),
            erraction("set MAX_CONCURRENT_AUTONOMOUS_TRANSACTIONS = 10.")));
        return;
    }
    Oid userId = (Oid)u_sess->misc_cxt.CurrentUserId;
    char statusChr = status ? 't' : 'f';
    /* User statement may contain sensitive information like password */
    if (u_sess->plsql_cxt.debug_query_string == NULL) {
        return;
    }
    const char* source = maskPassword(u_sess->plsql_cxt.debug_query_string);
    if (source == NULL) {
        source = u_sess->plsql_cxt.debug_query_string;
    }
    /* Execute autonomous transaction call for logging purpose */
    StringInfoData str;
    char* tmp = EscapeQuotes(name);
    initStringInfo(&str);
    appendStringInfoString(&str,
        "declare\n"
        "PRAGMA AUTONOMOUS_TRANSACTION;\n"
        "oldId int:=0;"
        "objId int:=0;"
        "allNum int:=0;\n"
        "begin\n ");
    appendStringInfo(&str,
        "select count(*) from dbe_pldeveloper.gs_source into allNum where "
		"nspid=%u and name=\'%s\' and type=\'%s\';", nspid, tmp, type);
    appendStringInfo(&str,
        "if allNum > 0 then "
        "select id from dbe_pldeveloper.gs_source into oldId where "
        "nspid=%u and name=\'%s\' and type=\'%s\';"
        "objId := oldId; "
        "else "
        "objId := %u;"
        "end if;", nspid, tmp, type, objId);
    appendStringInfo(&str,
        "delete from DBE_PLDEVELOPER.gs_source where nspid=%u and name=\'%s\' and type = \'%s\';\n",
        nspid, tmp, type);
    appendStringInfo(&str,
        "delete from DBE_PLDEVELOPER.gs_source where nspid=%u and name=\'%s\' and type = \'%s\';\n",
        nspid, tmp, type);
    if (!u_sess->attr.attr_common.plsql_show_all_error || status)  {
        appendStringInfo(&str,
            "delete from DBE_PLDEVELOPER.gs_errors where " 
            "nspid=%u and name=\'%s\' and type = \'%s\';\n",
            nspid, tmp, type);
        if (!strcmp(type, "package")) {
            appendStringInfo(&str,
                "delete from DBE_PLDEVELOPER.gs_errors where " 
                "nspid=%u and name=\'%s\' and type = \'package body\';\n",
                nspid, tmp);
        }
    }
    if (status && !strcmp(type, "package")) {
        appendStringInfo(&str,
            "delete from DBE_PLDEVELOPER.gs_source where " 
            "nspid=%u and name=\'%s\' and type = \'package body\';\n",
            nspid, tmp);
    }
    appendStringInfo(&str,
        "insert into DBE_PLDEVELOPER.gs_source values(objId, %u, %u,\'%s\', \'%s\', \'%c\', $gssource$%s$gssource$);\n",
        userId, nspid, tmp, type, statusChr, source);
    appendStringInfoString(&str,
        "EXCEPTION WHEN OTHERS THEN NULL; \n");
    appendStringInfoString(&str, "end;");
    List* rawParseList = raw_parser(str.data);
    pfree_ext(tmp);
    pfree_ext(str.data);
    DoStmt* stmt = (DoStmt *)linitial(rawParseList);
    int save_compile_status = getCompileStatus();
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    PG_TRY();
    {
        (void)CompileStatusSwtichTo(NONE_STATUS);
        u_sess->plsql_cxt.curr_compile_context = NULL;
        u_sess->plsql_cxt.is_insert_gs_source = true;
        ExecuteDoStmt(stmt, true);
        u_sess->plsql_cxt.is_insert_gs_source = false;
    }
    PG_CATCH();
    {
        if (temp != NULL) {
            MemoryContextSwitchTo(temp);
        }
        (void)CompileStatusSwtichTo(save_compile_status);
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.is_insert_gs_source = false;
        clearCompileContextList(save_compile_list_length);
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->plsql_cxt.curr_compile_context = save_compile_context;
    (void)CompileStatusSwtichTo(save_compile_status);
    if (temp != NULL) {
        MemoryContextSwitchTo(temp);
    }
}
static void PkgInsertGsSource(Oid pkgOid, bool isSpec, bool status)
{
    HeapTuple pkgTup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkgOid));
    Form_gs_package pkgStruct = (Form_gs_package)GETSTRUCT(pkgTup);
    if (!HeapTupleIsValid(pkgTup)) {
        ereport(ERROR,  (errmodule(MOD_PLSQL),  errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for package %u, while collecting gs_source log", pkgOid)));
    }
    char* name = NameStr(pkgStruct->pkgname);
    Oid nspid = pkgStruct->pkgnamespace;
    const char* type = (isSpec) ? ("package") : ("package body");
    InsertGsSource(pkgOid, nspid, name, type, status);
    ReleaseSysCache(pkgTup);
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
    Datum packageIdDatum = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_packageid, &isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The prokind of the function is null"),
                errhint("Check whether the definition of the function is complete in the pg_proc system table.")));
    }
    Oid packageOid = DatumGetObjectId(packageIdDatum);
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
    const char* type = (prokind == PROKIND_PROCEDURE) ? ("procedure") : ("function");
    if (strcasecmp(get_language_name((Oid)procStruct->prolang), "plpgsql") != 0) {
        ReleaseSysCache(procTup);
        return;
    }
    InsertGsSource(funcOid, nspid, name, type, status);

    ReleaseSysCache(procTup);

    return;
}
#endif


/*
 * process different situation in package,for example,duplicate declartion,duplicate definition,
 * and only declartion no definition.
 * the recordArr record the proc list which has been declared and which has been defined,
 * for example
 * recordArr[0] = 1; it means the location 0 is pairing location 1, so the location 1 value must be 0;
 * so recordArr[record[1]] = 1;
 */
#ifndef ENABLE_MULTIPLE_NODES
void processPackageProcList(PLpgSQL_package* pkg)
{
    ListCell* cell = NULL;
    CreateFunctionStmt** stmtArray = (CreateFunctionStmt**)palloc0(list_length(pkg->proc_list) *
                                        sizeof(CreateFunctionStmt*));
    int* recordArr = (int*)palloc0(list_length(pkg->proc_list) * sizeof(int));
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
        if (stmtArray[0]->isFunctionDeclare && pkg->is_bodycompiled) {
            processError(stmtArray[0], FunctionUndefined);
        } else if (stmtArray[0]->isFunctionDeclare && !pkg->is_bodycompiled) {
            return;
        }
    }
    for (int j = 0; j < funcStmtLength - 1; j++) {
        for (int k = j + 1; k < funcStmtLength; k++) {
            char* funcname1 = NULL;
            char* funcname2 = NULL;
            char* returnType1 = NULL;
            char* returnType2 = NULL;
            CreateFunctionStmt* funcStmt1 = stmtArray[j];
            CreateFunctionStmt* funcStmt2 = stmtArray[k];
            List* funcnameList1 = funcStmt1->funcname;
            List* funcnameList2 = funcStmt2->funcname;
            bool returnTypeSame = false;
            funcname1 = getFuncName(funcnameList1);
            funcname2 = getFuncName(funcnameList2);
            if (strcmp(funcname1, funcname2) != 0) {
                /* ignore return type when two procedure is not same */
                continue;
            }
            if (funcStmt1->returnType != NULL) {
                returnType1 = TypeNameToString((TypeName*)funcStmt1->returnType);
            } else {
                returnType1 = "void";
            }
            if (funcStmt2->returnType != NULL) {
                returnType2 = TypeNameToString((TypeName*)funcStmt2->returnType);
            } else {
                returnType2 = "void";
            }
            if (!isSameArgList(funcStmt1, funcStmt2)) {
                continue;
            }
            if (!isSameParameterList(funcStmt1->options, funcStmt2->options)) {
                processError(funcStmt1, FuncitonDefineError);
            }
            if (strcmp(returnType1, returnType2) != 0) {
                returnTypeSame = false;
            } else {
                returnTypeSame = true;
            }
            if (!returnTypeSame && (funcStmt1->isFunctionDeclare^funcStmt2->isFunctionDeclare)) {
                processError(funcStmt1, FunctionReturnTypeError);
            }
            if (funcStmt1->isProcedure != funcStmt2->isProcedure) {
                processError(funcStmt1, FuncitonDefineError);
            }
            if (funcStmt1->isFunctionDeclare && funcStmt2->isFunctionDeclare) {
                processError(funcStmt1, FunctionDuplicate);
            }
            if (!(funcStmt1->isFunctionDeclare || funcStmt2->isFunctionDeclare)) {
                processError(funcStmt1, FuncitonDefineError);
            }
            if (recordArr[j] == 0) {
                if (recordArr[k] != 0) {
                    processError(stmtArray[j], FuncitonDefineError);
                }
                recordArr[j] = k;
                recordArr[k] = j;
            } else if(recordArr[recordArr[j]] != j) {
                processError(stmtArray[j], FuncitonDefineError);
            }
            if (!funcStmt1->isPrivate || !funcStmt2->isPrivate) {
                funcStmt1->isPrivate = false;
                funcStmt2->isPrivate = false;
            }
        }
    }
    i = 0;
    for(i = 0; i < funcStmtLength; i++) {
        if (stmtArray[i]->isFunctionDeclare && pkg->is_bodycompiled) {
            if (recordArr[i] == 0) {
                errno_t rc;
                char message[MAXSTRLEN];
                rc = sprintf_s(message, MAXSTRLEN,
                                "Function definition not found: %s",
                                NameListToString(stmtArray[i]->funcname));
                securec_check_ss(rc, "\0", "\0");
                InsertErrorMessage(message, stmtArray[i]->startLineNumber);
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmodule(MOD_PLSQL),
                        errmsg("Function definition not found: %s", NameListToString(stmtArray[i]->funcname))));
            }
        }
    }
    pfree(recordArr);
    pfree(stmtArray);
}
#endif



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

            EmitWarningsOnPlaceholders("plpgsql");
            RegisterXactCallback(plpgsql_xact_cb, NULL);
            RegisterSubXactCallback(plpgsql_subxact_cb, NULL);
            /* Set up a rendezvous point with optional instrumentation plugin */
            u_sess->plsql_cxt.plugin_ptr = (PLpgSQL_plugin**)find_rendezvous_variable("PLpgSQL_plugin");

            u_sess->SPI_cxt.cur_tableof_index = (TableOfIndexPass*)MemoryContextAllocZero(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(TableOfIndexPass));
            u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = InvalidOid;
            u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = NULL;
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
            pfree_ext(u_sess->SPI_cxt.cur_tableof_index);
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

    return package_oid;
}

static bool CheckSelectElementParse(SelectStmt* stmt);

static bool CheckSelectTargetListParse(List * TargetList)
{
    bool result = true;

    if (TargetList == NULL) {
        return false;
    }

    ListCell *lcell = NULL;

    foreach(lcell, TargetList) {
        Node *node = (Node *)lfirst(lcell);
        switch (nodeTag(node)) {
            case T_ResTarget: {
                ResTarget *res_target = (ResTarget *)node;
                if (nodeTag(res_target->val) == T_SubLink) {
                    SubLink* sublink = (SubLink*) res_target->val;
                    SelectStmt* stmt = (SelectStmt*)sublink->subselect;
                    result = CheckSelectElementParse(stmt);
                } else {
                    result = false;
                }
            }
                break;
            case T_RangeFunction: {
                result = false;
            }
                break;

            default:
                result = true;
                break;
        }
    }
    return result;
}


/* if has select into clause, return true. */
static bool CheckSelectElementParse(SelectStmt* stmt)
{
    if (stmt != NULL) {
        /* don't support select into... */
        if (stmt->intoClause ||
            stmt->startWithClause ||
            stmt->whereClause ||
            stmt->groupClause ||
            stmt->havingClause ||
            stmt->windowClause ||
            stmt->withClause ||
            stmt->sortClause ||
            stmt->limitOffset) {
            return true;
        } else {
            return CheckSelectElementParse(stmt->larg) ||
                   CheckSelectElementParse(stmt->rarg) ||
                   CheckSelectTargetListParse(stmt->targetList) ||
                   CheckSelectTargetListParse(stmt->fromClause);
        }
    }
    return false;
}

extern bool CheckElementParsetreeTag(Node* parsetree)
{
    bool result = true;

    if (parsetree == NULL) {
        return false;
    }

    switch (nodeTag(parsetree)) {
         /* raw plannable queries */
        case T_InsertStmt:
        case T_DeleteStmt:
        case T_UpdateStmt:
        case T_MergeStmt:
            result = true;
            break;
        case T_SelectStmt:
            if (CheckSelectElementParse((SelectStmt*)parsetree)) {
                result = true;
            } else {
                result = false;
            }
            break;
        case T_DoStmt: {
            result = false;
        }
            break;
        default:
            result = true;
            break;
    }

    return result;
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
    PGSTAT_INIT_TIME_RECORD();
    bool needRecord = false;
    PLpgSQL_package* pkg = NULL;
    MemoryContext oldContext = CurrentMemoryContext;
    int pkgDatumsNumber = 0;
    bool savedisAllowCommitRollback = true;
    bool enableProcCoverage = u_sess->attr.attr_common.enable_proc_coverage;
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
    bool save_curr_status = GetCurrCompilePgObjStatus();
    bool save_is_exec_autonomous = u_sess->plsql_cxt.is_exec_autonomous;
    PG_TRY();
    {
        PGSTAT_START_PLSQL_TIME_RECORD();
        /*
         * save running package value
         */
        firstLevelPkgOid = saveCallFromPkgOid(package_oid);
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
            func = plpgsql_compile(fcinfo, false);
            if (enable_plpgsql_gsdependency_guc() && func != NULL) {
                SetPgObjectValid(func_oid, OBJECT_TYPE_PROC, GetCurrCompilePgObjStatus());
                if (!GetCurrCompilePgObjStatus()) {
                    ereport(WARNING, (errmodule(MOD_PLSQL),
                        errmsg("Function %s recompile with compilation errors, please use ALTER COMPILE to recompile.",
                               get_func_name(func_oid))));
                }
            }
        }
        if (func->fn_readonly) {
            stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_IMMUTABLE);
        }
        func->is_plpgsql_func_with_outparam = is_function_with_plpgsql_language_and_outparam(func->fn_oid);

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
                retval = PointerGetDatum(plpgsql_exec_trigger(func, (TriggerData*)fcinfo->context));
	    } else if (CALLED_AS_EVENT_TRIGGER(fcinfo)) {
                plpgsql_exec_event_trigger(func,
                    (EventTriggerData *) fcinfo->context);

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
                            errcause("package have autonmous function"),
                            erraction("redefine package")));
                }

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
                server_send_end_msg(func->debug);
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

            restoreCallFromPkgOid(secondLevelPkgOid);
            ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
                errmsg("%s clear curr_compile_context because of error.", __func__)));
            /* reset nest plpgsql compile */
            u_sess->plsql_cxt.curr_compile_context = save_compile_context;
            u_sess->plsql_cxt.compile_status = save_compile_status;
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
            server_send_end_msg(func->debug);
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
    int save_compile_status = getCompileStatus();
    PG_TRY();
    {
        func = plpgsql_compile_inline(codeblock->source_text);
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
            retval = plpgsql_exec_function(func, &fake_fcinfo, false);
            restoreCallFromPkgOid(old_value);
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
            /* if debuger is waiting for end msg, send end */
            server_send_end_msg(func->debug);
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
        clearCompileContextList(save_compile_list_length);
        /* AutonomousSession Disconnecting and releasing resources */
        DestoryAutonomousSession(true);
#ifdef ENABLE_MULTIPLE_NODES
        SetSendCommandId(saveSetSendCommandId);
#endif

        if (u_sess->SPI_cxt._connected == 0) {
            plpgsql_hashtable_clear_invalid_obj();
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
#ifndef ENABLE_MULTIPLE_NODES
    /* debug finished, close debug resource */
    if (func->debug) {
        /* if debuger is waiting for end msg, send end */
        server_send_end_msg(func->debug);
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
    bool is_dml_trigger = false;
    bool is_event_trigger = false;
    
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
            func = plpgsql_compile(&fake_fcinfo, true);
            u_sess->parser_cxt.isCreateFuncOrProc = false;
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
                }
            }
#endif
            u_sess->plsql_cxt.errorList = NULL;
            u_sess->plsql_cxt.procedure_first_line = 0;
            u_sess->plsql_cxt.procedure_start_line = 0;
            u_sess->plsql_cxt.package_as_line = 0;
            u_sess->plsql_cxt.package_first_line = 0;
            u_sess->plsql_cxt.isCreateFunction = false;
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

        if (!IsInitdb && (
#ifdef ENABLE_MULTIPLE_NODES
            !IS_MAIN_COORDINATOR ||
#endif
            u_sess->attr.attr_common.enable_full_encryption) &&
            isNotComipilePkg) {
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
    if (!IsInitdb && u_sess->plsql_cxt.isCreateFunction) {
        ProcInsertGsSource(funcoid, true);
    }
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
    Oid* savedPseudoCurrentUserId = u_sess->misc_cxt.Pseudo_CurrentUserId;
    _PG_init();
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
    /* save flag for nest plpgsql compile */
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    int save_compile_status = u_sess->plsql_cxt.compile_status;
    /* Postpone body checks if !u_sess->attr.attr_sql.check_function_bodies */
    PG_TRY();
    {
        pkg = plpgsql_pkg_compile(packageOid, true, isSpec, isCreate);
        u_sess->misc_cxt.Pseudo_CurrentUserId = savedPseudoCurrentUserId;
    }
    PG_CATCH();
    {
#ifndef ENABLE_MULTIPLE_NODES
        if (isCreate) {
            SPI_STACK_LOG("finish", NULL, NULL);
            SPI_finish();
            if (!IsInitdb) {
                PkgInsertGsSource(packageOid, isSpec, false);
            }
        }
#endif
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s clear curr_compile_context because of error.", __func__)));
        /* reset nest plpgsql compile */
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.compile_status = save_compile_status;
        clearCompileContextList(save_compile_list_length);
        u_sess->plsql_cxt.running_pkg_oid = InvalidOid;
        u_sess->plsql_cxt.procedure_first_line = 0;
        u_sess->plsql_cxt.procedure_start_line = 0;
        u_sess->plsql_cxt.package_first_line = 0;
        u_sess->plsql_cxt.package_as_line = 0;   
        u_sess->parser_cxt.in_package_function_compile = false;
        u_sess->misc_cxt.Pseudo_CurrentUserId = savedPseudoCurrentUserId;
        PG_RE_THROW();
    }
    PG_END_TRY();
#ifndef ENABLE_MULTIPLE_NODES
    if (isCreate && !IsInitdb) {
        PkgInsertGsSource(packageOid, isSpec, true);
    }
#endif
    u_sess->plsql_cxt.errorList = NULL;
    u_sess->plsql_cxt.procedure_first_line = 0;
    u_sess->plsql_cxt.procedure_start_line = 0;
    u_sess->plsql_cxt.package_as_line = 0;
    u_sess->plsql_cxt.package_first_line = 0;
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
#ifndef ENABLE_MULTIPLE_NODES
void PackageInit(PLpgSQL_package* pkg, bool isCreate, bool isSpec, bool isNeedCompileFunc) 
{
    if (likely(pkg != NULL)) {
        if (likely(pkg->isInit)) {
            return;
        }
    }
    int package_line = 0;
    package_line = u_sess->plsql_cxt.package_as_line;
    Oid old_value = saveCallFromPkgOid(pkg->pkg_oid);
    PushOverrideSearchPath(pkg->pkg_searchpath);
    ListCell* cell = NULL;
    int oldCompileStatus = getCompileStatus();
    CompileStatusSwtichTo(COMPILIE_PKG);
    
    PLpgSQL_compile_context* curr_compile = createCompileContext("PL/pgSQL package context");
    SPI_NESTCOMPILE_LOG(curr_compile->compile_cxt);
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt != NULL) {
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    u_sess->plsql_cxt.curr_compile_context = curr_compile;
    pushCompileContext();
    curr_compile->plpgsql_curr_compile_package = pkg;
    checkCompileMemoryContext(pkg->pkg_cxt);
    MemoryContext oldcxt = MemoryContextSwitchTo(pkg->pkg_cxt);
    if (isCreate) {
        int exception_num = 0;
        curr_compile->compile_tmp_cxt = oldcxt;
        processPackageProcList(pkg);
        foreach(cell, pkg->proc_list)
        {
            if (IsA(lfirst(cell), CreateFunctionStmt)) {
                CreateFunctionStmt* funcStmt = (CreateFunctionStmt*)lfirst(cell);
                if (!funcStmt->isExecuted) {
                    (void)CompileStatusSwtichTo(COMPILIE_PKG_FUNC);
                    char* funcStr = funcStmt->queryStr;
                    PG_TRY();
                    {
                        CreateFunction(funcStmt, funcStr, pkg->pkg_oid);
                    }
                    PG_CATCH();
                    {
                        set_create_plsql_type_end();
                        if (u_sess->plsql_cxt.create_func_error) {
                            u_sess->plsql_cxt.create_func_error = false;
                            exception_num += 1;
                            FlushErrorState();
                        } else {
                            PG_RE_THROW();
                        }
                    }
                    PG_END_TRY();
                    funcStmt->isExecuted = true;
                    (void)CompileStatusSwtichTo(oldCompileStatus);
                }
            }
        }
        if (exception_num > 0) {
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Debug mod,create procedure has error."),
                    errdetail("N/A"),
                    errcause("compile procedure error."),
                    erraction("check procedure error and redefine procedure")));
        }
    } else {
        if (pkg->is_bodycompiled && !isSpec && isNeedCompileFunc) {
            (void)CompileStatusSwtichTo(COMPILIE_PKG_FUNC);
            curr_compile->compile_tmp_cxt = oldcxt;
            FunctionInPackageCompile(pkg);
            (void)CompileStatusSwtichTo(oldCompileStatus);
        }
    }
    (void*)MemoryContextSwitchTo(oldcxt);
    if (u_sess->attr.attr_common.plsql_show_all_error) {
        PopOverrideSearchPath();
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s finish compile(show_all_error), level: %d",
                __func__, list_length(u_sess->plsql_cxt.compile_context_list))));
        u_sess->plsql_cxt.curr_compile_context = popCompileContext();
        CompileStatusSwtichTo(oldCompileStatus);
        clearCompileContext(curr_compile);
        if (temp != NULL) {
            MemoryContextSwitchTo(temp);
        }
        return;
    }
    cell = NULL;
    bool oldStatus = false;
    bool needResetErrMsg = stp_disable_xact_and_set_err_msg(&oldStatus, STP_XACT_PACKAGE_INSTANTIATION);
    /* save flag for nest plpgsql compile */
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    int save_compile_status = u_sess->plsql_cxt.compile_status;
    List* temp_tableof_index = NULL;
    bool save_is_package_instantiation = u_sess->plsql_cxt.is_package_instantiation;
    bool needExecDoStmt = true;
        if (enable_plpgsql_undefined()) {
        needExecDoStmt = GetCurrCompilePgObjStatus();
    }
    ResourceOwnerData* oldowner = NULL;
    int64 stackId = 0;
    if (isCreate && enable_plpgsql_gsdependency_guc() && !IsInitdb) {
        oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
        SPI_savepoint_create("PackageInit");
        stackId = u_sess->plsql_cxt.nextStackEntryId;
    }
    bool save_isPerform = u_sess->parser_cxt.isPerform;
    PG_TRY();
    {
        u_sess->plsql_cxt.is_package_instantiation = true;
        if (needExecDoStmt) {
            init_do_stmt(pkg, isCreate, cell, oldCompileStatus, curr_compile, temp_tableof_index, oldcxt);
        }
        if (isCreate && enable_plpgsql_gsdependency_guc() && !IsInitdb) {
            SPI_savepoint_release("PackageInit");
            stp_cleanup_subxact_resource(stackId);
            MemoryContextSwitchTo(oldcxt);
            t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
        }
        stp_reset_xact_state_and_err_msg(oldStatus, needResetErrMsg);
        u_sess->plsql_cxt.is_package_instantiation = save_is_package_instantiation;
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s finish compile, level: %d", __func__, list_length(u_sess->plsql_cxt.compile_context_list))));
        u_sess->plsql_cxt.curr_compile_context = popCompileContext();
        CompileStatusSwtichTo(oldCompileStatus);
        clearCompileContext(curr_compile);
        PopOverrideSearchPath();
    }
    PG_CATCH();
    {
        u_sess->parser_cxt.isPerform = save_isPerform;
        stp_reset_xact_state_and_err_msg(oldStatus, needResetErrMsg);
        u_sess->plsql_cxt.is_package_instantiation = false;
        free_temp_func_tableof_index(temp_tableof_index);
        PopOverrideSearchPath();
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s clear curr_compile_context because of error.", __func__)));
        /* reset nest plpgsql compile */
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.compile_status = save_compile_status;
        clearCompileContextList(save_compile_list_length);
        u_sess->plsql_cxt.curr_compile_context = popCompileContext();
        /*avoid memeory leak*/
        clearCompileContext(curr_compile);
        if (isCreate && enable_plpgsql_gsdependency_guc() && !IsInitdb) {
            SPI_savepoint_rollbackAndRelease("PackageInit", InvalidTransactionId);
            stp_cleanup_subxact_resource(stackId);
            if (likely(u_sess->SPI_cxt._curid >= 0)) {
                if (likely(u_sess->SPI_cxt._current == &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid]))) {
                    _SPI_end_call(true);
                }
            }
            SPI_finish();
            t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
            MemoryContextSwitchTo(oldcxt);
            ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
            ereport(WARNING,
                    (errmodule(MOD_PLSQL),
                     errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("%s", edata->message),
                     errdetail("N/A"),
                     errcause("compile package or procedure error."),
                     erraction("check package or procedure error and redefine")));
            FlushErrorState();
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    MemoryContextSwitchTo(oldcxt);
    restoreCallFromPkgOid(old_value);
}
#endif
void record_pkg_function_dependency(PLpgSQL_package* pkg, List** invalItems, Oid funcid, Oid pkgid)
{
    /*
     * For performance reasons, we don't bother to track built-in functions;
     * we just assume they'll never change.
     */
    if (funcid >= (Oid)FirstBootstrapObjectId || pkgid != InvalidOid) {
        /* if already has id item in list, just skip it */
        if (*invalItems) {
            ListCell* lc = NULL;
            foreach(lc, *invalItems) {
                FuncInvalItem* item = (FuncInvalItem*)lfirst(lc);
                if (item->objId == funcid || item->objId == pkgid) {
                    return;
                }
            }
        }
        FuncInvalItem* inval_item = makeNode(FuncInvalItem);
        if (pkgid == InvalidOid) {
            inval_item->cacheId = PROCOID;
            inval_item->objId = funcid;
        } else {
            inval_item->cacheId = PACKAGEOID;
            inval_item->objId = pkgid;
        }
        inval_item->dbId = u_sess->proc_cxt.MyDatabaseId;
        *invalItems = lappend(*invalItems, inval_item);
    }
}

void AddPackageUseCount(PLpgSQL_function* func)
{
    /* first check func is valid */
    if (func->ndatums == 0) {
        /* mean the function has beed freed */
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_PLPGSQL_ERROR),
             errmsg("concurrent error when call function."),
             errdetail("when call function \"%s\", it has been invalidated by other session.",
                 func->fn_signature),
             errcause("excessive concurrency"),
             erraction("reduce concurrency and retry")));
    }
    List *invalItems = func->invalItems;
    if (invalItems == NIL) {
        return;
    }

    ListCell* lc = NULL;
    FuncInvalItem* item = NULL;
    PLpgSQL_pkg_hashkey hashkey;
    PLpgSQL_package* pkg = NULL;
    bool pkgInvalid = false;
    List *pkgList = NIL;

    /* first, check all depend package if valid */
    foreach(lc, invalItems) {
        pkg = NULL;
        item = (FuncInvalItem*)lfirst(lc);
        if (item->cacheId == PROCOID) {
            continue;
        }
        hashkey.pkgOid = item->objId;
        pkg = plpgsql_pkg_HashTableLookup(&hashkey);
        if (pkg == NULL) {
            pkgInvalid = true;
            break;
        }
        pkgList = lappend(pkgList, pkg);
    }

    /* packages it depends has been invalid, need report error */
    if (pkgInvalid) {
        list_free(pkgList);
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_PLPGSQL_ERROR),
             errmsg("concurrent error when call function."),
             errdetail("when call function \"%s\", packages it depends on has been invalidated by other session.",
                 func->fn_signature),
             errcause("excessive concurrency"),
             erraction("reduce concurrency and retry")));
    }

    /* add package use count */
    foreach(lc, pkgList) {
        pkg = (PLpgSQL_package*)lfirst(lc);
        pkg->use_count++;
    }
    list_free(pkgList);
}

void DecreasePackageUseCount(PLpgSQL_function* func)
{
    List *invalItems = func->invalItems;
    if (invalItems == NIL) {
        return;
    }

    ListCell* lc = NULL;
    FuncInvalItem* item = NULL;
    PLpgSQL_pkg_hashkey hashkey;
    PLpgSQL_package* pkg = NULL;

    foreach(lc, invalItems) {
        pkg = NULL;
        item = (FuncInvalItem*)lfirst(lc);
        if (item->cacheId == PROCOID) {
            continue;
        }
        hashkey.pkgOid = item->objId;
        pkg = plpgsql_pkg_HashTableLookup(&hashkey);
        if (pkg != NULL) {
            pkg->use_count--;
        }
    }
}

static void init_do_stmt(PLpgSQL_package *pkg, bool isCreate, ListCell *cell, int oldCompileStatus,
                  PLpgSQL_compile_context *curr_compile, List *temp_tableof_index, MemoryContext oldcxt)
{
    foreach(cell, pkg->proc_list) {
        if (IsA(lfirst(cell), DoStmt)) {
            curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
            DoStmt* doStmt = (DoStmt*)lfirst(cell);
            if (!isCreate) {
                if (!doStmt->isExecuted) {
                    (void)CompileStatusSwtichTo(COMPILIE_PKG_ANON_BLOCK);
                    temp_tableof_index = u_sess->plsql_cxt.func_tableof_index;
                    u_sess->plsql_cxt.func_tableof_index = NULL;
                    if (u_sess->SPI_cxt._connected > -1 &&
                        u_sess->SPI_cxt._connected != u_sess->SPI_cxt._curid) {
                        SPI_STACK_LOG("begin", NULL, NULL);
                        _SPI_begin_call(false);
                        ExecuteDoStmt(doStmt, true);
                        SPI_STACK_LOG("end", NULL, NULL);
                        _SPI_end_call(false);
                    } else {
                        ExecuteDoStmt(doStmt, true);
                    }
                    if (!doStmt->isSpec) {
                        pkg->isInit = true;
                        
                    }
                    free_func_tableof_index();
                    u_sess->plsql_cxt.func_tableof_index = temp_tableof_index;
                    (void)CompileStatusSwtichTo(oldCompileStatus);
                    doStmt->isExecuted = true;
                }
            } else {
                if (isCreate && enable_plpgsql_gsdependency_guc() && !IsInitdb) {
                    MemoryContextSwitchTo(oldcxt);
                }
                if (doStmt->isSpec && !doStmt->isExecuted) {
                    (void)CompileStatusSwtichTo(COMPILIE_PKG_ANON_BLOCK);
                    temp_tableof_index = u_sess->plsql_cxt.func_tableof_index;
                    u_sess->plsql_cxt.func_tableof_index = NULL;
                    if (u_sess->SPI_cxt._connected > -1 &&
                        u_sess->SPI_cxt._connected != u_sess->SPI_cxt._curid) {
                        SPI_STACK_LOG("begin", NULL, NULL);
                        _SPI_begin_call(false);
                        ExecuteDoStmt(doStmt, true);
                        SPI_STACK_LOG("end", NULL, NULL);
                        _SPI_end_call(false);
                    } else if (!doStmt->isExecuted) {
                        ExecuteDoStmt(doStmt, true);
                    }
                    free_func_tableof_index();
                    u_sess->plsql_cxt.func_tableof_index = temp_tableof_index;
                    (void)CompileStatusSwtichTo(oldCompileStatus);
                    doStmt->isExecuted = true;
                }
            }
            (void*)MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
        }
    }
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

        for (int i = 1; i < rows; ++i) {
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

Datum generate_procoverage_report(PG_FUNCTION_ARGS)
{
    int64 beginId = PG_GETARG_INT64(0);
    int64 endId = PG_GETARG_INT64(1);
    bool isDefault = false;

    if (!u_sess->attr.attr_common.enable_proc_coverage) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("GUC enable_proc_coverage not turned on")));
    }
    if (beginId == -1 && endId == -1) {
        isDefault = true;
    }
    if (!isDefault && (beginId < 1 || endId < 1 || beginId > endId)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Incorrect begin/end value")));
    }

    StringInfoData result;
    const char* css =
        "<html lang=\"en\"><head><title>openGauss Procedure Coverage Report</title>\n"
        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n"
        "<style>.unexecuted{background-color: #ff6230;}\n"
        ".executed{background-color: #cad7fe;}.lineno{background-color: yellow;width: 120px;}</style>\n"
        "</head><body class=\"pcr\">\n";
    initStringInfo(&result);
    appendStringInfo(&result, "%s<h1 class=\"wdr\">Procedure Coverage Report</h1>\n", css);
    generate_procoverage_html(beginId, endId, &result, isDefault);
    appendStringInfoString(&result, "</body></html>");
    PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

static void generate_procoverage_html(int beginId, int endId, StringInfoData* result, bool isDefault)
{
    int rc = 0;
    /* connect SPI to execute query */
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("proc coverage SPI_connect failed: %s", SPI_result_code_string(rc))));
    }

    StringInfoData sql;
    initStringInfo(&sql);
    isDefault ?
        appendStringInfoString(
        &sql, "SELECT pro_name,db_name,coverage_arrays(pro_canbreak,pg_catalog.array_integer_sum(coverage)) "
        "as coverage_array, "
        "pro_querys, calculate_coverage(coverage_array) FROM coverage.proc_coverage "
        "GROUP BY pro_oid, db_name, pro_name, pro_querys, pro_canbreak order by db_name") :
        appendStringInfo(
        &sql, "SELECT pro_name,db_name,coverage_arrays(pro_canbreak,pg_catalog.array_integer_sum(coverage)) "
        "as coverage_array, "
        "pro_querys, calculate_coverage(coverage_array) FROM coverage.proc_coverage "
        "WHERE coverage_id BETWEEN %d AND %d GROUP BY pro_oid, db_name, pro_name, pro_querys, "
        "pro_canbreak order by db_name",
        beginId, endId);

    if (SPI_execute(sql.data, false, 0) != SPI_OK_SELECT) {
        FreeStringInfo(&sql);
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid query")));
    }
    FreeStringInfo(&sql);
    List* cstring_values = NIL;
    if (SPI_tuptable != NULL) {
        for (uint32 i = 0; i < SPI_processed; i++) {
            List* row_string = NIL;
            uint32 colNum = (uint32)SPI_tuptable->tupdesc->natts;
            for (uint32 j = 1; j <= colNum; j++) {
                char* value = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, j);
                row_string = lappend(row_string, value);
            }
            cstring_values = lappend(cstring_values, row_string);
        }
    }

    foreach_cell(outer_cell, cstring_values) {
        List* row_list = (List*)lfirst(outer_cell);
        List* coverage_array = NIL;
        List* pro_querys = NIL;
        int index = 0;
        foreach_cell(inner_cell, row_list) {
            char* value = (char*)lfirst(inner_cell);
            generate_procoverage_table(value, result, index, &coverage_array, &pro_querys);
            index++;
        }
        list_free_ext(coverage_array);
        list_free_deep(pro_querys);
    }
    list_free_deep(cstring_values);
    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
}

static void generate_procoverage_table(char* value, StringInfoData* result,
                                       int index, List** coverage_array, List** pro_querys)
{
    switch (index) {
        case PRO_NAME_COL:
            appendStringInfo(result, "<h2 class=\"pro_name\">%s()</h2>\n", value);
            break;
        case DB_NAME_COL:
            appendStringInfo(result, "<h4 class=\"db_name\">Database: %s</h4>\n", value);
            appendStringInfoString(result, "<table border=\"0\" class=\"proc_table\">\n");
            break;
        case COVERAGE_ARR_COL:
            deconstruct_coverage_array(coverage_array, value);
            break;
        case PRO_QUERYS_COL:
            deconstruct_querys_array(pro_querys, value);
            break;
        case COVERAGE_COL: {
            char* endptr;
            double coverageRate = strtod(value, &endptr);
            generate_procoverage_rows(result, *coverage_array, *pro_querys);
            appendStringInfo(result, "<h4 class=\"db_name\">Coverage rate: %.2f%%</h4>\n", coverageRate * 100);
            break;
        }
        default:
            break;
    }
}

static void generate_procoverage_rows(StringInfoData* result, List* coverage_array, List* pro_querys)
{
    ListCell* cover_cell = list_head(coverage_array);
    ListCell* query_cell = list_head(pro_querys);
    int lineno = -3;
    while (cover_cell != NULL && query_cell != NULL) {
        int cover = lfirst_int(cover_cell);
        char* query = (char*)lfirst(query_cell);
        const char* trClass = (cover == -1 ? "unbreakable" : (cover == 0 ? "unexecuted" : "executed"));
        appendStringInfo(result, "<tr class=\"%s\">\n", trClass);
        lineno < 1 ? appendStringInfoString(result, "<th class=\"lineno\"></th>\n") :
                     appendStringInfo(result, "<th class=\"lineno\">%d</th>\n", lineno);
        cover == -1 ? appendStringInfoString(result, "<th></th>\n") : appendStringInfo(result, "<th>%d</th>\n", cover);
        appendStringInfo(result, "<th>%s</th>\n</tr>\n", query);
        cover_cell = lnext(cover_cell);
        query_cell = lnext(query_cell);
        lineno++;
    }
    appendStringInfoString(result, "</table>");
}

static void deconstruct_coverage_array(List** coverage_array, char* array_string)
{
    char *p = array_string;
    int num;
    if (*p++ != '{') {
        return;
    }
    while (*p && *p != '}') {
        while (*p == ',') {
            p++;
        }
        if (sscanf_s(p, "%d", &num) == 1) {
            *coverage_array = lappend_int(*coverage_array, num);
        }
        while (*p && *p != ',' && *p != '}') {
            p++;
        }
    }
}

static void deconstruct_querys_array(List** pro_querys, const char* querys_string)
{
    char* buffer = NULL;
    char* token = NULL;
    char* saveStr = NULL;
    const char delim[2] = "\n";
    buffer = pstrdup(querys_string);
    token = strtok_r(buffer, delim, &saveStr);

    while (token != NULL) {
        char* copy = replace_html_entity(token);
        *pro_querys = lappend(*pro_querys, copy);
        token = strtok_r(NULL, delim, &saveStr);
    }
    pfree(buffer);
}

static char* replace_html_entity(const char* input)
{
    size_t input_length = strlen(input);
    size_t new_length = 0;

    for (size_t i = 0; i < input_length; i++) {
        switch (input[i]) {
            case '<':
            case '>':
                new_length += HTML_LT_LEN;
                break;
            case '&':
                new_length += HTML_AMP_LEN;
                break;
            case '\"':
                new_length += HTML_QUOT_LEN;
                break;
            default:
                new_length += 1;
                break;
        }
    }
    char* result = (char*)palloc(new_length + 1);

    size_t j = 0;
    for (size_t i = 0; i < input_length; i++) {
        errno_t rc;
        switch (input[i]) {
            case '<':
                rc = strcpy_s(&result[j], HTML_LT_LEN + 1, "&lt;");
                securec_check_c(rc, "\0", "\0");
                j += HTML_LT_LEN;
                break;
            case '>':
                rc = strcpy_s(&result[j], HTML_LT_LEN + 1, "&gt;");
                securec_check_c(rc, "\0", "\0");
                j += HTML_LT_LEN;
                break;
            case '&':
                rc = strcpy_s(&result[j], HTML_AMP_LEN + 1, "&amp;");
                securec_check_c(rc, "\0", "\0");
                j += HTML_AMP_LEN;
                break;
            case '\"':
                rc = strcpy_s(&result[j], HTML_QUOT_LEN + 1, "&quot;");
                securec_check_c(rc, "\0", "\0");
                j += HTML_QUOT_LEN;
                break;
            default:
                result[j] = input[i];
                j += 1;
                break;
        }
    }
    result[j] = '\0';
    return result;
}
