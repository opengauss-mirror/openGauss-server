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

static void InsertGsSource(Oid objId, Oid nspid, const char* name, const char* type, bool status)
{
    bool notInsert = u_sess->attr.attr_common.upgrade_mode != 0 || IsSystemNamespace(nspid) || 
        IsToastNamespace(nspid) || IsCStoreNamespace(nspid) || 
        IsPackageSchemaOid(nspid) || SKIP_GS_SOURCE;
    if (notInsert) {
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
		"nspid=%u and name=\'%s\' and type=\'%s\';", nspid, name, type);
    appendStringInfo(&str,
        "if allNum > 0 then "
        "select id from dbe_pldeveloper.gs_source into oldId where "
        "nspid=%u and name=\'%s\' and type=\'%s\';"
        "objId := oldId; "
        "else "
        "objId := %u;"
        "end if;", nspid, name, type, objId);
    appendStringInfo(&str,
        "delete from DBE_PLDEVELOPER.gs_source where nspid=%u and name=\'%s\' and type = \'%s\';\n",
        nspid, name, type);
    appendStringInfo(&str,
        "delete from DBE_PLDEVELOPER.gs_source where nspid=%u and name=\'%s\' and type = \'%s\';\n",
        nspid, name, type);
    if (!u_sess->attr.attr_common.plsql_show_all_error || status)  {
        appendStringInfo(&str,
            "delete from DBE_PLDEVELOPER.gs_errors where " 
            "nspid=%u and name=\'%s\' and type = \'%s\';\n",
            nspid, name, type);
        if (!strcmp(type, "package")) {
            appendStringInfo(&str,
                "delete from DBE_PLDEVELOPER.gs_errors where " 
                "nspid=%u and name=\'%s\' and type = \'package body\';\n",
                nspid, name);
        }
    }
    if (status && !strcmp(type, "package")) {
        appendStringInfo(&str,
            "delete from DBE_PLDEVELOPER.gs_source where " 
            "nspid=%u and name=\'%s\' and type = \'package body\';\n",
            nspid, name);
    }
    appendStringInfo(&str,
        "insert into DBE_PLDEVELOPER.gs_source values(objId, %u, %u,\'%s\', \'%s\', \'%c\', $gssource$%s$gssource$);\n",
        userId, nspid, name, type, statusChr, source);
    appendStringInfoString(&str,
        "EXCEPTION WHEN OTHERS THEN NULL; \n");
    appendStringInfoString(&str, "end;");
    List* rawParseList = raw_parser(str.data);
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
    pfree_ext(str.data);
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
                securec_check_ss_c(rc, "\0", "\0");
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
    PLpgSQL_package* pkg = NULL;
    MemoryContext oldContext = CurrentMemoryContext;
    FormatCallStack* plcallstack = NULL;
    int pkgDatumsNumber = 0;
    bool savedisAllowCommitRollback = true;
    /*
     * if the atomic stored in fcinfo is false means allow
     * commit/rollback within stored procedure.
     * set the nonatomic and will be reused within function.
     */
    nonatomic = fcinfo->context && IsA(fcinfo->context, FunctionScanState) &&
        !castNode(FunctionScanState, fcinfo->context)->atomic;

    /* get cast owner and make sure current user is cast owner when execute cast-func */
    GetUserIdAndSecContext(&old_user, &save_sec_context);
    cast_owner = u_sess->exec_cxt.cast_owner;
    if (InvalidCastOwnerId == cast_owner || !OidIsValid(cast_owner)) {
        ereport(LOG, (errmsg("old system table pg_cast does not have castowner column, use old default permission")));
    } else {
        SetUserIdAndSecContext(cast_owner, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
        has_switch = true;
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
    int outerDop = u_sess->opt_cxt.query_dop;
    u_sess->opt_cxt.query_dop = 1;
#endif

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

    int connect = SPI_connectid();
    Oid firstLevelPkgOid = InvalidOid;
    PG_TRY();
    {
        PGSTAT_START_PLSQL_TIME_RECORD();
        /*
         * save running package value
         */
        firstLevelPkgOid = saveCallFromPkgOid(package_oid);
        bool saved_current_stp_with_exception = plpgsql_get_current_value_stp_with_exception();
        /* Find or compile the function */
        if (func == NULL) {
            func = plpgsql_compile(fcinfo, false);
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

        PG_TRY();
        {
            /*
             * Determine if called as function or trigger and call appropriate
             * subhandler
             */
            if (CALLED_AS_TRIGGER(fcinfo)) {
                retval = PointerGetDatum(plpgsql_exec_trigger(func, (TriggerData*)fcinfo->context));
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
                    retval = plpgsql_exec_function(func, fcinfo, false);
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
            /* Put the new code behind this code snippet. Otherwise, unexpected errors may occur. */
            plcallstack = t_thrd.log_cxt.call_stack;

#ifndef ENABLE_MULTIPLE_NODES
            estate_cursor_set(plcallstack);

#endif
            if (plcallstack != NULL) {
                t_thrd.log_cxt.call_stack = plcallstack->prev;
            }

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

        // resume the search_path when the procedure has executed
        PopOverrideSearchPath();

        if (AUDIT_EXEC_ENABLED) {
            auditExecPLpgSQLFunction(func, AUDIT_OK);
        }

        u_sess->misc_cxt.Pseudo_CurrentUserId = saved_Pseudo_CurrentUserId;
    }
    PG_CATCH();
    {
        /* clean stp save pointer if the outermost function is end. */
        if (u_sess->SPI_cxt._connected == 0) {
            t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
        }
#ifdef ENABLE_MULTIPLE_NODES
        SetSendCommandId(saveSetSendCommandId);
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
    /* clean stp save pointer if the outermost function is end. */
    if (u_sess->SPI_cxt._connected == 0) {
        t_thrd.utils_cxt.STPSavedResourceOwner = NULL;
    }
#ifdef ENABLE_MULTIPLE_NODES
    SetSendCommandId(saveSetSendCommandId);
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
#else
    /* Saves the status of whether to send commandId. */
    bool saveSetSendCommandId = IsSendCommandId();
#endif


    _PG_init();

    AssertEreport(IsA(codeblock, InlineCodeBlock), MOD_PLSQL, "Inline code block is required.");

    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    rc = SPI_connect_ext(DestSPI, NULL, NULL, codeblock->atomic ? 0 : SPI_OPT_NONATOMIC);
    if (rc != SPI_OK_CONNECT) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_connect failed: %s when execute anonymous block.", SPI_result_code_string(rc))));
    }
    PGSTAT_START_PLSQL_TIME_RECORD();

    /* Compile the anonymous code block */
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
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
        popToOldCompileContext(save_compile_context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PGSTAT_END_PLSQL_TIME_RECORD(PL_COMPILATION_TIME);

    func->is_insert_gs_source = u_sess->plsql_cxt.is_insert_gs_source;
    /* Mark packages the function use, so them can't be deleted from under us */
    AddPackageUseCount(func);
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
    /* save flag for nest plpgsql compile */
    save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    int save_compile_status = u_sess->plsql_cxt.compile_status;
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
        /* Put the new code behind this code snippet. Otherwise, unexpected errors may occur. */
        FormatCallStack* plcallstack = t_thrd.log_cxt.call_stack;
#ifndef ENABLE_MULTIPLE_NODES
        estate_cursor_set(plcallstack);
#endif

        if (plcallstack != NULL) {
            t_thrd.log_cxt.call_stack = plcallstack->prev;
        }
        /* Decrement package use-count */
        DecreasePackageUseCount(func);

#ifndef ENABLE_MULTIPLE_NODES
        /* for restore parent session and automn session package var values */
        (void)processAutonmSessionPkgsInException(func);
#endif
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

        PG_RE_THROW();
    }
    PG_END_TRY();

#ifdef ENABLE_MULTIPLE_NODES
    SetSendCommandId(saveSetSendCommandId);
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

    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
    if (((rc = SPI_finish()) != SPI_OK_FINISH)) {
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
        if (istrigger) {
            errorno = memset_s(&trigdata, sizeof(trigdata), 0, sizeof(trigdata));
            securec_check(errorno, "", "");
            trigdata.type = T_TriggerData;
            fake_fcinfo.context = (Node*)&trigdata;
        }
        /* save flag for nest plpgsql compile */
        PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
        int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
        int save_compile_status = u_sess->plsql_cxt.compile_status;
        /* Test-compile the function */
        PG_TRY();
        {
            u_sess->parser_cxt.isCreateFuncOrProc = true;
            func = plpgsql_compile(&fake_fcinfo, true);
            u_sess->parser_cxt.isCreateFuncOrProc = false;
        }
        PG_CATCH();
        {
#ifndef ENABLE_MULTIPLE_NODES
            bool insertError = (u_sess->attr.attr_common.plsql_show_all_error ||
                                    !u_sess->attr.attr_sql.check_function_bodies) &&
                                    u_sess->plsql_cxt.isCreateFunction;
            if (insertError) {
                if (!IsInitdb) {
                    ProcInsertGsSource(funcoid, false);
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
        /* Skip validation on Initdb */
#ifndef ENABLE_MULTIPLE_NODES
        if (!IsInitdb && u_sess->plsql_cxt.isCreateFunction) {
            ProcInsertGsSource(funcoid, true);
        }
#endif

        bool isNotComipilePkg = u_sess->plsql_cxt.curr_compile_context == NULL ||
            (u_sess->plsql_cxt.curr_compile_context != NULL &&
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL);

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
    }

#ifndef ENABLE_MULTIPLE_NODES
    u_sess->plsql_cxt.isCreateFunction = false;
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
        bool insertError = (u_sess->attr.attr_common.plsql_show_all_error ||
                                !u_sess->attr.attr_sql.check_function_bodies) &&
                                isCreate;
        if (insertError) {
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
void PackageInit(PLpgSQL_package* pkg, bool isCreate) 
{
    if (likely(pkg != NULL)) {
        if (likely(pkg->isInit)) {
            return;
        }
    }
    int package_line = 0;
    package_line = u_sess->plsql_cxt.package_as_line;
    PushOverrideSearchPath(pkg->pkg_searchpath);
    ListCell* cell = NULL;
    int oldCompileStatus = getCompileStatus();
    if (isCreate) {
        CompileStatusSwtichTo(COMPILIE_PKG);
    }
    
    PLpgSQL_compile_context* curr_compile = createCompileContext("PL/pgSQL package context");
    SPI_NESTCOMPILE_LOG(curr_compile->compile_cxt);
    MemoryContext temp = NULL;
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        temp = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    }
    u_sess->plsql_cxt.curr_compile_context = curr_compile;
    pushCompileContext();
    curr_compile->plpgsql_curr_compile_package = pkg;
    checkCompileMemoryContext(pkg->pkg_cxt);
    if (isCreate) {
        int exception_num = 0;
        curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
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
                        if (u_sess->plsql_cxt.create_func_error) {
                            u_sess->plsql_cxt.create_func_error = false;
                            exception_num += 1;
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
        (void*)MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
    } else {
        if (pkg->is_bodycompiled) {
            (void)CompileStatusSwtichTo(COMPILIE_PKG_FUNC);
            curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
            FunctionInPackageCompile(pkg);
            (void*)MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
            (void)CompileStatusSwtichTo(oldCompileStatus);
        }
    }
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
    PG_TRY();
    {
        u_sess->plsql_cxt.is_package_instantiation = true;
        foreach(cell, pkg->proc_list) {
            if (IsA(lfirst(cell), DoStmt)) {
                curr_compile->compile_tmp_cxt = MemoryContextSwitchTo(pkg->pkg_cxt);
                DoStmt* doStmt = (DoStmt*)lfirst(cell);
                if (!isCreate) {
                    if (!doStmt->isExecuted) {
                        (void)CompileStatusSwtichTo(COMPILIE_PKG_ANON_BLOCK);
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
                        (void)CompileStatusSwtichTo(oldCompileStatus);
                        doStmt->isExecuted = true;
                    }
                } else {
                    if (doStmt->isSpec && !doStmt->isExecuted) {
                        (void)CompileStatusSwtichTo(COMPILIE_PKG_ANON_BLOCK);
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
                        
                        (void)CompileStatusSwtichTo(oldCompileStatus);
                        doStmt->isExecuted = true;
                    }
                }
                (void*)MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
            }
        }
        stp_reset_xact_state_and_err_msg(oldStatus, needResetErrMsg);
        u_sess->plsql_cxt.is_package_instantiation = false;
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s finish compile, level: %d", __func__, list_length(u_sess->plsql_cxt.compile_context_list))));
        u_sess->plsql_cxt.curr_compile_context = popCompileContext();
        CompileStatusSwtichTo(oldCompileStatus);
        clearCompileContext(curr_compile);
    }
    PG_CATCH();
    {
        stp_reset_xact_state_and_err_msg(oldStatus, needResetErrMsg);
        u_sess->plsql_cxt.is_package_instantiation = false;
        PopOverrideSearchPath();
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s clear curr_compile_context because of error.", __func__)));
        /* reset nest plpgsql compile */
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.compile_status = save_compile_status;
        clearCompileContextList(save_compile_list_length);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PopOverrideSearchPath();
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

