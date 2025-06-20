#include "miscadmin.h"
#include "src/pltsql/pltsql.h"
#include "src/backend_parser/scanner.h"
#include "parser/parser.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "shark.h"

PG_MODULE_MAGIC;

static bool global_hook_inited = false;
static uint32 shark_index;

extern List* tsql_raw_parser(const char* str, List** query_string_locationlist);
extern void assign_tablecmds_hook(void);
static List* RewriteTypmodExpr(List *expr_list);
static bool CheckIsMssqlHex(char *str);
static Node *make_int_const(int val, int location);

static char* get_collation_name_for_db(Oid dbOid);
static bool is_login(Oid id);
static RangeVar* pltsqlMakeRangeVarFromName(const char *ident);
static Oid get_table_identity(Oid tableOid);
static int128 get_last_value_from_seq(Oid seqid);

void _PG_init(void)
{}

static bool CheckIsMssqlHex(char *str)
{
    if (str == NULL || strlen(str) <= 2) {
        return false;
    }
    if (str[0] == '0' && (str[1] == 'x' || str[1] == 'X')) {
        return true;
    }
    return false;
}

static List *RewriteTypmodExpr(List *expr_list)
{
    /*
     * Look for ( max ) if we are in tsql dialect, MAX can be used in
     * sys.varchar, sys.nvarchar, sys.binary and sys.varbinary. map it to
     * TSQL_MAX_TYPMOD
     */
    Node       *expr;

    expr = (Node*)linitial(expr_list);
    if (list_length(expr_list) == 1 && IsA(expr, ColumnRef)) {
        ColumnRef  *columnref = (ColumnRef *) expr;

        if (list_length(columnref->fields) == 1) {
            char *str = ((Value*)linitial(columnref->fields))->val.str;
            if (strcmp(str, "max") == 0)
                return list_make1(make_int_const(TSQL_MAX_TYPMOD, -1));
        }
    }

    return expr_list;            /* nothing to do */
}

static Node *make_int_const(int val, int location)
{
    A_Const *n = makeNode(A_Const);

    n->val.type = T_Integer;
    n->val.val.ival = val;
    n->location = location;

    return (Node *)n;
}

void init_session_vars(void)
{
    if (!DB_IS_CMPT(D_FORMAT)) {
        return;
    }
    if (!global_hook_inited) {
        g_instance.raw_parser_hook[DB_CMPT_D] = (void*)tsql_raw_parser;
        global_hook_inited = true;
    }
    u_sess->hook_cxt.coreYYlexHook = (void*)pgtsql_core_yylex;
    u_sess->hook_cxt.plsqlCompileHook = (void*)pltsql_compile;
    u_sess->hook_cxt.checkVaildUserHook = (void*)check_vaild_username;
    u_sess->hook_cxt.fetchStatusHook = (void*)fetch_cursor_end_hook;
    u_sess->hook_cxt.rowcountHook = (void*)rowcount_hook;
    u_sess->hook_cxt.checkIsMssqlHexHook = (void*)CheckIsMssqlHex;
    u_sess->hook_cxt.rewriteTypmodExprHook = (void*)RewriteTypmodExpr;

    RepallocSessionVarsArrayIfNecessary();
    SharkContext *cxt = (SharkContext*) MemoryContextAlloc(u_sess->self_mem_cxt, sizeof(sharkContext));
    u_sess->attr.attr_common.extension_session_vars_array[shark_index] = cxt;
    cxt->dialect_sql = false;
    cxt->rowcount = 0;
    cxt->fetch_status = FETCH_STATUS_SUCCESS;
    cxt->procid = InvalidOid;
    cxt->lastUsedScopeSeqIdentity = NULL;
    cxt->pltsqlScopeIdentityNestLevel = 0;

    assign_tablecmds_hook();
    AssignIdentitycmdsHook();
}

SharkContext* GetSessionContext()
{
    if (u_sess->attr.attr_common.extension_session_vars_array[shark_index] == NULL) {
        init_session_vars();
    }
    return (SharkContext*) u_sess->attr.attr_common.extension_session_vars_array[shark_index];
}

void set_extension_index(uint32 index)
{
    shark_index = index;
}

void _PG_fini(void)
{}

void fetch_cursor_end_hook(int fetch_status)
{
    SharkContext *cxt = GetSessionContext();
    switch(fetch_status) {
        case FETCH_STATUS_SUCCESS:
        case FETCH_STATUS_FAIL:
        case FETCH_STATUS_NOT_EXIST:
        case FETCH_STATUS_NOT_FETCH:
            cxt->fetch_status = fetch_status;
            break;
        default:
            cxt->fetch_status = FETCH_STATUS_FAIL;
            break;
    }
}

void rowcount_hook(int64 rowcount)
{
    SharkContext *cxt = GetSessionContext();
    cxt->rowcount = rowcount;
}

PG_FUNCTION_INFO_V1(fetch_status);
Datum fetch_status(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_INT32(cxt->fetch_status);
}

PG_FUNCTION_INFO_V1(rowcount);
Datum rowcount(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_INT32(cxt->rowcount);
}

PG_FUNCTION_INFO_V1(rowcount_big);
Datum rowcount_big(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_INT64(cxt->rowcount);
}

void set_procid(Oid oid)
{
    SharkContext *cxt = GetSessionContext();
    cxt->procid = oid;
}

Oid get_procid()
{
    SharkContext *cxt = GetSessionContext();
    return cxt->procid;
}

PG_FUNCTION_INFO_V1(procid);
Datum procid(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_OID(cxt->procid);
}

PG_FUNCTION_INFO_V1(databasepropertyex);
Datum databasepropertyex(PG_FUNCTION_ARGS)
{
    Datum vch = 0;
    int64_t intVal = 0;
    const char* strVal = NULL;
    const char* dbname = text_to_cstring(PG_GETARG_TEXT_P(0));
    bool resisnull = false;
    Oid dboid = get_database_oid(dbname, true);
    pfree_ext(dbname);
    if (dboid == InvalidOid) {
        PG_RETURN_NULL();
    }

    const char* property = text_to_cstring(PG_GETARG_TEXT_P(1));

    if (strcasecmp(property, "Collation") == 0) {
        strVal = get_collation_name_for_db(dboid);
    } else if (strcasecmp(property, "ComparisonStyle") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "Edition") == 0) {
        strVal = pstrdup("Standard");
    } else if (strcasecmp(property, "IsAnsiNullDefault") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsAnsiNullsEnabled") == 0) {
        intVal = 1;
    } else if (strcasecmp(property, "IsAnsiPaddingEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsAnsiWarningsEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsArithmeticAbortEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsAutoClose") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsAutoCreateStatistics") == 0) {
        intVal = 1;
    } else if (strcasecmp(property, "IsAutoCreateStatisticsIncremental") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsAutoShrink") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsAutoUpdateStatistics") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsClone") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsCloseCursorsOnCommitEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsFulltextEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsInStandBy") == 0) {
        intVal = RecoveryInProgress() ? 1 : 0;
    } else if (strcasecmp(property, "IsLocalCursorsDefault") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsMemoryOptimizedElevateToSnapshotEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsNullConcat") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsNumericRoundAbortEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsParameterizationForced	") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsQuotedIdentifiersEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsPublished") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsRecursiveTriggersEnabled") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsSubscribed") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsSyncWithBackup") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsTornPageDetectionEnabled") == 0) {
        intVal = (u_sess->attr.attr_storage.fullPageWrites) ? 1 : 0;
    } else if (strcasecmp(property, "IsVerifiedClone") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "IsXTPSupported") == 0) {
        intVal = 0;
    } else if (strcasecmp(property, "LastGoodCheckDbTime") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "LCID") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "MaxSizeInBytes") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "Recovery") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "ServiceObjective") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "ServiceObjectiveId") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "SQLSortOrder") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "Status") == 0) {
        strVal = pstrdup("ONLINE");
    } else if (strcasecmp(property, "Updateability") == 0) {
        strVal = u_sess->attr.attr_common.XactReadOnly ? pstrdup("READ_ONLY") : pstrdup("READ_WRITE");
    } else if (strcasecmp(property, "UserAccess") == 0) {
        resisnull = true;
    } else if (strcasecmp(property, "Version") == 0) {
        intVal = t_thrd.proc->workingVersionNum;
    } else if (strcasecmp(property, "ReplicaID") == 0) {
        resisnull = true;
    } else {
        /* no property name matches, return NULL */
        resisnull = true;
    }

    if (resisnull) {
        pfree_ext(property);
        PG_RETURN_NULL();
    } else if (strVal != nullptr) {
        vch = DirectFunctionCall3(sql_variantin, CStringGetDatum(strVal), ObjectIdGetDatum(0), Int32GetDatum(-1));
        pfree_ext(strVal);
    } else {
        const int rellen = 10;
        char* vchIntVal = static_cast<char*>(palloc0(rellen));
        pg_ltoa(intVal, vchIntVal);
        vch = DirectFunctionCall3(sql_variantin, CStringGetDatum(vchIntVal), ObjectIdGetDatum(0), Int32GetDatum(-1));
        pfree_ext(vchIntVal);
    }
    pfree_ext(property);
    return vch;
}

static char* get_collation_name_for_db(Oid dbOid)
{
    HeapTuple tuple = nullptr;
    Form_pg_database sysdb = nullptr;
    char* collationName = nullptr;
    tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("Could not find database: \"%u\"", dbOid)));
    }
    sysdb = ((Form_pg_database)GETSTRUCT(tuple));
    collationName = pstrdup(NameStr(sysdb->datcollate));
    ReleaseSysCache(tuple);
    return collationName;
}

PG_FUNCTION_INFO_V1(suser_name);
Datum suser_name(PG_FUNCTION_ARGS)
{
    Oid server_user_id = InvalidOid;
    char* ret = nullptr;

    server_user_id  = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
    if (!OidIsValid(server_user_id)) {
        PG_RETURN_NULL();
    }

    ret = GetUserNameById(server_user_id);
    if (!ret) {
        PG_RETURN_NULL();
    }

    /*
     * The CREATE LOGIN syntax is currently not supported,
     * so there is temporarily no need to check the original login username.
     */
    if (!is_login(server_user_id)) {
        pfree_ext(ret);
        PG_RETURN_NULL();
    }
    text* restext = cstring_to_text(ret);
    pfree_ext(ret);
    PG_RETURN_TEXT_P(restext);
}


PG_FUNCTION_INFO_V1(suser_id);
Datum suser_id(PG_FUNCTION_ARGS)
{
    char* login = nullptr;
    Oid ret = InvalidOid;
    HeapTuple auth_tp = nullptr;
    Form_pg_authid authid_struct = nullptr;

    login = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(0));
    if (!login) {
        ret = GetSessionUserId();
    } else {
        int i = 0;
        i = strlen(login);
        while (i > 0 && isspace(static_cast<unsigned char>(login[i - 1]))) {
            login[--i] = '\0';
        }
        for (i = 0; login[i] != '\0'; i++) {
            login[i] = tolower(login[i]);
        }
        auth_tp = SearchSysCache1(AUTHNAME, CStringGetDatum(login));
        if (!HeapTupleIsValid(auth_tp)) {
            pfree_ext(login);
            PG_RETURN_NULL();
        }
        ret = HeapTupleGetOid(auth_tp);
        ReleaseSysCache(auth_tp);
    }
    if (!is_login(ret)) {
        pfree_ext(login);
        PG_RETURN_NULL();
    }
    PG_RETURN_OID(ret);
}

static bool is_login(Oid id)
{
    HeapTuple auth_tp = nullptr;
    Form_pg_authid authid_struct = nullptr;
    bool isLogin =  false;
    auth_tp = SearchSysCache1(AUTHOID, ObjectIdGetDatum(id));
    if (!HeapTupleIsValid(auth_tp)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for proc owner %u", id)));
    }
    authid_struct = (Form_pg_authid)GETSTRUCT(auth_tp);
    isLogin = authid_struct->rolcanlogin;
    ReleaseSysCache(auth_tp);
    return isLogin;
}

PG_FUNCTION_INFO_V1(get_scope_identity);
Datum get_scope_identity(PG_FUNCTION_ARGS)
{
    PG_TRY();
    {
        PG_RETURN_INT128(last_scope_identity_value());
    }
    PG_CATCH();
    {
        FlushErrorState();
        PG_RETURN_NULL();
    }
    PG_END_TRY();
}

PG_FUNCTION_INFO_V1(get_ident_current);
Datum get_ident_current(PG_FUNCTION_ARGS)
{
    text* tablename = PG_GETARG_TEXT_PP(0);
    char* table = text_to_cstring(tablename);
    char *curDbName = nullptr;
    RangeVar* tablerv = nullptr;
    Oid tableOid = InvalidOid;
    Oid seqid = InvalidOid;

    PG_TRY();
    {
        tablerv = pltsqlMakeRangeVarFromName(table);
        pfree_ext(table);
        curDbName = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (tablerv->schemaname && curDbName) {
            tablerv->schemaname = GetPhysicalSchemaName(curDbName, tablerv->schemaname);
        }

        /* Look up table name. Can't lock it - we might not have privileges. */
        tableOid = RangeVarGetRelid(tablerv, NoLock, false);
        /* Check permissions */
        if (pg_class_aclcheck(tableOid, GetUserId(), ACL_SELECT | ACL_USAGE) != ACLCHECK_OK) {
            PG_RETURN_NULL();
        }

        seqid = get_table_identity(tableOid);

        PG_TRY();
        {
            /* Check the tuple directly. Catch error if NULL */
            PG_RETURN_INT128(get_last_value_from_seq(seqid));
        }
        PG_CATCH();
        {
            FlushErrorState();
        }
        PG_END_TRY();

        /* If the relation exists, return the seed */
        if (seqid != InvalidOid) {
            int64 uuid = 0;
            int64 start = 0;
            int64 increment = 0;
            int64 maxvalue = 0;
            int64 minvalue = 0;
            int64 cachevalue = 0;
            bool cycle = false;
            Relation relseq = relation_open(seqid, AccessShareLock);
            get_sequence_params(relseq, &uuid, &start, &increment, &maxvalue, &minvalue, &cachevalue, &cycle);
            relation_close(relseq, AccessShareLock);
            PG_RETURN_INT64(start);
        }
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
    PG_RETURN_NULL();
}

/*
 * pltsqlMakeRangeVarFromName - convert pltsql identifiers to RangeVar
 */
static RangeVar* pltsqlMakeRangeVarFromName(const char *ident)
{
    const char* str = "SELECT * FROM ";
    StringInfoData query;
    List* parsetree = NIL;
    SelectStmt* sel_stmt = nullptr;
    Node* dst_expr = nullptr;
    Node* n = nullptr;

    /* Create a fake SELECT statement to get the identifier names */
    initStringInfo(&query);
    appendStringInfoString(&query, str);
    appendStringInfoString(&query, ident);

    parsetree = tsql_raw_parser(query.data, nullptr);
    /* get SelectStmt from parsetree */
    Assert(list_length(parsetree) == 1);
    dst_expr = (Node*)linitial(parsetree);
    Assert(IsA(dst_expr, SelectStmt));
    sel_stmt = (SelectStmt*)dst_expr;
    n = (Node*)linitial(sel_stmt->fromClause);
    Assert(IsA(n, RangeVar));
    return (RangeVar*)n;
}

/*
 * Get the table's identity sequence OID.
 */
static Oid get_table_identity(Oid tableOid)
{
    Relation	rel = nullptr;
    text*       relname = nullptr;
    TupleDesc	tupdesc = nullptr;
    AttrNumber	attnum = 0;
    Oid			seqid = InvalidOid;

    rel = RelationIdGetRelation(tableOid);
    relname = cstring_to_text(RelationGetRelationName(rel));
    tupdesc = RelationGetDescr(rel);

    for (attnum = 0; attnum < tupdesc->natts; attnum++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum);
        text* att_name = cstring_to_text(NameStr(attr->attname));

        if (OidIsValid(seqid = pg_get_serial_sequence_oid(relname, att_name, true))) {
            pfree_ext(att_name);
            break;
        }
        pfree_ext(att_name);
    }

    RelationClose(rel);
    pfree_ext(relname);
    return seqid;
}

static int128 get_last_value_from_seq(Oid seqid)
{
    int128 last_value = 0;
    HeapTupleHeader td = nullptr;
    TupleDesc tupdesc = nullptr;
    HeapTupleData tup;
    td = DatumGetHeapTupleHeader(DirectFunctionCall1(pg_sequence_last_value,
                                                     ObjectIdGetDatum(seqid)));
    tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
    tup.t_len = HeapTupleHeaderGetDatumLength(td);
    tup.t_data = td;
    Datum *values = (Datum *)palloc(sizeof(Datum) * tupdesc->natts);
    bool *nulls = (bool *)palloc(sizeof(bool) * tupdesc->natts);
    heap_deform_tuple(&tup, tupdesc, values, nulls);
    last_value = DatumGetInt128(values[1]);
    pfree(values);
    pfree(nulls);
    return last_value;
}