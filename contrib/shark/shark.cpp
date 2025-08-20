#include "miscadmin.h"
#include "src/pltsql/pltsql.h"
#include "src/backend_parser/scanner.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "common/int.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/numeric.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "shark.h"

PG_MODULE_MAGIC;

static bool global_hook_inited = false;
static uint32 shark_index;

extern List* tsql_raw_parser(const char* str, List** query_string_locationlist);
extern void assign_tablecmds_hook(void);
extern Oid pg_get_serial_sequence_internal(Oid tableOid, AttrNumber attnum, bool find_identity, char** out_seq_name);
static List* RewriteTypmodExpr(List *expr_list);
static bool CheckIsMssqlHex(char *str);
static Node *make_int_const(int val, int location);

static char* get_collation_name_for_db(Oid dbOid);
static bool is_login(Oid id);
static RangeVar* pltsqlMakeRangeVarFromName(const char *ident);
static Oid get_table_identity(Oid tableOid);
static int128 get_last_value_from_seq(Oid seqid);
static bool get_seed(Oid seqid, int64* start, int128* res, bool* success);
static int days_in_date(int day, int month, int year);
static bool int64_multiply_add(int64 val, int64 multiplier, int64 *sum);
static bool int32_multiply_add(int32 val, int32 multiplier, int32 *sum);
static int32 diff_cal(int val, struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2);
static int64 diff_cal_big(int val, struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2);
static int32 int32_year_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int32 int32_quarter_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int32 int32_month_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int32 int32_week_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int32 int32_day_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int32 int32_hour_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow);
static int32 int32_minute_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow);
static int32 int32_second_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow);
static int32 int32_millisec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow);
static int32 int32_microsec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow);
static int32 int32_nano_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow);

static int64 int64_year_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int64 int64_quarter_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int64 int64_month_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int64 int64_week_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int64 int64_day_diff(struct pg_tm* tm1, struct pg_tm* tm2);
static int64 int64_hour_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow);
static int64 int64_minute_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow);
static int64 int64_second_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow);
static int64 int64_millisec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow);
static int64 int64_microsec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow);
static int64 int64_nano_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow);

#define DTK_NANO 32

void _PG_init(void)
{
    InitIntervalLookup();
}

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
    int128 res = 0;

    PG_TRY();
    {
        res = last_scope_identity_value();
    }
    PG_CATCH();
    {
        FlushErrorState();
        PG_RETURN_NULL();
    }
    PG_END_TRY();

    PG_RETURN_INT128(res);
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
    int128 res  = 0;
    bool success = false;
    int64 start = 0;
    bool seqidSuccess = false;

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

        seqidSuccess = get_seed(seqid, &start, &res, &success);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
    if (success) {
        PG_RETURN_INT128(res);
    }
    if (seqidSuccess) {
        PG_RETURN_INT64(start);
    }
    PG_RETURN_NULL();
}

static bool get_seed(Oid seqid, int64* start, int128* res, bool* success)
{
    PG_TRY();
    {
        /* Check the tuple directly. Catch error if NULL */
        *res = get_last_value_from_seq(seqid);
        *success = true;
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();

    /* If the relation exists, return the seed */
    if (seqid != InvalidOid) {
        int64 uuid = 0;
        int64 increment = 0;
        int64 maxvalue = 0;
        int64 minvalue = 0;
        int64 cachevalue = 0;
        bool cycle = false;
        Relation relseq = relation_open(seqid, AccessShareLock);
        get_sequence_params(relseq, &uuid, start, &increment, &maxvalue, &minvalue, &cachevalue, &cycle);
        relation_close(relseq, AccessShareLock);
        return true;
    }
    return false;
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
    TupleDesc	tupdesc = nullptr;
    AttrNumber	attnum = 0;
    Oid			seqid = InvalidOid;

    rel = RelationIdGetRelation(tableOid);
    tupdesc = RelationGetDescr(rel);

    for (attnum = 0; attnum < tupdesc->natts; attnum++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum);
        if (attr->attisdropped) {
            continue;
        }
        seqid = pg_get_serial_sequence_internal(tableOid, attr->attnum, true, NULL);
        if (OidIsValid(seqid)) {
            break;
        }
    }

    RelationClose(rel);
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

/*
 * Returns the difference of two timestamps based on a provided unit
 * INT64 representation for bigints
 */
PG_FUNCTION_INFO_V1(shark_timestamp_diff);
Datum shark_timestamp_diff(PG_FUNCTION_ARGS)
{
    text* field = PG_GETARG_TEXT_PP(0);
    Timestamp timestamp1 = PG_GETARG_TIMESTAMP(1);
    Timestamp timestamp2 = PG_GETARG_TIMESTAMP(2);
    int32 diff = -1;
    int tm1Valid = 0;
    int tm2Valid = 0;
    struct pg_tm tt1;
    struct pg_tm* tm1 = &tt1;
    fsec_t fsec1 = 0;
    struct pg_tm tt2;
    struct pg_tm* tm2 = &tt2;
    fsec_t fsec2 = 0;
    int type = 0;
    int val = 0;
    char* lower_case_units = nullptr;

    tm1Valid = timestamp2tm(timestamp1, NULL, tm1, &fsec1, NULL, NULL);
    tm2Valid = timestamp2tm(timestamp2, NULL, tm2, &fsec2, NULL, NULL);
    lower_case_units = downcase_truncate_identifier(VARDATA_ANY(field), VARSIZE_ANY_EXHDR(field), false);
    type = DecodeUnits(0, lower_case_units, &val);

    // Decode units does not handle doy properly
    if (strncmp(lower_case_units, "doy", 3) == 0) {
        type = UNITS;
        val = DTK_DOY;
    }
    if (strncmp(lower_case_units, "nanosecond", 11) == 0) {
        type = UNITS;
        val = DTK_NANO;
    }
    if (strncmp(lower_case_units, "weekday", 7) == 0) {
        type = UNITS;
        val = DTK_DAY;
    }
    if (type == UNITS) {
        if (tm1Valid == 0 && tm2Valid == 0) {
            diff = diff_cal(val, tm1, tm2, fsec1, fsec2);
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("\'%s\' is not a recognized %s option", lower_case_units, "datediff")));
    }

    PG_RETURN_INT32(diff);
}

static int32 diff_cal(int val, struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2)
{
    int32 diff = -1;
    bool overflow = false;
    switch (val) {
        case DTK_YEAR:
            diff = int32_year_diff(tm1, tm2);
            break;
        case DTK_QUARTER:
            diff = int32_quarter_diff(tm1, tm2);
            break;
        case DTK_MONTH:
            diff = int32_month_diff(tm1, tm2);
            break;
        case DTK_WEEK:
            diff = int32_week_diff(tm1, tm2);
            break;
        case DTK_DAY:
        case DTK_DOY:
            diff = int32_day_diff(tm1, tm2);
            break;
        case DTK_HOUR:
            diff = int32_hour_diff(tm1, tm2, &overflow);
            break;
        case DTK_MINUTE:
            diff = int32_minute_diff(tm1, tm2, &overflow);
            break;
        case DTK_SECOND:
            diff = int32_second_diff(tm1, tm2, &overflow);
            break;
        case DTK_MILLISEC:
            diff = int32_millisec_diff(tm1, tm2, fsec1, fsec2, &overflow);
            break;
        case DTK_MICROSEC:
            diff = int32_microsec_diff(tm1, tm2, fsec1, fsec2, &overflow);
            break;
        case DTK_NANO:
            diff = int32_nano_diff(tm1, tm2, fsec1, fsec2, &overflow);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("wrong input unit name")));
            break;
    }
    if (overflow) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("The datediff function resulted in an overflow. The number of dateparts separating two "
                               "date/time instances is too large. Try to use datediff with a less precise datepart")));
    }
    return diff;
}

/*
 * Returns the difference of two timestamps based on a provided unit
 * INT64 representation for bigints
 */
PG_FUNCTION_INFO_V1(shark_timestamp_diff_big);
Datum shark_timestamp_diff_big(PG_FUNCTION_ARGS)
{
    text* field = PG_GETARG_TEXT_PP(0);
    Timestamp timestamp1 = PG_GETARG_TIMESTAMP(1);
    Timestamp timestamp2 = PG_GETARG_TIMESTAMP(2);
    int64 diff = -1;
    int tm1Valid = 0;
    int tm2Valid = 0;
    struct pg_tm tt1;
    struct pg_tm* tm1 = &tt1;
    fsec_t fsec1 = 0;
    struct pg_tm tt2;
    struct pg_tm* tm2 = &tt2;
    fsec_t fsec2 = 0;
    int type = 0;
    int val = 0;
    char* lower_case_units = nullptr;

    tm1Valid = timestamp2tm(timestamp1, NULL, tm1, &fsec1, NULL, NULL);
    tm2Valid = timestamp2tm(timestamp2, NULL, tm2, &fsec2, NULL, NULL);
    lower_case_units = downcase_truncate_identifier(VARDATA_ANY(field), VARSIZE_ANY_EXHDR(field), false);
    type = DecodeUnits(0, lower_case_units, &val);

    // Decode units does not handle doy or nano properly
    if (strncmp(lower_case_units, "doy", 3) == 0) {
        type = UNITS;
        val = DTK_DOY;
    }
    if (strncmp(lower_case_units, "nanosecond", 11) == 0) {
        type = UNITS;
        val = DTK_NANO;
    }
    if (strncmp(lower_case_units, "weekday", 7) == 0) {
        type = UNITS;
        val = DTK_DAY;
    }

    if (type == UNITS) {
        if (tm1Valid == 0 && tm2Valid == 0) {
            diff = diff_cal_big(val, tm1, tm2, fsec1, fsec2);
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("\'%s\' is not a recognized %s option", lower_case_units, "datediff")));
    }

    PG_RETURN_INT64(diff);
}

static int64 diff_cal_big(int val, struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2)
{
    int64 diff = -1;
    bool overflow = false;
    switch (val) {
        case DTK_YEAR:
            diff = int64_year_diff(tm1, tm2);
            break;
        case DTK_QUARTER:
            diff = int64_quarter_diff(tm1, tm2);
            break;
        case DTK_MONTH:
            diff = int64_month_diff(tm1, tm2);
            break;
        case DTK_WEEK:
            diff = int64_week_diff(tm1, tm2);
            break;
        case DTK_DAY:
        case DTK_DOY:
            diff = int64_day_diff(tm1, tm2);
            break;
        case DTK_HOUR:
            diff = int64_hour_diff(tm1, tm2, &overflow);
            break;
        case DTK_MINUTE:
            diff = int64_minute_diff(tm1, tm2, &overflow);
            break;
        case DTK_SECOND:
            diff = int64_second_diff(tm1, tm2, &overflow);
            break;
        case DTK_MILLISEC:
            diff = int64_millisec_diff(tm1, tm2, fsec1, fsec2, &overflow);
            break;
        case DTK_MICROSEC:
            diff = int64_microsec_diff(tm1, tm2, fsec1, fsec2, &overflow);
            break;
        case DTK_NANO:
            diff = int64_nano_diff(tm1, tm2, fsec1, fsec2, &overflow);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("wrong input unit name")));
            break;
    }
    if (overflow) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("The datediff function resulted in an overflow. The number of dateparts separating two "
                               "date/time instances is too large. Try to use datediff with a less precise datepart")));
    }
    return diff;
}

static bool int64_multiply_add(int64 val, int64 multiplier, int64* sum)
{
    int64 product = 0;

    if (pg_mul_s64_overflow(val, multiplier, &product) || pg_add_s64_overflow(*sum, product, sum)) {
        return false;
    }
    return true;
}

static bool int32_multiply_add(int32 val, int32 multiplier, int32* sum)
{
    int32 product = 0;

    if (pg_mul_s32_overflow(val, multiplier, &product) || pg_add_s32_overflow(*sum, product, sum)) {
        return false;
    }
    return true;
}

static int days_in_date(int day, int month, int year)
{
    int days = year * 365 + day;
    for (int i = 1; i < month; i++) {
        if (i == 2) {
            days += 28;
        } else if (i == 4 || i == 6 || i == 9 || i == 11) {
            days += 30;
        } else {
            days += 31;
        }
    }
    if (month <= 2) {
        year -= 1;
    }
    days += (year / 4 - year / 100 + year / 400);
    return days;
}

PG_FUNCTION_INFO_V1(numeric_log10);
Datum numeric_log10(PG_FUNCTION_ARGS)
{
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 result = 0;
    Numeric arg1_numeric = 0;
    Numeric arg2_numeric = 0;
    Numeric result_numeric = 0;

    arg1_numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric, Float8GetDatum(arg1)));
    arg2_numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, 10));
    result_numeric =
        DatumGetNumeric(DirectFunctionCall2(numeric_log, NumericGetDatum(arg2_numeric), NumericGetDatum(arg1_numeric)));
    result = DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(result_numeric)));

    PG_RETURN_FLOAT8(result);
}

static int32 int32_year_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int32 diff = -1;

    diff = tm2->tm_year - tm1->tm_year;
    return diff;
}
static int32 int32_quarter_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int32 diff = -1;
    int32 yeardiff = 0;
    int32 monthdiff = 0;

    yeardiff = tm2->tm_year - tm1->tm_year;
    monthdiff = tm2->tm_mon - tm1->tm_mon;
    diff = (yeardiff * 12 + monthdiff) / 3;
    return diff;
}
static int32 int32_month_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int32 diff = -1;
    int32 yeardiff = 0;
    int32 monthdiff = 0;
    yeardiff = tm2->tm_year - tm1->tm_year;
    monthdiff = tm2->tm_mon - tm1->tm_mon;
    diff = yeardiff * 12 + monthdiff;
    return diff;
}
static int32 int32_week_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int32 diff = -1;
    int32 daydiff = 0;
    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    diff = daydiff / 7;
    if (daydiff % 7 >= 4) {
        diff++;
    }
    return diff;
}
static int32 int32_day_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int32 diff = -1;
    diff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    return diff;
}
static int32 int32_hour_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow)
{
    int32 diff = -1;
    int32 daydiff = 0;
    int32 hourdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    *overflow = (*overflow || !(int32_multiply_add(daydiff, 24, &hourdiff)));
    diff = hourdiff;
    return diff;
}
static int32 int32_minute_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow)
{
    int32 diff = -1;
    int32 daydiff = 0;
    int32 hourdiff = 0;
    int32 minutediff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    *overflow = (*overflow || !(int32_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int32_multiply_add(hourdiff, 60, &minutediff)));
    diff = minutediff;
    return diff;
}
static int32 int32_second_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow)
{
    int32 diff = -1;
    int32 daydiff = 0;
    int32 hourdiff = 0;
    int32 minutediff = 0;
    int32 seconddiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    *overflow = (*overflow || !(int32_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int32_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int32_multiply_add(minutediff, 60, &seconddiff)));
    diff = seconddiff;
    return diff;
}
static int32 int32_millisec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow)
{
    int32 diff = -1;
    int32 daydiff = 0;
    int32 hourdiff = 0;
    int32 minutediff = 0;
    int32 seconddiff = 0;
    int32 millisecdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    millisecdiff = (fsec2 / 1000) - (fsec1 / 1000);
    *overflow = (*overflow || !(int32_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int32_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int32_multiply_add(minutediff, 60, &seconddiff)));
    *overflow = (*overflow || !(int32_multiply_add(seconddiff, 1000, &millisecdiff)));
    diff = millisecdiff;
    return diff;
}
static int32 int32_microsec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow)
{
    int diff = -1;
    int32 yeardiff = 0;
    int32 monthdiff = 0;
    int32 daydiff = 0;
    int32 hourdiff = 0;
    int32 minutediff = 0;
    int32 seconddiff = 0;
    int32 millisecdiff = 0;
    int32 microsecdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    microsecdiff = fsec2 - fsec1;
    *overflow = (*overflow || !(int32_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int32_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int32_multiply_add(minutediff, 60, &seconddiff)));
    *overflow = (*overflow || !(int32_multiply_add(seconddiff, 1000000, &microsecdiff)));
    diff = microsecdiff;

    return diff;
}
static int32 int32_nano_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow)
{
    int32 diff = -1;
    int32 yeardiff = 0;
    int32 monthdiff = 0;
    int32 daydiff = 0;
    int32 hourdiff = 0;
    int32 minutediff = 0;
    int32 seconddiff = 0;
    int32 millisecdiff = 0;
    int32 microsecdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    microsecdiff = fsec2 - fsec1;
    *overflow = (*overflow || !(int32_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int32_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int32_multiply_add(minutediff, 60, &seconddiff)));
    *overflow = (*overflow || !(int32_multiply_add(seconddiff, 1000000, &microsecdiff)));
    *overflow = (*overflow || (pg_mul_s32_overflow(microsecdiff, 1000, &diff)));
    return diff;
}

static int64 int64_year_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int64 diff = -1;

    diff = tm2->tm_year - tm1->tm_year;
    return diff;
}
static int64 int64_quarter_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int64 diff = -1;
    int64 yeardiff = 0;
    int64 monthdiff = 0;

    yeardiff = tm2->tm_year - tm1->tm_year;
    monthdiff = tm2->tm_mon - tm1->tm_mon;
    diff = (yeardiff * 12 + monthdiff) / 3;
    return diff;
}
static int64 int64_month_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int64 diff = -1;
    int64 yeardiff = 0;
    int64 monthdiff = 0;
    yeardiff = tm2->tm_year - tm1->tm_year;
    monthdiff = tm2->tm_mon - tm1->tm_mon;
    diff = yeardiff * 12 + monthdiff;
    return diff;
}
static int64 int64_week_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int64 diff = -1;
    int64 daydiff = 0;
    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    diff = daydiff / 7;
    if (daydiff % 7 >= 4) {
        diff++;
    }
    return diff;
}
static int64 int64_day_diff(struct pg_tm* tm1, struct pg_tm* tm2)
{
    int64 diff = -1;
    diff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    return diff;
}
static int64 int64_hour_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow)
{
    int64 diff = -1;
    int64 daydiff = 0;
    int64 hourdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    *overflow = (*overflow || !(int64_multiply_add(daydiff, 24, &hourdiff)));
    diff = hourdiff;
    return diff;
}
static int64 int64_minute_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow)
{
    int64 diff = -1;
    int64 daydiff = 0;
    int64 hourdiff = 0;
    int64 minutediff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    *overflow = (*overflow || !(int64_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int64_multiply_add(hourdiff, 60, &minutediff)));
    diff = minutediff;
    return diff;
}
static int64 int64_second_diff(struct pg_tm* tm1, struct pg_tm* tm2, bool *overflow)
{
    int64 diff = -1;
    int64 daydiff = 0;
    int64 hourdiff = 0;
    int64 minutediff = 0;
    int64 seconddiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    *overflow = (*overflow || !(int64_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int64_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int64_multiply_add(minutediff, 60, &seconddiff)));
    diff = seconddiff;
    return diff;
}
static int64 int64_millisec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow)
{
    int64 diff = -1;
    int64 daydiff = 0;
    int64 hourdiff = 0;
    int64 minutediff = 0;
    int64 seconddiff = 0;
    int64 millisecdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    millisecdiff = (fsec2 / 1000) - (fsec1 / 1000);
    *overflow = (*overflow || !(int64_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int64_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int64_multiply_add(minutediff, 60, &seconddiff)));
    *overflow = (*overflow || !(int64_multiply_add(seconddiff, 1000, &millisecdiff)));
    diff = millisecdiff;
    return diff;
}
static int64 int64_microsec_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow)
{
    int diff = -1;
    int64 yeardiff = 0;
    int64 monthdiff = 0;
    int64 daydiff = 0;
    int64 hourdiff = 0;
    int64 minutediff = 0;
    int64 seconddiff = 0;
    int64 millisecdiff = 0;
    int64 microsecdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    microsecdiff = fsec2 - fsec1;
    *overflow = (*overflow || !(int64_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int64_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int64_multiply_add(minutediff, 60, &seconddiff)));
    *overflow = (*overflow || !(int64_multiply_add(seconddiff, 1000000, &microsecdiff)));
    diff = microsecdiff;

    return diff;
}
static int64 int64_nano_diff(struct pg_tm* tm1, struct pg_tm* tm2, fsec_t fsec1, fsec_t fsec2, bool *overflow)
{
    int64 diff = -1;
    int64 yeardiff = 0;
    int64 monthdiff = 0;
    int64 daydiff = 0;
    int64 hourdiff = 0;
    int64 minutediff = 0;
    int64 seconddiff = 0;
    int64 millisecdiff = 0;
    int64 microsecdiff = 0;

    daydiff =
        days_in_date(tm2->tm_mday, tm2->tm_mon, tm2->tm_year) - days_in_date(tm1->tm_mday, tm1->tm_mon, tm1->tm_year);
    hourdiff = tm2->tm_hour - tm1->tm_hour;
    minutediff = tm2->tm_min - tm1->tm_min;
    seconddiff = tm2->tm_sec - tm1->tm_sec;
    microsecdiff = fsec2 - fsec1;
    *overflow = (*overflow || !(int64_multiply_add(daydiff, 24, &hourdiff)));
    *overflow = (*overflow || !(int64_multiply_add(hourdiff, 60, &minutediff)));
    *overflow = (*overflow || !(int64_multiply_add(minutediff, 60, &seconddiff)));
    *overflow = (*overflow || !(int64_multiply_add(seconddiff, 1000000, &microsecdiff)));
    *overflow = (*overflow || (pg_mul_s64_overflow(microsecdiff, 1000, &diff)));
    return diff;
}
