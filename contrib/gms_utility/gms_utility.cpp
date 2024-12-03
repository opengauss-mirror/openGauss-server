/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
 *
 * gms_utility.cpp
 *  gms_utility provides various utility subprograms.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_utility/gms_utility.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"

#include "access/skey.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/heap.h"
#include "commands/sqladvisor.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/md5.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "tcop/tcopprot.h"
#include "parser/keywords.h"
#include "gms_utility.h"

PG_MODULE_MAGIC;

static const char CANON_INVALID_CHARS[] = 
    {'`', '~', '!', '@', '%', '^', '&', '*', '(', ')', '-', '=', '+', '[', ']', 
     '{', '}', '/', '\\', '|', ';', ':', '?', '<', '>', ','};
static const char VALID_IDENT_CHARS[] = {'#', '$', '_'};

static const char TOKENIZE_DANGER_CHARS[] = {'`', '~', '%', '\\', ';', '?'};
static const char TOKENIZE_TRUNCATE_CHARS[] = 
    {'!', '^', '&', '*', '(', ')', '-', '=', '+', '[', ']', '{', '}', '/', '|', ':', '<', '>'};

typedef Oid (*SearchOidByName)(Oid namespaceId, char* name, NameResolveVar* var);

static List* GetRelationsInSchema(char *namespc);
static void DoAnalyzeSchemaStatistic(char* schema, AnalyzeVar* var, AnalyzeMethodOpt methodOpt);
static void DoDeleteSchemaStatistic(char* schema);
static bool DetectKeyword(char* words);
static bool CheckLegalIdenty(char* w, bool checkDigital, bool checkKeyword);
static TokenizeVar* MakeTokenizeVar();
static void DestoryTokenizeVar(TokenizeVar* var);
static TokenizeVar* NameParseInternal(char* name, int len);
static void ResolveContextName(NameResolveContext context, Oid namespaceId, char* resolveName,
                               NameResolveVar* var, SearchOidByName searchmtd);

PG_FUNCTION_INFO_V1(gms_analyze_schema);
PG_FUNCTION_INFO_V1(gms_canonicalize);
PG_FUNCTION_INFO_V1(gms_compile_schema);
PG_FUNCTION_INFO_V1(gms_expand_sql_text);
PG_FUNCTION_INFO_V1(gms_get_cpu_time);
PG_FUNCTION_INFO_V1(gms_get_endianness);
PG_FUNCTION_INFO_V1(gms_get_sql_hash);
PG_FUNCTION_INFO_V1(gms_name_tokenize);
PG_FUNCTION_INFO_V1(gms_name_resolve);
PG_FUNCTION_INFO_V1(gms_is_bit_set);
PG_FUNCTION_INFO_V1(gms_old_current_schema);

static List* GetIndexColName(List* colnamesList, Oid relid, int2 attnum)
{
    if (attnum <= 0) {
        /* Hidden columns */
        return colnamesList;
    }

    char* colname = NULL;
    Form_pg_attribute pgAttributeTuple = NULL;
    HeapTuple attrTuple;

    attrTuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(attrTuple)) {
        return colnamesList;
    }
    pgAttributeTuple = (Form_pg_attribute)GETSTRUCT(attrTuple);
    colnamesList = lappend(colnamesList, pstrdup(pgAttributeTuple->attname.data));

    ReleaseSysCache(attrTuple);
    return colnamesList;
}

static void VacuumTableAclCheck(Oid relid, char* relname)
{
    AclResult aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_VACUUM);
    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    Form_pg_class rel = (Form_pg_class)GETSTRUCT(tuple);

    if (aclresult != ACLCHECK_OK &&
        !(pg_class_ownercheck(relid, GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !rel->relisshared) ||
                (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        ReleaseSysCache(tuple);
        aclcheck_error(aclresult, ACL_KIND_CLASS, relname);
    }

    ReleaseSysCache(tuple);
}

static List* GetRelationsInSchema(char *namespc)
{
    Relation pgClassRel = NULL;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    List* tbl_relnames = NIL;
    Oid nspid;

    nspid = get_namespace_oid(namespc, false);

    ScanKeyInit(&skey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspid));
    pgClassRel = heap_open(RelationRelationId, AccessShareLock);
    sysscan = systable_beginscan(pgClassRel, InvalidOid, false, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);
        if ((reltup->relkind == RELKIND_RELATION || reltup->relkind == RELKIND_MATVIEW)
            && !(reltup->relpersistence == RELPERSISTENCE_TEMP
                 || reltup->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)) {
            VacuumTableAclCheck(HeapTupleGetOid(tuple), reltup->relname.data);
            tbl_relnames = lappend(tbl_relnames, pstrdup(reltup->relname.data));
        }
    }
    systable_endscan(sysscan);
    heap_close(pgClassRel, AccessShareLock);
    return tbl_relnames;
}

static char* GetIndexNameById(Oid indexoid)
{
    char* indexName = NULL;
    HeapTuple idxRelTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(indexoid));
    if (!HeapTupleIsValid(idxRelTuple)) {
        return NULL;
    }
    Form_pg_class reltup = (Form_pg_class)GETSTRUCT(idxRelTuple);
    indexName = pstrdup(reltup->relname.data);

    ReleaseSysCache(idxRelTuple);

    return indexName;
}

static List* GetAllIndexColumnNames(char* schemaname, char* relname) 
{
    List* colnamesList = NIL;
    Oid relid = InvalidOid;
    Oid namespaceId = InvalidOid;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    Relation indexRel;
    HeapTuple idxTuple;
    Form_pg_index pgIndexTuple;
    int n;

    namespaceId = LookupExplicitNamespace(schemaname);
    relid = get_relname_relid(relname, namespaceId);

    ScanKeyInit(&skey[0], Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    indexRel = heap_open(IndexRelationId, AccessShareLock);
    sysscan = systable_beginscan(indexRel, IndexIndrelidIndexId, true, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(idxTuple = systable_getnext(sysscan))) {
        pgIndexTuple = (Form_pg_index)GETSTRUCT(idxTuple);
        if (!(IndexIsUsable(pgIndexTuple) && IndexIsValid(pgIndexTuple)
                && GetIndexEnableStateByTuple(idxTuple))) {
            systable_endscan(sysscan);
            heap_close(indexRel, AccessShareLock);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("index \"%s\" is not available", GetIndexNameById(pgIndexTuple->indexrelid))));
        }
        for (n = 0; n < pgIndexTuple->indnatts; n++) {
            colnamesList = GetIndexColName(colnamesList, relid, pgIndexTuple->indkey.values[n]);
        }
    }
    systable_endscan(sysscan);
    heap_close(indexRel, AccessShareLock);

    return colnamesList;
}

static void UpdateRelTuplesAndPages(Oid relid)
{
    Relation pgClassRel = NULL;
    HeapTuple tuple;
    HeapTuple newtuple;
    Form_pg_class reltuple;
    Datum values[Natts_pg_class] = {0};
    bool nulls[Natts_pg_class] = {false};
    bool replaces[Natts_pg_class] = {false};

    pgClassRel = heap_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for relation %u", relid)));
    }

    reltuple = (Form_pg_class)GETSTRUCT(tuple);
    values[Anum_pg_class_relpages - 1] = Float8GetDatum(0);
    replaces[Anum_pg_class_relpages - 1] = true;
    values[Anum_pg_class_reltuples - 1] = Float8GetDatum(0);
    replaces[Anum_pg_class_reltuples - 1] = true;

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgClassRel), values, nulls, replaces);
    simple_heap_update(pgClassRel, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(pgClassRel, newtuple);

    heap_freetuple_ext(tuple);
    heap_freetuple_ext(newtuple);

    heap_close(pgClassRel, RowExclusiveLock);
}

static AnalyzeMethodOpt GetMethodOpFromStr(char* val)
{
    bool canFollowSize = false;
    int offsetSize = 0;
    AnalyzeMethodOpt opt;
    char* str = val;

    if (0 == strncasecmp(str, "FORTABLE", 8)) {
        opt = METHOD_OPT_TABLE;
        offsetSize = 8;
    } else if (0 == strncasecmp(str, "FORALLINDEXES", 13)) {
        opt = METHOD_OPT_ALL_INDEX;
        offsetSize = 13;
    } else if (0 == strncasecmp(str, "FORALLCOLUMNS", 13)) {
        opt = METHOD_OPT_ALL_COLUMN;
        canFollowSize = true;
        offsetSize = 13;
    } else if (0 == strncasecmp(str, "FORALLINDEXEDCOLUMNS", 20)) {
        opt = METHOD_OPT_ALL_INDEX;
        canFollowSize = true;
        offsetSize = 20;
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY), errmsg("unrecognized param value")));
    }

    str += offsetSize;

    if (canFollowSize) {
        if (*str == '\0') {
            return opt;
        }

        if (0 != strncasecmp(str, "SIZE", 4)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY), errmsg("unrecognized param value")));
        }

        str += 4;
        if (1 != sscanf(str, "%d", &offsetSize) || offsetSize <= 0) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY), errmsg("unrecognized param value")));
        }
    } else {
        if (*str != '\0') {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid input value for method_opt")));
        }
    }
    return opt;
}

static char* GetNextTokenSplitBySpace(char* val, int* travelLen) 
{
    int len = 0;
    char* result;
    char* str = val + *travelLen;

    while (*str != '\0' && isspace((unsigned char)*str)) {
        str++;
        (*travelLen)++;
    }
    while (*(str + len) != '\0' && !isspace((unsigned char)*(str + len))) {
        len++;
    }
    (*travelLen) += len;
    if (len == 0) return NULL;

    result = (char *) palloc0(len + 1);
    errno_t rc = memcpy_s(result, len + 1, str, len);
    securec_check(rc, "\0", "\0");

    return result;
}

static AnalyzeMethodOpt HandleMethodOpt(text* methodOpt)
{
    if (NULL == methodOpt) {
        return METHOD_OPT_TABLE;
    }
    char* str = TextDatumGetCString(methodOpt);
    char* token;
    int travelLen;
    StringInfo format;
    AnalyzeMethodOpt tOpt;
    AnalyzeMethodOpt opt = METHOD_OPT_ALL_INDEX;

    format = makeStringInfo();
    travelLen = 0;

    while ((token = GetNextTokenSplitBySpace(str, &travelLen)) != NULL) {
        if (format->len == 0 && 0 != strcasecmp(token, "FOR")) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY),
                            errmsg("unrecognized param value: \"%s\"",  str)));
        }
        if (0 == strcasecmp(token, "FOR") && format->len > 0) {
            tOpt = GetMethodOpFromStr(format->data);
            opt = (int)tOpt < (int)opt ? tOpt : opt;
            resetStringInfo(format);
        }
        appendStringInfo(format, "%s", token);

        pfree(token);
        token = NULL;
    }
    pfree_ext(str);

    tOpt = GetMethodOpFromStr(format->data);
    opt = (int)tOpt < (int)opt ? tOpt : opt;

    DestroyStringInfo(format);

    return opt;
}

static AnalyzeVar* MakeAnalyzeVar(char* method, FunctionCallInfo fcinfo, int argOffset)
{
    Numeric estimateRows;
    Numeric estimatePercent;
    AnalyzeVar* result = (AnalyzeVar *) palloc(sizeof(AnalyzeVar));
    result->isEstimate = 0 == pg_strcasecmp(method, "ESTIMATE");
    if (!result->isEstimate) {
        return result;
    }
    result->validRows = !PG_ARGISNULL(argOffset);
    result->validPercent = !PG_ARGISNULL(argOffset + 1);

    if(result->validRows) {
        estimateRows = PG_GETARG_NUMERIC(argOffset);
        int64 rows = convert_short_numeric_to_int64_byscale(estimateRows, NUMERIC_DSCALE(estimateRows));
        if (rows < 0) {
            pfree(result);
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
                            errmsg("invalid value \"%ld\" for \"estimate_rows\"", rows)));
        }
        result->estimateRows = rows;
    }
    if (!result->validRows && result->validPercent) {
        estimatePercent = PG_GETARG_NUMERIC(argOffset + 1);
        int64 percent = convert_short_numeric_to_int64_byscale(estimatePercent, NUMERIC_DSCALE(estimatePercent));
        if (percent < 0 || percent > 100) {
            pfree(result);
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
                            errmsg("invalid value \"%ld\" for \"estimate_percent\"", percent)));
        }
        result->estimatePercent = percent;
    }

    return result;
}

Datum gms_analyze_schema(PG_FUNCTION_ARGS)
{
    char* schema;
    char* method;
    AnalyzeMethodOpt analyzeMethodOpt;
    AnalyzeVar* var;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
                        errmsg("Unsupported NULL input value")));
    }

    schema = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    method = TextDatumGetCString(PG_GETARG_TEXT_P(1));
    if (PG_ARGISNULL(4)) {
        analyzeMethodOpt = METHOD_OPT_TABLE;
    } else {
        analyzeMethodOpt = HandleMethodOpt(PG_GETARG_TEXT_P(4));
    }

    if (0 == pg_strcasecmp(method, "ESTIMATE") || 0 == pg_strcasecmp(method, "COMPUTE")) {
        var = MakeAnalyzeVar(method, fcinfo, 2);
        DoAnalyzeSchemaStatistic(schema, var, analyzeMethodOpt);
        pfree(var);
    } else if (0 == pg_strcasecmp(method, "DELETE")) {
        if (!PG_ARGISNULL(4)) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("The Command did not end correctly")));
        }
        DoDeleteSchemaStatistic(schema);
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY), 
                        errmsg("Unrecognized param value: \"%s\"", method)));
    }

    PG_RETURN_VOID();
}

static void DoAnalyzeSchemaStatistic(char* schema, AnalyzeVar* var, AnalyzeMethodOpt methodOpt)
{
    List* relnamesList;
    List* colnamesList;
    ListCell* lcRel;
    ListCell* lcCol;
    StringInfo executeSql;
    VacuumStmt* stmt;
    int saveEstimate;

    relnamesList = GetRelationsInSchema(schema);
    executeSql = makeStringInfo();
    
    if (var->isEstimate) {
        saveEstimate = default_statistics_target;
        default_statistics_target = var->validRows ? var->estimateRows
                                    : var->validPercent ? -(var->estimatePercent)
                                    : default_statistics_target;
    }

    foreach(lcRel, relnamesList) {
        char* relname = (char*)lfirst(lcRel);
        appendStringInfo(executeSql, "ANALYZE  %s.%s", quote_identifier(schema), quote_identifier(relname));
        if (methodOpt == METHOD_OPT_ALL_INDEX) {
            colnamesList = GetAllIndexColumnNames(schema, relname);
            if (colnamesList != NIL && list_length(colnamesList) > 0) {
                appendStringInfo(executeSql, "(");

                int n = 0, colSize = list_length(colnamesList);
                foreach(lcCol, colnamesList) {
                    char* colname = (char*)lfirst(lcCol);
                    appendStringInfo(executeSql, "%s", quote_identifier(colname));
                    if (++n < colSize) {
                        appendStringInfo(executeSql, ",");
                    }
                    pfree(colname);
                }
                appendStringInfo(executeSql, ")");
            }
            list_free(colnamesList);
        }
        appendStringInfo(executeSql, ";");

        List* parsetree_list = NULL;
        ListCell* parsetree_item = NULL;
        parsetree_list = raw_parser(executeSql->data, NULL);
        foreach (parsetree_item, parsetree_list) {
            Node* parsetree = (Node*)lfirst(parsetree_item);
            stmt = (VacuumStmt*)parsetree;
        }
        vacuum(stmt, InvalidOid, true, NULL, true);

        pfree_ext(relname);
        list_free(parsetree_list);
        resetStringInfo(executeSql);
    }

    default_statistics_target = saveEstimate;

    DestroyStringInfo(executeSql);
    list_free(relnamesList);
}

static void DoDeleteSchemaStatistic(char* schema)
{
    List* relnamesList;
    ListCell* lcRel;
    Oid relid = InvalidOid;
    Oid namespaceId = InvalidOid;

    namespaceId = LookupExplicitNamespace(schema);
    relnamesList = GetRelationsInSchema(schema);
    foreach(lcRel, relnamesList) {
        char* relname = (char*)lfirst(lcRel);

        relid = get_relname_relid(relname, namespaceId);
        RemoveStatistics<'c'>(relid, 0);
        UpdateRelTuplesAndPages(relid);

        pfree_ext(relname);
    }
    list_free(relnamesList);
}

static bool InvalidCanonChars(char val)
{
    for (char c : CANON_INVALID_CHARS) {
        if (c == val) return true;
    }
    return false;
}

/*
 * Check for keyword. return true except for unreserved ones.
 */
static bool DetectKeyword(char* w)
{
    int len = strlen(w);
    if (len <= 0) {
        return true;
    }

    char* str = (char *) palloc0(len + 1);
    errno_t rc = memcpy_s(str, len + 1, w, len);
    securec_check(rc, "\0", "\0");
    str = pg_strtolower(str);
    int kwnum = ScanKeywordLookup(str, &ScanKeywords);
    pfree_ext(str);

    return kwnum >= 0 && ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD;
}

static bool CheckLegalIdenty(char* w, bool checkDigital, bool checkKeyword)
{
    if (strlen(w) >= NAMEDATALEN) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("identifier too long, max length is %d", (NAMEDATALEN - 1))));
    }
    /* For gms_canonicalize, legual identy must not start with digit */
    if (checkDigital && isdigit(*w)) {
        return false;
    }
    if (checkKeyword && DetectKeyword(w)) {
        return false;
    }
    return true;
}

static void TrimEndSpace(char* str)
{
    if (str == NULL) {
        return;
    }

    int i = 0;
    int len = strlen(str);

    for (i = len - 1; i >= 0 && isspace((unsigned char)str[i]); i--);

    str[i + 1] = '\0';
}

static bool CheckAllDigital(char* w)
{
    if (w == NULL) {
        return false;
    }

    char* p = w;
    for (; *p != '\0' && !isdigit(*p); p++) {
        return false;
    }

    return true;
}

Datum gms_canonicalize(PG_FUNCTION_ARGS)
{
    char* name;
    int32 canonLen;
    int len;
    bool dotted = false;
    StringInfo tmp;
    StringInfo result;
    bits8 quoteState = QUOTE_NONE;
    char curChar = '\0';
    int traveLen = 0;
    text* output;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Input parameter \"canon_len\" is NULL")));
    }

    name = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    len = VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_P(0));
    canonLen = PG_GETARG_INT32(1);

    if (canonLen < 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Input parameter \"canon_len\" value \"%d\" is less than zero", canonLen)));
    }

    tmp = makeStringInfo();
    result = makeStringInfo();

    while (*name != '\0' && traveLen < len) {
        curChar = *name;
        traveLen++;
        name++;

        if (!IS_QUOTE_STARTED(quoteState) && curChar == ' ' && tmp->len == 0) {
            continue;
        } else if (curChar == '"') {
            if (BEFORE_QUOTE_STARTED(quoteState)) {
                if (tmp->len != 0) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid parameter value \"%s\" before quotation", (name - traveLen))));
                }
                quoteState = QUOTE_STARTED;
            } else if (IS_QUOTE_STARTED(quoteState)) {
                if (tmp->len == 0) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid paramter value \"%s\" with zero length", (name - traveLen))));
                }
                quoteState = QUOTE_ENDED;
            } else {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("Invalid parameter value \"%s\" after quotation", (name - traveLen))));
            }
        } else if (curChar == '.') {
            if (IS_QUOTE_STARTED(quoteState)) {
                appendStringInfoChar(tmp, curChar);
            } else {
                dotted = true;
                if (BEFORE_QUOTE_STARTED(quoteState) && !CheckLegalIdenty(tmp->data, true, dotted)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Invalid paramter value \"%s\" with special words", (name - traveLen))));
                }
                TrimEndSpace(tmp->data);
                if (BEFORE_QUOTE_STARTED(quoteState) && strstr(tmp->data, " ") != NULL) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid paramter value \"%s\"", (name - traveLen))));
                }
                if (tmp->len == 0) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid paramter value \"%s\" with zero length", (name - traveLen))));
                }
                appendStringInfo(result, dotted ? "\"%s\"." : "%s.", tmp->data);
                resetStringInfo(tmp);
                quoteState = QUOTE_NONE;
            }
        } else {
            if (IS_QUOTE_STARTED(quoteState)) {
                appendStringInfoChar(tmp, curChar);
            } else if (IS_QUOTE_END(quoteState)) {
                if (curChar == ' ') continue;
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("Invalid parameter value \"%s\" after quotation", (name - traveLen))));
            } else {
                if (InvalidCanonChars(curChar)) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid paramter value \"%s\" with special character", (name - traveLen))));
                }
                appendStringInfoChar(tmp, pg_toupper(curChar));
            }
        }
    }

    if (IS_QUOTE_STARTED(quoteState)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Invalid paramter value \"%s\" with quotation not closed", (name - traveLen))));
    }
    if (dotted && tmp->len == 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Invalid paramter value \"%s\"", (name - traveLen))));
    }
    TrimEndSpace(tmp->data);
    if (BEFORE_QUOTE_STARTED(quoteState) && strstr(tmp->data, " ") != NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Invalid paramter value \"%s\"", (name - traveLen))));
    }
    if (tmp->len > 0) {
        if (!(!dotted && CheckAllDigital(tmp->data))
             && BEFORE_QUOTE_STARTED(quoteState) && !CheckLegalIdenty(tmp->data, true, dotted)) {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Invalid paramter value \"%s\" with special words", (name - traveLen))));
        }
        appendStringInfo(result, dotted ? "\"%s\"" : "%s", tmp->data);
    }

    traveLen = strlen(result->data);
    canonLen = canonLen < traveLen ? canonLen : traveLen;
    output = cstring_to_text_with_len(result->data, canonLen);

    DestroyStringInfo(tmp);
    DestroyStringInfo(result);

    PG_RETURN_TEXT_P(output);
}

static void DoCompileFuncAndProcedure(Oid namespaceId, bool compileAll)
{
    Relation pgProcRel = NULL;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    bool isNull = false;
    Oid funcOid;
    Oid pkgOid;
    char pprokind;

    ScanKeyInit(&skey[0], Anum_pg_proc_pronamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));
    pgProcRel = heap_open(ProcedureRelationId, AccessShareLock);
    sysscan = systable_beginscan(pgProcRel, InvalidOid, false, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        funcOid = HeapTupleGetOid(tuple);
        if (!compileAll && GetPgObjectValid(funcOid, OBJECT_TYPE_PROC)) {
            continue;
        }
        pprokind = CharGetDatum(SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prokind, &isNull));
        if (!isNull && (PROC_IS_FUNC(pprokind) || PROC_IS_PRO(pprokind))) {
            pkgOid = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_packageid, &isNull);
            /* If function or procedure is belong to a package, don't recompile here  */
            if (!OidIsValid(pkgOid)) {
                RecompileSingleFunction(funcOid, PROC_IS_PRO(pprokind));
            }
        }
    }
    systable_endscan(sysscan);
    heap_close(pgProcRel, AccessShareLock);
}

static void DoCompilePackage(Oid namespaceId, bool compileAll)
{
    Relation pgPkgRel = NULL;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    bool isNull = false;
    Oid pkgOid;

    ScanKeyInit(&skey[0], Anum_gs_package_pkgnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));
    pgPkgRel = heap_open(PackageRelationId, AccessShareLock);
    sysscan = systable_beginscan(pgPkgRel, InvalidOid, false, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        SysCacheGetAttr(PACKAGEOID, tuple, Anum_gs_package_pkgbodydeclsrc, &isNull);
        pkgOid = HeapTupleGetOid(tuple);
        if (!compileAll && GetPgObjectValid(pkgOid, isNull ? OBJECT_TYPE_PKGSPEC : OBJECT_TYPE_PKGBODY)) {
            continue;
        }
        if (OidIsValid(pkgOid)) {
            RecompileSinglePackage(pkgOid, isNull);
        }
    }
    systable_endscan(sysscan);
    heap_close(pgPkgRel, AccessShareLock);
}

static void DoCompileView(Oid namespaceId, bool compileAll)
{
    Relation pgClassRel = NULL;
    ScanKeyData skey[2];
    SysScanDesc sysscan;
    HeapTuple tuple;
    List* records = NIL;
    Oid viewOid;

    ScanKeyInit(&skey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));
    ScanKeyInit(&skey[1], Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_VIEW));
    pgClassRel = heap_open(RelationRelationId, AccessShareLock);
    sysscan = systable_beginscan(pgClassRel, InvalidOid, false, SnapshotNow, 2, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        viewOid = HeapTupleGetOid(tuple);
        if (!ValidateDependViewDetectRecursion(viewOid, OBJECT_TYPE_VIEW, compileAll, records)) {
            Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);
            ereport(WARNING,
                   (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Compile view \"%s\" failed", reltup->relname.data)));
        }
    }
    list_free(records);
    systable_endscan(sysscan);
    heap_close(pgClassRel, AccessShareLock);
}

Datum gms_compile_schema(PG_FUNCTION_ARGS)
{
    char* nspname;
    bool compileAll;
    Oid namespaceId;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                        errmsg("Input parameter \"schema\" is NULL")));
    }

    nspname = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    compileAll = PG_GETARG_BOOL(1);

    namespaceId = get_namespace_oid(nspname, true);
    if (!OidIsValid(namespaceId)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("schema \"%s\" does not exists", nspname)));
    }

    DoCompileFuncAndProcedure(namespaceId, compileAll);

    DoCompilePackage(namespaceId, compileAll);

    DoCompileView(namespaceId, compileAll);

    /* Trigger compilation is not yet supported */
    PG_RETURN_VOID();
}

Datum gms_expand_sql_text(PG_FUNCTION_ARGS)
{
    text* inputSqlText;
    char* inputSql = NULL;
    text* result;
    List* parsetreeList = NIL;
    List* rewriteList = NIL;
    Node* parseNode;
    Query* query;
    StringInfo buf;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    inputSqlText = PG_GETARG_TEXT_P(0);
    FUNC_CHECK_HUGE_POINTER(false, inputSqlText, "gms_expand_sql_text");

    inputSql = TextDatumGetCString(inputSqlText);
    parsetreeList = pg_parse_query(inputSql);

    if (list_length(parsetreeList) != 1) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Expand_sql_text only support one query")));
    }
    parseNode = (Node*) linitial(parsetreeList);
    if (!IsA(parseNode, SelectStmt)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Unsupported query type, only support SELECT query")));
    }

    rewriteList = pg_analyze_and_rewrite(parseNode, inputSql, NULL, 0);
    query = (Query *) linitial(rewriteList);
    buf = makeStringInfo();
#ifdef PGXC
    deparse_query(query, buf, NIL, false, false);
#else
    get_query_def(query, buf, NIL, NULL, PRETTYFLAG_PAREN, WRAP_COLUMN_DEFAULT, 0);
#endif

    result = cstring_to_text(buf->data);

    list_free(parsetreeList);
    list_free(rewriteList);
    DestroyStringInfo(buf);

    PG_RETURN_TEXT_P(result);
}

Datum gms_get_cpu_time(PG_FUNCTION_ARGS)
{
    int64 result;

    clock_t clk = clock();
    /* get_cpu_time returns the number of 100th's of a second */
    result = (int64) (clk / (CLOCKS_PER_SEC / 100));

    return DirectFunctionCall1(int8_numeric, Int64GetDatum(result));
}

Datum gms_get_endianness(PG_FUNCTION_ARGS)
{
    int32 result;
    /* 1 for big-endian or 2 for little-endian */
    union {  
        uint32_t num;  
        uint8_t bytes[4];  
    } u_num;
    u_num.num = 0x12345678;
    if (u_num.bytes[0] == 0x12) {
        result = 1;
    } else if (u_num.bytes[0] == 0x78) {
        result = 2;
    } else {  
        ereport(ERROR, (errmsg("unkonwn endianness")));
    } 
    PG_RETURN_INT32(result);
}

Datum gms_get_sql_hash(PG_FUNCTION_ARGS)
{
    text* sqlText;
    int len;
    char hexsum[MD5_HASH_LEN + 1] = "\0";
    StringInfo buf;
    int64 result;
    TupleDesc tupdesc;
    Datum values[2];
    bool isnull[2];
    char last4Bytes[8 + 1] = "\0";

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    sqlText = PG_GETARG_TEXT_PP(0);
    FUNC_CHECK_HUGE_POINTER(false, sqlText, "gms_get_sql_hash");

    len = VARSIZE_ANY_EXHDR(sqlText);
    buf = makeStringInfo();

    tupdesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "hash", RAWOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "pre10ihash", NUMERICOID, -1, 0);
    BlessTupleDesc(tupdesc);

    if (!pg_md5_hash(VARDATA_ANY(sqlText), len, hexsum)) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("out of memory")));
    }

    values[0] = DirectFunctionCall1(rawin, CStringGetDatum(hexsum));
    isnull[0] = false;

    last4Bytes[0] = hexsum[MD5_HASH_LEN - 2];
    last4Bytes[1] = hexsum[MD5_HASH_LEN - 1];
    last4Bytes[2] = hexsum[MD5_HASH_LEN - 4];
    last4Bytes[3] = hexsum[MD5_HASH_LEN - 3];
    last4Bytes[4] = hexsum[MD5_HASH_LEN - 6];
    last4Bytes[5] = hexsum[MD5_HASH_LEN - 5];
    last4Bytes[6] = hexsum[MD5_HASH_LEN - 8];
    last4Bytes[7] = hexsum[MD5_HASH_LEN - 7];
    result = strtoll(last4Bytes, NULL, 16);

    /* pre10ihash is not support, returns last4bytes instead */
    values[1] = DirectFunctionCall1(int8_numeric, Int64GetDatum(result));
    isnull[1] = false;

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

static TokenizeVar* MakeTokenizeVar()
{
    TokenizeVar* var = (TokenizeVar *) palloc(sizeof(TokenizeVar));
    var->dblink = NULL;
    var->list = NIL;
    var->nextpos = 0;
    return var;
}

static void DestoryTokenizeVar(TokenizeVar* var)
{
    if (var != NULL) {
        list_free(var->list);
        pfree_ext(var->dblink);
        pfree(var);
    }
}

static bool IsDangerousChars(char ch)
{
    for (char c : TOKENIZE_DANGER_CHARS) {
        if (c == ch) return true;
    }
    return false;
}

static bool IsTruncateChars(char ch)
{
    for (char c : TOKENIZE_TRUNCATE_CHARS) {
        if (c == ch) return true;
    }
    return false;
}

static TokenizeVar* NameParseInternal(char* name, int len)
{
    List* list = NIL;
    int traveLen = 0;
    bits8 quoteState = QUOTE_NONE;
    char curChar = '\0';
    bool startLink = false;
    char* token = NULL;
    errno_t rc;
    TokenizeVar* var;
    StringInfo tmp = makeStringInfo();

    var = MakeTokenizeVar();

    while (*name != '\0' && traveLen < len) {
        curChar = *name;
        traveLen++;
        name++;

        if (!IS_QUOTE_STARTED(quoteState) && curChar == ' ') {
            continue;
        } else if (curChar == '"') {
            if (BEFORE_QUOTE_STARTED(quoteState)) {
                if (tmp->len != 0) {
                    /* If there exists valid char before double quote, end read. */
                    traveLen--;
                    break;
                }
                quoteState = QUOTE_STARTED;
            } else if (IS_QUOTE_STARTED(quoteState)) {
                if (tmp->len == 0) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid input value \"%s\" with zero length", (name - traveLen))));
                }
                quoteState = QUOTE_ENDED;
            } else {
                /* If there are more than one pair of double quote, end read. */
                break;
            }
        } else if (curChar == '.' || curChar == '@') {
            if (IS_QUOTE_STARTED(quoteState) || startLink) {
                appendStringInfoChar(tmp, curChar);
            } else {
                if (BEFORE_QUOTE_STARTED(quoteState) && !CheckLegalIdenty(tmp->data, false, true)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Invalid input value \"%s\" with special words", (name - traveLen))));
                }
                if (tmp->len == 0) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid input value \"%s\" with zero length", (name - traveLen))));
                }
                if (list_length(list) >= 3) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid input value \"%s\"", (name - traveLen))));
                }
                if (isdigit(*(tmp->data))) {
                    /* If word start with digit, end read. */
                    break;
                }
                token = (char *) palloc0(tmp->len + 1);
                rc = strcpy_s(token, tmp->len + 1, tmp->data);
                securec_check(rc, "\0", "\0");
                list = lappend(list, token);
                resetStringInfo(tmp);
                quoteState = QUOTE_NONE;
            }
            if (curChar == '@') {
                startLink = true;
            }
        } else {
            if (IS_QUOTE_STARTED(quoteState)) {
                appendStringInfoChar(tmp, curChar);
            } else if (IS_QUOTE_END(quoteState)) {
                traveLen--;
                break;
            } else {
                if (IsDangerousChars(curChar)) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("Invalid input value \"%s\" with special character", (name - traveLen))));
                }
                if (IsTruncateChars(curChar)) {
                    /* If meet special char, end read. */
                    traveLen--;
                    break;
                }
                appendStringInfoChar(tmp, pg_toupper(curChar));
            }
        }
    }

    if (IS_QUOTE_STARTED(quoteState)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Invalid input value \"%s\" with quotation not closed", (name - traveLen))));
    }
    if (tmp->len == 0 && !startLink) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Invalid input value \"%s\"", (name - traveLen))));
    }
    if (BEFORE_QUOTE_STARTED(quoteState) && !CheckLegalIdenty(tmp->data, false, true)) {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Invalid input value \"%s\" with special words", (name - traveLen))));
    }
    if (startLink) {
        if (tmp->len == 0) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("dblink is empty \"%s\"", (name - traveLen))));
        } else if (isdigit(*(tmp->data))) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Invalid dblink value \"%s\"", (name - traveLen))));
        } else {
            var->dblink = (char *) palloc0(tmp->len + 1);
            errno_t rc = strcpy_s(var->dblink, tmp->len + 1, tmp->data);
            securec_check(rc, "\0", "\0");
        }
    } else {
        if (list_length(list) >= 3) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Invalid input value \"%s\"", (name - traveLen))));
        }
        if (isdigit(*(tmp->data))) {
            traveLen -= (tmp->len + 2);
        } else {
            token = (char *) palloc0(tmp->len + 1);
            rc = strcpy_s(token, tmp->len + 1, tmp->data);
            securec_check(rc, "\0", "\0");
            list = lappend(list, token);
        }
    }

    var->nextpos = traveLen;
    var->list = list_copy(list);

    DestroyStringInfo(tmp);
    list_free(list);

    return var;
}

Datum gms_name_tokenize(PG_FUNCTION_ARGS)
{
    char* name = NULL;
    int len;
    ListCell* lc;
    TupleDesc tupdesc;
    Datum values[5];
    bool isnull[5];
    int tc = 0;
    TokenizeVar* var;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Invalid input value")));
    }
    name = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    len = VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_P(0));
    
    var = NameParseInternal(name, len);

    if (var->list == NIL || list_length(var->list) == 0 || list_length(var->list) > 3) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid Input value \"%s\"", name)));
    }

    tupdesc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "a", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "b", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "c", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "dblink", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "nextpos", INT4OID, -1, 0);

    BlessTupleDesc(tupdesc);

    errno_t rc = memset_s(isnull, 5, 1, 5);
    securec_check(rc, "\0", "\0");
    foreach (lc, var->list) {
        char* token = (char *) lfirst(lc);
        values[tc] = CStringGetTextDatum(token);
        isnull[tc++] = false;
    }
    if (var->dblink != NULL && strlen(var->dblink) > 0) {
        values[3] = CStringGetTextDatum(var->dblink);
        isnull[3] = false;
    }
    values[4] = Int32GetDatum(var->nextpos);
    isnull[4] = false;

    DestoryTokenizeVar(var);

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

static NameResolveVar* MakeNameResolveVar(List* list, char* name)
{
    int c = 0;
    ListCell* lc;
    errno_t rc;

    NameResolveVar* var = (NameResolveVar *) palloc0(sizeof(NameResolveVar));
    /* identifier length can't greater than NAMEDATALEN */
    var->schema = (char *) palloc0(NAMEDATALEN);
    var->part1 = (char *) palloc0(NAMEDATALEN);
    var->part2 = (char *) palloc0(NAMEDATALEN);
    var->part1Type = NAME_RESOLVE_TYPE_NONE;
    var->objectId = InvalidOid;

    foreach (lc, list) {
        char* token = (char *) lfirst(lc);
        token = pg_strtolower(token);
        if (c == 0) {
            rc = strcpy_s(var->schema, NAMEDATALEN, token);
        } else if (c == 1) {
            rc = strcpy_s(var->part1, NAMEDATALEN, token);
        } else {
            rc = strcpy_s(var->part2, NAMEDATALEN, token);
        }
        securec_check(rc, "\0", "\0");
        c++;
    }
    var->len = list_length(list);
    var->synonym = false;

    return var;
}

static void DestoryNameResolveVar(NameResolveVar* var)
{
    if (var != NULL) {
        pfree_ext(var->part2);
        pfree_ext(var->part1);
        pfree_ext(var->schema);
        pfree(var);
    }
}

static void ReportNameResolveAclErr(NameResolveVar* var)
{
    DestoryNameResolveVar(var);
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("The Object is not exists")));
}

static List* GetFillSchemaList()
{
    List* list = NIL;
    Oid namespaceId = InvalidOid;

    char* username = GetUserNameById(GetUserId());
    namespaceId = get_namespace_oid(username, true);
    pfree_ext(username);
    if (OidIsValid(namespaceId)) {
        list = lappend_oid(list, namespaceId);
    }
    list = lappend_oid(list, PG_PUBLIC_NAMESPACE);
    return list;
}

static Oid GetSchemaOidWithErrHandled(char* schemaName, NameResolveVar* var, HeapTuple synTuple = NULL)
{
    Oid namespaceId = get_namespace_oid(schemaName, true);
    if (!OidIsValid(namespaceId)) {
        if (synTuple != NULL) {
            ReleaseSysCache(synTuple);
        }
        ReportNameResolveAclErr(var);
    }
    AclResult aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK) {
        if (synTuple != NULL) {
            ReleaseSysCache(synTuple);
        }
        ReportNameResolveAclErr(var);
    }
    return namespaceId;
}

static Oid SearchPkgOidByName(Oid namespaceId, char* pkgName, NameResolveVar* var)
{
    Oid pkgOid = InvalidOid;
    AclResult aclresult;

    pkgOid = GetSysCacheOid2(PKGNAMENSP, CStringGetDatum(pkgName), ObjectIdGetDatum(namespaceId));
    if (OidIsValid(pkgOid)) {
        aclresult = pg_package_aclcheck(pkgOid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK) {
            ReportNameResolveAclErr(var);
        }
        var->part1Type = NAME_RESOLVE_TYPE_PACKAGE;
    }
    return pkgOid;
}

static void ResolvePkgNameFillSchema(NameResolveVar* var)
{
    List* tempActiveSearchPath = NIL;
    ListCell* lc;
    errno_t rc;
    char* part1 = pstrdup(var->part1);

    tempActiveSearchPath = GetFillSchemaList();
    foreach (lc, tempActiveSearchPath) {
        Oid namespaceId = lfirst_oid(lc);
        ResolveContextName(NR_CONTEXT_PLSQL, namespaceId, var->schema, var, SearchPkgOidByName);
        if (OidIsValid(var->objectId)) {
            /* Fill schema part1 -> part2, schema -> part1, selectSchema -> schema */
            rc = strcpy_s(var->part2, NAMEDATALEN, part1);
            securec_check(rc, "\0", "\0");
            if (!var->synonym) {
                /* not synonym object */
                rc = strcpy_s(var->part1, NAMEDATALEN, var->schema);
                securec_check(rc, "\0", "\0");
                rc = strcpy_s(var->schema, NAMEDATALEN, get_namespace_name(namespaceId));
                securec_check(rc, "\0", "\0");
            }
            break;
        }
    }
    pfree_ext(part1);
    list_free(tempActiveSearchPath);
}

static Oid SearchProcOidByName(Oid namespaceId, char* procName, NameResolveVar* var)
{
    CatCList* catlist = NULL;
    HeapTuple tuple;
    Form_pg_proc procform;
    Oid procOid = InvalidOid;
    bool isNull = true;
    char pprokind = '\0';

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(procName));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(procName));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(procName));
#endif

    for (int i = 0; i < catlist->n_members; i++) {
        tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        procform = (Form_pg_proc)GETSTRUCT(tuple);
        if (procform->pronamespace != namespaceId) continue;
        
        pprokind = CharGetDatum(SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prokind, &isNull));
        if (!isNull && (PROC_IS_FUNC(pprokind) || PROC_IS_PRO(pprokind))) {
            procOid = HeapTupleGetOid(tuple);
            var->part1Type = PROC_IS_FUNC(pprokind) ? NAME_RESOLVE_TYPE_FUNCTION
                                : PROC_IS_PRO(pprokind) ? NAME_RESOLVE_TYPE_PROCEDURE
                                : NAME_RESOLVE_TYPE_NONE;
        }
        AclResult aclresult = pg_proc_aclcheck(procOid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK) {
            ReleaseSysCacheList(catlist);
            ReportNameResolveAclErr(var);
        }
        break;
    }
    ReleaseSysCacheList(catlist);

    return procOid;
}

static Oid SearchRelOidByName(Oid namespaceId, char* relName, NameResolveVar* var)
{
    Oid relOid = InvalidOid;
    AclResult aclResult;

    relOid =  get_relname_relid(relName, namespaceId);
    if (OidIsValid(relOid)) {
        aclResult = pg_class_aclcheck(relOid, GetUserId(), ACL_SELECT);
        if (aclResult != ACLCHECK_OK) {
            ReportNameResolveAclErr(var);
        }
    }
    return relOid;
}

static Oid SearchTriggerOidByName(Oid namespaceId, char* triggerName, NameResolveVar* var)
{
    Relation tgrel;
    ScanKeyData keys[1];
    SysScanDesc tgscan;
    HeapTuple tuple;
    bool found = false;
    Oid tgOid = InvalidOid;

    ScanKeyInit(&keys[0], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(triggerName));

    tgrel = heap_open(TriggerRelationId, AccessShareLock);
    tgscan = systable_beginscan(tgrel, TriggerNameIndexId, true, SnapshotNow, 1, keys);
    while (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
        if (found) {
            systable_endscan(tgscan);
            heap_close(tgrel, AccessShareLock);
            DestoryNameResolveVar(var);
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate trigger found")));
        }
        tgOid = HeapTupleGetOid(tuple);
        found = true;
    }
    systable_endscan(tgscan);
    heap_close(tgrel, AccessShareLock);
    return tgOid;
}

static Oid SearchTypeOidByName(Oid namespaceId, char* typName, NameResolveVar* var)
{
    HeapTuple tuple;
    Oid typId = InvalidOid;

    tuple = SearchSysCache2(TYPENAMENSP, CStringGetDatum(typName), ObjectIdGetDatum(namespaceId));

    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }
    typId = HeapTupleGetOid(tuple);
    if (OidIsValid(typId)) {
        AclResult aclResult = pg_type_aclcheck(typId, GetUserId(), ACL_USAGE);
        if (aclResult != ACLCHECK_OK) {
            ReleaseSysCache(tuple);
            ReportNameResolveAclErr(var);
        }
    }
    ReleaseSysCache(tuple);
    return typId;
}

static void ResolveContextName(NameResolveContext context, Oid namespaceId, char* resolveName,
                               NameResolveVar* var, SearchOidByName searchmtd)
{
    Oid id = InvalidOid;
    HeapTuple synTuple;
    Oid newNamespaceId = InvalidOid;

    id = searchmtd(namespaceId, resolveName, var);
    if (OidIsValid(id)) {
        var->objectId = id;
        return;
    }

    if (!(context == NR_CONTEXT_PLSQL || context == NR_CONTEXT_TABLE || context == NR_CONTEXT_SEQUENCES
        || context == NR_CONTEXT_TYPE)) {
        return;
    }
    /* Search synonym */
    synTuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(resolveName), ObjectIdGetDatum(namespaceId));
    if (!HeapTupleIsValid(synTuple)) {
        return;
    }
    Form_pg_synonym synForm = (Form_pg_synonym)GETSTRUCT(synTuple);
    newNamespaceId = GetSchemaOidWithErrHandled(NameStr(synForm->synobjschema), var, synTuple);
    id = searchmtd(newNamespaceId, NameStr(synForm->synobjname), var);
    if (OidIsValid(id)) {
        /* Replace synonym to real name, and do it for schema too. */
        errno_t rc = strcpy_s(var->schema, NAMEDATALEN, NameStr(synForm->synobjschema));
        securec_check(rc, "\0", "\0");
        rc = strcpy_s(var->part1, NAMEDATALEN, NameStr(synForm->synobjname));
        securec_check(rc, "\0", "\0");
        var->objectId = id;
        var->synonym = true;
    }
    ReleaseSysCache(synTuple);
}

static void ResolveObjectNameByContext(NameResolveContext context, Oid namespaceId, char* resolveName, NameResolveVar* var)
{
    errno_t rc;

    switch (context) {
        case NR_CONTEXT_PLSQL:
            /* Search func、proc、package */
            ResolveContextName(context, namespaceId, resolveName, var, SearchProcOidByName);
            break;
        case NR_CONTEXT_TABLE:
        case NR_CONTEXT_SEQUENCES:
        case NR_CONTEXT_INDEX:
            ResolveContextName(context, namespaceId, resolveName, var, SearchRelOidByName);
            var->part1Type = context == NR_CONTEXT_TABLE ? NAME_RESOLVE_TYPE_TABLE
                             : context == NR_CONTEXT_INDEX ? NAME_RESOLVE_TYPE_INDEX
                             : NAME_RESOLVE_TYPE_SEQUENCE;
            break;
        case NR_CONTEXT_TRIGGER:
            ResolveContextName(context, namespaceId, resolveName, var, SearchTriggerOidByName);
            var->part1Type = NAME_RESOLVE_TYPE_TRIGGER;
            break;
        case NR_CONTEXT_TYPE:
            ResolveContextName(context, namespaceId, resolveName, var, SearchTypeOidByName);
            var->part1Type = NAME_RESOLVE_TYPE_TYPE;
            break;
        case NR_CONTEXT_UNKNOWN:
            /* Only support for context value 10 */
            ReportNameResolveAclErr(var);
            break;
        default:
            var->part1Type = NAME_RESOLVE_TYPE_NONE;
            break;
    }

    if (var->len == 1 && !var->synonym) {
        rc = strcpy_s(var->part1, NAMEDATALEN, var->schema);
        securec_check(rc, "\0", "\0");
        rc = strcpy_s(var->schema, NAMEDATALEN, get_namespace_name(namespaceId));
        securec_check(rc, "\0", "\0");
    }

    /* top-level function | procedure, part1 is empty, part2 is real function | procedure name */
    if ((var->part1Type == NAME_RESOLVE_TYPE_FUNCTION || var->part1Type == NAME_RESOLVE_TYPE_PROCEDURE)
        && (var->part2 == NULL || strlen(var->part2) == 0)) {
        rc = strcpy_s(var->part2, NAMEDATALEN, var->part1);
        securec_check(rc, "\0", "\0");
        rc = memset_s(var->part1, NAMEDATALEN, 0, NAMEDATALEN);
        securec_check(rc, "\0", "\0");
    }
}

static NameResolveContext GetNameResolveContext(Numeric num)
{
    int32 contextNumber = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(num)));
    if (contextNumber < 0 || contextNumber > 10) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("context argument must be number 0 to 10")));
    }
    NameResolveContext context = (NameResolveContext) contextNumber;
    if (context == NR_CONTEXT_JAVA_SOURCE || context == NR_CONTEXT_JAVA_RESOURCE || context ==NR_CONTEXT_JAVA_CLASS
        || context == NR_CONTEXT_JAVA_SHARED_DATA) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Unsupported context type: %d", (int)context)));
    }
    return context;
}

static void SetResultValues(Datum* values, bool* isnull, NameResolveVar* var)
{
    if (var->schema != NULL && strlen(var->schema) > 0) {
        values[0] = CStringGetTextDatum(pg_strtoupper(var->schema));
        isnull[0] = false;
    }
    if (var->part1 != NULL && strlen(var->part1) > 0) {
        values[1] = CStringGetTextDatum(pg_strtoupper(var->part1));
        isnull[1] = false;
    }
    if (var->part2 != NULL && strlen(var->part2) > 0) {
        values[2] = CStringGetTextDatum(pg_strtoupper(var->part2));
        isnull[2] = false;
    }
}

Datum gms_name_resolve(PG_FUNCTION_ARGS)
{
    char* name = NULL;
    NameResolveContext context;
    int len;
    TupleDesc tupdesc;
    const int outputParamLen = 6;
    Datum values[outputParamLen];
    bool isnull[outputParamLen];
    TokenizeVar* tokenizeVar;
    NameResolveVar* var;
    int listLen;
    Oid namespaceId = InvalidOid;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid input value")));
    }
    name = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    len = VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_P(0));
    context = GetNameResolveContext(PG_GETARG_NUMERIC(1));
    
    tokenizeVar = NameParseInternal(name, len);
    listLen = list_length(tokenizeVar->list);

    if (tokenizeVar->list == NIL || listLen == 0 || listLen > 3) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid Input value \"%s\"", name)));
    }

    tupdesc = CreateTemplateTupleDesc(outputParamLen, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "schema", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "part1", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "part2", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "dblink", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "part1_type", NUMERICOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "object_number", NUMERICOID, -1, 0);

    BlessTupleDesc(tupdesc);

    errno_t rc = memset_s(isnull, outputParamLen, 1, outputParamLen);
    securec_check(rc, "\0", "\0");

    var = MakeNameResolveVar(tokenizeVar->list, name);
    pfree_ext(name);

    /* Set part1_type and object_number default value */
    values[4] = DirectFunctionCall1(int4_numeric, Int32GetDatum(var->part1Type));
    isnull[4] = false;
    values[5] = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
    isnull[5] = false;

    /* If dblink is not null, don't resolve */
    if (tokenizeVar->dblink != NULL && strlen(tokenizeVar->dblink) > 0) {
        SetResultValues(values, isnull, var);
        values[3] = CStringGetTextDatum(tokenizeVar->dblink);
        isnull[3] = false;
        DestoryTokenizeVar(tokenizeVar);
        DestoryNameResolveVar(var);
        return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
    }
    DestoryTokenizeVar(tokenizeVar);

    if (listLen == 3) {
        namespaceId = GetSchemaOidWithErrHandled(var->schema, var);
        ResolveContextName(context, namespaceId, var->part1, var, SearchPkgOidByName);
    } else if (listLen == 2) {
        /* If is pkg.c, fill schema and search */
        if (context == NR_CONTEXT_PLSQL) {
            ResolvePkgNameFillSchema(var);
        }
        if (!OidIsValid(var->objectId)) {
            namespaceId = GetSchemaOidWithErrHandled(var->schema, var);
            ResolveObjectNameByContext(context, namespaceId, var->part1, var);
        }
    } else {
        ListCell* lc;
        List* tempActiveSearchPath = GetFillSchemaList();
        foreach (lc, tempActiveSearchPath) {
            namespaceId = lfirst_oid(lc);
            ResolveObjectNameByContext(context, namespaceId, var->schema, var);
            if (OidIsValid(var->objectId)) {
                break;
            }
        }
        list_free(tempActiveSearchPath);
    }

    if (!OidIsValid(var->objectId)) {
        ReportNameResolveAclErr(var);
    }
    SetResultValues(values, isnull, var);
    values[4] = DirectFunctionCall1(int4_numeric, Int32GetDatum(var->part1Type));
    values[5] = DirectFunctionCall1(int4_numeric, Int32GetDatum(var->objectId));
    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

static int HexCharToDec(char hexChar) {  
    char lowerHexChar = pg_tolower(hexChar);

    if (lowerHexChar >= '0' && lowerHexChar <= '9') {
        return lowerHexChar - '0';
    } else if (lowerHexChar >= 'a' && lowerHexChar <= 'f') {
        return lowerHexChar - 'a' + 10;
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid hexadecimal digit: \"%d\"", hexChar)));
    }
    return -1; /* make compiler quiet */
} 

/*
 * Checks the bit setting for the given bit in the given RAW value.
 * - raw: input raw value;
 * - pos: bit in raw to check, start from 1.
 */
Datum gms_is_bit_set(PG_FUNCTION_ARGS)
{
    bytea* data;
    char* hexStr = NULL;
    int len;
    int pos;
    int chVal;
    uint32 result;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid input value")));
    }

    data = PG_GETARG_BYTEA_P(0);
    len = VARSIZE_ANY_EXHDR(data);

    hexStr = (char *) palloc0(len * 2 + 1);
    hex_encode(VARDATA_ANY(data), len, hexStr);
    len = strlen(hexStr);

    if (PG_ARGISNULL(1)) {
        /* if input is null, get last bit */
        pos = len * 4;
    } else {
        pos = DirectFunctionCall1(numeric_int4, PG_GETARG_DATUM(1));
    }
    
    if (pos <= 0 || pos > INT32_MAX) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid second param value range")));
    }

    /* convert pos to cycle 0~(len * 4 - 1) */
    pos = (pos - 1) % (len * 4);

    /*
     * Find binary position in chars with 'pos',
     * from high to low with the lowest bit being
     * bit number 1.
     * for example: hex input: 124578AF, pos: 9
     * 0001 0010 0100 0101 0111 1000 1010 1111
     *                             ^
     *                             9 8765 4321
     * and each hex char indicate 4 bits, find offset
     * bit pos in single char from right, use pos % 4 
     * get right offset.
     */
    chVal = HexCharToDec(hexStr[len - 1 - pos / 4]);
    result = (chVal >> (pos % 4)) & 0x01;

    pfree_ext(hexStr);

    return DirectFunctionCall1(int4_numeric, Int32GetDatum(result));
}

Datum gms_old_current_schema(PG_FUNCTION_ARGS)
{
    Name schemaName = DatumGetName(OidFunctionCall0(CURRENTSCHEMAFUNCOID));
    return CStringGetTextDatum(schemaName->data);
}
