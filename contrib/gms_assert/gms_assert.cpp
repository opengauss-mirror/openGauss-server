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
 * gms_assert.cpp
 *  gms_assert can provide an interface to validate properties of the input value
 *
 *
 * IDENTIFICATION
 *        contrib/gms_assert/gms_assert.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/namespace.h"
#include "catalog/gs_package.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_proc.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

#include "gms_assert.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(noop);
PG_FUNCTION_INFO_V1(enquote_literal);
PG_FUNCTION_INFO_V1(simple_sql_name);
PG_FUNCTION_INFO_V1(enquote_name);
PG_FUNCTION_INFO_V1(qualified_sql_name);
PG_FUNCTION_INFO_V1(schema_name);
PG_FUNCTION_INFO_V1(sql_object_name);

#define IS_SIMPLE_NAME_START(c) (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || \
                                (c == '_') || ('\200' <= c && c <= '\377'))
#define IS_SIMPLE_NAME_CONT(c) (IS_SIMPLE_NAME_START(c) || ('0' <= c && c <= '9') || \
                                (c == '$') || (c == '#'))

static char* QuoteString(char* str, char qMark);
static bool SimpleNameInternal(char* rawname);
static char* TrimSqlName(char* rawname);
static bool LocalQualifiedName(char* rawname);
static void SplitQualifiedName(char* rawname, char sep, List** namelist);
static void QualifiedSqlNameInternal(text* textIn);
static bool FuncObjectName(char* funcname, Oid namespaceId);

char* QuoteString(char* str, char qMark)
{
    int len = strlen(str);
    char* result = static_cast<char*>(palloc((len + 3) * sizeof(char)));
    result[0] = qMark;
    int rc = memcpy_s(result+1, len, str, len);
    securec_check(rc, "\0", "\0");
    result[len + 1] = qMark;
    result[len + 2] = '\0';
    return result;
}

bool SimpleNameInternal(char* strIn)
{
    if (strIn == NULL || strlen(strIn) == 0) {
        return false;
    }

    char* ptr = strIn;
    bool quoteHead = false;
    quoteHead = *ptr == '\"';

    if (quoteHead) {
        ptr++;
        while (*ptr != '\0') {
            if (*ptr == '\"') {
                if (*(ptr + 1) == '\0') {
                    return true;
                }
                if (*(ptr + 1) != '\"') {
                    return false;
                }
                ptr++;
            }
            ptr++;
        }
        /* the str has a quote mark at the begining,
         but dont't have a corresponding one in the end */
        return false;
    } else {
        if (!IS_SIMPLE_NAME_START(*ptr)) {
            return false;
        }
        while (*ptr != '\0') {
            if (!IS_SIMPLE_NAME_CONT(*ptr)) {
                return false;
            }
            ptr++;
        }
        return true;
    }
}

char* TrimSqlName(char* rawname)
{
    int headSpaceLen = 0;
    int nameLen = 0;
    char* ptr = rawname;
    char* result;

    while (isspace((unsigned char)*ptr)) {
        headSpaceLen++;
        ptr++;
    }
    while (!isspace((unsigned char)*ptr) && *ptr != '\0') {
        if (*ptr == '\"') {
            ptr++;
            nameLen++;
            while (*ptr != '\"') {
                if (*ptr == '\0') {
                    return NULL;
                }
                ptr++;
                nameLen++;
            }
        }
        nameLen++;
        ptr++;
    }
    while (isspace((unsigned char)*ptr)) ptr++;
    if (*ptr != '\0') {
        return NULL;
    }

    result = static_cast<char*>(palloc((nameLen + 1) * sizeof(char)));
    int rc = memcpy_s(result, nameLen, rawname+headSpaceLen, nameLen);
    securec_check(rc, "\0", "\0");
    result[nameLen] = '\0';
    return result;
}

bool LocalQualifiedName(char* rawname)
{
    if (strlen(rawname) == 0) {
        return false;
    }

    List* simpleNameList = NIL;
    ListCell* lc = NULL;

    SplitQualifiedName(rawname, '.', &simpleNameList);
    if (simpleNameList == NIL) {
        return false;
    }
    foreach (lc, simpleNameList) {
        char* simpleName = (char*)lfirst(lc);
        if (!SimpleNameInternal(simpleName)) {
            return false;
        }
    }
    return true;
}

/* SplitIdentifierString cannot deal with situation like: "a"."b"@c,
   and it eats the quote mark. */
void SplitQualifiedName(char* rawname, char sep, List** namelist)
{
    char* nextp = rawname;
    *namelist = NIL;
    while (isspace((unsigned char)* nextp)) {
        nextp++;
    }

    char* curname = NULL;
    char* startp = nextp;
    char* endp = NULL;
    bool done = false;
    do {
        if (*nextp == '\"') {
            for (;;) {
                endp = strchr(nextp + 1, '\"');
                if (endp == NULL) {
                    return;
                }
                if (endp[1] != '\"') {
                    nextp = endp + 1;
                    break;
                } else {
                    nextp = endp + 2;
                }
            }
        } else {
            int len;
            while (*nextp && *nextp != sep && *nextp != '\"' &&
             !isspace((unsigned char)* nextp)) {
                nextp++;
            }
            endp = nextp;
            if (startp == nextp) {
                return;
            }
            len = endp - startp;
            if (*nextp == sep) {
                nextp++;
            } else if (*nextp == '\0') {
                done = true;
            } else if (*nextp == '\"') {
                continue;
            } else {
                while (isspace((unsigned char)* nextp)) {
                    nextp++;
                }
                if (*nextp != '\0') {
                    *namelist = NIL;
                    return;
                }
                done = true;
            }
            *endp = '\0';
            *namelist = lappend(*namelist, startp);
            startp = nextp;
        }
    } while(!done);
}

void QualifiedSqlNameInternal(text* txtIn)
{
    List* qualifiedNameList = NIL;
    ListCell* lc = NULL;
    int lLen = 0;
    int i = 1;

    SplitQualifiedName(text_to_cstring(txtIn), '@', &qualifiedNameList);
    lLen = list_length(qualifiedNameList);
    if (lLen > 3 || lLen == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid qualified SQL name")));
    }
    foreach (lc, qualifiedNameList) {
        char* localName = (char*)lfirst(lc);
        if (lLen == 3 && lc->next == NULL) {
            if (!SimpleNameInternal(localName)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid qualified SQL name")));
            }
        } else {
            if (!LocalQualifiedName(localName)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid qualified SQL name")));
            }
        }
    }
}

bool FuncObjectName(char* funcname, Oid namespaceId)
{
    CatCList* catlist = NULL;
    Oid funcoid = InvalidOid;
    int i;
    bool ispackage;

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif

    for (i = 0; i < catlist->n_members; i++) {
        bool isNull;
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        if (!HeapTupleIsValid(proctup) || !OidIsValid(HeapTupleGetOid(proctup))) {
            continue;
        }
        funcoid = HeapTupleGetOid(proctup);
        ispackage = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_package, &isNull);
        Oid pronamespace =
            DatumGetObjectId(SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_pronamespace, &isNull));
        /* pkgname have already been handled, skip functions in package */
        if (!isNull && ispackage) {
            continue;
        }
        if (OidIsValid(namespaceId)) {
            if (pronamespace != namespaceId) {
                continue;
            }
        } else {
            ListCell* nsp = NULL;
            foreach (nsp, u_sess->catalog_cxt.activeSearchPath) {
                if (pronamespace == lfirst_oid(nsp) &&
                    pronamespace != u_sess->catalog_cxt.myTempNamespace) {
                    break;
                }
            }
            if (nsp == NULL) {
                continue;
            }
        }

        if (OidIsValid(funcoid)) {
            bool objectValid = GetPgObjectValid(funcoid, OBJECT_TYPE_PROC);
            if (objectValid) {
                ReleaseSysCacheList(catlist);
                return true;
            }
        }
    }
    ReleaseSysCacheList(catlist);
    return false;
}

Datum noop(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

Datum enquote_literal(PG_FUNCTION_ARGS)
{
    char* result;
    text* resTxt;
    if (PG_ARGISNULL(0)) {
        result = "\'\'";
        PG_RETURN_TEXT_P(cstring_to_text(result));
    }

    text* txtIn = PG_GETARG_TEXT_PP(0);
    char* rawstr = text_to_cstring(txtIn);
    char* ptr = rawstr;
    bool quoteHead = *ptr == '\'';
    bool quoteTail = false;

    if (quoteHead) {
        ptr++;
    }

    while (*ptr != '\0') {
        if (*ptr == '\'') {
            if (*(ptr + 1) == '\0') {
                quoteTail = true;
                break;
            }
            if (*(ptr + 1) != '\'') {
                ereport(ERROR, (
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("numeric or value error")));
            }
            ptr++;
        }
        ptr++;
    }

    if (quoteHead && quoteTail) {
        /* already quoted */
        result = rawstr;
    } else if (quoteHead || quoteTail) {
        ereport(ERROR, (
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("numeric or value error")));
    } else {
        result = QuoteString(rawstr, '\'');
        pfree(rawstr);
    }
    resTxt = cstring_to_text(result);
    pfree(result);
    PG_RETURN_TEXT_P(resTxt);
}

Datum simple_sql_name(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid SQL name")));
    }

    char* trimed = TrimSqlName(text_to_cstring(PG_GETARG_TEXT_PP(0)));
    if (SimpleNameInternal(trimed)) {
        text* resTxt = cstring_to_text(trimed);
        pfree(trimed);
        PG_RETURN_TEXT_P(resTxt);
    } else {
        ereport(ERROR, (
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid SQL name")));
    }
}

Datum enquote_name(PG_FUNCTION_ARGS)
{
    char* result;
    text* resTxt;
    if (PG_ARGISNULL(0)) {
        result = "\"\"";
        PG_RETURN_TEXT_P(cstring_to_text(result));
    }

    text* txtIn = PG_GETARG_TEXT_PP(0);
    bool capitalize = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
    char* rawstr = text_to_cstring(txtIn);
    char* ptr = rawstr;
    bool quoteHead = *ptr == '\"';

    if (quoteHead) {
        result = rawstr;
    } else {
        char* capital = rawstr;
        if (capitalize) {
            Oid collation = PG_GET_COLLATION();
            capital = text_to_cstring(DatumGetTextP(
                DirectFunctionCall1Coll(upper, collation, PG_GETARG_DATUM(0))));
            pfree(rawstr);
        }
        result = QuoteString(capital, '\"');
        pfree(capital);
    }

    if (SimpleNameInternal(result)) {
        resTxt = cstring_to_text(result);
        pfree(result);
        PG_RETURN_TEXT_P(resTxt);
    } else {
        ereport(ERROR, (
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid SQL name")));
    }
}

Datum qualified_sql_name(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid qualified SQL name")));
    }

    text* txtIn = PG_GETARG_TEXT_PP(0);
    QualifiedSqlNameInternal(txtIn);
    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

Datum schema_name(PG_FUNCTION_ARGS)
{
    if (!PG_ARGISNULL(0)) {
        text* txtIn = PG_GETARG_TEXT_PP(0);
        Oid nspoid = get_namespace_oid(text_to_cstring(txtIn), true);
        if (OidIsValid(nspoid)) {
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
        }
    }

    ereport(ERROR, (
        errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("invalid schema")));
}

Datum sql_object_name(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid object name")));
    }
    
    text* rawname = PG_GETARG_TEXT_PP(0);
    List* rawnamelist = NIL;
    List* namelist = NIL;
    ListCell* lc = NULL;
    char* schemaname = NULL;
    char* pkgname = NULL;
    char* objname = NULL;
    Oid namespaceId = InvalidOid;
    Oid objectId = InvalidOid;
    bool objectValid;

    QualifiedSqlNameInternal(rawname);

    SplitIdentifierString(text_to_cstring(rawname), '@', &rawnamelist);
    if (list_length(rawnamelist) > 1) {
        PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    SplitIdentifierString(text_to_cstring(rawname), '.', &rawnamelist);
    /* remove quote marks before and after names, if any */
    foreach (lc, rawnamelist) {
        char* name = NULL;
        char* quotedName = (char*)lfirst(lc);
        errno_t rc;
        int len;
        if (quotedName[0] == '"') {
            len = strlen(quotedName) - 2;
            name = static_cast<char*>(palloc(len * sizeof(char)));
            rc = memcpy_s(name, len, quotedName + 1, len);
            securec_check(rc, "\0", "\0");
            pfree(quotedName);
        } else {
            name = quotedName;
        }
        namelist = lappend(namelist, makeString(name));
    }
    list_free(rawnamelist);

    MemoryContext oldcxt = CurrentMemoryContext;
    PG_TRY();
    {
        DeconstructQualifiedName(namelist, &schemaname, &objname, &pkgname);
        if (schemaname != NULL) {
            namespaceId = LookupExplicitNamespace(schemaname);
        } else {
            recomputeNamespacePath();
        }
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid object name")));
    }
    PG_END_TRY();

    /* pkgname is specified, just make sure the package is valid.
       It doesn't matter whether the object of the package is valid. */
    char* packageName = pkgname == NULL ? objname : pkgname;
    /* Suppose the object is a package */
    objectId = PackageNameGetOid(packageName, namespaceId);
    if (OidIsValid(objectId)) {
        objectValid = GetPgObjectValid(objectId, OBJECT_TYPE_PKGSPEC);
        if (objectValid) {
            PG_RETURN_TEXT_P(rawname);
        }
    }

    /* Suppose the object is a table */
    char* errDetail = OidIsValid(namespaceId) ?
                      get_relname_relid_extend(objname, namespaceId, &objectId, true, NULL) :
                      RelnameGetRelidExtended(objname, &objectId);
    if (OidIsValid(objectId)) {
        objectValid = GetPgObjectValid(objectId,
                        GetPgObjectTypePgClass(get_rel_relkind(objectId)));
        if (objectValid) {
            PG_RETURN_TEXT_P(rawname);
        } else {
            /* Suppose the object is a system catalog */
            HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objectId));
            Form_pg_class classfrom = (Form_pg_class)GETSTRUCT(tuple);
            
            if (IsSystemClass(classfrom)) {
                ReleaseSysCache(tuple);
                PG_RETURN_TEXT_P(rawname);
            }
            ReleaseSysCache(tuple);
        }
    }

    /* Suppose the object is a function */
    objectValid = FuncObjectName(objname, namespaceId);
    if (objectValid) {
        PG_RETURN_TEXT_P(rawname);
    }

    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid object name")));

    PG_RETURN_TEXT_P(rawname);
}