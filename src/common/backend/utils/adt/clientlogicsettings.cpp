/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * clientlogicsettings.cpp
 *
 * IDENTIFICATION
 *	  src\common\backend\utils\adt\clientlogicsettings.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utils/builtins.h"
#include "miscadmin.h"
#include "access/skey.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/gs_client_global_keys.h"
#include "catalog/gs_column_keys.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

/* ****************************************************************************
 * 	 USER I/O ROUTINES														 *
 * *************************************************************************** */

/*
 * globalsettingin		- converts "keyname" to keyc OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing pg_keyc entry.
 */
Datum globalsettingin(PG_FUNCTION_ARGS)
{
    char *key_name_or_oid = PG_GETARG_CSTRING(0);
    Oid result = InvalidOid;
    List *names = NIL;

    _KeyCandidateList *clist = NULL;
    /* '-' ? */
    if (strcmp(key_name_or_oid, "-") == 0)
        PG_RETURN_OID(InvalidOid);

    /* Numeric OID? */
    if (key_name_or_oid[0] >= '0' && key_name_or_oid[0] <= '9' &&
        strspn(key_name_or_oid, "0123456789") == strlen(key_name_or_oid)) {
        result = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(key_name_or_oid)));
        PG_RETURN_OID(result);
    }

    /* Else it's a name, possibly schema-qualified */

    /*
     * In bootstrap mode we assume the given name is not schema-qualified, and
     * just search pg_keyc for a unique match.	This is needed for
     * initializing other system catalogs (pg_namespace may not exist yet, and
     * certainly there are no schemas other than pg_catalog).
     */
    if (IsBootstrapProcessingMode()) {
        int matches = 0;
        Relation hdesc;
        ScanKeyData skey[1];
        SysScanDesc sysscan;
        HeapTuple tuple;

        ScanKeyInit(&skey[0], Anum_gs_client_global_keys_global_key_name, BTEqualStrategyNumber, F_NAMEEQ,
            CStringGetDatum(key_name_or_oid));

        hdesc = heap_open(ClientLogicGlobalSettingsId, AccessShareLock);
        sysscan = systable_beginscan(hdesc, ClientLogicGlobalSettingsNameIndexId, true, NULL, 1, skey);

        while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
            result = Oid HeapTupleGetOid(tuple);
            if (++matches > 1)
                break;
        }

        systable_endscan(sysscan);
        heap_close(hdesc, AccessShareLock);

        if (matches == 0)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_KEY), errmsg("client master key \"%s\" does not exist", key_name_or_oid)));

        else if (matches > 1)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_KEY),
                errmsg("more than one client master key named \"%s\"", key_name_or_oid)));

        PG_RETURN_OID(result);
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_keyc entries in the current search path.
     */
    names = stringToQualifiedNameList(key_name_or_oid);
    clist = GlobalSettingGetCandidates(names, false);
    if (clist == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_KEY), errmsg("client master key \"%s\" does not exist", key_name_or_oid)));
    else if (clist->next != NULL)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_KEY), errmsg("more than one key named \"%s\"", key_name_or_oid)));

    result = clist->oid;

    PG_RETURN_OID(result);
}

/*
 * columnsettingin		- converts "keyname" to keyc OID
 *
 * We also accept a numeric OID, for symmetry with the output routine.
 *
 * '-' signifies unknown (OID 0).  In all other cases, the input must
 * match an existing gs_cl_column_setting entry.
 */
Datum columnsettingin(PG_FUNCTION_ARGS)
{
    char *key_name_or_oid = PG_GETARG_CSTRING(0);
    Oid result = InvalidOid;
    List *names = NIL;

    _KeyCandidateList *clist = NULL;
    /* '-' ? */
    if (strcmp(key_name_or_oid, "-") == 0)
        PG_RETURN_OID(InvalidOid);

    /* Numeric OID? */
    if (key_name_or_oid[0] >= '0' && key_name_or_oid[0] <= '9' &&
        strspn(key_name_or_oid, "0123456789") == strlen(key_name_or_oid)) {
        result = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(key_name_or_oid)));
        PG_RETURN_OID(result);
    }

    /* Else it's a name, possibly schema-qualified */

    /*
     * In bootstrap mode we assume the given name is not schema-qualified, and
     * just search pg_keyc for a unique match.	This is needed for
     * initializing other system catalogs (pg_namespace may not exist yet, and
     * certainly there are no schemas other than pg_catalog).
     */
    if (IsBootstrapProcessingMode()) {
        int matches = 0;
        Relation hdesc;
        ScanKeyData skey[1];
        SysScanDesc sysscan;
        HeapTuple tuple;

        ScanKeyInit(&skey[0], Anum_gs_column_keys_column_key_name, BTEqualStrategyNumber, F_NAMEEQ,
            CStringGetDatum(key_name_or_oid));

        hdesc = heap_open(ClientLogicColumnSettingsId, AccessShareLock);
        sysscan = systable_beginscan(hdesc, ClientLogicColumnSettingsNameIndexId, true, NULL, 1, skey);

        while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
            result = Oid HeapTupleGetOid(tuple);
            if (++matches > 1)
                break;
        }

        systable_endscan(sysscan);
        heap_close(hdesc, AccessShareLock);

        if (matches == 0)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY),
                errmsg("column encryption key \"%s\" does not exist", key_name_or_oid)));

        else if (matches > 1)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_KEY),
                errmsg("more than one column encryption key named \"%s\"", key_name_or_oid)));

        PG_RETURN_OID(result);
    }

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_keyc entries in the current search path.
     */
    names = stringToQualifiedNameList(key_name_or_oid);
    clist = CeknameGetCandidates(names, false);
    if (clist == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_KEY), errmsg("column encryption key \"%s\" does not exist", key_name_or_oid)));
    else if (clist->next != NULL)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_KEY), errmsg("more than one key named \"%s\"", key_name_or_oid)));

    result = clist->oid;

    PG_RETURN_OID(result);
}
