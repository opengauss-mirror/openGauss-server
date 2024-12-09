/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * pg_object_type.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/pg_object_type.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "access/transam.h"
#include "catalog/indexing.h"
#include "catalog/dependency.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "commands/defrem.h"
#include "commands/typecmds.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "catalog/pg_object.h"
#include "catalog/pg_object_type.h"
#include "fmgr.h"
#include "fmgr/fmgr_comp.h"
#include "funcapi.h"

/* Get object type of object table */
static Oid GetObjectTypeOfObjectRelation(Oid objreltyp)
{
    HeapTuple tp = NULL;
    Form_pg_type typtup = NULL;
    HeapTuple classtuple = NULL;
    Form_pg_class pgclasstuple = NULL;
    Oid typid = InvalidOid;

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objreltyp));
    if (!HeapTupleIsValid(tp)) {
        return InvalidOid;
    }

    typtup = (Form_pg_type)GETSTRUCT(tp);
    if ((typtup->typtype != TYPTYPE_COMPOSITE) && (!OidIsValid(typtup->typrelid))) {
        ReleaseSysCache(tp);
        return InvalidOid;
    }

    classtuple = SearchSysCache1(RELOID, ObjectIdGetDatum(typtup->typrelid));
    if (!HeapTupleIsValid(classtuple)) {
        ReleaseSysCache(tp);
        return InvalidOid;
    }
    pgclasstuple = (Form_pg_class)GETSTRUCT(classtuple);
    typid = pgclasstuple->reloftype;
    ReleaseSysCache(classtuple);
    ReleaseSysCache(tp);

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp)) {
        return InvalidOid;
    }
    typtup = NULL;
    typtup = (Form_pg_type)GETSTRUCT(tp);
    if (typtup->typtype != TYPTYPE_ABSTRACT_OBJECT) {
        ReleaseSysCache(tp);
        return InvalidOid;
    }
    ReleaseSysCache(tp);
    return typid;

}

/* Determine if this type is an object type and get its own map function or order function */
bool isNeedObjectCmp(Oid typid, Oid *mapid, Oid *orderid)
{
    HeapTuple tp = NULL;
    Form_pg_type typtup = NULL;
    HeapTuple objecttuple = NULL;
    Form_pg_object_type objecttypetup = NULL;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typid)));
    }
    typtup = (Form_pg_type)GETSTRUCT(tp);
    if (typtup->typtype == TYPTYPE_COMPOSITE) {
        /* Get object type of object table */
        typid = GetObjectTypeOfObjectRelation(typid);
        if (!OidIsValid(typid)) {
            ReleaseSysCache(tp);
            return false;
        }
    } else if (typtup->typtype != TYPTYPE_ABSTRACT_OBJECT) {
        ReleaseSysCache(tp);
        return false;
    }

    /* Get order or map method */
    objecttuple = SearchSysCache1(OBJECTTYPE, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(objecttuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for object type %u", typid)));
    }

    objecttypetup = (Form_pg_object_type)GETSTRUCT(objecttuple);
    if (!OidIsValid(objecttypetup->mapmethod) && (!OidIsValid(objecttypetup->ordermethod))) {
        ReleaseSysCache(tp);
        ReleaseSysCache(objecttuple);
        return false;
    }

    if (!objecttypetup->isbodydefined) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_TYPE_BODY_DEFINE_ERROR),
        errmsg("type body \"%s\" not define", get_typename(typid))));
    }
    *orderid = objecttypetup->ordermethod;
    *mapid = objecttypetup->mapmethod;
    ReleaseSysCache(tp);
    ReleaseSysCache(objecttuple);

    return true;
}

int ObjectIntanceCmp(Datum arg1, Datum arg2, Oid mapid, Oid orderid)
{
    /* Compare using order method */
    if (OidIsValid(orderid)) {
        int cmpresult = DatumGetInt32(OidFunctionCall2(orderid, arg1, arg2));
        if (cmpresult < 0) {
            /* arg1 is less than arg2 */
            return -1;
        } else if (cmpresult > 0) {
            /* arg1 is greater than arg2 */
            return 1;
        } else {
            return 0;
        }
    } else {
        /* map method comapre */
        HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(mapid));
        Form_pg_proc proc = NULL;
        TypeCacheEntry* typentry = NULL;
        if (!HeapTupleIsValid(proctup))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for function %u", mapid)));
        proc = (Form_pg_proc)GETSTRUCT(proctup);

        /* get return type of map function */
        typentry = lookup_type_cache(proc->prorettype, TYPECACHE_CMP_PROC_FINFO);
        if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("could not identify a comparison function for type %s", format_type_be(proc->prorettype))));
        ReleaseSysCache(proctup);
        return DatumGetInt32(OidFunctionCall2(typentry->cmp_proc_finfo.fn_oid,
            OidFunctionCall1(mapid, arg1), OidFunctionCall1(mapid, arg2)));
    }
}
Datum object_table_value(FunctionCallInfo fcinfo)
{
    HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(0);
    Oid reltypid = HeapTupleHeaderGetTypeId(record);
    Oid objtypid = InvalidOid;
    HeapTuple tp = NULL;
    Form_pg_type typtup = NULL;
    HeapTuple classtuple = NULL;
    Form_pg_class pgclasstuple = NULL;
    
    if (A_FORMAT != u_sess->attr.attr_sql.sql_compatibility) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("value function is only supported in database which dbcompatibility='A'.")));
    }
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(reltypid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup up failed for type %u", reltypid)));
    }

    typtup = (Form_pg_type)GETSTRUCT(tp);
    if (!OidIsValid(typtup->typrelid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("value function is only support table of object type")));
    }

    classtuple = SearchSysCache1(RELOID, ObjectIdGetDatum(typtup->typrelid));
    if (!HeapTupleIsValid(classtuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup up failed for type %u", typtup->typrelid)));
    }
    pgclasstuple = (Form_pg_class)GETSTRUCT(classtuple);
    objtypid = pgclasstuple->reloftype;
    if (!OidIsValid(objtypid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("value function is only support table of object type")));
    }
    ReleaseSysCache(classtuple);
    ReleaseSysCache(tp);

    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objtypid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup up failed for type %u", objtypid)));
    }
    typtup = NULL;
    typtup = (Form_pg_type)GETSTRUCT(tp);
    if (typtup->typtype != TYPTYPE_ABSTRACT_OBJECT) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("value function is only support table of object type")));
    }
    ReleaseSysCache(tp);
    PG_RETURN_HEAPTUPLEHEADER(record);
}

bool isObjectTypeAttributes(Oid objtypeid, char* attr)
{
    TupleDesc tupleDesc;
    AttrNumber parent_attno;

    HeapTuple tuple = NULL;
    Form_pg_type typeform = NULL;
    int32 typmod = -1;
    /* 1. Find parent type correspond information from pg_type and validate it */
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objtypeid));
    if (!HeapTupleIsValid(tuple)) {
        return false;
    }
    typeform = (Form_pg_type)GETSTRUCT(tuple);
    /* error report while not abstract type */
    if (!OidIsValid(typeform->typrelid) || typeform->typtype != TYPTYPE_ABSTRACT_OBJECT) {
        ReleaseSysCache(tuple);
        return false;
    }
    typmod = typeform->typtypmod;
    ReleaseSysCache(tuple);
    
    tupleDesc = lookup_rowtype_tupdesc(objtypeid, typmod);

    for (parent_attno = 1; parent_attno <= tupleDesc->natts; parent_attno++) {
        Form_pg_attribute attribute = &tupleDesc->attrs[parent_attno -1 ];
        char* attributeName = NameStr(attribute->attname);

        if (0 == strcmp(attributeName, attr)) {
            ReleaseTupleDesc(tupleDesc);
            return true;
        }
    }
    ReleaseTupleDesc(tupleDesc);
    return false;
}

bool isObjectTypeClassFunction(Oid funcid)
{
    HeapTuple objTuple = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(funcid), CharGetDatum(OBJECT_TYPE_PROC));
    char proctype = OBJECTTYPE_NULL_PROC;
    if (HeapTupleIsValid(objTuple)) {
        bool isnull = true;
        Datum object_options = SysCacheGetAttr(PGOBJECTID, objTuple, Anum_pg_object_options, &isnull);
        proctype = GET_PROTYPEKIND(object_options);
        ReleaseSysCache(objTuple);
    }
    if (proctype == OBJECTTYPE_MEMBER_PROC || proctype == OBJECTTYPE_STATIC_PROC) {
        return true;
    }
    return false;
}