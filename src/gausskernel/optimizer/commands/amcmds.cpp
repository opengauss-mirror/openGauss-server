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
 *-------------------------------------------------------------------------
 *
 * amcmds.cpp
 *    Routines for SQL commands that manipulate access methods.
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/commands/amcmds.cpp
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "storage/lock/lock.h"
#include "securec.h"


static Oid lookup_index_am_handler_func(List *handlerName);
static Oid lookup_regproc_am_handler_func(int16 procIndex, IndexAmRoutine *amRoutine);

#define FILL_ANUM_PG_AM_REGPROC_VAULE(pos, funcname)            \
{                                                               \
    if (0 != strlen(funcname)) {                                \
        amOid = lookup_regproc_am_handler_func(pos, amRoutine); \
        values[(pos) - 1] = ObjectIdGetDatum(amOid);            \
        referencedOids = lappend_oid(referencedOids, amOid);    \
    }                                                           \
}

/*
 * CreateAccessMethod
 *      Registers a new access method.
 */
ObjectAddress CreateAccessMethod(CreateAmStmt *stmt)
{
    Oid amOid;
    Relation rel;
    HeapTuple tup;
    Oid amHandler;
    ObjectAddress myself;
    bool nulls[Natts_pg_am];
    Datum values[Natts_pg_am];
    Datum amHandlerDatum;
    IndexAmRoutine *amRoutine;
    List *referencedOids = NIL;
    ListCell *referencedCell = NULL;

    rel = heap_open(AccessMethodRelationId, RowExclusiveLock);

    /* Must be super user */
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied to create access method \"%s\"", stmt->amname),
            errhint("Must be superuser to create an access method.")));

    /* Check if name is used */
    amOid = GetSysCacheOid1(AMNAME, CStringGetDatum(stmt->amname));
    if (OidIsValid(amOid)) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
            errmsg("access method \"%s\" already exists", stmt->amname)));
    }

    /*
     * Get the handler function oid, verifying the AM type while at it.
     */
    amHandler = lookup_index_am_handler_func(stmt->handler_name);
    amHandlerDatum = OidFunctionCall0(amHandler);
    amRoutine = (IndexAmRoutine *)DatumGetPointer(amHandlerDatum);

    /*
     * Insert tuple into pg_am.
     */
    errno_t rc;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_am_amname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->amname));
    values[Anum_pg_am_amstrategies - 1] = UInt16GetDatum(amRoutine->amstrategies);

    values[Anum_pg_am_amsupport - 1] = UInt16GetDatum(amRoutine->amsupport);
    values[Anum_pg_am_amcanorder - 1] = BoolGetDatum(amRoutine->amcanorder);
    values[Anum_pg_am_amcanorderbyop - 1] = BoolGetDatum(amRoutine->amcanorderbyop);
    values[Anum_pg_am_amcanbackward - 1] = BoolGetDatum(amRoutine->amcanbackward);
    values[Anum_pg_am_amcanunique - 1] = BoolGetDatum(amRoutine->amcanunique);
    values[Anum_pg_am_amcanmulticol - 1] = BoolGetDatum(amRoutine->amcanmulticol);
    values[Anum_pg_am_amoptionalkey - 1] = BoolGetDatum(amRoutine->amoptionalkey);
    values[Anum_pg_am_amsearcharray - 1] = BoolGetDatum(amRoutine->amsearcharray);
    values[Anum_pg_am_amsearchnulls - 1] = BoolGetDatum(amRoutine->amsearchnulls);
    values[Anum_pg_am_amstorage - 1] = BoolGetDatum(amRoutine->amstorage);
    values[Anum_pg_am_amclusterable - 1] = BoolGetDatum(amRoutine->amclusterable);
    values[Anum_pg_am_ampredlocks - 1] = BoolGetDatum(amRoutine->ampredlocks);
    values[Anum_pg_am_amkeytype - 1] = ObjectIdGetDatum(amRoutine->amkeytype);

    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_aminsert, amRoutine->aminsertfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_ambeginscan, amRoutine->ambeginscanfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amgettuple, amRoutine->amgettuplefuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amgetbitmap, amRoutine->amgetbitmapfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amrescan, amRoutine->amrescanfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amendscan, amRoutine->amendscanfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_ammarkpos, amRoutine->ammarkposfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amrestrpos, amRoutine->amrestrposfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_ammerge, amRoutine->ammergefuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_ambuild, amRoutine->ambuildfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_ambuildempty, amRoutine->ambuildemptyfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_ambulkdelete, amRoutine->ambulkdeletefuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amvacuumcleanup, amRoutine->amvacuumcleanupfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amcanreturn, amRoutine->amcanreturnfuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amcostestimate, amRoutine->amcostestimatefuncname);
    FILL_ANUM_PG_AM_REGPROC_VAULE(Anum_pg_am_amoptions, amRoutine->amoptionsfuncname);

    values[Anum_pg_am_amhandler - 1] = ObjectIdGetDatum(amHandler);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    amOid = simple_heap_insert(rel, tup);
    CatalogUpdateIndexes(rel, tup);
    heap_freetuple(tup);

    myself.classId = AccessMethodRelationId;
    myself.objectId = amOid;
    myself.objectSubId = 0;

    int referencedNum = list_length(referencedOids) + 1;
    ObjectAddress *referenced = (ObjectAddress *)palloc(sizeof(ObjectAddress) * referencedNum);
    ObjectAddress *cursor = referenced;

    cursor->classId = ProcedureRelationId;
    cursor->objectId = amHandler;
    cursor->objectSubId = 0;
    cursor++;

    foreach (referencedCell, referencedOids) {
        cursor->classId = ProcedureRelationId;
        cursor->objectId = lfirst_oid(referencedCell);
        cursor->objectSubId = 0;
        cursor++;
    }

    recordMultipleDependencies(&myself, referenced, referencedNum, DEPENDENCY_NORMAL);
    pfree_ext(referenced);
    list_free_ext(referencedOids);

    recordDependencyOnCurrentExtension(&myself, false);

    heap_close(rel, RowExclusiveLock);

    return myself;
}

void RemoveAccessMethodById(Oid amOid)
{
    Relation relation;
    HeapTuple tup;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("must be superuser to drop an access method.")));

    if (IsSystemObjOid(amOid))
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg("amOid %u is a builtin access method, it can not be droped", amOid)));

    relation = heap_open(AccessMethodRelationId, RowExclusiveLock);
    tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for access method %u", amOid);

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * Convert a handler function name to an Oid.  If the return type of the
 * function doesn't match the given AM type, an error is raised.
 *
 * This function either return valid function Oid or throw an error.
 */
static Oid lookup_index_am_handler_func(List *handlerName)
{
    Oid handlerOid;
    Oid funcArgTypes[1] = {INTERNALOID};

    if (handlerName == NIL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("handler function is not specified")));

    /* handlers have one argument of type internal */
    handlerOid = LookupFuncName(handlerName, 1, funcArgTypes, false);

    return handlerOid;
}

static Oid lookup_regproc_am_handler_func(int16 procIndex, IndexAmRoutine *amRoutine)
{
    int nargs = 0;
    char *funcName = NULL;
    List * handlerName = NIL;
    Oid handlerOid = InvalidOid;
    Oid funcArgTypes[PG_AM_FUNC_MAX_ARGS_NUM] = {INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID};

    switch (procIndex) {
        case Anum_pg_am_aminsert:
            nargs = PG_AM_INSERT_ARGS_NUM;
            funcName = amRoutine->aminsertfuncname;
            break;
        case Anum_pg_am_ambeginscan:
            nargs = PG_AM_BEGINSCAN_ARGS_NUM;
            funcName = amRoutine->ambeginscanfuncname;
            break;
        case Anum_pg_am_amgettuple:
            nargs = PG_AM_GETTUPLE_ARGS_NUM;
            funcName = amRoutine->amgettuplefuncname;
            break;
        case Anum_pg_am_amrescan:
            nargs = PG_AM_RESCAN_ARGS_NUM;
            funcName = amRoutine->amrescanfuncname;
            break;
        case Anum_pg_am_amendscan:
            nargs = PG_AM_ENDSCAN_ARGS_NUM;
            funcName = amRoutine->amendscanfuncname;
            break;
        case Anum_pg_am_ambuild:
            nargs = PG_AM_BUILD_ARGS_NUM;
            funcName = amRoutine->ambuildfuncname;
            break;
        case Anum_pg_am_ambuildempty:
            nargs = PG_AM_BUILDEMPTY_ARGS_NUM;
            funcName = amRoutine->ambuildemptyfuncname;
            break;
        case Anum_pg_am_ambulkdelete:
            nargs = PG_AM_BULKDELETE_ARGS_NUM;
            funcName = amRoutine->ambulkdeletefuncname;
            break;
        case Anum_pg_am_amvacuumcleanup:
            nargs = PG_AM_VACUUMCLEANUP_ARGS_NUM;
            funcName = amRoutine->amvacuumcleanupfuncname;
            break;
        case Anum_pg_am_amcostestimate:
            nargs = PG_AM_COSTESTIMATE_ARGS_NUM;
            funcName = amRoutine->amcostestimatefuncname;
            break;
        case Anum_pg_am_amoptions:
            nargs = PG_AM_OPTIONS_ARGS_NUM;
            funcName = amRoutine->amoptionsfuncname;
            break;
        default:
            return InvalidOid;
    }

    handlerName = list_make1(makeString(funcName));
    handlerOid = LookupFuncName(handlerName, nargs, funcArgTypes, false);
    return handlerOid;
}
