/* -------------------------------------------------------------------------
 *
 * pg_aggregate.cpp
 *	  routines to support manipulation of the pg_aggregate relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_aggregate.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_language.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

static Oid lookup_agg_function(List* fnName, int nargs, Oid* input_types, Oid* rettype);

typedef bool (*aggIsSupportedFunc)(const char* aggName);

static void InternalAggIsSupported(const char *aggName)
{
    static const char *supportList[] = {
        "listagg",
        "median",
        "mode",
        "json_agg",
        "json_object_agg",
        "st_summarystatsagg",
        "st_union",
        "wm_concat",
        "group_concat",
        "json_objectagg",
        "json_arrayagg"
#ifndef ENABLE_MULTIPLE_NODES
        ,
        "st_collect",
        "st_clusterintersecting",
        "st_clusterwithin",
        "st_polygonize",
        "st_makeline",
        "st_asmvt",
        "st_asgeobuf",
        "st_asflatgeobuf"
#endif
        ,
        "age_collect",
        "age_percentilecont",
        "age_percentiledisc"
    };

    uint len = lengthof(supportList);
    for (uint i = 0; i < len; i++) {
        if (pg_strcasecmp(aggName, supportList[i]) == 0) {
            return;
        }
    }

    if (u_sess->hook_cxt.aggIsSupportedHook != NULL &&
        ((aggIsSupportedFunc)(u_sess->hook_cxt.aggIsSupportedHook))(aggName)) {
        return;
    }

    ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("unsafe use of pseudo-type \"internal\""),
                errdetail("Transition type can not be \"internal\".")));
}
void CheckAggregateCreatePrivilege(Oid aggNamespace, const char* aggName)
{
    if (!isRelSuperuser() &&
        (aggNamespace == PG_CATALOG_NAMESPACE ||
        aggNamespace == PG_PUBLIC_NAMESPACE)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied to create aggregate \"%s\"", aggName),
            errhint("must be %s to create a aggregate in %s schema.",
            g_instance.attr.attr_security.enablePrivilegesSeparate ? "initial user" : "sysadmin",
            get_namespace_name(aggNamespace))));
    }

    if (!IsInitdb && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        !g_instance.attr.attr_common.allow_create_sysobject &&
        IsSysSchema(aggNamespace)) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create aggregate \"%s\"", aggName),
                errhint("not allowd to create a aggregate in %s schema when allow_create_sysobject is off.",
                    get_namespace_name(aggNamespace))));
    }
}
/*
 * AggregateCreate
 * aggKind, aggregation function kind, 'n' for normal aggregation, 'o' for ordered set aggregation.
 */
ObjectAddress AggregateCreate(const char* aggName, Oid aggNamespace, char aggKind, Oid* aggArgTypes, int numArgs, 
                     List* aggtransfnName,
#ifdef PGXC
    List* aggcollectfnName,
#endif
    List* aggfinalfnName, List* aggsortopName, Oid aggTransType,
#ifdef PGXC
    const char* agginitval, const char* agginitcollect)
#else
    const char* agginitval)
#endif
{
    Relation aggdesc;
    HeapTuple tup;
    bool nulls[Natts_pg_aggregate];
    Datum values[Natts_pg_aggregate];
    Form_pg_proc proc;
    Oid transfn;
#ifdef PGXC
    Oid collectfn = InvalidOid; /* can be omitted */
#endif
    Oid finalfn = InvalidOid; /* can be omitted */
    Oid sortop = InvalidOid;  /* can be omitted */
    bool hasPolyArg = false;
    bool hasInternalArg = false;
    Oid rettype;
    Oid finaltype;
    Oid* fnArgs = NULL;
    int nargs_transfn;
    Oid procOid;
    TupleDesc tupDesc;
    int i;
    ObjectAddress myself, referenced;
    AclResult aclresult;
    Oid proowner = InvalidOid;

    // if "isalter" is true,and if the owner of the namespce has the same name
    // as the namescpe, change the owner of the objects
    bool isalter = false;
    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        proowner = GetUserIdFromNspId(aggNamespace);

        if (!OidIsValid(proowner))
            proowner = GetUserId();
        else if (proowner != GetUserId())
            isalter = true;
    } else {
        proowner = GetUserId();
    }

    /* sanity checks (caller should have caught these) */
    if (aggName == NULL)
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_NAME), errmsg("no aggregate name supplied")));

    if (aggtransfnName == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("aggregate must have a transition function")));

#ifdef PGXC
    if (aggTransType == INTERNALOID) {
        InternalAggIsSupported(aggName);
    }
#endif
    /* check for polymorphic and INTERNAL arguments */
    hasPolyArg = false;
    hasInternalArg = false;
    for (i = 0; i < numArgs; i++) {
        if (IsPolymorphicType(aggArgTypes[i]))
            hasPolyArg = true;
        else if (aggArgTypes[i] == INTERNALOID)
            hasInternalArg = true;
    }

    /*
     * If transtype is polymorphic, must have polymorphic argument also; else
     * we will have no way to deduce the actual transtype.
     */
    if (IsPolymorphicType(aggTransType) && !hasPolyArg)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("cannot determine transition data type"),
                errdetail(
                    "An aggregate using a polymorphic transition type must have at least one polymorphic argument.")));

    /* find the transfn */
    nargs_transfn = numArgs + 1;
#ifdef PGXC
    /* Need to allocate at least 2 items for collection function */
    fnArgs = (Oid*)palloc((nargs_transfn < 2 ? 2 : nargs_transfn) * sizeof(Oid));
#else
    fnArgs = (Oid*)palloc(nargs_transfn * sizeof(Oid));
#endif
    fnArgs[0] = aggTransType;
    if (numArgs > 0) {
        errno_t rc = memcpy_s(fnArgs + 1, numArgs * sizeof(Oid), aggArgTypes, numArgs * sizeof(Oid));
        securec_check(rc, "", "");
    }
    transfn = lookup_agg_function(aggtransfnName, nargs_transfn, fnArgs, &rettype);

    /*
     * Return type of transfn (possibly after refinement by
     * enforce_generic_type_consistency, if transtype isn't polymorphic) must
     * exactly match declared transtype.
     *
     * In the non-polymorphic-transtype case, it might be okay to allow a
     * rettype that's binary-coercible to transtype, but I'm not quite
     * convinced that it's either safe or useful.  When transtype is
     * polymorphic we *must* demand exact equality.
     */
    if (rettype != aggTransType)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("return type of transition function %s is not %s",
                    NameListToString(aggtransfnName),
                    format_type_be(aggTransType))));

    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(transfn));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", transfn)));
    proc = (Form_pg_proc)GETSTRUCT(tup);

    /*
     * If the transfn is strict and the initval is NULL, make sure first input
     * type and transtype are the same (or at least binary-compatible), so
     * that it's OK to use the first input value as the initial transValue.
     */
    if (proc->proisstrict && agginitval == NULL) {
        if (numArgs < 1 || !IsBinaryCoercible(aggArgTypes[0], aggTransType))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("must not omit initial value when transition function is strict and transition type is not "
                           "compatible with input type")));
    }
    ReleaseSysCache(tup);

#ifdef PGXC
    if (aggcollectfnName != NULL) {
        /*
         * Collection function must be of two arguments, both of type aggTransType
         * and return type is also aggTransType
         */
        fnArgs[0] = aggTransType;
        fnArgs[1] = aggTransType;
        collectfn = lookup_agg_function(aggcollectfnName, 2, fnArgs, &rettype);
        if (rettype != aggTransType)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("return type of collection function %s is not %s",
                        NameListToString(aggcollectfnName),
                        format_type_be(aggTransType))));
    }

#endif
    /* handle finalfn, if supplied */
    if (aggfinalfnName != NULL) {
        fnArgs[0] = aggTransType;
        finalfn = lookup_agg_function(aggfinalfnName, 1, fnArgs, &finaltype);
    } else {
        /*
         * If no finalfn, aggregate result type is type of the state value
         */
        finaltype = aggTransType;
    }
    Assert(OidIsValid(finaltype));

    /*
     * If finaltype (i.e. aggregate return type) is polymorphic, inputs must
     * be polymorphic also, else parser will fail to deduce result type.
     * (Note: given the previous test on transtype and inputs, this cannot
     * happen, unless someone has snuck a finalfn definition into the catalogs
     * that itself violates the rule against polymorphic result with no
     * polymorphic input.)
     */
    if (IsPolymorphicType(finaltype) && !hasPolyArg)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("cannot determine result data type"),
                errdetail("An aggregate returning a polymorphic type "
                          "must have at least one polymorphic argument.")));

    /*
     * Also, the return type can't be INTERNAL unless there's at least one
     * INTERNAL argument.  This is the same type-safety restriction we enforce
     * for regular functions, but at the level of aggregates.  We must test
     * this explicitly because we allow INTERNAL as the transtype.
     */
    if (finaltype == INTERNALOID && !hasInternalArg)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("unsafe use of pseudo-type \"internal\""),
                errdetail("A function returning \"internal\" must have at least one \"internal\" argument.")));

    /* handle sortop, if supplied */
    if (aggsortopName != NULL) {
        if (numArgs != 1)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("sort operator can only be specified for single-argument aggregates")));
        sortop = LookupOperName(NULL, aggsortopName, aggArgTypes[0], aggArgTypes[0], false, -1);
    }

    /*
     * permission checks on used types
     */
    for (i = 0; i < numArgs; i++) {
        aclresult = pg_type_aclcheck(aggArgTypes[i], GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, aggArgTypes[i]);

        if (isalter) {
            aclresult = pg_type_aclcheck(aggArgTypes[i], proowner, ACL_USAGE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error_type(aclresult, aggArgTypes[i]);
        }
    }

    aclresult = pg_type_aclcheck(aggTransType, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, aggTransType);

    if (isalter) {
        aclresult = pg_type_aclcheck(aggTransType, proowner, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, aggTransType);
    }

    aclresult = pg_type_aclcheck(finaltype, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, finaltype);

    if (isalter) {
        aclresult = pg_type_aclcheck(finaltype, proowner, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, finaltype);
    }

    CheckAggregateCreatePrivilege(aggNamespace, aggName);

    /*
     * Everything looks okay.  Try to create the pg_proc entry for the
     * aggregate.  (This could fail if there's already a conflicting entry.)
     * Fenced mode is not supportted for create aggregate, so set false for
     * argument fenced.
     */
    myself = ProcedureCreate(aggName,
        aggNamespace,
        InvalidOid,
        false,                                /* A db compatible*/
        false,                                /* no replacement */
        false,                                /* doesn't return a set */
        finaltype,                            /* returnType */
        proowner,                             /* proowner */
        INTERNALlanguageId,                   /* languageObjectId */
        InvalidOid,                           /* no validator */
        "aggregate_dummy",                    /* placeholder proc */
        NULL,                                 /* probin */
        true,                                 /* isAgg */
        false,                                /* isWindowFunc */
        false,                                /* security invoker (currently not
                                               * definable for agg) */
        false,                                /* isLeakProof */
        false,                                /* isStrict (not needed for agg) */
        PROVOLATILE_IMMUTABLE,                /* volatility (not
                                               * needed for agg) */
        buildoidvector(aggArgTypes, numArgs), /* paramTypes */
        PointerGetDatum(NULL),                /* allParamTypes */
        PointerGetDatum(NULL),                /* parameterModes */
        PointerGetDatum(NULL),                /* parameterNames */
        NIL,                                  /* parameterDefaults */
        PointerGetDatum(NULL),                /* proconfig */
        1,                                    /* procost */
        0,                                    /* prorows */
        NULL,                                 /* default value postion array */
        false,
        false,
        false,
        false,
        NULL);                               /* default value for proisprocedure */
     procOid = myself.objectId;
    /*
     * Okay to create the pg_aggregate entry.
     */

    /* initialize nulls and values */
    for (i = 0; i < Natts_pg_aggregate; i++) {
        nulls[i] = false;
        values[i] = (Datum)NULL;
    }
    values[Anum_pg_aggregate_aggfnoid - 1] = ObjectIdGetDatum(procOid);
    values[Anum_pg_aggregate_aggtransfn - 1] = ObjectIdGetDatum(transfn);
    values[Anum_pg_aggregate_aggfinalfn - 1] = ObjectIdGetDatum(finalfn);
    values[Anum_pg_aggregate_aggsortop - 1] = ObjectIdGetDatum(sortop);
    values[Anum_pg_aggregate_aggtranstype - 1] = ObjectIdGetDatum(aggTransType);
#ifdef PGXC
    values[Anum_pg_aggregate_aggcollectfn - 1] = ObjectIdGetDatum(collectfn);
#endif
    if (agginitval != NULL)
        values[Anum_pg_aggregate_agginitval - 1] = CStringGetTextDatum(agginitval);
    else
        nulls[Anum_pg_aggregate_agginitval - 1] = true;
#ifdef PGXC
    if (agginitcollect != NULL)
        values[Anum_pg_aggregate_agginitcollect - 1] = CStringGetTextDatum(agginitcollect);
    else
        nulls[Anum_pg_aggregate_agginitcollect - 1] = true;
#endif
    /* handle ordered set aggregate with no direct args. */
    values[Anum_pg_aggregate_aggkind - 1] = CharGetDatum(aggKind);
    values[Anum_pg_aggregate_aggnumdirectargs - 1] = Int8GetDatum(AGGNUMDIRECTARGS_DEFAULT);

    aggdesc = heap_open(AggregateRelationId, RowExclusiveLock);
    tupDesc = aggdesc->rd_att;

    tup = heap_form_tuple(tupDesc, values, nulls);
    (void)simple_heap_insert(aggdesc, tup);

    CatalogUpdateIndexes(aggdesc, tup);

    heap_close(aggdesc, RowExclusiveLock);

    /*
     * Create dependencies for the aggregate (above and beyond those already
     * made by ProcedureCreate).  Note: we don't need an explicit dependency
     * on aggTransType since we depend on it indirectly through transfn.
     */
    myself.classId = ProcedureRelationId;
    myself.objectId = procOid;
    myself.objectSubId = 0;

    /* Depends on transition function */
    referenced.classId = ProcedureRelationId;
    referenced.objectId = transfn;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

#ifdef PGXC
    if (OidIsValid(collectfn)) {
        /* Depends on collection function */
        referenced.classId = ProcedureRelationId;
        referenced.objectId = collectfn;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

#endif
    /* Depends on final function, if any */
    if (OidIsValid(finalfn)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = finalfn;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /* Depends on sort operator, if any */
    if (OidIsValid(sortop)) {
        referenced.classId = OperatorRelationId;
        referenced.objectId = sortop;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }
    return myself;
}

/*
 * lookup_agg_function -- common code for finding both transfn and finalfn
 */
static Oid lookup_agg_function(List* fnName, int nargs, Oid* input_types, Oid* rettype)
{
    Oid fnOid;
    bool retset = false;
    int nvargs;
    Oid vatype;
    Oid* true_oid_array = NULL;
    FuncDetailCode fdresult;
    AclResult aclresult;
    int i;

    /*
     * func_get_detail looks up the function in the catalogs, does
     * disambiguation for polymorphic functions, handles inheritance, and
     * returns the funcid and type and set or singleton status of the
     * function's return value.  it also returns the true argument types to
     * the function.
     */
    fdresult = func_get_detail(fnName,
        NIL,
        NIL,
        nargs,
        input_types,
        false,
        false,
        &fnOid,
        rettype,
        &retset,
        &nvargs,
        &vatype,
        &true_oid_array,
        NULL);

    /* only valid case is a normal function not returning a set */
    if (fdresult != FUNCDETAIL_NORMAL || !OidIsValid(fnOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function %s does not exist", func_signature_string(fnName, nargs, NIL, input_types))));
    if (retset)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("function %s returns a set", func_signature_string(fnName, nargs, NIL, input_types))));

    /*
     * If there are any polymorphic types involved, enforce consistency, and
     * possibly refine the result type.  It's OK if the result is still
     * polymorphic at this point, though.
     */
    *rettype = enforce_generic_type_consistency(input_types, true_oid_array, nargs, *rettype, true);

    /*
     * func_get_detail will find functions requiring run-time argument type
     * coercion, but nodeAgg.c isn't prepared to deal with that
     */
    for (i = 0; i < nargs; i++) {
        if (!IsPolymorphicType(true_oid_array[i]) && !IsBinaryCoercible(input_types[i], true_oid_array[i]))
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("function %s requires run-time type coercion",
                        func_signature_string(fnName, nargs, NIL, true_oid_array))));
    }

    /* Check aggregate creator has permission to call the function */
    aclresult = pg_proc_aclcheck(fnOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(fnOid));

    return fnOid;
}
