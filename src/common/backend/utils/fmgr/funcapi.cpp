/* -------------------------------------------------------------------------
 *
 * funcapi.c
 *	  Utility and convenience functions for fmgr functions that return
 *	  sets and/or composite types.
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/backend/utils/fmgr/funcapi.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/sysattr.h"
#include "knl/knl_variable.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/namespace.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "catalog/pg_proc_fn.h"

static void shutdown_multi_func_call(Datum arg);
static TypeFuncClass internal_get_result_type(Oid funcid, Node* call_expr, ReturnSetInfo* rsinfo, Oid* resultTypeId,
    TupleDesc* resultTupleDesc, int4* resultTypeId_orig);
static bool resolve_polymorphic_tupdesc(TupleDesc tupdesc, oidvector* declared_args, Node* call_expr);
static TypeFuncClass get_type_func_class(Oid typid);

/*
 * init_MultiFuncCall
 * Create an empty FuncCallContext data structure
 * and do some other basic Multi-function call setup
 * and error checking
 */
FuncCallContext* init_MultiFuncCall(PG_FUNCTION_ARGS)
{
    FuncCallContext* retval = NULL;

    /*
     * Bail if we're called in the wrong context
     */
    if (fcinfo->resultinfo == NULL || !IsA(fcinfo->resultinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    }

    if (fcinfo->flinfo->fn_extra == NULL) {
        /*
         * First call
         */
        ReturnSetInfo* rsi = (ReturnSetInfo*)fcinfo->resultinfo;
        MemoryContext multi_call_ctx;

        /*
         * Create a suitably long-lived context to hold cross-call data
         */
        multi_call_ctx = AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
            "SRF multi-call context",
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);

        /*
         * Allocate suitably long-lived space and zero it
         */
        retval = (FuncCallContext*)MemoryContextAllocZero(multi_call_ctx, sizeof(FuncCallContext));

        /*
         * initialize the elements
         */
        retval->call_cntr = 0;
        retval->max_calls = 0;
        retval->slot = NULL;
        retval->user_fctx = NULL;
        retval->attinmeta = NULL;
        retval->tuple_desc = NULL;
        retval->multi_call_memory_ctx = multi_call_ctx;

        /*
         * save the pointer for cross-call use
         */
        fcinfo->flinfo->fn_extra = retval;

        /*
         * Ensure we will get shut down cleanly if the exprcontext is not run
         * to completion.
         */
        RegisterExprContextCallback(rsi->econtext, shutdown_multi_func_call, PointerGetDatum(fcinfo->flinfo));
    } else {
        /* second and subsequent calls */
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("init_MultiFuncCall cannot be called more than once")));

        /* never reached, but keep compiler happy */
        retval = NULL;
    }

    return retval;
}

/*
 * per_MultiFuncCall
 *
 * Do Multi-function per-call setup
 */
FuncCallContext* per_MultiFuncCall(PG_FUNCTION_ARGS)
{
    FuncCallContext* retval = (FuncCallContext*)fcinfo->flinfo->fn_extra;

    /*
     * Clear the TupleTableSlot, if present.  This is for safety's sake: the
     * Slot will be in a long-lived context (it better be, if the
     * FuncCallContext is pointing to it), but in most usage patterns the
     * tuples stored in it will be in the function's per-tuple context. So at
     * the beginning of each call, the Slot will hold a dangling pointer to an
     * already-recycled tuple.	We clear it out here.
     *
     * Note: use of retval->slot is obsolete as of 8.0, and we expect that it
     * will always be NULL.  This is just here for backwards compatibility in
     * case someone creates a slot anyway.
     */
    if (retval->slot != NULL) {
        (void)ExecClearTuple(retval->slot);
    }

    return retval;
}

/*
 * end_MultiFuncCall
 * Clean up after init_MultiFuncCall
 */
void end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext* funcctx)
{
    ReturnSetInfo* rsi = (ReturnSetInfo*)fcinfo->resultinfo;

    /* Deregister the shutdown callback */
    UnregisterExprContextCallback(rsi->econtext, shutdown_multi_func_call, PointerGetDatum(fcinfo->flinfo));

    /* But use it to do the real work */
    shutdown_multi_func_call(PointerGetDatum(fcinfo->flinfo));
}

/*
 * shutdown_multi_func_call
 * Shutdown function to clean up after init_MultiFuncCall
 */
static void shutdown_multi_func_call(Datum arg)
{
    FmgrInfo* flinfo = (FmgrInfo*)DatumGetPointer(arg);
    FuncCallContext* funcctx = (FuncCallContext*)flinfo->fn_extra;

    /* unbind from flinfo */
    flinfo->fn_extra = NULL;

    /*
     * Delete context that holds all multi-call data, including the
     * FuncCallContext itself
     */
    MemoryContextDelete(funcctx->multi_call_memory_ctx);
}

bool is_function_with_plpgsql_language_and_outparam(Oid funcid)
{
#ifndef ENABLE_MULTIPLE_NODES
    if (!enable_out_param_override() || u_sess->attr.attr_sql.sql_compatibility != A_FORMAT || funcid == InvalidOid) {
        return false;
    }
#else
    return false;
#endif
    char* funclang = get_func_langname(funcid);
    if (strcasecmp(funclang, "plpgsql") != 0) {
        pfree(funclang);
        return false;
    }
    pfree(funclang);
    Oid schema_oid = get_func_namespace(funcid);
    if (IsAformatStyleFunctionOid(schema_oid)) {
        return false;
    }
    HeapTuple tp;
    bool existOutParam = false;
    bool isNull = false;
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        (errmsg("cache lookup failed for function %u", funcid), errdetail("N/A."),
                         errcause("System error."), erraction("Contact engineer to support."))));
    }
    Datum pprokind = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prokind, &isNull);
    if ((!isNull && !PROC_IS_FUNC(pprokind))) {
        ReleaseSysCache(tp);
        return false;
    }
    isNull = false;
    (void)SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_proallargtypes, &isNull);
    if (!isNull) {
        existOutParam = true;
    }
    ReleaseSysCache(tp);
    return existOutParam;
}

TupleDesc get_func_param_desc(HeapTuple tp, Oid resultTypeId, int* return_out_args_num)
{
    Oid *p_argtypes = NULL;
    char **p_argnames = NULL;
    char *p_argmodes = NULL;
    int p_nargs = get_func_arg_info(tp, &p_argtypes, &p_argnames, &p_argmodes);
    char* p_name = NameStr(((Form_pg_proc)GETSTRUCT(tp))->proname);
    int out_args_num = 0;
    for (int i = 0; i < p_nargs; i++) {
        if (p_argmodes[i] == 'o' || p_argmodes[i] == 'b') {
            out_args_num++;
        }
    }
    if (return_out_args_num != NULL) {
        *return_out_args_num = out_args_num;
    }
    /* The return field is in the first column, and the parameter starts in the second column. */
    TupleDesc resultTupleDesc = CreateTemplateTupleDesc(out_args_num + 1, false);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, p_name, resultTypeId, -1, 0);
    int attindex = 2;
    for (int i = 0; i < p_nargs; i++) {
        if (p_argmodes[i] == 'o' || p_argmodes[i] == 'b') {
            if (unlikely(p_argnames == NULL)) {
                TupleDescInitEntry(resultTupleDesc, (AttrNumber)attindex, NULL,
                                    p_argtypes[i], p_argmodes[i], 0);
            } else {
                TupleDescInitEntry(resultTupleDesc, (AttrNumber)attindex,
                                    p_argnames[i], p_argtypes[i], p_argmodes[i], 0);
            }
            attindex++;
        }
    }

    return resultTupleDesc;
}

void construct_func_param_desc(Oid funcid, TypeFuncClass* typclass, TupleDesc* tupdesc, Oid* resultTypeId)
{
    if (tupdesc == NULL || resultTypeId == NULL || typclass == NULL) {
        return;
    }
    bool isWithOutParam = is_function_with_plpgsql_language_and_outparam(funcid);
    if (!isWithOutParam) {
        return;
    }
    /* Contruct argument tuple descriptor */
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmodule(MOD_PLSQL),
                        errmsg("Cache lookup failed for function %u", funcid),
                        errdetail("Fail to get the function param type oid.")));
    }

    *tupdesc = get_func_param_desc(tp, *resultTypeId);
    *resultTypeId = RECORDOID;
    *typclass = TYPEFUNC_COMPOSITE;

    ReleaseSysCache(tp);
}

/*
 * get_call_result_type
 *		Given a function's call info record, determine the kind of datatype
 *		it is supposed to return.  If resultTypeId isn't NULL, *resultTypeId
 *		receives the actual datatype OID (this is mainly useful for scalar
 *		result types).	If resultTupleDesc isn't NULL, *resultTupleDesc
 *		receives a pointer to a TupleDesc when the result is of a composite
 *		type, or NULL when it's a scalar result.
 *
 * One hard case that this handles is resolution of actual rowtypes for
 * functions returning RECORD (from either the function's OUT parameter
 * list, or a ReturnSetInfo context node).	TYPEFUNC_RECORD is returned
 * only when we couldn't resolve the actual rowtype for lack of information.
 *
 * The other hard case that this handles is resolution of polymorphism.
 * We will never return polymorphic pseudotypes (ANYELEMENT etc), either
 * as a scalar result type or as a component of a rowtype.
 *
 * This function is relatively expensive --- in a function returning set,
 * try to call it only the first time through.
 */
TypeFuncClass get_call_result_type(FunctionCallInfo fcinfo, Oid* resultTypeId, TupleDesc* resultTupleDesc)
{
    return internal_get_result_type(fcinfo->flinfo->fn_oid,
        fcinfo->flinfo->fn_expr,
        (ReturnSetInfo*)fcinfo->resultinfo,
        resultTypeId,
        resultTupleDesc,
        NULL);
}

/*
 * get_expr_result_type
 *		As above, but work from a calling expression node tree
 */
TypeFuncClass get_expr_result_type(Node* expr, Oid* resultTypeId, TupleDesc* resultTupleDesc, int4* resultTypeId_orig)
{
    TypeFuncClass result;

    if (expr && IsA(expr, FuncExpr)) {
        result = internal_get_result_type(((FuncExpr*)expr)->funcid, expr, NULL, resultTypeId, resultTupleDesc, 
            resultTypeId_orig);

        /* A_FORMAT return param seperation */
        construct_func_param_desc(((FuncExpr*)expr)->funcid, &result, resultTupleDesc, resultTypeId);
    } else if (expr && IsA(expr, OpExpr)) {
        result = internal_get_result_type(get_opcode(((OpExpr*)expr)->opno), expr, NULL, resultTypeId, resultTupleDesc,
            NULL);
    } else {
        /* handle as a generic expression; no chance to resolve RECORD */
        Oid typid = exprType(expr);

        if (resultTypeId != NULL) {
            *resultTypeId = typid;
        }

        if (resultTupleDesc != NULL) {
            *resultTupleDesc = NULL;
        }

        result = get_type_func_class(typid);
        if (result == TYPEFUNC_COMPOSITE && resultTupleDesc) {
            *resultTupleDesc = lookup_rowtype_tupdesc_copy(typid, -1);
        }
    }

    return result;
}

/*
 * get_func_result_type
 *		As above, but work from a function's OID only
 *
 * This will not be able to resolve pure-RECORD results nor polymorphism.
 */
TypeFuncClass get_func_result_type(Oid functionId, Oid* resultTypeId, TupleDesc* resultTupleDesc)
{
    return internal_get_result_type(functionId, NULL, NULL, resultTypeId, resultTupleDesc, NULL);
}

/*
 * internal_get_result_type -- workhorse code implementing all the above
 *
 * funcid must always be supplied.	call_expr and rsinfo can be NULL if not
 * available.  We will return TYPEFUNC_RECORD, and store NULL into
 * *resultTupleDesc, if we cannot deduce the complete result rowtype from
 * the available information.
 */
static TypeFuncClass internal_get_result_type(Oid funcid, Node* call_expr, ReturnSetInfo* rsinfo, Oid* resultTypeId,
    TupleDesc* resultTupleDesc, int4* resultTypeId_orig)
{
    TypeFuncClass result;
    HeapTuple tp;
    Form_pg_proc procform;
    Oid rettype;
    TupleDesc tupdesc;
    int4 rettype_orig = -1;

    /* First fetch the function's pg_proc row to inspect its rettype */
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    }

    procform = (Form_pg_proc)GETSTRUCT(tp);
    rettype = procform->prorettype;
    if (IsClientLogicType(rettype)) {
        HeapTuple gstup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(funcid));
        if (!HeapTupleIsValid(gstup)) /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
        Form_gs_encrypted_proc gsform = (Form_gs_encrypted_proc)GETSTRUCT(gstup);

        rettype_orig = gsform->prorettype_orig;
        ReleaseSysCache(gstup);
    }
    /* Check for OUT parameters defining a RECORD result */
    tupdesc = build_function_result_tupdesc_t(tp);
    if (tupdesc) {
        /*
         * It has OUT parameters, so it's basically like a regular composite
         * type, except we have to be able to resolve any polymorphic OUT
         * parameters.
         */
        if (resultTypeId != NULL) {
            *resultTypeId = rettype;
            if (IsClientLogicType(rettype) && resultTypeId_orig != NULL) {
                *resultTypeId_orig = rettype_orig;
            }
        }

        oidvector* proargs = ProcedureGetArgTypes(tp);

        if (resolve_polymorphic_tupdesc(tupdesc, proargs, call_expr)) {
            if (tupdesc->tdtypeid == RECORDOID && tupdesc->tdtypmod < 0) {
                assign_record_type_typmod(tupdesc);
            }

            if (resultTupleDesc != NULL) {
                *resultTupleDesc = tupdesc;
            }

            result = TYPEFUNC_COMPOSITE;
        } else {
            if (resultTupleDesc != NULL) {
                *resultTupleDesc = NULL;
            }

            result = TYPEFUNC_RECORD;
        }

        ReleaseSysCache(tp);
        return result;
    }

    /*
     * If scalar polymorphic result, try to resolve it.
     */
    if (IsPolymorphicType(rettype)) {
        Oid newrettype = exprType(call_expr);
        if (newrettype == InvalidOid) {/* this probably should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not determine actual result type for function \"%s\" declared to return type %s",
                        NameStr(procform->proname),
                        format_type_be(rettype))));
        }

        rettype = newrettype;
    }

    if (resultTypeId != NULL) {
        *resultTypeId = rettype;
        if (IsClientLogicType(rettype) && resultTypeId_orig != NULL) {
            *resultTypeId_orig = rettype_orig;
        }
    }

    if (resultTupleDesc != NULL) {
        *resultTupleDesc = NULL; /* default result */
    }

    /* Classify the result type */
    result = get_type_func_class(rettype);

    switch (result) {
        case TYPEFUNC_COMPOSITE:
            if (resultTupleDesc != NULL) {
                *resultTupleDesc = lookup_rowtype_tupdesc_copy(rettype, -1);
            }

            /* Named composite types can't have any polymorphic columns */
            break;

        case TYPEFUNC_SCALAR:
            break;

        case TYPEFUNC_RECORD:

            /* We must get the tupledesc from call context */
            if (rsinfo && IsA(rsinfo, ReturnSetInfo) && rsinfo->expectedDesc != NULL) {
                result = TYPEFUNC_COMPOSITE;

                if (resultTupleDesc != NULL) {
                    *resultTupleDesc = rsinfo->expectedDesc;
                }

                /* Assume no polymorphic columns here, either */
            }

            break;

        default:
            break;
    }

    ReleaseSysCache(tp);
    return result;
}

bool deducePolymorphic(
    Oid* anyelement_type, Oid* anyarray_type, Oid anyrange_type, bool have_anyelement_result, bool have_anyarray_result)
{
    if (!OidIsValid(*anyelement_type) && !OidIsValid(*anyarray_type) && !OidIsValid(anyrange_type)) {
        return false;
    }

    /* If needed, deduce one polymorphic type from others */
    if (have_anyelement_result && !OidIsValid(*anyelement_type)) {
        if (OidIsValid(*anyarray_type)) {
            *anyelement_type = resolve_generic_type(ANYELEMENTOID, *anyarray_type, ANYARRAYOID);
        }

        if (OidIsValid(anyrange_type)) {
            Oid subtype = resolve_generic_type(ANYELEMENTOID, anyrange_type, ANYRANGEOID);

            /* check for inconsistent array and range results */
            if (OidIsValid(*anyelement_type) && *anyelement_type != subtype) {
                return false;
            }
            *anyelement_type = subtype;
        }
    }

    if (have_anyarray_result && !OidIsValid(*anyarray_type)) {
        *anyarray_type = resolve_generic_type(ANYARRAYOID, *anyelement_type, ANYELEMENTOID);
    }
    return true;
}
/*
 * Given the result tuple descriptor for a function with OUT parameters,
 * replace any polymorphic columns (ANYELEMENT etc) with correct data types
 * deduced from the input arguments. Returns TRUE if able to deduce all types,
 * FALSE if not.
 */
static bool resolve_polymorphic_tupdesc(TupleDesc tupdesc, oidvector* declared_args, Node* call_expr)
{
    int natts = tupdesc->natts;
    int nargs = declared_args->dim1;
    bool have_anyelement_result = false;
    bool have_anyarray_result = false;
    bool have_anyrange_result = false;
    bool have_anynonarray = false;
    bool have_anyenum = false;
    Oid anyelement_type = InvalidOid;
    Oid anyarray_type = InvalidOid;
    Oid anyrange_type = InvalidOid;
    Oid anycollation = InvalidOid;
    int i;

    /* See if there are any polymorphic outputs; quick out if not */
    for (i = 0; i < natts; i++) {
        switch (tupdesc->attrs[i].atttypid) {
            case ANYELEMENTOID:
                have_anyelement_result = true;
                break;

            case ANYARRAYOID:
                have_anyarray_result = true;
                break;

            case ANYNONARRAYOID:
                have_anyelement_result = true;
                have_anynonarray = true;
                break;

            case ANYENUMOID:
                have_anyelement_result = true;
                have_anyenum = true;
                break;

            case ANYRANGEOID:
                have_anyrange_result = true;
                break;

            default:
                break;
        }
    }

    if (!have_anyelement_result && !have_anyarray_result && !have_anyrange_result) {
        return true;
    }

    /*
     * Otherwise, extract actual datatype(s) from input arguments.	(We assume
     * the parser already validated consistency of the arguments.)
     */
    if (call_expr == NULL){
        return false; /* no hope */
    }

    for (i = 0; i < nargs; i++) {
        switch (declared_args->values[i]) {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                if (!OidIsValid(anyelement_type))
                    anyelement_type = get_call_expr_argtype(call_expr, i);

                break;

            case ANYARRAYOID:
                if (!OidIsValid(anyarray_type))
                    anyarray_type = get_call_expr_argtype(call_expr, i);

                break;

            case ANYRANGEOID:
                if (!OidIsValid(anyrange_type))
                    anyrange_type = get_call_expr_argtype(call_expr, i);

                break;

            default:
                break;
        }
    }

    if (!deducePolymorphic(
            &anyelement_type, &anyarray_type, anyrange_type, have_anyelement_result, have_anyarray_result)) {
        return false;
    }

    /*
     * We can't deduce a range type from other polymorphic inputs, because
     * there may be multiple range types for the same subtype.
     */
    if (have_anyrange_result && !OidIsValid(anyrange_type)) {
        return false;
    }

    /* Enforce ANYNONARRAY if needed */
    if (have_anynonarray && type_is_array(anyelement_type)) {
        return false;
    }

    /* Enforce ANYENUM if needed */
    if (have_anyenum && !type_is_enum(anyelement_type)) {
        return false;
    }

    /*
     * Identify the collation to use for polymorphic OUT parameters. (It'll
     * necessarily be the same for both anyelement and anyarray.)  Note that
     * range types are not collatable, so any possible internal collation of a
     * range type is not considered here.
     */
    if (OidIsValid(anyelement_type)) {
        anycollation = get_typcollation(anyelement_type);
    } else if (OidIsValid(anyarray_type)) {
        anycollation = get_typcollation(anyarray_type);
    }

    if (OidIsValid(anycollation)) {
        /*
         * The types are collatable, so consider whether to use a nondefault
         * collation.  We do so if we can identify the input collation used
         * for the function.
         */
        Oid inputcollation = exprInputCollation(call_expr);
        if (OidIsValid(inputcollation)) {
            anycollation = inputcollation;
        }
    }

    /* And finally replace the tuple column types as needed */
    for (i = 0; i < natts; i++) {
        switch (tupdesc->attrs[i].atttypid) {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                TupleDescInitEntry(tupdesc, i + 1, NameStr(tupdesc->attrs[i].attname), anyelement_type, -1, 0);
                TupleDescInitEntryCollation(tupdesc, i + 1, anycollation);
                break;

            case ANYARRAYOID:
                TupleDescInitEntry(tupdesc, i + 1, NameStr(tupdesc->attrs[i].attname), anyarray_type, -1, 0);
                TupleDescInitEntryCollation(tupdesc, i + 1, anycollation);
                break;

            case ANYRANGEOID:
                TupleDescInitEntry(tupdesc, i + 1, NameStr(tupdesc->attrs[i].attname), anyrange_type, -1, 0);
                /* no collation should be attached to a range type */
                break;

            default:
                break;
        }
    }

    return true;
}

/*
 * Given the declared argument types and modes for a function, replace any
 * polymorphic types (ANYELEMENT etc) with correct data types deduced from the
 * input arguments.  Returns TRUE if able to deduce all types, FALSE if not.
 * This is the same logic as resolve_polymorphic_tupdesc, but with a different
 * argument representation.
 *
 * argmodes may be NULL, in which case all arguments are assumed to be IN mode.
 */
bool resolve_polymorphic_argtypes(int numargs, Oid* argtypes, const char* argmodes, Node* call_expr)
{
    bool have_anyelement_result = false;
    bool have_anyarray_result = false;
    bool have_anyrange_result = false;
    Oid anyelement_type = InvalidOid;
    Oid anyarray_type = InvalidOid;
    Oid anyrange_type = InvalidOid;
    int inargno;
    int i;

    /* First pass: resolve polymorphic inputs, check for outputs */
    inargno = 0;

    for (i = 0; i < numargs; i++) {
        char argmode = argmodes ? argmodes[i] : PROARGMODE_IN;

        switch (argtypes[i]) {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                if (argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE) {
                    have_anyelement_result = true;
                } else {
                    if (!OidIsValid(anyelement_type)) {
                        anyelement_type = get_call_expr_argtype(call_expr, inargno);
                        if (!OidIsValid(anyelement_type)) {
                            return false;
                        }
                    }

                    argtypes[i] = anyelement_type;
                }

                break;

            case ANYARRAYOID:
                if (argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE) {
                    have_anyarray_result = true;
                } else {
                    if (!OidIsValid(anyarray_type)) {
                        anyarray_type = get_call_expr_argtype(call_expr, inargno);

                        if (!OidIsValid(anyarray_type)) {
                            return false;
                        }
                    }

                    argtypes[i] = anyarray_type;
                }
                break;

            case ANYRANGEOID:
                if (argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE) {
                    have_anyrange_result = true;
                } else {
                    if (!OidIsValid(anyrange_type)) {
                        anyrange_type = get_call_expr_argtype(call_expr, inargno);
                        if (!OidIsValid(anyrange_type)) {
                            return false;
                        }
                    }

                    argtypes[i] = anyrange_type;
                }

                break;

            default:
                break;
        }

        if (argmode != PROARGMODE_OUT && argmode != PROARGMODE_TABLE) {
            inargno++;
        }
    }

    /* Done? */
    if (!have_anyelement_result && !have_anyarray_result && !have_anyrange_result) {
        return true;
    }
    if (!deducePolymorphic(
            &anyelement_type, &anyarray_type, anyrange_type, have_anyelement_result, have_anyarray_result)) {
        return false;
    }

    /*
     * We can't deduce a range type from other polymorphic inputs, because
     * there may be multiple range types for the same subtype.
     */
    if (have_anyrange_result && !OidIsValid(anyrange_type)) {
        return false;
    }

    /* XXX do we need to enforce ANYNONARRAY or ANYENUM here?  I think not */

    /* And finally replace the output column types as needed */
    for (i = 0; i < numargs; i++) {
        switch (argtypes[i]) {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                argtypes[i] = anyelement_type;
                break;

            case ANYARRAYOID:
                argtypes[i] = anyarray_type;
                break;

            case ANYRANGEOID:
                argtypes[i] = anyrange_type;
                break;

            default:
                break;
        }
    }

    return true;
}

/*
 * get_type_func_class
 *		Given the type OID, obtain its TYPEFUNC classification.
 *
 * This is intended to centralize a bunch of formerly ad-hoc code for
 * classifying types.  The categories used here are useful for deciding
 * how to handle functions returning the datatype.
 */
static TypeFuncClass get_type_func_class(Oid typid)
{
    switch (get_typtype(typid)) {
        case TYPTYPE_COMPOSITE:
            return TYPEFUNC_COMPOSITE;

        case TYPTYPE_BASE:
        case TYPTYPE_DOMAIN:
        case TYPTYPE_ENUM:
        case TYPTYPE_RANGE:
        case TYPTYPE_TABLEOF:
            return TYPEFUNC_SCALAR;

        case TYPTYPE_PSEUDO:
            if (typid == RECORDOID) {
                return TYPEFUNC_RECORD;
            }

            /*
             * We treat VOID and CSTRING as legitimate scalar datatypes,
             * mostly for the convenience of the JDBC driver (which wants to
             * be able to do "SELECT * FROM foo()" for all legitimately
             * user-callable functions).
             */
            if (typid == VOIDOID || typid == CSTRINGOID) {
                return TYPEFUNC_SCALAR;
            }

            return TYPEFUNC_OTHER;
        default:
            break;
    }

    /* shouldn't get here, probably */
    return TYPEFUNC_OTHER;
}

/*
 * get_func_arg_info
 *
 * Fetch info about the argument types, names, and IN/OUT modes from the
 * pg_proc tuple.  Return value is the total number of arguments.
 * Other results are palloc'd.  *p_argtypes is always filled in, but
 * *p_argnames and *p_argmodes will be set NULL in the default cases
 * (no names, and all IN arguments, respectively).
 *
 * Note that this function simply fetches what is in the pg_proc tuple;
 * it doesn't do any interpretation of polymorphic types.
 */
int get_func_arg_info(HeapTuple procTup, Oid** p_argtypes, char*** p_argnames, char** p_argmodes)
{
    Datum proallargtypes;
    Datum proargmodes;
    Datum proargnames;
    bool isNull = false;
    ArrayType* arr = NULL;
    int numargs;
    Datum* elems = NULL;
    int nelems;
    int i;
    errno_t rc;

    /* First discover the total number of parameters and get their types */
    proallargtypes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proallargtypes, &isNull);
    if (!isNull) {
        /*
         * We expect the arrays to be 1-D arrays of the right types; verify
         * that.  For the OID and char arrays, we don't need to use
         * deconstruct_array() since the array data is just going to look like
         * a C array of values.
         */
        arr = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
        numargs = ARR_DIMS(arr)[0];

        if (ARR_NDIM(arr) != 1 || numargs < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }
        Assert(numargs >= ((Form_pg_proc)GETSTRUCT(procTup))->pronargs);

        *p_argtypes = (Oid*)palloc(numargs * sizeof(Oid));
        rc = memcpy_s(*p_argtypes, numargs * sizeof(Oid), ARR_DATA_PTR(arr), numargs * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    } else {
        /* If no proallargtypes, use proargtypes */
        oidvector* proargs = ProcedureGetArgTypes(procTup);
        numargs = proargs->dim1;
        Assert(numargs == ((Form_pg_proc)GETSTRUCT(procTup))->pronargs);

        *p_argtypes = (Oid*)palloc(numargs * sizeof(Oid));

        if (numargs > 0) {
            rc = memcpy_s(*p_argtypes, numargs * sizeof(Oid), proargs->values, numargs * sizeof(Oid));
            securec_check(rc, "\0", "\0");
        }
    }

    /* Get argument names, if available */
    proargnames = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proargnames, &isNull);

    if (isNull) {
        *p_argnames = NULL;
    } else {
        deconstruct_array(DatumGetArrayTypeP(proargnames), TEXTOID, -1, false, 'i', &elems, NULL, &nelems);

        if (nelems != numargs) { /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("proargnames must have the same number of elements as the function has arguments")));
        }

        *p_argnames = (char**)palloc(sizeof(char*) * numargs);
        for (i = 0; i < numargs; i++) {
            (*p_argnames)[i] = TextDatumGetCString(elems[i]);
        }
    }

    /* Get argument modes, if available */
    proargmodes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proargmodes, &isNull);
    if (isNull) {
        *p_argmodes = NULL;
    } else {
        arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
        if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        *p_argmodes = (char*)palloc(numargs * sizeof(char));
        errno_t ret = memcpy_s(*p_argmodes, numargs * sizeof(char), ARR_DATA_PTR(arr), numargs * sizeof(char));
        securec_check(ret, "\0", "\0");
    }

    return numargs;
}

/*
 * get_func_input_arg_names
 *
 * Extract the names of input arguments only, given a function's
 * proargnames and proargmodes entries in Datum form.
 *
 * Returns the number of input arguments, which is the length of the
 * palloc'd array returned to *arg_names.  Entries for unnamed args
 * are set to NULL.  You don't get anything if proargnames is NULL.
 */
int get_func_input_arg_names(Datum proargnames, Datum proargmodes, char*** arg_names)
{
    ArrayType* arr = NULL;
    int numargs;
    Datum* argnames = NULL;
    char* argmodes = NULL;
    char** inargnames;
    int numinargs;
    int i;

    /* Do nothing if null proargnames */
    if (proargnames == PointerGetDatum(NULL)) {
        *arg_names = NULL;
        return 0;
    }

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     * For proargmodes, we don't need to use deconstruct_array() since the
     * array data is just going to look like a C array of values.
     */
    arr = DatumGetArrayTypeP(proargnames); /* ensure not toasted */

    if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
    }

    deconstruct_array(arr, TEXTOID, -1, false, 'i', &argnames, NULL, &numargs);
    if (proargmodes != PointerGetDatum(NULL)) {
        arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */

        if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        argmodes = (char*)ARR_DATA_PTR(arr);
    } else {
        argmodes = NULL;
    }
    /* zero elements probably shouldn't happen, but handle it gracefully */
    if (numargs <= 0) {
        *arg_names = NULL;
        return 0;
    }

    /* extract input-argument names */
    inargnames = (char**)palloc(numargs * sizeof(char*));
    numinargs = 0;

    for (i = 0; i < numargs; i++) {
        if (argmodes == NULL || argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_INOUT ||
            argmodes[i] == PROARGMODE_VARIADIC) {
            char* pname = TextDatumGetCString(argnames[i]);

            if (pname[0] != '\0') {
                inargnames[numinargs] = pname;
            } else {
                inargnames[numinargs] = NULL;
            }

            numinargs++;
        }
    }

    *arg_names = inargnames;
    return numinargs;
}

void checkOidArray(ArrayType* arr, int numargs)
{
    if (ARR_NDIM(arr) != 1 || numargs != ARR_DIMS(arr)[0] || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
    }
}
/*
 * get_func_result_name
 *
 * If the function has exactly one output parameter, and that parameter
 * is named, return the name (as a palloc'd string).  Else return NULL.
 *
 * This is used to determine the default output column name for functions
 * returning scalar types.
 */
char* get_func_result_name(Oid functionId)
{
    char* result = NULL;
    HeapTuple proc_tuple;
    Datum proargmodes;
    Datum proargnames;
    bool isnull = false;
    ArrayType* arr = NULL;
    int numargs;
    char* argmodes = NULL;
    Datum* argnames = NULL;
    int numoutargs;
    int nargnames;
    int i;

    /* First fetch the function's pg_proc row */
    proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));

    if (!HeapTupleIsValid(proc_tuple)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", functionId)));
    }

    /* If there are no named OUT parameters, return NULL */
    if (heap_attisnull(proc_tuple, Anum_pg_proc_proargmodes, NULL) ||
        heap_attisnull(proc_tuple, Anum_pg_proc_proargnames, NULL)) {
        result = NULL;
    } else {
        /* Get the data out of the tuple */
        proargmodes = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargmodes, &isnull);
        Assert(!isnull);
        proargnames = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargnames, &isnull);
        Assert(!isnull);

        /*
         * We expect the arrays to be 1-D arrays of the right types; verify
         * that.  For the char array, we don't need to use deconstruct_array()
         * since the array data is just going to look like a C array of
         * values.
         */
        arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
        numargs = ARR_DIMS(arr)[0];

        if (ARR_NDIM(arr) != 1 || numargs < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }
        argmodes = (char*)ARR_DATA_PTR(arr);
        arr = DatumGetArrayTypeP(proargnames); /* ensure not toasted */
        checkOidArray(arr, numargs);
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &argnames, NULL, &nargnames);
        Assert(nargnames == numargs);

        /* scan for output argument(s) */
        result = NULL;
        numoutargs = 0;

        for (i = 0; i < numargs; i++) {
            if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC) {
                continue;
            }

            Assert(argmodes[i] == PROARGMODE_OUT || argmodes[i] == PROARGMODE_INOUT || argmodes[i] == PROARGMODE_TABLE);
            if (++numoutargs > 1) {
                /* multiple out args, so forget it */
                result = NULL;
                break;
            }

            result = TextDatumGetCString(argnames[i]);
            if (result == NULL || result[0] == '\0') {
                /* Parameter is not named, so forget it */
                result = NULL;
                break;
            }
        }
    }

    ReleaseSysCache(proc_tuple);
    return result;
}

/*
 * build_function_result_tupdesc_t
 *
 * Given a pg_proc row for a function, return a tuple descriptor for the
 * result rowtype, or NULL if the function does not have OUT parameters.
 *
 * Note that this does not handle resolution of polymorphic types;
 * that is deliberate.
 */
TupleDesc build_function_result_tupdesc_t(HeapTuple proc_tuple)
{
    Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proc_tuple);
    Datum proallargtypes;
    Datum proargmodes;
    Datum proargnames;
    bool isnull = false;

    /* Return NULL if the function isn't declared to return RECORD */
    if (procform->prorettype != RECORDOID) {
        return NULL;
    }

    /* If there are no OUT parameters, return NULL */
    if (heap_attisnull(proc_tuple, Anum_pg_proc_proallargtypes, NULL) ||
        heap_attisnull(proc_tuple, Anum_pg_proc_proargmodes, NULL)) {
        return NULL;
    }

    /* Get the data out of the tuple */
    proallargtypes = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proallargtypes, &isnull);
    Assert(!isnull);
    proargmodes = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargmodes, &isnull);
    Assert(!isnull);
    proargnames = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargnames, &isnull);
    if (isnull) {
        proargnames = PointerGetDatum(NULL); /* just to be sure */
    }
    Datum funcid = 0;
    funcid = SysCacheGetAttr(PROCOID, proc_tuple, ObjectIdAttributeNumber, &isnull);
    return build_function_result_tupdesc_d(proallargtypes, proargmodes, proargnames, funcid);
}

bool read_origin_type(Datum funcid, int4** argtypes_orig, int4** outargtypes_orig, int numargs)
{
    HeapTuple gstup = SearchSysCache1(GSCLPROCID, funcid);
    if (!HeapTupleIsValid(gstup)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("failed to read gs_encrypted_proc catalog table")));
        return false;
    }
    bool isnull = false;
    Datum proallargtypes_orig =
        SysCacheGetAttr(GSCLPROCID, gstup, Anum_gs_encrypted_proc_proallargtypes_orig, &isnull);
    if (proallargtypes_orig == PointerGetDatum(NULL)) {
        ReleaseSysCache(gstup);
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("failed to read proallargtypes_orig attribute in gs_encrypted_proc catalog table")));
        return false;
    }
    ArrayType* arr = DatumGetArrayTypeP(proallargtypes_orig); /* ensure not toasted */
    *argtypes_orig = (int4*)ARR_DATA_PTR(arr);
    *outargtypes_orig = (int4*)palloc(numargs * sizeof(int4));
    if (*outargtypes_orig == NULL) {
        ReleaseSysCache(gstup);
        return false;
    }
    securec_check(memset_s(*outargtypes_orig, numargs * sizeof(int4), -1, numargs * sizeof(int4)), "", "");
    ReleaseSysCache(gstup);
    return true;
}

/*
 * build_function_result_tupdesc_d
 *
 * Build a RECORD function's tupledesc from the pg_proc proallargtypes,
 * proargmodes, and proargnames arrays.  This is split out for the
 * convenience of ProcedureCreate, which needs to be able to compute the
 * tupledesc before actually creating the function.
 *
 * Returns NULL if there are not at least two OUT or INOUT arguments.
 */
TupleDesc build_function_result_tupdesc_d(Datum proallargtypes, Datum proargmodes, Datum proargnames, Datum funcid)
{
    TupleDesc desc;
    ArrayType* arr = NULL;
    int numargs = 0;
    Oid* argtypes = NULL;
    int4* argtypes_orig = NULL;
    char* argmodes = NULL;
    Datum* argnames = NULL;
    Oid* outargtypes = NULL;
    int4* outargtypes_orig = NULL;
    char** outargnames = NULL;
    int numoutargs = 0;
    int nargnames = 0;
    int i;

    /* Can't have output args if columns are null */
    if (proallargtypes == PointerGetDatum(NULL) || proargmodes == PointerGetDatum(NULL)) {
        return NULL;
    }

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     * For the OID and char arrays, we don't need to use deconstruct_array()
     * since the array data is just going to look like a C array of values.
     */
    arr = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
    numargs = ARR_DIMS(arr)[0];

    bool isOidOidArray = ARR_NDIM(arr) != 1 || numargs < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID;
    if (isOidOidArray) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
    }
    argtypes = (Oid*)ARR_DATA_PTR(arr);
    arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */

    bool isCharOidArray = ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs || ARR_HASNULL(arr) ||
        ARR_ELEMTYPE(arr) != CHAROID;
    if (isCharOidArray) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
    }

    argmodes = (char*)ARR_DATA_PTR(arr);
    if (proargnames != PointerGetDatum(NULL)) {
        arr = DatumGetArrayTypeP(proargnames); /* ensure not toasted */
        checkOidArray(arr, numargs);
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &argnames, NULL, &nargnames);
        Assert(nargnames == numargs);
    }

    /*
     * If there is no output argument, or only one, the function does not return tuples.
     * If the number of total arguments is less than 2 than the number of output parameters is less than 2.
     */
    if (numargs < 2) {
        return NULL;
    }

    /* extract output-argument types and names */
    outargtypes = (Oid*)palloc(numargs * sizeof(Oid));
    outargnames = (char**)palloc(numargs * sizeof(char*));
    numoutargs = 0;
    bool gs_cl_proc_was_read = false;

    for (i = 0; i < numargs; i++) {
        char* pname = NULL;

        bool is_only_input_params = argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC;
        if (is_only_input_params) {
            continue;
        }

        Assert(argmodes[i] == PROARGMODE_OUT || argmodes[i] == PROARGMODE_INOUT || argmodes[i] == PROARGMODE_TABLE);
        outargtypes[numoutargs] = argtypes[i];
        /*
         * if any OUT arguments are client-logic arguments, then 
         * 1. read the original data types from gs_cl_proc into the argtypes_orig array 
         * 2. initialize the outargtypes_orig array that we are building in parallel to the outargtypes array
         * if funcid == 0 then there is no need to check the original data types
         */
        bool need_to_read = funcid > 0 && !gs_cl_proc_was_read && IsClientLogicType(argtypes[i]);
        if (need_to_read) {
            if (!read_origin_type(funcid, &argtypes_orig, &outargtypes_orig, numargs)) {
                return NULL;
            }
            gs_cl_proc_was_read = true;
        }
        /*
            add original data type to the array of original data types
        */
        if (gs_cl_proc_was_read && IsClientLogicType(argtypes[i])) {
            outargtypes_orig[numoutargs] = argtypes_orig[i];
        }

        pname = (argnames != NULL) ? TextDatumGetCString(argnames[i]) : NULL;

        if (pname == NULL || pname[0] == '\0') {
            /* Parameter is not named, so gin up a column name */
            const size_t len = 32;
            pname = (char*)palloc(len);
            errno_t rc = snprintf_s(pname, len, len - 1, "column%d", numoutargs + 1);
            securec_check_ss_c(rc, "\0", "\0");
        }

        outargnames[numoutargs] = pname;
        numoutargs++;
    }

    /*
     * If there is no output argument, or only one, the function does not
     * return tuples.
     */
    if (numoutargs < 2) {
        return NULL;
    }

    /*
     * functions use default heap tuple operations like heap_form_tuple, and they are
     * accessed via tam type in tuple descriptor.
     */
    desc = CreateTemplateTupleDesc(numoutargs, false);
    for (int i = 0; i < numoutargs; i++) {
        if (outargtypes_orig != NULL) {
            /* in case of a client-logic parameter, we pass the original data type in the typmod field */
            TupleDescInitEntry(desc, i + 1, outargnames[i], outargtypes[i], outargtypes_orig[i], 0);
        } else {
            TupleDescInitEntry(desc, i + 1, outargnames[i], outargtypes[i], -1, 0);
        }
    }

    return desc;
}

/*
 * RelationNameGetTupleDesc
 *
 * Given a (possibly qualified) relation name, build a TupleDesc.
 *
 * Note: while this works as advertised, it's seldom the best way to
 * build a tupdesc for a function's result type.  It's kept around
 * only for backwards compatibility with existing user-written code.
 */
TupleDesc RelationNameGetTupleDesc(const char* relname)
{
    RangeVar* relvar = NULL;
    Relation rel;
    TupleDesc tupdesc;
    List* relname_list = NIL;

    /* Open relation and copy the tuple description */
    relname_list = stringToQualifiedNameList(relname);
    relvar = makeRangeVarFromNameList(relname_list);
    rel = relation_openrv(relvar, AccessShareLock);
    tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
    relation_close(rel, AccessShareLock);

    return tupdesc;
}

/*
 * TypeGetTupleDesc
 *
 * Given a type Oid, build a TupleDesc.  (In most cases you should be
 * using get_call_result_type or one of its siblings instead of this
 * routine, so that you can handle OUT parameters, RECORD result type,
 * and polymorphic results.)
 *
 * If the type is composite, *and* a colaliases List is provided, *and*
 * the List is of natts length, use the aliases instead of the relation
 * attnames.  (NB: this usage is deprecated since it may result in
 * creation of unnecessary transient record types.)
 *
 * If the type is a base type, a single item alias List is required.
 */
TupleDesc TypeGetTupleDesc(Oid typeoid, List* colaliases)
{
    TypeFuncClass functypclass = get_type_func_class(typeoid);
    TupleDesc tupdesc = NULL;

    /*
     * Build a suitable tupledesc representing the output rows
     */
    if (functypclass == TYPEFUNC_COMPOSITE) {
        /* Composite data type, e.g. a table's row type */
        tupdesc = lookup_rowtype_tupdesc_copy(typeoid, -1);

        if (colaliases != NIL) {
            int natts = tupdesc->natts;
            int varattno;

            /* does the list length match the number of attributes? */
            if (list_length(colaliases) != natts) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("number of aliases does not match number of columns")));
            }

            /* OK, use the aliases instead */
            for (varattno = 0; varattno < natts; varattno++) {
                char* label = strVal(list_nth(colaliases, varattno));

                if (label != NULL) {
                    (void)namestrcpy(&(tupdesc->attrs[varattno].attname), label);
                }
            }

            /* The tuple type is now an anonymous record type */
            tupdesc->tdtypeid = RECORDOID;
            tupdesc->tdtypmod = -1;
        }
    } else if (functypclass == TYPEFUNC_SCALAR) {
        /* Base data type, i.e. scalar */
        char* attname = NULL;

        /* the alias list is required for base types */
        if (colaliases == NIL) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("no column alias was provided")));
        }

        /* the alias list length must be 1 */
        if (list_length(colaliases) != 1) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("number of aliases does not match number of columns")));
        }

        /* OK, get the column alias */
        attname = strVal(linitial(colaliases));
        tupdesc = CreateTemplateTupleDesc(1, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, attname, typeoid, -1, 0);
    } else if (functypclass == TYPEFUNC_RECORD) {
        /* XXX can't support this because typmod wasn't passed in ... */
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("could not determine row description for function returning record")));
    } else {
        /* crummy error message, but parser should have caught this */
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("function in FROM has unsupported return type")));
    }

    return tupdesc;
}
