/* -------------------------------------------------------------------------
 *
 * equalfuncs.cpp
 *	  Equality functions to compare node trees.
 *
 * NOTE: we currently support comparing all node types found in parse
 * trees.  We do not support comparing executor state trees; there
 * is no need for that, and no point in maintaining all the code that
 * would be needed.  We also do not support comparing Path trees, mainly
 * because the circular linkages between RelOptInfo and Path nodes can't
 * be handled easily in a simple depth-first traversal.
 *
 * Currently, in fact, equal() doesn't know how to compare Plan trees
 * either.	This might need to be fixed someday.
 *
 * NOTE: it is intentional that parse location fields (in nodes that have
 * one) are not compared.  This is because we want, for example, a variable
 * "x" to be considered equal() to another reference to "x" in the query.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/equalfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "bulkload/dist_fdw.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "storage/proc.h"
#include "storage/tcap.h"

/*
 * Macros to simplify comparison of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

/* Compare a simple scalar field (int, float, bool, enum, etc) */
#define COMPARE_SCALAR_FIELD(fldname)  \
    do {                               \
        if (a->fldname != b->fldname) {\
            return false;              \
        }                              \
    } while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname)          \
    do {                                     \
        if (!equal(a->fldname, b->fldname)) {\
            return false;                    \
        }                                    \
    } while (0)

/* Compare a field that is a pointer to a Bitmapset */
#define COMPARE_BITMAPSET_FIELD(fldname)         \
    do {                                         \
        if (!bms_equal(a->fldname, b->fldname)) {\
            return false;                        \
        }                                        \
    } while (0)

/* Compare a field that is a pointer to a C string, or perhaps NULL */
#define COMPARE_STRING_FIELD(fldname)           \
    do {                                        \
        if (!equalstr(a->fldname, b->fldname)) {\
            return false;                       \
        }                                       \
    } while (0)

/* Macro for comparing string fields that might be NULL */
#define equalstr(a, b) (((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : ((a) == (b)))

/* Compare a field that is a pointer to a simple palloc'd object of size sz */
#define COMPARE_POINTER_FIELD(fldname, sz)             \
    do {                                               \
        if (memcmp(a->fldname, b->fldname, (sz)) != 0) \
            return false;                              \
    } while (0)

/* Compare a parse location field (this is a no-op, per note above) */
#define COMPARE_LOCATION_FIELD(fldname) ((void)0)

/* Compare a CoercionForm field (also a no-op, per comment in primnodes.h) */
#define COMPARE_COERCIONFORM_FIELD(fldname) ((void)0)

/*
 *	Stuff from primnodes.h
 */
static bool _equalAlias(const Alias* a, const Alias* b)
{
    COMPARE_STRING_FIELD(aliasname);
    COMPARE_NODE_FIELD(colnames);

    return true;
}

static bool _equalRangeVar(const RangeVar* a, const RangeVar* b)
{
    COMPARE_STRING_FIELD(catalogname);
    COMPARE_STRING_FIELD(schemaname);
    COMPARE_STRING_FIELD(relname);
    COMPARE_STRING_FIELD(partitionname);
    COMPARE_STRING_FIELD(subpartitionname);
    COMPARE_SCALAR_FIELD(inhOpt);
    COMPARE_SCALAR_FIELD(relpersistence);
    COMPARE_NODE_FIELD(alias);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(ispartition);
    COMPARE_SCALAR_FIELD(issubpartition);
    COMPARE_NODE_FIELD(partitionKeyValuesList);
    COMPARE_SCALAR_FIELD(isbucket);
    COMPARE_NODE_FIELD(buckets);
    COMPARE_SCALAR_FIELD(withVerExpr);

    return true;
}

static bool _equalIntoClause(const IntoClause* a, const IntoClause* b)
{
    COMPARE_NODE_FIELD(rel);
    COMPARE_NODE_FIELD(colNames);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(onCommit);
    COMPARE_SCALAR_FIELD(row_compress);
    COMPARE_STRING_FIELD(tableSpaceName);
    COMPARE_SCALAR_FIELD(skipData);
    COMPARE_SCALAR_FIELD(ivm);
    COMPARE_SCALAR_FIELD(relkind);

    return true;
}

/*
 * We don't need an _equalExpr because Expr is an abstract supertype which
 * should never actually get instantiated.	Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out comparing the common fields...
 */
static bool _equalVar(const Var* a, const Var* b)
{
    COMPARE_SCALAR_FIELD(varno);
    COMPARE_SCALAR_FIELD(varattno);
    COMPARE_SCALAR_FIELD(vartype);
    COMPARE_SCALAR_FIELD(vartypmod);
    COMPARE_SCALAR_FIELD(varcollid);
    COMPARE_SCALAR_FIELD(varlevelsup);
    COMPARE_SCALAR_FIELD(varnoold);
    COMPARE_SCALAR_FIELD(varoattno);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalConst(const Const* a, const Const* b)
{
    COMPARE_SCALAR_FIELD(consttype);
    COMPARE_SCALAR_FIELD(consttypmod);
    COMPARE_SCALAR_FIELD(constcollid);
    COMPARE_SCALAR_FIELD(constlen);
    COMPARE_SCALAR_FIELD(constisnull);
    COMPARE_SCALAR_FIELD(constbyval);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(ismaxvalue);

    /*
     * We treat all NULL constants of the same type as equal. Someday this
     * might need to change?  But datumIsEqual doesn't work on nulls, so...
     */
    if (a->constisnull)
        return true;
    return datumIsEqual(a->constvalue, b->constvalue, a->constbyval, a->constlen);
}

static bool _equalParam(const Param* a, const Param* b)
{
    COMPARE_SCALAR_FIELD(paramkind);
    COMPARE_SCALAR_FIELD(paramid);
    COMPARE_SCALAR_FIELD(paramtype);
    COMPARE_SCALAR_FIELD(paramtypmod);
    COMPARE_SCALAR_FIELD(paramcollid);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(tableOfIndexType);
    COMPARE_SCALAR_FIELD(recordVarTypOid);

    return true;
}

static bool _equalAggref(const Aggref* a, const Aggref* b)
{
    COMPARE_SCALAR_FIELD(aggfnoid);
    COMPARE_SCALAR_FIELD(aggtype);
#ifdef PGXC
    COMPARE_SCALAR_FIELD(aggtrantype);
    COMPARE_SCALAR_FIELD(agghas_collectfn);
    COMPARE_SCALAR_FIELD(aggstage);
#endif /* PGXC */
    COMPARE_SCALAR_FIELD(aggcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(aggdirectargs);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(aggorder);
    COMPARE_NODE_FIELD(aggdistinct);
    COMPARE_SCALAR_FIELD(aggstar);
    COMPARE_SCALAR_FIELD(aggvariadic);
    COMPARE_SCALAR_FIELD(aggkind);
    COMPARE_SCALAR_FIELD(agglevelsup);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalGroupingFunc(const GroupingFunc* a, const GroupingFunc* b)
{
    COMPARE_NODE_FIELD(args);

    /*
     * We must not compare the refs or cols field
     */
    COMPARE_SCALAR_FIELD(agglevelsup);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalWindowFunc(const WindowFunc* a, const WindowFunc* b)
{
    COMPARE_SCALAR_FIELD(winfnoid);
    COMPARE_SCALAR_FIELD(wintype);
    COMPARE_SCALAR_FIELD(wincollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(winref);
    COMPARE_SCALAR_FIELD(winstar);
    COMPARE_SCALAR_FIELD(winagg);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalArrayRef(const ArrayRef* a, const ArrayRef* b)
{
    COMPARE_SCALAR_FIELD(refarraytype);
    COMPARE_SCALAR_FIELD(refelemtype);
    COMPARE_SCALAR_FIELD(reftypmod);
    COMPARE_SCALAR_FIELD(refcollid);
    COMPARE_NODE_FIELD(refupperindexpr);
    COMPARE_NODE_FIELD(reflowerindexpr);
    COMPARE_NODE_FIELD(refexpr);
    COMPARE_NODE_FIELD(refassgnexpr);

    return true;
}

static bool _equalFuncExpr(const FuncExpr* a, const FuncExpr* b)
{
    COMPARE_SCALAR_FIELD(funcid);
    COMPARE_SCALAR_FIELD(funcresulttype);
    COMPARE_SCALAR_FIELD(funcresulttype_orig);
    COMPARE_SCALAR_FIELD(funcretset);
    COMPARE_COERCIONFORM_FIELD(funcformat);
    COMPARE_SCALAR_FIELD(funccollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(refSynOid);

    return true;
}

static bool _equalNamedArgExpr(const NamedArgExpr* a, const NamedArgExpr* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_STRING_FIELD(name);
    COMPARE_SCALAR_FIELD(argnumber);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

template <typename T>
static bool _euqalCommonExprPart(const T* a, const T* b)
{
    COMPARE_SCALAR_FIELD(opno);

    /*
     * Special-case opfuncid: it is allowable for it to differ if one node
     * contains zero and the other doesn't.  This just means that the one node
     * isn't as far along in the parse/plan pipeline and hasn't had the
     * opfuncid cache filled yet.
     */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0) {
        return false;
    }

    COMPARE_SCALAR_FIELD(opresulttype);
    COMPARE_SCALAR_FIELD(opretset);
    COMPARE_SCALAR_FIELD(opcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}
static bool _equalOpExpr(const OpExpr* a, const OpExpr* b)
{
    return _euqalCommonExprPart<OpExpr>(a, b);
}

static bool _equalDistinctExpr(const DistinctExpr* a, const DistinctExpr* b)
{
    return _euqalCommonExprPart<DistinctExpr>(a, b);
}

static bool _equalNullIfExpr(const NullIfExpr* a, const NullIfExpr* b)
{
    return _euqalCommonExprPart<NullIfExpr>(a, b);
}

static bool _equalScalarArrayOpExpr(const ScalarArrayOpExpr* a, const ScalarArrayOpExpr* b)
{
    COMPARE_SCALAR_FIELD(opno);

    /*
     * Special-case opfuncid: it is allowable for it to differ if one node
     * contains zero and the other doesn't.  This just means that the one node
     * isn't as far along in the parse/plan pipeline and hasn't had the
     * opfuncid cache filled yet.
     */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0) {
        return false;
    }

    COMPARE_SCALAR_FIELD(useOr);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalBoolExpr(const BoolExpr* a, const BoolExpr* b)
{
    COMPARE_SCALAR_FIELD(boolop);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalSubLink(const SubLink* a, const SubLink* b)
{
    COMPARE_SCALAR_FIELD(subLinkType);
    COMPARE_NODE_FIELD(testexpr);
    COMPARE_NODE_FIELD(operName);
    COMPARE_NODE_FIELD(subselect);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalSubPlan(const SubPlan* a, const SubPlan* b)
{
    COMPARE_SCALAR_FIELD(subLinkType);
    COMPARE_NODE_FIELD(testexpr);
    COMPARE_NODE_FIELD(paramIds);
    COMPARE_SCALAR_FIELD(plan_id);
    COMPARE_STRING_FIELD(plan_name);
    COMPARE_SCALAR_FIELD(firstColType);
    COMPARE_SCALAR_FIELD(firstColTypmod);
    COMPARE_SCALAR_FIELD(firstColCollation);
    COMPARE_SCALAR_FIELD(useHashTable);
    COMPARE_SCALAR_FIELD(unknownEqFalse);
    COMPARE_NODE_FIELD(setParam);
    COMPARE_NODE_FIELD(parParam);
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(startup_cost);
    COMPARE_SCALAR_FIELD(per_call_cost);

    return true;
}

static bool _equalAlternativeSubPlan(const AlternativeSubPlan* a, const AlternativeSubPlan* b)
{
    COMPARE_NODE_FIELD(subplans);

    return true;
}

static bool _equalFieldSelect(const FieldSelect* a, const FieldSelect* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(fieldnum);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resulttypmod);
    COMPARE_SCALAR_FIELD(resultcollid);

    return true;
}

static bool _equalFieldStore(const FieldStore* a, const FieldStore* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(newvals);
    COMPARE_NODE_FIELD(fieldnums);
    COMPARE_SCALAR_FIELD(resulttype);

    return true;
}

static bool _equalRelabelType(const RelabelType* a, const RelabelType* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resulttypmod);
    COMPARE_SCALAR_FIELD(resultcollid);
    COMPARE_COERCIONFORM_FIELD(relabelformat);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCoerceViaIO(const CoerceViaIO* a, const CoerceViaIO* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resultcollid);
    COMPARE_COERCIONFORM_FIELD(coerceformat);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalArrayCoerceExpr(const ArrayCoerceExpr* a, const ArrayCoerceExpr* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(elemfuncid);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resulttypmod);
    COMPARE_SCALAR_FIELD(resultcollid);
    COMPARE_SCALAR_FIELD(isExplicit);
    COMPARE_COERCIONFORM_FIELD(coerceformat);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalConvertRowtypeExpr(const ConvertRowtypeExpr* a, const ConvertRowtypeExpr* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_COERCIONFORM_FIELD(convertformat);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCollateExpr(const CollateExpr* a, const CollateExpr* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(collOid);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCaseExpr(const CaseExpr* a, const CaseExpr* b)
{
    COMPARE_SCALAR_FIELD(casetype);
    COMPARE_SCALAR_FIELD(casecollid);
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(defresult);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCaseWhen(const CaseWhen* a, const CaseWhen* b)
{
    COMPARE_NODE_FIELD(expr);
    COMPARE_NODE_FIELD(result);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCaseTestExpr(const CaseTestExpr* a, const CaseTestExpr* b)
{
    COMPARE_SCALAR_FIELD(typeId);
    COMPARE_SCALAR_FIELD(typeMod);
    COMPARE_SCALAR_FIELD(collation);

    return true;
}

static bool _equalArrayExpr(const ArrayExpr* a, const ArrayExpr* b)
{
    COMPARE_SCALAR_FIELD(array_typeid);
    COMPARE_SCALAR_FIELD(array_collid);
    COMPARE_SCALAR_FIELD(element_typeid);
    COMPARE_NODE_FIELD(elements);
    COMPARE_SCALAR_FIELD(multidims);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalRowExpr(const RowExpr* a, const RowExpr* b)
{
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(row_typeid);
    COMPARE_COERCIONFORM_FIELD(row_format);
    COMPARE_NODE_FIELD(colnames);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalRowCompareExpr(const RowCompareExpr* a, const RowCompareExpr* b)
{
    COMPARE_SCALAR_FIELD(rctype);
    COMPARE_NODE_FIELD(opnos);
    COMPARE_NODE_FIELD(opfamilies);
    COMPARE_NODE_FIELD(inputcollids);
    COMPARE_NODE_FIELD(largs);
    COMPARE_NODE_FIELD(rargs);

    return true;
}

static bool _equalCoalesceExpr(const CoalesceExpr* a, const CoalesceExpr* b)
{
    COMPARE_SCALAR_FIELD(coalescetype);
    COMPARE_SCALAR_FIELD(coalescecollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);
    // modify NVL display to A db's style "NVL" instead of "COALESCE"
    COMPARE_SCALAR_FIELD(isnvl);

    return true;
}

static bool _equalMinMaxExpr(const MinMaxExpr* a, const MinMaxExpr* b)
{
    COMPARE_SCALAR_FIELD(minmaxtype);
    COMPARE_SCALAR_FIELD(minmaxcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_SCALAR_FIELD(op);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalXmlExpr(const XmlExpr* a, const XmlExpr* b)
{
    COMPARE_SCALAR_FIELD(op);
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(named_args);
    COMPARE_NODE_FIELD(arg_names);
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(xmloption);
    COMPARE_SCALAR_FIELD(type);
    COMPARE_SCALAR_FIELD(typmod);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalNullTest(const NullTest* a, const NullTest* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(nulltesttype);
    COMPARE_SCALAR_FIELD(argisrow);

    return true;
}

static bool _equalHashFilter(const HashFilter* a, const HashFilter* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(typeOids);
    COMPARE_NODE_FIELD(nodeList);

    return true;
}

static bool _equalBooleanTest(const BooleanTest* a, const BooleanTest* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(booltesttype);

    return true;
}

static bool _equalCoerceToDomain(const CoerceToDomain* a, const CoerceToDomain* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resulttypmod);
    COMPARE_SCALAR_FIELD(resultcollid);
    COMPARE_COERCIONFORM_FIELD(coercionformat);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCoerceToDomainValue(const CoerceToDomainValue* a, const CoerceToDomainValue* b)
{
    COMPARE_SCALAR_FIELD(typeId);
    COMPARE_SCALAR_FIELD(typeMod);
    COMPARE_SCALAR_FIELD(collation);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalSetToDefault(const SetToDefault* a, const SetToDefault* b)
{
    COMPARE_SCALAR_FIELD(typeId);
    COMPARE_SCALAR_FIELD(typeMod);
    COMPARE_SCALAR_FIELD(collation);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCurrentOfExpr(const CurrentOfExpr* a, const CurrentOfExpr* b)
{
    COMPARE_SCALAR_FIELD(cvarno);
    COMPARE_STRING_FIELD(cursor_name);
    COMPARE_SCALAR_FIELD(cursor_param);

    return true;
}

static bool _equalTargetEntry(const TargetEntry* a, const TargetEntry* b)
{
    COMPARE_NODE_FIELD(expr);
    COMPARE_SCALAR_FIELD(resno);
    COMPARE_STRING_FIELD(resname);
    COMPARE_SCALAR_FIELD(ressortgroupref);
    COMPARE_SCALAR_FIELD(resorigtbl);
    COMPARE_SCALAR_FIELD(resorigcol);
    COMPARE_SCALAR_FIELD(resjunk);

    return true;
}

static bool _equalPseudoTargetEntry(const PseudoTargetEntry * a, const PseudoTargetEntry * b)
{
    COMPARE_NODE_FIELD(tle);
    COMPARE_NODE_FIELD(srctle);

    return true;
}

static bool _equalRangeTblRef(const RangeTblRef* a, const RangeTblRef* b)
{
    COMPARE_SCALAR_FIELD(rtindex);

    return true;
}

static bool _equalJoinExpr(const JoinExpr* a, const JoinExpr* b)
{
    COMPARE_SCALAR_FIELD(jointype);
    COMPARE_SCALAR_FIELD(isNatural);
    COMPARE_NODE_FIELD(larg);
    COMPARE_NODE_FIELD(rarg);
    COMPARE_NODE_FIELD(usingClause);
    COMPARE_NODE_FIELD(quals);
    COMPARE_NODE_FIELD(alias);
    COMPARE_SCALAR_FIELD(rtindex);

    return true;
}

static bool _equalFromExpr(const FromExpr* a, const FromExpr* b)
{
    COMPARE_NODE_FIELD(fromlist);
    COMPARE_NODE_FIELD(quals);

    return true;
}

static bool _equalMergeAction(const MergeAction* a, const MergeAction* b)
{
    COMPARE_SCALAR_FIELD(matched);
    COMPARE_NODE_FIELD(qual);
    COMPARE_SCALAR_FIELD(commandType);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(pulluped_targetList);

    return true;
}

static bool _equalUpsertExpr(const UpsertExpr* a, const UpsertExpr* b)
{
    COMPARE_SCALAR_FIELD(upsertAction);
    COMPARE_NODE_FIELD(updateTlist);
    COMPARE_NODE_FIELD(exclRelTlist);
    COMPARE_SCALAR_FIELD(exclRelIndex);
    COMPARE_NODE_FIELD(upsertWhere);

    return true;
}
/*
 * Stuff from relation.h
 */
static bool _equalPathKey(const PathKey* a, const PathKey* b)
{
    /*
     * This is normally used on non-canonicalized PathKeys, so must chase up
     * to the topmost merged EquivalenceClass and see if those are the same
     * (by pointer equality).
     */
    EquivalenceClass* a_eclass = NULL;
    EquivalenceClass* b_eclass = NULL;

    a_eclass = a->pk_eclass;
    while (a_eclass->ec_merged)
        a_eclass = a_eclass->ec_merged;
    b_eclass = b->pk_eclass;
    while (b_eclass->ec_merged)
        b_eclass = b_eclass->ec_merged;
    if (a_eclass != b_eclass)
        return false;
    COMPARE_SCALAR_FIELD(pk_opfamily);
    COMPARE_SCALAR_FIELD(pk_strategy);
    COMPARE_SCALAR_FIELD(pk_nulls_first);

    return true;
}

static bool _equalRestrictInfo(const RestrictInfo* a, const RestrictInfo* b)
{
    COMPARE_NODE_FIELD(clause);
    COMPARE_SCALAR_FIELD(is_pushed_down);
    COMPARE_SCALAR_FIELD(outerjoin_delayed);
    COMPARE_SCALAR_FIELD(security_level);
    COMPARE_BITMAPSET_FIELD(required_relids);
    COMPARE_BITMAPSET_FIELD(outer_relids);
    COMPARE_BITMAPSET_FIELD(nullable_relids);

    /*
     * We ignore all the remaining fields, since they may not be set yet, and
     * should be derivable from the clause anyway.
     */
    return true;
}

static bool _equalPlaceHolderVar(const PlaceHolderVar* a, const PlaceHolderVar* b)
{
    /*
     * We intentionally do not compare phexpr.	Two PlaceHolderVars with the
     * same ID and levelsup should be considered equal even if the contained
     * expressions have managed to mutate to different states.	One way in
     * which that can happen is that initplan sublinks would get replaced by
     * differently-numbered Params when sublink folding is done.  (The end
     * result of such a situation would be some unreferenced initplans, which
     * is annoying but not really a problem.)
     *
     * COMPARE_NODE_FIELD(phexpr);
     */
    COMPARE_BITMAPSET_FIELD(phrels);
    COMPARE_SCALAR_FIELD(phid);
    COMPARE_SCALAR_FIELD(phlevelsup);

    return true;
}

static bool _equalSpecialJoinInfo(const SpecialJoinInfo* a, const SpecialJoinInfo* b)
{
    COMPARE_BITMAPSET_FIELD(min_lefthand);
    COMPARE_BITMAPSET_FIELD(min_righthand);
    COMPARE_BITMAPSET_FIELD(syn_lefthand);
    COMPARE_BITMAPSET_FIELD(syn_righthand);
    COMPARE_SCALAR_FIELD(jointype);
    COMPARE_SCALAR_FIELD(lhs_strict);
    COMPARE_SCALAR_FIELD(delay_upper_joins);
    COMPARE_NODE_FIELD(join_quals);

    return true;
}

static bool
_equalLateralJoinInfo(const LateralJoinInfo *a, const LateralJoinInfo *b)
{
   COMPARE_SCALAR_FIELD(lateral_rhs);
   COMPARE_BITMAPSET_FIELD(lateral_lhs);

   return true;
}

static bool _equalAppendRelInfo(const AppendRelInfo* a, const AppendRelInfo* b)
{
    COMPARE_SCALAR_FIELD(parent_relid);
    COMPARE_SCALAR_FIELD(child_relid);
    COMPARE_SCALAR_FIELD(parent_reltype);
    COMPARE_SCALAR_FIELD(child_reltype);
    COMPARE_NODE_FIELD(translated_vars);
    COMPARE_SCALAR_FIELD(parent_reloid);

    return true;
}

static bool _equalPlaceHolderInfo(const PlaceHolderInfo* a, const PlaceHolderInfo* b)
{
    COMPARE_SCALAR_FIELD(phid);
    COMPARE_NODE_FIELD(ph_var);
    COMPARE_BITMAPSET_FIELD(ph_eval_at);
    COMPARE_BITMAPSET_FIELD(ph_needed);
    COMPARE_SCALAR_FIELD(ph_width);

    return true;
}

/*
 * Stuff from parsenodes.h
 */
static bool _equalQuery(const Query* a, const Query* b)
{
    COMPARE_SCALAR_FIELD(commandType);
    COMPARE_SCALAR_FIELD(querySource);
    /* we intentionally ignore queryId, since it might not be set */
    COMPARE_SCALAR_FIELD(canSetTag);
    COMPARE_NODE_FIELD(utilityStmt);
    COMPARE_SCALAR_FIELD(resultRelation);
    COMPARE_SCALAR_FIELD(hasAggs);
    COMPARE_SCALAR_FIELD(hasWindowFuncs);
    COMPARE_SCALAR_FIELD(hasSubLinks);
    COMPARE_SCALAR_FIELD(hasDistinctOn);
    COMPARE_SCALAR_FIELD(hasRecursive);
    COMPARE_SCALAR_FIELD(hasModifyingCTE);
    COMPARE_SCALAR_FIELD(hasForUpdate);
    COMPARE_SCALAR_FIELD(hasRowSecurity);
    COMPARE_SCALAR_FIELD(hasSynonyms);
    COMPARE_NODE_FIELD(cteList);
    COMPARE_NODE_FIELD(rtable);
    COMPARE_NODE_FIELD(jointree);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(groupClause);
    COMPARE_NODE_FIELD(groupingSets);
    COMPARE_NODE_FIELD(havingQual);
    COMPARE_NODE_FIELD(windowClause);
    COMPARE_NODE_FIELD(distinctClause);
    COMPARE_NODE_FIELD(sortClause);
    COMPARE_NODE_FIELD(limitOffset);
    COMPARE_NODE_FIELD(limitCount);
    COMPARE_NODE_FIELD(rowMarks);
    COMPARE_NODE_FIELD(setOperations);
    COMPARE_NODE_FIELD(constraintDeps);

#ifdef PGXC
    COMPARE_STRING_FIELD(sql_statement);
    COMPARE_SCALAR_FIELD(is_local);
    COMPARE_SCALAR_FIELD(has_to_save_cmd_id);
    COMPARE_SCALAR_FIELD(vec_output);
    COMPARE_SCALAR_FIELD(tdTruncCastStatus);
    COMPARE_NODE_FIELD(equalVars);
#endif
    COMPARE_SCALAR_FIELD(mergeTarget_relation);
    COMPARE_NODE_FIELD(mergeSourceTargetList);
    COMPARE_NODE_FIELD(mergeActionList);
    COMPARE_NODE_FIELD(upsertQuery);
    COMPARE_NODE_FIELD(upsertClause);
    COMPARE_SCALAR_FIELD(isRowTriggerShippable);
    COMPARE_SCALAR_FIELD(use_star_targets);
    COMPARE_SCALAR_FIELD(is_from_full_join_rewrite);
    COMPARE_SCALAR_FIELD(can_push);
    COMPARE_SCALAR_FIELD(unique_check);

    return true;
}

static bool _equalInsertStmt(const InsertStmt* a, const InsertStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(cols);
    COMPARE_NODE_FIELD(selectStmt);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(withClause);
    COMPARE_NODE_FIELD(upsertClause);

    return true;
}

static bool _equalDeleteStmt(const DeleteStmt* a, const DeleteStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(usingClause);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(withClause);
    COMPARE_NODE_FIELD(limitClause);

    return true;
}

static bool _equalUpdateStmt(const UpdateStmt* a, const UpdateStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(fromClause);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(withClause);

    return true;
}

static bool _equalMergeStmt(const MergeStmt* a, const MergeStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(source_relation);
    COMPARE_NODE_FIELD(join_condition);
    COMPARE_NODE_FIELD(mergeWhenClauses);
    COMPARE_NODE_FIELD(insert_stmt);
    COMPARE_SCALAR_FIELD(is_insert_update);

    return true;
}

static bool _equalMergeWhenClause(const MergeWhenClause* a, const MergeWhenClause* b)
{
    COMPARE_SCALAR_FIELD(matched);
    COMPARE_SCALAR_FIELD(commandType);
    COMPARE_NODE_FIELD(condition);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(cols);
    COMPARE_NODE_FIELD(values);

    return true;
}

static bool _equalSelectStmt(const SelectStmt* a, const SelectStmt* b)
{
    COMPARE_NODE_FIELD(distinctClause);
    COMPARE_NODE_FIELD(intoClause);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(fromClause);
    COMPARE_NODE_FIELD(startWithClause);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(groupClause);
    COMPARE_NODE_FIELD(havingClause);
    COMPARE_NODE_FIELD(windowClause);
    COMPARE_NODE_FIELD(withClause);
    COMPARE_NODE_FIELD(valuesLists);
    COMPARE_NODE_FIELD(sortClause);
    COMPARE_NODE_FIELD(limitOffset);
    COMPARE_NODE_FIELD(limitCount);
    COMPARE_NODE_FIELD(lockingClause);
    COMPARE_SCALAR_FIELD(op);
    COMPARE_SCALAR_FIELD(all);
    COMPARE_NODE_FIELD(larg);
    COMPARE_NODE_FIELD(rarg);
    COMPARE_SCALAR_FIELD(hasPlus);

    return true;
}

static bool _equalSetOperationStmt(const SetOperationStmt* a, const SetOperationStmt* b)
{
    COMPARE_SCALAR_FIELD(op);
    COMPARE_SCALAR_FIELD(all);
    COMPARE_NODE_FIELD(larg);
    COMPARE_NODE_FIELD(rarg);
    COMPARE_NODE_FIELD(colTypes);
    COMPARE_NODE_FIELD(colTypmods);
    COMPARE_NODE_FIELD(colCollations);
    COMPARE_NODE_FIELD(groupClauses);

    return true;
}

static bool _equalAlterTableStmt(const AlterTableStmt* a, const AlterTableStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(cmds);
    COMPARE_SCALAR_FIELD(relkind);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalAlterTableCmd(const AlterTableCmd* a, const AlterTableCmd* b)
{
    COMPARE_SCALAR_FIELD(subtype);
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(def);
    COMPARE_SCALAR_FIELD(behavior);
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_NODE_FIELD(exchange_with_rel);
    COMPARE_SCALAR_FIELD(check_validation);
    COMPARE_SCALAR_FIELD(exchange_verbose);
    COMPARE_STRING_FIELD(target_partition_tablespace);
    COMPARE_NODE_FIELD(bucket_list);
    COMPARE_SCALAR_FIELD(alterGPI);

    return true;
}

static bool _equalAlterDomainStmt(const AlterDomainStmt* a, const AlterDomainStmt* b)
{
    COMPARE_SCALAR_FIELD(subtype);
    COMPARE_NODE_FIELD(typname);
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(def);
    COMPARE_SCALAR_FIELD(behavior);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalGrantStmt(const GrantStmt* a, const GrantStmt* b)
{
    COMPARE_SCALAR_FIELD(is_grant);
    COMPARE_SCALAR_FIELD(targtype);
    COMPARE_SCALAR_FIELD(objtype);
    COMPARE_NODE_FIELD(objects);
    COMPARE_NODE_FIELD(privileges);
    COMPARE_NODE_FIELD(grantees);
    COMPARE_SCALAR_FIELD(grant_option);
    COMPARE_SCALAR_FIELD(behavior);

    return true;
}

static bool _equalPrivGrantee(const PrivGrantee* a, const PrivGrantee* b)
{
    COMPARE_STRING_FIELD(rolname);

    return true;
}

static bool _equalFuncWithArgs(const FuncWithArgs* a, const FuncWithArgs* b)
{
    COMPARE_NODE_FIELD(funcname);
    COMPARE_NODE_FIELD(funcargs);

    return true;
}

static bool _equalAccessPriv(const AccessPriv* a, const AccessPriv* b)
{
    COMPARE_STRING_FIELD(priv_name);
    COMPARE_NODE_FIELD(cols);

    return true;
}

static bool _equalGrantRoleStmt(const GrantRoleStmt* a, const GrantRoleStmt* b)
{
    COMPARE_NODE_FIELD(granted_roles);
    COMPARE_NODE_FIELD(grantee_roles);
    COMPARE_SCALAR_FIELD(is_grant);
    COMPARE_SCALAR_FIELD(admin_opt);
    COMPARE_STRING_FIELD(grantor);
    COMPARE_SCALAR_FIELD(behavior);

    return true;
}

static bool _equalDbPriv(const DbPriv* a, const DbPriv* b)
{
    COMPARE_STRING_FIELD(db_priv_name);

    return true;
}

static bool _equalGrantDbStmt(const GrantDbStmt* a, const GrantDbStmt* b)
{
    COMPARE_SCALAR_FIELD(is_grant);
    COMPARE_NODE_FIELD(privileges);
    COMPARE_NODE_FIELD(grantees);
    COMPARE_SCALAR_FIELD(admin_opt);

    return true;
}

static bool _equalAlterDefaultPrivilegesStmt(const AlterDefaultPrivilegesStmt* a, const AlterDefaultPrivilegesStmt* b)
{
    COMPARE_NODE_FIELD(options);
    COMPARE_NODE_FIELD(action);

    return true;
}

static bool _equalDeclareCursorStmt(const DeclareCursorStmt* a, const DeclareCursorStmt* b)
{
    COMPARE_STRING_FIELD(portalname);
    COMPARE_SCALAR_FIELD(options);
    COMPARE_NODE_FIELD(query);

    return true;
}

static bool _equalClosePortalStmt(const ClosePortalStmt* a, const ClosePortalStmt* b)
{
    COMPARE_STRING_FIELD(portalname);

    return true;
}

static bool _equalClusterStmt(const ClusterStmt* a, const ClusterStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_STRING_FIELD(indexname);
    COMPARE_SCALAR_FIELD(verbose);

    return true;
}

static bool _equalCopyStmt(const CopyStmt* a, const CopyStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(query);
    COMPARE_NODE_FIELD(attlist);
    COMPARE_SCALAR_FIELD(is_from);
    COMPARE_STRING_FIELD(filename);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalCreateStmt(const CreateStmt* a, const CreateStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(tableElts);
    COMPARE_NODE_FIELD(inhRelations);
    COMPARE_NODE_FIELD(ofTypename);
    COMPARE_NODE_FIELD(constraints);
    COMPARE_NODE_FIELD(options);
    COMPARE_NODE_FIELD(clusterKeys);
    COMPARE_SCALAR_FIELD(oncommit);
    COMPARE_STRING_FIELD(tablespacename);
    COMPARE_SCALAR_FIELD(if_not_exists);
    COMPARE_SCALAR_FIELD(ivm);
    COMPARE_NODE_FIELD(partTableState);
#ifdef PGXC
    COMPARE_NODE_FIELD(distributeby);
    COMPARE_NODE_FIELD(subcluster);
#endif
    COMPARE_STRING_FIELD(internalData);
    COMPARE_NODE_FIELD(uuids);
    COMPARE_SCALAR_FIELD(relkind);

    return true;
}

static bool _equalRangePartitionDefState(const RangePartitionDefState* a, const RangePartitionDefState* b)
{
    COMPARE_STRING_FIELD(partitionName);
    COMPARE_NODE_FIELD(boundary);
    COMPARE_STRING_FIELD(tablespacename);
    COMPARE_SCALAR_FIELD(curStartVal);
    COMPARE_STRING_FIELD(partitionInitName);

    return true;
}

static bool _equalListPartitionDefState(const ListPartitionDefState* a, const ListPartitionDefState* b)
{
    COMPARE_STRING_FIELD(partitionName);
    COMPARE_NODE_FIELD(boundary);
    COMPARE_STRING_FIELD(tablespacename);

    return true;
}

static bool _equalHashPartitionDefState(const HashPartitionDefState* a, const HashPartitionDefState* b)
{
    COMPARE_STRING_FIELD(partitionName);
    COMPARE_NODE_FIELD(boundary);
    COMPARE_STRING_FIELD(tablespacename);

    return true;
}

static bool _equalRangePartitionStartEndDefState(
    const RangePartitionStartEndDefState* a, const RangePartitionStartEndDefState* b)
{
    COMPARE_STRING_FIELD(partitionName);
    COMPARE_NODE_FIELD(startValue);
    COMPARE_NODE_FIELD(endValue);
    COMPARE_NODE_FIELD(everyValue);
    COMPARE_STRING_FIELD(tableSpaceName);

    return true;
}

static bool _equalAddPartitionState(const AddPartitionState* a, const AddPartitionState* b)
{
    COMPARE_NODE_FIELD(partitionList);
    COMPARE_SCALAR_FIELD(isStartEnd);

    return true;
}

static bool _equalAddSubPartitionState(const AddSubPartitionState* a, const AddSubPartitionState* b)
{
    COMPARE_STRING_FIELD(partitionName);
    COMPARE_NODE_FIELD(subPartitionList);

    return true;
}

static bool _equalIntervalPartitionDefState(const IntervalPartitionDefState* a, const IntervalPartitionDefState* b)
{
    COMPARE_SCALAR_FIELD(partInterval);
    COMPARE_NODE_FIELD(intervalTablespaces);

    return true;
}

static bool _equalRangePartitionindexDefState(
    const RangePartitionindexDefState* a, const RangePartitionindexDefState* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_STRING_FIELD(tablespace);
    COMPARE_NODE_FIELD(sublist);

    return true;
}

static bool _equalPartitionState(const PartitionState* a, const PartitionState* b)
{
    COMPARE_SCALAR_FIELD(partitionStrategy);
    COMPARE_NODE_FIELD(intervalPartDef);
    COMPARE_NODE_FIELD(partitionKey);
    COMPARE_NODE_FIELD(partitionList);
    COMPARE_SCALAR_FIELD(rowMovement);
    COMPARE_NODE_FIELD(subPartitionState);
    COMPARE_NODE_FIELD(partitionNameList);
    return true;
}

static bool _equalSplitInfo(const SplitInfo* a, const SplitInfo* b)
{
    COMPARE_SCALAR_FIELD(filePath);
    COMPARE_SCALAR_FIELD(fileName);
    COMPARE_NODE_FIELD(partContentList);
    COMPARE_SCALAR_FIELD(ObjectSize);
    COMPARE_SCALAR_FIELD(eTag);
    COMPARE_SCALAR_FIELD(prefixSlashNum);
    return true;
}

static bool _equalDistFdwFileSegment(const DistFdwFileSegment* a, const DistFdwFileSegment* b)
{
    COMPARE_STRING_FIELD(filename);
    COMPARE_SCALAR_FIELD(begin);
    COMPARE_SCALAR_FIELD(end);
    return true;
}

static bool _equalDistFdwDataNodeTask(const DistFdwDataNodeTask* a, const DistFdwDataNodeTask* b)
{
    COMPARE_STRING_FIELD(dnName);
    COMPARE_NODE_FIELD(task);
    return true;
}

static bool _equalSplitPartitionState(const SplitPartitionState* a, const SplitPartitionState* b)
{
    COMPARE_SCALAR_FIELD(splitType);
    COMPARE_SCALAR_FIELD(src_partition_name);
    COMPARE_NODE_FIELD(partition_for_values);
    COMPARE_NODE_FIELD(split_point);
    COMPARE_NODE_FIELD(dest_partition_define_list);
    COMPARE_NODE_FIELD(newListSubPartitionBoundry);
    return true;
}

static bool _equalTableLikeClause(const TableLikeClause* a, const TableLikeClause* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_SCALAR_FIELD(options);

    return true;
}

static bool _equalDefineStmt(const DefineStmt* a, const DefineStmt* b)
{
    COMPARE_SCALAR_FIELD(kind);
    COMPARE_SCALAR_FIELD(oldstyle);
    COMPARE_NODE_FIELD(defnames);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(definition);

    return true;
}

static bool _equalDropStmt(const DropStmt* a, const DropStmt* b)
{
    COMPARE_NODE_FIELD(objects);
    COMPARE_NODE_FIELD(arguments);
    COMPARE_SCALAR_FIELD(removeType);
    COMPARE_SCALAR_FIELD(behavior);
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_SCALAR_FIELD(concurrent);

    return true;
}

static bool _equalTruncateStmt(const TruncateStmt* a, const TruncateStmt* b)
{
    COMPARE_NODE_FIELD(relations);
    COMPARE_SCALAR_FIELD(restart_seqs);
    COMPARE_SCALAR_FIELD(behavior);
    COMPARE_SCALAR_FIELD(purge);

    return true;
}

static bool EqualPurgeStmt(const PurgeStmt* a, const PurgeStmt* b)
{
    COMPARE_SCALAR_FIELD(purtype);
    COMPARE_NODE_FIELD(purobj);

    return true;
}

static bool EqualTimeCapsuleStmt(const TimeCapsuleStmt* a, const TimeCapsuleStmt* b)
{
    COMPARE_SCALAR_FIELD(tcaptype);
    COMPARE_NODE_FIELD(relation);
    COMPARE_STRING_FIELD(new_relname);

    COMPARE_NODE_FIELD(tvver);
    COMPARE_SCALAR_FIELD(tvtype);

    return true;
}

static bool _equalCommentStmt(const CommentStmt* a, const CommentStmt* b)
{
    COMPARE_SCALAR_FIELD(objtype);
    COMPARE_NODE_FIELD(objname);
    COMPARE_NODE_FIELD(objargs);
    COMPARE_STRING_FIELD(comment);

    return true;
}

static bool _equalSecLabelStmt(const SecLabelStmt* a, const SecLabelStmt* b)
{
    COMPARE_SCALAR_FIELD(objtype);
    COMPARE_NODE_FIELD(objname);
    COMPARE_NODE_FIELD(objargs);
    COMPARE_STRING_FIELD(provider);
    COMPARE_STRING_FIELD(label);

    return true;
}

static bool _equalFetchStmt(const FetchStmt* a, const FetchStmt* b)
{
    COMPARE_SCALAR_FIELD(direction);
    COMPARE_SCALAR_FIELD(howMany);
    COMPARE_STRING_FIELD(portalname);
    COMPARE_SCALAR_FIELD(ismove);

    return true;
}

static bool _equalIndexStmt(const IndexStmt* a, const IndexStmt* b)
{
    COMPARE_STRING_FIELD(schemaname);
    COMPARE_STRING_FIELD(idxname);
    COMPARE_NODE_FIELD(relation);
    COMPARE_STRING_FIELD(accessMethod);
    COMPARE_STRING_FIELD(tableSpace);
    COMPARE_NODE_FIELD(indexParams);
    COMPARE_NODE_FIELD(indexIncludingParams);
    COMPARE_SCALAR_FIELD(isGlobal);
    COMPARE_NODE_FIELD(options);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(excludeOpNames);
    COMPARE_STRING_FIELD(idxcomment);
    COMPARE_SCALAR_FIELD(indexOid);
    COMPARE_SCALAR_FIELD(oldNode);
    COMPARE_NODE_FIELD(partClause);
    COMPARE_SCALAR_FIELD(isPartitioned);
    COMPARE_SCALAR_FIELD(unique);
    COMPARE_SCALAR_FIELD(primary);
    COMPARE_SCALAR_FIELD(isconstraint);
    COMPARE_SCALAR_FIELD(deferrable);
    COMPARE_SCALAR_FIELD(initdeferred);
    COMPARE_SCALAR_FIELD(concurrent);

    return true;
}

static bool _equalCreateFunctionStmt(const CreateFunctionStmt* a, const CreateFunctionStmt* b)
{
    COMPARE_SCALAR_FIELD(replace);
    COMPARE_NODE_FIELD(funcname);
    COMPARE_NODE_FIELD(parameters);
    COMPARE_NODE_FIELD(returnType);
    COMPARE_NODE_FIELD(options);
    COMPARE_NODE_FIELD(withClause);

    return true;
}

static bool _equalFunctionParameter(const FunctionParameter* a, const FunctionParameter* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(argType);
    COMPARE_SCALAR_FIELD(mode);
    COMPARE_NODE_FIELD(defexpr);

    return true;
}

static bool _equalAlterFunctionStmt(const AlterFunctionStmt* a, const AlterFunctionStmt* b)
{
    COMPARE_NODE_FIELD(func);
    COMPARE_NODE_FIELD(actions);

    return true;
}

static bool _equalDoStmt(const DoStmt* a, const DoStmt* b)
{
    COMPARE_NODE_FIELD(args);

    return true;
}

static bool _equalRenameStmt(const RenameStmt* a, const RenameStmt* b)
{
    COMPARE_SCALAR_FIELD(renameType);
    COMPARE_SCALAR_FIELD(relationType);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(object);
    COMPARE_NODE_FIELD(objarg);
    COMPARE_STRING_FIELD(subname);
    COMPARE_STRING_FIELD(newname);
    COMPARE_SCALAR_FIELD(behavior);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalAlterObjectSchemaStmt(const AlterObjectSchemaStmt* a, const AlterObjectSchemaStmt* b)
{
    COMPARE_SCALAR_FIELD(objectType);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(object);
    COMPARE_NODE_FIELD(objarg);
    COMPARE_STRING_FIELD(addname);
    COMPARE_STRING_FIELD(newschema);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalAlterOwnerStmt(const AlterOwnerStmt* a, const AlterOwnerStmt* b)
{
    COMPARE_SCALAR_FIELD(objectType);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(object);
    COMPARE_NODE_FIELD(objarg);
    COMPARE_STRING_FIELD(addname);
    COMPARE_STRING_FIELD(newowner);

    return true;
}

static bool _equalRuleStmt(const RuleStmt* a, const RuleStmt* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_STRING_FIELD(rulename);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_SCALAR_FIELD(event);
    COMPARE_SCALAR_FIELD(instead);
    COMPARE_NODE_FIELD(actions);
    COMPARE_SCALAR_FIELD(replace);

    return true;
}

static bool _equalNotifyStmt(const NotifyStmt* a, const NotifyStmt* b)
{
    COMPARE_STRING_FIELD(conditionname);
    COMPARE_STRING_FIELD(payload);

    return true;
}

static bool _equalListenStmt(const ListenStmt* a, const ListenStmt* b)
{
    COMPARE_STRING_FIELD(conditionname);

    return true;
}

static bool _equalUnlistenStmt(const UnlistenStmt* a, const UnlistenStmt* b)
{
    COMPARE_STRING_FIELD(conditionname);

    return true;
}

static bool _equalTransactionStmt(const TransactionStmt* a, const TransactionStmt* b)
{
    COMPARE_SCALAR_FIELD(kind);
    COMPARE_NODE_FIELD(options);
    COMPARE_STRING_FIELD(gid);

    return true;
}

static bool _equalCompositeTypeStmt(const CompositeTypeStmt* a, const CompositeTypeStmt* b)
{
    COMPARE_NODE_FIELD(typevar);
    COMPARE_NODE_FIELD(coldeflist);

    return true;
}

static bool _equalTableOfTypeStmt(const TableOfTypeStmt* a, const TableOfTypeStmt* b)
{
    COMPARE_NODE_FIELD(typname);
    COMPARE_NODE_FIELD(reftypname);

    return true;
}

static bool _equalCreateEnumStmt(const CreateEnumStmt* a, const CreateEnumStmt* b)
{
    COMPARE_NODE_FIELD(typname);
    COMPARE_NODE_FIELD(vals);

    return true;
}

static bool _equalCreateRangeStmt(const CreateRangeStmt* a, const CreateRangeStmt* b)
{
    COMPARE_NODE_FIELD(typname);
    COMPARE_NODE_FIELD(params);

    return true;
}

static bool _equalAlterEnumStmt(const AlterEnumStmt* a, const AlterEnumStmt* b)
{
    COMPARE_NODE_FIELD(typname);
    COMPARE_STRING_FIELD(oldVal);
    COMPARE_STRING_FIELD(newVal);
    COMPARE_STRING_FIELD(newValNeighbor);
    COMPARE_SCALAR_FIELD(newValIsAfter);
    COMPARE_SCALAR_FIELD(skipIfNewValExists);

    return true;
}

static bool _equalViewStmt(const ViewStmt* a, const ViewStmt* b)
{
    COMPARE_NODE_FIELD(view);
    COMPARE_NODE_FIELD(aliases);
    COMPARE_NODE_FIELD(query);
    COMPARE_SCALAR_FIELD(replace);
    COMPARE_SCALAR_FIELD(ivm);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(relkind);

    return true;
}

static bool _equalLoadStmt(const LoadStmt* a, const LoadStmt* b)
{
    COMPARE_STRING_FIELD(filename);

    COMPARE_NODE_FIELD(pre_load_options);
    COMPARE_SCALAR_FIELD(is_load_data);
    COMPARE_SCALAR_FIELD(is_only_special_filed);
    COMPARE_NODE_FIELD(load_options);
    COMPARE_SCALAR_FIELD(load_type);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(rel_options);

    return true;
}

static bool _equalCreateDomainStmt(const CreateDomainStmt* a, const CreateDomainStmt* b)
{
    COMPARE_NODE_FIELD(domainname);
    COMPARE_NODE_FIELD(typname);
    COMPARE_NODE_FIELD(collClause);
    COMPARE_NODE_FIELD(constraints);

    return true;
}

static bool _equalCreateOpClassStmt(const CreateOpClassStmt* a, const CreateOpClassStmt* b)
{
    COMPARE_NODE_FIELD(opclassname);
    COMPARE_NODE_FIELD(opfamilyname);
    COMPARE_STRING_FIELD(amname);
    COMPARE_NODE_FIELD(datatype);
    COMPARE_NODE_FIELD(items);
    COMPARE_SCALAR_FIELD(isDefault);

    return true;
}

static bool _equalCreateOpClassItem(const CreateOpClassItem* a, const CreateOpClassItem* b)
{
    COMPARE_SCALAR_FIELD(itemtype);
    COMPARE_NODE_FIELD(name);
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(number);
    COMPARE_NODE_FIELD(order_family);
    COMPARE_NODE_FIELD(class_args);
    COMPARE_NODE_FIELD(storedtype);

    return true;
}

static bool _equalCreateOpFamilyStmt(const CreateOpFamilyStmt* a, const CreateOpFamilyStmt* b)
{
    COMPARE_NODE_FIELD(opfamilyname);
    COMPARE_STRING_FIELD(amname);

    return true;
}

static bool _equalAlterOpFamilyStmt(const AlterOpFamilyStmt* a, const AlterOpFamilyStmt* b)
{
    COMPARE_NODE_FIELD(opfamilyname);
    COMPARE_STRING_FIELD(amname);
    COMPARE_SCALAR_FIELD(isDrop);
    COMPARE_NODE_FIELD(items);

    return true;
}

static bool _equalCreatedbStmt(const CreatedbStmt* a, const CreatedbStmt* b)
{
    COMPARE_STRING_FIELD(dbname);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterDatabaseStmt(const AlterDatabaseStmt* a, const AlterDatabaseStmt* b)
{
    COMPARE_STRING_FIELD(dbname);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterDatabaseSetStmt(const AlterDatabaseSetStmt* a, const AlterDatabaseSetStmt* b)
{
    COMPARE_STRING_FIELD(dbname);
    COMPARE_NODE_FIELD(setstmt);

    return true;
}

static bool _equalDropdbStmt(const DropdbStmt* a, const DropdbStmt* b)
{
    COMPARE_STRING_FIELD(dbname);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalVacuumStmt(const VacuumStmt* a, const VacuumStmt* b)
{
    COMPARE_SCALAR_FIELD(options);
    COMPARE_SCALAR_FIELD(flags);
    COMPARE_SCALAR_FIELD(rely_oid);
    COMPARE_SCALAR_FIELD(freeze_min_age);
    COMPARE_SCALAR_FIELD(freeze_table_age);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(va_cols);
    COMPARE_NODE_FIELD(onepartrel);
    COMPARE_NODE_FIELD(onepart);
    COMPARE_NODE_FIELD(partList);

    return true;
}

static bool _equalExplainStmt(const ExplainStmt* a, const ExplainStmt* b)
{
    COMPARE_NODE_FIELD(statement);
    COMPARE_NODE_FIELD(query);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalCreateTableAsStmt(const CreateTableAsStmt* a, const CreateTableAsStmt* b)
{
    COMPARE_NODE_FIELD(query);
    COMPARE_NODE_FIELD(into);
    COMPARE_SCALAR_FIELD(relkind);
    COMPARE_SCALAR_FIELD(is_select_into);

    return true;
}

static bool _equalRefreshMatViewStmt(const RefreshMatViewStmt *a, const RefreshMatViewStmt *b)
{
   COMPARE_SCALAR_FIELD(skipData);
   COMPARE_SCALAR_FIELD(incremental);
   COMPARE_NODE_FIELD(relation);

   return true;
}

static bool _equalReplicaIdentityStmt(const ReplicaIdentityStmt* a, const ReplicaIdentityStmt* b)
{
    COMPARE_SCALAR_FIELD(identity_type);
    COMPARE_STRING_FIELD(name);

    return true;
}

#ifndef ENABLE_MULTIPLE_NODES
static bool _equalAlterSystemStmt(const AlterSystemStmt* a, const AlterSystemStmt* b)
{
    COMPARE_NODE_FIELD(setstmt);
    return true;
}
#endif

static bool _equalCreateSeqStmt(const CreateSeqStmt* a, const CreateSeqStmt* b)
{
    COMPARE_NODE_FIELD(sequence);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(ownerId);
#ifdef PGXC
    COMPARE_SCALAR_FIELD(is_serial);
#endif
    COMPARE_SCALAR_FIELD(uuid);
    COMPARE_SCALAR_FIELD(canCreateTempSeq);
    COMPARE_SCALAR_FIELD(is_large);

    return true;
}

static bool _equalAlterSeqStmt(const AlterSeqStmt* a, const AlterSeqStmt* b)
{
    COMPARE_NODE_FIELD(sequence);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_SCALAR_FIELD(is_large);

    return true;
}

static bool _equalVariableSetStmt(const VariableSetStmt* a, const VariableSetStmt* b)
{
    COMPARE_SCALAR_FIELD(kind);
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(is_local);

    return true;
}

static bool _equalVariableShowStmt(const VariableShowStmt* a, const VariableShowStmt* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_STRING_FIELD(likename);

    return true;
}

static bool _equalDiscardStmt(const DiscardStmt* a, const DiscardStmt* b)
{
    COMPARE_SCALAR_FIELD(target);

    return true;
}

static bool _equalCreateTableSpaceStmt(const CreateTableSpaceStmt* a, const CreateTableSpaceStmt* b)
{
    COMPARE_STRING_FIELD(tablespacename);
    COMPARE_STRING_FIELD(owner);
    COMPARE_STRING_FIELD(location);
    COMPARE_STRING_FIELD(maxsize);
    COMPARE_SCALAR_FIELD(relative);

    return true;
}

static bool _equalDropTableSpaceStmt(const DropTableSpaceStmt* a, const DropTableSpaceStmt* b)
{
    COMPARE_STRING_FIELD(tablespacename);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalAlterTableSpaceOptionsStmt(const AlterTableSpaceOptionsStmt* a, const AlterTableSpaceOptionsStmt* b)
{
    COMPARE_STRING_FIELD(tablespacename);
    COMPARE_STRING_FIELD(maxsize);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(isReset);

    return true;
}

static bool _equalCreateExtensionStmt(const CreateExtensionStmt* a, const CreateExtensionStmt* b)
{
    COMPARE_STRING_FIELD(extname);
    COMPARE_SCALAR_FIELD(if_not_exists);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterExtensionStmt(const AlterExtensionStmt* a, const AlterExtensionStmt* b)
{
    COMPARE_STRING_FIELD(extname);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterExtensionContentsStmt(const AlterExtensionContentsStmt* a, const AlterExtensionContentsStmt* b)
{
    COMPARE_STRING_FIELD(extname);
    COMPARE_SCALAR_FIELD(action);
    COMPARE_SCALAR_FIELD(objtype);
    COMPARE_NODE_FIELD(objname);
    COMPARE_NODE_FIELD(objargs);

    return true;
}

static bool _equalCreateFdwStmt(const CreateFdwStmt* a, const CreateFdwStmt* b)
{
    COMPARE_STRING_FIELD(fdwname);
    COMPARE_NODE_FIELD(func_options);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterFdwStmt(const AlterFdwStmt* a, const AlterFdwStmt* b)
{
    COMPARE_STRING_FIELD(fdwname);
    COMPARE_NODE_FIELD(func_options);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalCreateForeignServerStmt(const CreateForeignServerStmt* a, const CreateForeignServerStmt* b)
{
    COMPARE_STRING_FIELD(servername);
    COMPARE_STRING_FIELD(servertype);
    COMPARE_STRING_FIELD(version);
    COMPARE_STRING_FIELD(fdwname);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterForeignServerStmt(const AlterForeignServerStmt* a, const AlterForeignServerStmt* b)
{
    COMPARE_STRING_FIELD(servername);
    COMPARE_STRING_FIELD(version);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(has_version);

    return true;
}

static bool _equalCreateUserMappingStmt(const CreateUserMappingStmt* a, const CreateUserMappingStmt* b)
{
    COMPARE_STRING_FIELD(username);
    COMPARE_STRING_FIELD(servername);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterUserMappingStmt(const AlterUserMappingStmt* a, const AlterUserMappingStmt* b)
{
    COMPARE_STRING_FIELD(username);
    COMPARE_STRING_FIELD(servername);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalDropUserMappingStmt(const DropUserMappingStmt* a, const DropUserMappingStmt* b)
{
    COMPARE_STRING_FIELD(username);
    COMPARE_STRING_FIELD(servername);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalCreateForeignTableStmt(const CreateForeignTableStmt* a, const CreateForeignTableStmt* b)
{
    if (!_equalCreateStmt(&a->base, &b->base))
        return false;

    COMPARE_STRING_FIELD(servername);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(write_only);

    return true;
}

static bool _equalCreateDataSourceStmt(const CreateDataSourceStmt* a, const CreateDataSourceStmt* b)
{
    COMPARE_STRING_FIELD(srcname);
    COMPARE_STRING_FIELD(srctype);
    COMPARE_STRING_FIELD(version);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterSchemaStmt(const AlterSchemaStmt* a, const AlterSchemaStmt* b)
{
    COMPARE_STRING_FIELD(schemaname);
    COMPARE_STRING_FIELD(authid);
    COMPARE_SCALAR_FIELD(hasBlockChain);

    return true;
}

static bool _equalAlterDataSourceStmt(const AlterDataSourceStmt* a, const AlterDataSourceStmt* b)
{
    COMPARE_STRING_FIELD(srcname);
    COMPARE_STRING_FIELD(srctype);
    COMPARE_STRING_FIELD(version);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(has_version);

    return true;
}

static bool _equalCreateRlsPolicyStmt(const CreateRlsPolicyStmt* a, const CreateRlsPolicyStmt* b)
{
    COMPARE_STRING_FIELD(policyName);
    COMPARE_NODE_FIELD(relation);
    COMPARE_STRING_FIELD(cmdName);
    COMPARE_SCALAR_FIELD(isPermissive);
    COMPARE_SCALAR_FIELD(fromExternal);
    COMPARE_NODE_FIELD(roleList);
    COMPARE_NODE_FIELD(usingQual);

    return true;
}

static bool _equalAlterRlsPolicyStmt(const AlterRlsPolicyStmt* a, const AlterRlsPolicyStmt* b)
{
    COMPARE_STRING_FIELD(policyName);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(roleList);
    COMPARE_NODE_FIELD(usingQual);

    return true;
}

static bool _equalCreateWeakPasswordDictionaryStmt(const CreateWeakPasswordDictionaryStmt* a, const CreateWeakPasswordDictionaryStmt* b)
{
    COMPARE_NODE_FIELD(weak_password_string_list);

    return true;
}

static bool _equalDropWeakPasswordDictionaryStmt(const DropWeakPasswordDictionaryStmt* a, const DropWeakPasswordDictionaryStmt* b)
{
    return true;
}

static bool _equalCreatePolicyLabelStmt(const CreatePolicyLabelStmt* a, const CreatePolicyLabelStmt* b)
{
    COMPARE_SCALAR_FIELD(if_not_exists);
    COMPARE_STRING_FIELD(label_type);
    COMPARE_STRING_FIELD(label_name);
    COMPARE_NODE_FIELD(label_items);
    return true;
}
static bool _equalAlterPolicyLabelStmt(const AlterPolicyLabelStmt* a, const AlterPolicyLabelStmt* b)
{
    COMPARE_STRING_FIELD(stmt_type);
    COMPARE_STRING_FIELD(label_name);
    COMPARE_NODE_FIELD(label_items);
    return true;
}
static bool _equalDropPolicyLabelStmt(const DropPolicyLabelStmt* a, const DropPolicyLabelStmt* b)
{
    COMPARE_SCALAR_FIELD(if_exists);
    COMPARE_NODE_FIELD(label_names);
    return true;
}
static bool _equalCreateAuditPolicyStmt(const CreateAuditPolicyStmt* a, const CreateAuditPolicyStmt* b)
{
    COMPARE_SCALAR_FIELD(if_not_exists);
    COMPARE_STRING_FIELD(policy_type);
    COMPARE_STRING_FIELD(policy_name);
    COMPARE_NODE_FIELD(policy_targets);
    COMPARE_NODE_FIELD(policy_filters);
    COMPARE_SCALAR_FIELD(policy_enabled);
    return true;
}
static bool _equalAlterAuditPolicyStmt(const AlterAuditPolicyStmt* a, const AlterAuditPolicyStmt* b)
{
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_STRING_FIELD(policy_name);
    COMPARE_STRING_FIELD(policy_action);
    COMPARE_STRING_FIELD(policy_type);
    COMPARE_NODE_FIELD(policy_items);
    COMPARE_NODE_FIELD(policy_filters);
    COMPARE_STRING_FIELD(policy_comments);
    COMPARE_NODE_FIELD(policy_enabled);
    return true;
}
static bool _equalDropAuditPolicyStmt(const DropAuditPolicyStmt* a, const DropAuditPolicyStmt* b)
{
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_NODE_FIELD(policy_names);
    return true;
}
static bool _equalCreateMaskingPolicyStmt(const CreateMaskingPolicyStmt* a, const CreateMaskingPolicyStmt* b)
{
    COMPARE_SCALAR_FIELD(if_not_exists);
    COMPARE_STRING_FIELD(policy_name);
    COMPARE_NODE_FIELD(policy_data);
    COMPARE_NODE_FIELD(policy_condition);
    COMPARE_NODE_FIELD(policy_filters);
    COMPARE_SCALAR_FIELD(policy_enabled);
    return true;
}
static bool _equalAlterMaskingPolicyStmt(const AlterMaskingPolicyStmt* a, const AlterMaskingPolicyStmt* b)
{
    COMPARE_STRING_FIELD(policy_name);
    COMPARE_STRING_FIELD(policy_action);
    COMPARE_NODE_FIELD(policy_items);
    COMPARE_NODE_FIELD(policy_condition);
    COMPARE_NODE_FIELD(policy_filters);
    COMPARE_STRING_FIELD(policy_comments);
    COMPARE_NODE_FIELD(policy_enabled);
    return true;
}
static bool _equalDropMaskingPolicyStmt(const DropMaskingPolicyStmt* a, const DropMaskingPolicyStmt* b)
{
    COMPARE_SCALAR_FIELD(if_exists);
    COMPARE_NODE_FIELD(policy_names);
    return true;
}
static bool _equalMaskingPolicyCondition(const MaskingPolicyCondition* a, const MaskingPolicyCondition* b)
{
    COMPARE_NODE_FIELD(fqdn);
    COMPARE_STRING_FIELD(_operator);
    COMPARE_NODE_FIELD(arg);
    return true;
}
static bool _equalPolicyFilterNode(const PolicyFilterNode* a, const PolicyFilterNode* b)
{
    COMPARE_STRING_FIELD(node_type);
    COMPARE_STRING_FIELD(op_value);
    COMPARE_STRING_FIELD(filter_type);
    COMPARE_NODE_FIELD(values);
    COMPARE_SCALAR_FIELD(has_not_operator);
    COMPARE_NODE_FIELD(left);
    COMPARE_NODE_FIELD(right);
    return true;
}


static bool _equalCreateTrigStmt(const CreateTrigStmt* a, const CreateTrigStmt* b)
{
    COMPARE_STRING_FIELD(trigname);
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(funcname);
    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(row);
    COMPARE_SCALAR_FIELD(timing);
    COMPARE_SCALAR_FIELD(events);
    COMPARE_NODE_FIELD(columns);
    COMPARE_NODE_FIELD(whenClause);
    COMPARE_SCALAR_FIELD(isconstraint);
    COMPARE_SCALAR_FIELD(deferrable);
    COMPARE_SCALAR_FIELD(initdeferred);
    COMPARE_NODE_FIELD(constrrel);

    return true;
}

static bool _equalCreatePLangStmt(const CreatePLangStmt* a, const CreatePLangStmt* b)
{
    COMPARE_SCALAR_FIELD(replace);
    COMPARE_STRING_FIELD(plname);
    COMPARE_NODE_FIELD(plhandler);
    COMPARE_NODE_FIELD(plinline);
    COMPARE_NODE_FIELD(plvalidator);
    COMPARE_SCALAR_FIELD(pltrusted);

    return true;
}

static bool _equalCreateRoleStmt(const CreateRoleStmt* a, const CreateRoleStmt* b)
{
    COMPARE_SCALAR_FIELD(stmt_type);
    COMPARE_STRING_FIELD(role);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterRoleStmt(const AlterRoleStmt* a, const AlterRoleStmt* b)
{
    COMPARE_STRING_FIELD(role);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(action);
    COMPARE_SCALAR_FIELD(lockstatus);

    return true;
}

static bool _equalAlterRoleSetStmt(const AlterRoleSetStmt* a, const AlterRoleSetStmt* b)
{
    COMPARE_STRING_FIELD(role);
    COMPARE_STRING_FIELD(database);
    COMPARE_NODE_FIELD(setstmt);

    return true;
}

static bool _equalDropRoleStmt(const DropRoleStmt* a, const DropRoleStmt* b)
{
    COMPARE_NODE_FIELD(roles);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalLockStmt(const LockStmt* a, const LockStmt* b)
{
    COMPARE_NODE_FIELD(relations);
    COMPARE_SCALAR_FIELD(mode);
    COMPARE_SCALAR_FIELD(nowait);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COMPARE_SCALAR_FIELD(waitSec);
    }

    return true;
}

static bool _equalConstraintsSetStmt(const ConstraintsSetStmt* a, const ConstraintsSetStmt* b)
{
    COMPARE_NODE_FIELD(constraints);
    COMPARE_SCALAR_FIELD(deferred);

    return true;
}

static bool _equalReindexStmt(const ReindexStmt* a, const ReindexStmt* b)
{
    COMPARE_SCALAR_FIELD(kind);
    COMPARE_NODE_FIELD(relation);
    COMPARE_STRING_FIELD(name);
    COMPARE_SCALAR_FIELD(do_system);
    COMPARE_SCALAR_FIELD(do_user);

    return true;
}

static bool _equalCreateSchemaStmt(const CreateSchemaStmt* a, const CreateSchemaStmt* b)
{
    COMPARE_STRING_FIELD(schemaname);
    COMPARE_STRING_FIELD(authid);
    COMPARE_SCALAR_FIELD(hasBlockChain);
    COMPARE_NODE_FIELD(schemaElts);

    return true;
}

static bool _equalCreateConversionStmt(const CreateConversionStmt* a, const CreateConversionStmt* b)
{
    COMPARE_NODE_FIELD(conversion_name);
    COMPARE_STRING_FIELD(for_encoding_name);
    COMPARE_STRING_FIELD(to_encoding_name);
    COMPARE_NODE_FIELD(func_name);
    COMPARE_SCALAR_FIELD(def);

    return true;
}

static bool _equalCreateCastStmt(const CreateCastStmt* a, const CreateCastStmt* b)
{
    COMPARE_NODE_FIELD(sourcetype);
    COMPARE_NODE_FIELD(targettype);
    COMPARE_NODE_FIELD(func);
    COMPARE_SCALAR_FIELD(context);
    COMPARE_SCALAR_FIELD(inout);

    return true;
}

static bool _equalPrepareStmt(const PrepareStmt* a, const PrepareStmt* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(argtypes);
    COMPARE_NODE_FIELD(query);

    return true;
}

static bool _equalExecuteStmt(const ExecuteStmt* a, const ExecuteStmt* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(params);

    return true;
}

static bool _equalDeallocateStmt(const DeallocateStmt* a, const DeallocateStmt* b)
{
    COMPARE_STRING_FIELD(name);

    return true;
}

static bool _equalDropOwnedStmt(const DropOwnedStmt* a, const DropOwnedStmt* b)
{
    COMPARE_NODE_FIELD(roles);
    COMPARE_SCALAR_FIELD(behavior);

    return true;
}

static bool _equalReassignOwnedStmt(const ReassignOwnedStmt* a, const ReassignOwnedStmt* b)
{
    COMPARE_NODE_FIELD(roles);
    COMPARE_STRING_FIELD(newrole);

    return true;
}

static bool _equalAlterTSDictionaryStmt(const AlterTSDictionaryStmt* a, const AlterTSDictionaryStmt* b)
{
    COMPARE_NODE_FIELD(dictname);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterTSConfigurationStmt(const AlterTSConfigurationStmt* a, const AlterTSConfigurationStmt* b)
{
    COMPARE_NODE_FIELD(cfgname);
    COMPARE_NODE_FIELD(tokentype);
    COMPARE_NODE_FIELD(dicts);
    COMPARE_SCALAR_FIELD(override);
    COMPARE_SCALAR_FIELD(replace);
    COMPARE_SCALAR_FIELD(missing_ok);

    return true;
}

static bool _equalAExpr(const A_Expr* a, const A_Expr* b)
{
    COMPARE_SCALAR_FIELD(kind);
    COMPARE_NODE_FIELD(name);
    COMPARE_NODE_FIELD(lexpr);
    COMPARE_NODE_FIELD(rexpr);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalColumnRef(const ColumnRef* a, const ColumnRef* b)
{
    COMPARE_NODE_FIELD(fields);
    COMPARE_SCALAR_FIELD(prior);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalParamRef(const ParamRef* a, const ParamRef* b)
{
    COMPARE_SCALAR_FIELD(number);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalAConst(const A_Const* a, const A_Const* b)
{
    if (!equal(&a->val, &b->val)) /* hack for in-line Value field */
        return false;
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalFuncCall(const FuncCall* a, const FuncCall* b)
{
    COMPARE_NODE_FIELD(funcname);
    COMPARE_STRING_FIELD(colname);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(agg_order);
    COMPARE_SCALAR_FIELD(agg_within_group);
    COMPARE_SCALAR_FIELD(agg_star);
    COMPARE_SCALAR_FIELD(agg_distinct);
    COMPARE_SCALAR_FIELD(func_variadic);
    COMPARE_NODE_FIELD(over);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(call_func);

    return true;
}

static bool _equalAStar(const A_Star* a, const A_Star* b)
{
    return true;
}

static bool _equalAIndices(const A_Indices* a, const A_Indices* b)
{
    COMPARE_NODE_FIELD(lidx);
    COMPARE_NODE_FIELD(uidx);

    return true;
}

static bool _equalA_Indirection(const A_Indirection* a, const A_Indirection* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(indirection);

    return true;
}

static bool _equalA_ArrayExpr(const A_ArrayExpr* a, const A_ArrayExpr* b)
{
    COMPARE_NODE_FIELD(elements);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalResTarget(const ResTarget* a, const ResTarget* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(indirection);
    COMPARE_NODE_FIELD(val);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalTypeName(const TypeName* a, const TypeName* b)
{
    COMPARE_NODE_FIELD(names);
    COMPARE_SCALAR_FIELD(typeOid);
    COMPARE_SCALAR_FIELD(setof);
    COMPARE_SCALAR_FIELD(pct_type);
    COMPARE_NODE_FIELD(typmods);
    COMPARE_SCALAR_FIELD(typemod);
    COMPARE_NODE_FIELD(arrayBounds);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_LOCATION_FIELD(end_location);

    return true;
}

static bool _equalTypeCast(const TypeCast* a, const TypeCast* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(typname);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCollateClause(const CollateClause* a, const CollateClause* b)
{
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(collname);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalColumnParam (const ClientLogicColumnParam* a, const ClientLogicColumnParam* b)
{
    COMPARE_SCALAR_FIELD(key);
    COMPARE_STRING_FIELD(value);
    COMPARE_SCALAR_FIELD(len);
    COMPARE_LOCATION_FIELD(location); 
    return true;
}
static bool _equalGlobalParam (const ClientLogicGlobalParam* a, const ClientLogicGlobalParam* b)
{
    COMPARE_SCALAR_FIELD(key);
    COMPARE_STRING_FIELD(value);
    COMPARE_SCALAR_FIELD(len);
    COMPARE_LOCATION_FIELD(location); 
    return true;
}
static bool _equalGlobalSetting (const CreateClientLogicGlobal* a, const CreateClientLogicGlobal* b)
{
    COMPARE_NODE_FIELD(global_key_name);
    COMPARE_NODE_FIELD(global_setting_params);
    return true;
}
static bool _equalColumnSetting (const CreateClientLogicColumn* a, const CreateClientLogicColumn* b)
{
    COMPARE_NODE_FIELD(column_key_name);
    COMPARE_NODE_FIELD(column_setting_params);
    return true;
}
static bool _equalEncryptedColumn(const ClientLogicColumnRef *a, const ClientLogicColumnRef *b)
{
    COMPARE_NODE_FIELD(column_key_name);
    COMPARE_SCALAR_FIELD(columnEncryptionAlgorithmType);
    COMPARE_NODE_FIELD(orig_typname);  
    COMPARE_NODE_FIELD(dest_typname);  
    COMPARE_LOCATION_FIELD(location); 
    return true;
}

static bool _equalSortBy(const SortBy* a, const SortBy* b)
{
    COMPARE_NODE_FIELD(node);
    COMPARE_SCALAR_FIELD(sortby_dir);
    COMPARE_SCALAR_FIELD(sortby_nulls);
    COMPARE_NODE_FIELD(useOp);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalWindowDef(const WindowDef* a, const WindowDef* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_STRING_FIELD(refname);
    COMPARE_NODE_FIELD(partitionClause);
    COMPARE_NODE_FIELD(orderClause);
    COMPARE_SCALAR_FIELD(frameOptions);
    COMPARE_NODE_FIELD(startOffset);
    COMPARE_NODE_FIELD(endOffset);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalRangeSubselect(const RangeSubselect* a, const RangeSubselect* b)
{
    COMPARE_SCALAR_FIELD(lateral);
    COMPARE_NODE_FIELD(subquery);
    COMPARE_NODE_FIELD(alias);

    return true;
}

static bool _equalRangeFunction(const RangeFunction* a, const RangeFunction* b)
{
    COMPARE_SCALAR_FIELD(lateral);
    COMPARE_NODE_FIELD(funccallnode);
    COMPARE_NODE_FIELD(alias);
    COMPARE_NODE_FIELD(coldeflist);

    return true;
}

/*
 * Description: a compare with b
 *
 * Parameters:
 *	@in a: one RangeTableSample
 *	@in b: another RangeTableSample
 *
 * Return: bool (if a be equal to b return true, else return false)
 */
static bool _equalRangeTableSample(const RangeTableSample* a, const RangeTableSample* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(method);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(repeatable);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

/*
 * Description: a compare with b
 *
 * Parameters: @in a: one RangeTimeCapsule
 *             @in b: another RangeTableSample
 *
 * Return: bool (if a be equal to b return true, else return false)
 */
static bool EqualRangeTimeCapsule(const RangeTimeCapsule* a, const RangeTimeCapsule* b)
{
    COMPARE_NODE_FIELD(relation);
    COMPARE_SCALAR_FIELD(tvtype);
    COMPARE_NODE_FIELD(tvver);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalIndexElem(const IndexElem* a, const IndexElem* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(expr);
    COMPARE_STRING_FIELD(indexcolname);
    COMPARE_NODE_FIELD(collation);
    COMPARE_NODE_FIELD(opclass);
    COMPARE_SCALAR_FIELD(ordering);
    COMPARE_SCALAR_FIELD(nulls_ordering);

    return true;
}

static bool _equalColumnDef(const ColumnDef* a, const ColumnDef* b)
{
    COMPARE_STRING_FIELD(colname);
    COMPARE_NODE_FIELD(typname);
    COMPARE_SCALAR_FIELD(kvtype);
    COMPARE_SCALAR_FIELD(inhcount);
    COMPARE_SCALAR_FIELD(is_local);
    COMPARE_SCALAR_FIELD(is_not_null);
    COMPARE_SCALAR_FIELD(is_from_type);
    COMPARE_SCALAR_FIELD(is_serial);
    COMPARE_SCALAR_FIELD(storage);
    COMPARE_SCALAR_FIELD(cmprs_mode);
    COMPARE_NODE_FIELD(raw_default);
    COMPARE_NODE_FIELD(cooked_default);
    COMPARE_SCALAR_FIELD(generatedCol);
    COMPARE_NODE_FIELD(collClause);
    COMPARE_NODE_FIELD(clientLogicColumnRef);
    COMPARE_SCALAR_FIELD(collOid);
    COMPARE_NODE_FIELD(constraints);
    COMPARE_NODE_FIELD(fdwoptions);

    return true;
}

static bool _equalConstraint(const Constraint* a, const Constraint* b)
{
    COMPARE_SCALAR_FIELD(contype);
    COMPARE_STRING_FIELD(conname);
    COMPARE_SCALAR_FIELD(deferrable);
    COMPARE_SCALAR_FIELD(initdeferred);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(is_no_inherit);
    COMPARE_NODE_FIELD(raw_expr);
    COMPARE_STRING_FIELD(cooked_expr);
    COMPARE_NODE_FIELD(keys);
    COMPARE_NODE_FIELD(including);
    COMPARE_NODE_FIELD(exclusions);
    COMPARE_NODE_FIELD(options);
    COMPARE_STRING_FIELD(indexname);
    COMPARE_STRING_FIELD(indexspace);
    COMPARE_STRING_FIELD(access_method);
    COMPARE_NODE_FIELD(where_clause);
    COMPARE_NODE_FIELD(pktable);
    COMPARE_NODE_FIELD(fk_attrs);
    COMPARE_NODE_FIELD(pk_attrs);
    COMPARE_SCALAR_FIELD(fk_matchtype);
    COMPARE_SCALAR_FIELD(fk_upd_action);
    COMPARE_SCALAR_FIELD(fk_del_action);
    COMPARE_NODE_FIELD(old_conpfeqop);
    COMPARE_SCALAR_FIELD(old_pktable_oid);
    COMPARE_SCALAR_FIELD(skip_validation);
    COMPARE_SCALAR_FIELD(initially_valid);

    return true;
}

static bool _equalDefElem(const DefElem* a, const DefElem* b)
{
    COMPARE_STRING_FIELD(defnamespace);
    COMPARE_STRING_FIELD(defname);
    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(defaction);
    COMPARE_SCALAR_FIELD(begin_location);
    COMPARE_SCALAR_FIELD(end_location);

    return true;
}

static bool _equalLockingClause(const LockingClause* a, const LockingClause* b)
{
    COMPARE_NODE_FIELD(lockedRels);
    COMPARE_SCALAR_FIELD(forUpdate);
    COMPARE_SCALAR_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        COMPARE_SCALAR_FIELD(strength);
    }
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COMPARE_SCALAR_FIELD(waitSec);
    }

    return true;
}

static bool _equalRangeTblEntry(const RangeTblEntry* a, const RangeTblEntry* b)
{
    COMPARE_SCALAR_FIELD(rtekind);
    COMPARE_SCALAR_FIELD(relid);
    COMPARE_SCALAR_FIELD(partitionOid);
    COMPARE_SCALAR_FIELD(isContainPartition);
    COMPARE_SCALAR_FIELD(subpartitionOid);
    COMPARE_SCALAR_FIELD(isContainSubPartition);
    COMPARE_SCALAR_FIELD(refSynOid);
    COMPARE_NODE_FIELD(partid_list);
    COMPARE_SCALAR_FIELD(relkind);
    COMPARE_SCALAR_FIELD(isResultRel);
    COMPARE_NODE_FIELD(tablesample);
    COMPARE_NODE_FIELD(timecapsule);
    COMPARE_SCALAR_FIELD(ispartrel);
    COMPARE_NODE_FIELD(subquery);
    COMPARE_SCALAR_FIELD(security_barrier);
    COMPARE_SCALAR_FIELD(jointype);
    COMPARE_NODE_FIELD(joinaliasvars);
    COMPARE_NODE_FIELD(funcexpr);
    COMPARE_NODE_FIELD(funccoltypes);
    COMPARE_NODE_FIELD(funccoltypmods);
    COMPARE_NODE_FIELD(funccolcollations);
    COMPARE_NODE_FIELD(values_lists);
    COMPARE_NODE_FIELD(values_collations);
    COMPARE_STRING_FIELD(ctename);
    COMPARE_SCALAR_FIELD(ctelevelsup);
    COMPARE_SCALAR_FIELD(self_reference);
    COMPARE_SCALAR_FIELD(cterecursive);
    COMPARE_NODE_FIELD(ctecoltypes);
    COMPARE_NODE_FIELD(ctecoltypmods);
    COMPARE_NODE_FIELD(ctecolcollations);
    COMPARE_SCALAR_FIELD(swConverted);
    COMPARE_NODE_FIELD(origin_index);
    COMPARE_SCALAR_FIELD(swAborted);
    COMPARE_SCALAR_FIELD(swSubExist);
    COMPARE_NODE_FIELD(alias);
    COMPARE_NODE_FIELD(eref);
    COMPARE_NODE_FIELD(pname);
    COMPARE_NODE_FIELD(plist);
    COMPARE_SCALAR_FIELD(lateral);
    COMPARE_SCALAR_FIELD(inh);
    COMPARE_SCALAR_FIELD(inFromCl);
    COMPARE_SCALAR_FIELD(requiredPerms);
    COMPARE_SCALAR_FIELD(checkAsUser);
    COMPARE_BITMAPSET_FIELD(selectedCols);
    COMPARE_BITMAPSET_FIELD(insertedCols);
    COMPARE_BITMAPSET_FIELD(updatedCols);
    COMPARE_BITMAPSET_FIELD(extraUpdatedCols);
    COMPARE_SCALAR_FIELD(orientation);
    COMPARE_NODE_FIELD(securityQuals);
    COMPARE_SCALAR_FIELD(subquery_pull_up);
    COMPARE_SCALAR_FIELD(correlated_with_recursive_cte);
    COMPARE_SCALAR_FIELD(relhasbucket);
    COMPARE_SCALAR_FIELD(isbucket);
    COMPARE_SCALAR_FIELD(bucketmapsize);
    COMPARE_NODE_FIELD(buckets);
    COMPARE_SCALAR_FIELD(isexcluded);
    COMPARE_SCALAR_FIELD(sublink_pull_up);
    COMPARE_SCALAR_FIELD(is_ustore);
    COMPARE_SCALAR_FIELD(pulled_from_subquery);

    return true;
}

/*
 * Description: a compare with b
 *
 * Parameters: @in a: one TableSampleClause
 *             @in b: another TableSampleClause
 *
 * Return: bool (if a be equal to b return true, else return false)
 */
static bool _equalTableSampleClause(const TableSampleClause* a, const TableSampleClause* b)
{
    COMPARE_SCALAR_FIELD(sampleType);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(repeatable);

    return true;
}

/*
 * Description: a compare with b
 *
 * Parameters:
 *	@in a: one TimeCapsuleClause
 *	@in b: another TimeCapsuleClause
 *
 * Return: bool (if a be equal to b return true, else return false)
 */
static bool EqualTimeCapsuleClause(const TimeCapsuleClause* a, const TimeCapsuleClause* b)
{
    COMPARE_SCALAR_FIELD(tvtype);
    COMPARE_NODE_FIELD(tvver);

    return true;
}


static bool _equalSortGroupClause(const SortGroupClause* a, const SortGroupClause* b)
{
    COMPARE_SCALAR_FIELD(tleSortGroupRef);
    COMPARE_SCALAR_FIELD(eqop);
    COMPARE_SCALAR_FIELD(sortop);
    COMPARE_SCALAR_FIELD(nulls_first);
    COMPARE_SCALAR_FIELD(hashable);
    COMPARE_SCALAR_FIELD(groupSet);

    return true;
}

static bool _equalGroupingSet(const GroupingSet* a, const GroupingSet* b)
{
    COMPARE_SCALAR_FIELD(kind);
    COMPARE_NODE_FIELD(content);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalWindowClause(const WindowClause* a, const WindowClause* b)
{
    COMPARE_STRING_FIELD(name);
    COMPARE_STRING_FIELD(refname);
    COMPARE_NODE_FIELD(partitionClause);
    COMPARE_NODE_FIELD(orderClause);
    COMPARE_SCALAR_FIELD(frameOptions);
    COMPARE_NODE_FIELD(startOffset);
    COMPARE_NODE_FIELD(endOffset);
    COMPARE_SCALAR_FIELD(winref);
    COMPARE_SCALAR_FIELD(copiedOrder);

    return true;
}

static bool _equalRowMarkClause(const RowMarkClause* a, const RowMarkClause* b)
{
    COMPARE_SCALAR_FIELD(rti);
    COMPARE_SCALAR_FIELD(forUpdate);
    COMPARE_SCALAR_FIELD(noWait);
    if (t_thrd.proc->workingVersionNum >= WAIT_N_TUPLE_LOCK_VERSION_NUM) {
        COMPARE_SCALAR_FIELD(waitSec);
    }

    COMPARE_SCALAR_FIELD(pushedDown);
    if (t_thrd.proc->workingVersionNum >= ENHANCED_TUPLE_LOCK_VERSION_NUM) {
        COMPARE_SCALAR_FIELD(strength);
    }

    return true;
}

static bool _equalWithClause(const WithClause* a, const WithClause* b)
{
    COMPARE_NODE_FIELD(ctes);
    COMPARE_SCALAR_FIELD(recursive);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_NODE_FIELD(sw_clause);

    return true;
}

static bool _equalStartWithClause(const StartWithClause * a, const StartWithClause * b)
{
    COMPARE_NODE_FIELD(startWithExpr);
    COMPARE_NODE_FIELD(connectByExpr);
    COMPARE_NODE_FIELD(siblingsOrderBy);

    COMPARE_SCALAR_FIELD(priorDirection);
    COMPARE_SCALAR_FIELD(nocycle);
    COMPARE_SCALAR_FIELD(opt);

    return true;
}

static bool _equalUpsertClause(const UpsertClause* a, const UpsertClause* b)
{
   COMPARE_NODE_FIELD(targetList);
   COMPARE_LOCATION_FIELD(location);
   COMPARE_NODE_FIELD(whereClause);

   return true;
}

static bool _equalStartWithTargetRelInfo(const StartWithTargetRelInfo *a,
                                                       const StartWithTargetRelInfo *b)
{
    /* simple compare pointer */
    return a == b;
}

static bool _equalCommonTableExpr(const CommonTableExpr* a, const CommonTableExpr* b)
{
    COMPARE_STRING_FIELD(ctename);
    COMPARE_NODE_FIELD(aliascolnames);
    COMPARE_SCALAR_FIELD(ctematerialized);
    COMPARE_NODE_FIELD(ctequery);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(cterecursive);
    COMPARE_SCALAR_FIELD(cterefcount);
    COMPARE_NODE_FIELD(ctecolnames);
    COMPARE_NODE_FIELD(ctecoltypes);
    COMPARE_NODE_FIELD(ctecoltypmods);
    COMPARE_NODE_FIELD(ctecolcollations);
    COMPARE_SCALAR_FIELD(locator_type);
    COMPARE_NODE_FIELD(swoptions);
    COMPARE_SCALAR_FIELD(self_reference);
    COMPARE_SCALAR_FIELD(referenced_by_subquery);
    return true;
}

static bool _equalStartWithOptions(const StartWithOptions * a, const StartWithOptions * b)
{
    COMPARE_NODE_FIELD(siblings_orderby_clause);
    COMPARE_NODE_FIELD(prior_key_index);
    COMPARE_NODE_FIELD(prior_key_index);

    COMPARE_SCALAR_FIELD(connect_by_type);
    COMPARE_NODE_FIELD(connect_by_level_quals);
    COMPARE_NODE_FIELD(connect_by_other_quals);
    COMPARE_SCALAR_FIELD(nocycle);

    return true;
}

static bool _equalXmlSerialize(const XmlSerialize* a, const XmlSerialize* b)
{
    COMPARE_SCALAR_FIELD(xmloption);
    COMPARE_NODE_FIELD(expr);
    COMPARE_NODE_FIELD(typname);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCreateDirectoryStmt(const CreateDirectoryStmt* a, const CreateDirectoryStmt* b)
{
    COMPARE_SCALAR_FIELD(replace);
    COMPARE_STRING_FIELD(directoryname);
    COMPARE_STRING_FIELD(owner);
    COMPARE_STRING_FIELD(location);

    return true;
}

static bool _equalDropDirectoryStmt(const DropDirectoryStmt* a, const DropDirectoryStmt* b)
{
    COMPARE_STRING_FIELD(directoryname);

    return true;
}

static bool _equalCreateSynonymStmt(CreateSynonymStmt* a, CreateSynonymStmt* b)
{
    COMPARE_SCALAR_FIELD(replace);
    COMPARE_NODE_FIELD(synName);
    COMPARE_NODE_FIELD(objName);

    return true;
}

static bool _equalDropSynonymStmt(DropSynonymStmt* a, DropSynonymStmt* b)
{
    COMPARE_NODE_FIELD(synName);
    COMPARE_SCALAR_FIELD(missing);
    COMPARE_SCALAR_FIELD(behavior);

    return true;
}

static bool _equalRownum(Rownum* a, Rownum* b)
{
    COMPARE_SCALAR_FIELD(rownumcollid);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCreatePublicationStmt(const CreatePublicationStmt *a, const CreatePublicationStmt *b)
{
    COMPARE_STRING_FIELD(pubname);
    COMPARE_NODE_FIELD(options);
    COMPARE_NODE_FIELD(tables);
    COMPARE_SCALAR_FIELD(for_all_tables);

    return true;
}

static bool _equalAlterPublicationStmt(const AlterPublicationStmt *a, const AlterPublicationStmt *b)
{
    COMPARE_STRING_FIELD(pubname);
    COMPARE_NODE_FIELD(options);
    COMPARE_NODE_FIELD(tables);
    COMPARE_SCALAR_FIELD(for_all_tables);
    COMPARE_SCALAR_FIELD(tableAction);

    return true;
}

static bool _equalCreateSubscriptionStmt(const CreateSubscriptionStmt *a, const CreateSubscriptionStmt *b)
{
    COMPARE_STRING_FIELD(subname);
    COMPARE_STRING_FIELD(conninfo);
    COMPARE_NODE_FIELD(publication);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalAlterSubscriptionStmt(const AlterSubscriptionStmt *a, const AlterSubscriptionStmt *b)
{
    COMPARE_STRING_FIELD(subname);
    COMPARE_NODE_FIELD(options);

    return true;
}

static bool _equalDropSubscriptionStmt(const DropSubscriptionStmt *a, const DropSubscriptionStmt *b)
{
    COMPARE_STRING_FIELD(subname);
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_SCALAR_FIELD(behavior);

    return true;
}

/*
 * Stuff from pg_list.h
 */

static bool _equalList(const List* a, const List* b)
{
    const ListCell* item_a = NULL;
    const ListCell* item_b = NULL;

    /*
     * Try to reject by simple scalar checks before grovelling through all the
     * list elements...
     */
    COMPARE_SCALAR_FIELD(type);
    COMPARE_SCALAR_FIELD(length);

    /*
     * We place the switch outside the loop for the sake of efficiency; this
     * may not be worth doing...
     */
    switch (a->type) {
        case T_List:
            forboth(item_a, a, item_b, b) {
                if (!equal(lfirst(item_a), lfirst(item_b))) {
                    return false;
                }
            }
            break;
        case T_IntList:
            forboth(item_a, a, item_b, b) {
                if (lfirst_int(item_a) != lfirst_int(item_b)) {
                    return false;
                }
            }
            break;
        case T_OidList:
            forboth(item_a, a, item_b, b) {
                if (lfirst_oid(item_a) != lfirst_oid(item_b)) {
                    return false;
                }
            }
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized list node type: %d", (int)a->type)));
            return false; /* keep compiler quiet */
    }

    /*
     * If we got here, we should have run out of elements of both lists
     */
    Assert(item_a == NULL);
    Assert(item_b == NULL);

    return true;
}

/*
 * Stuff from value.h
 */
static bool _equalValue(const Value* a, const Value* b)
{
    COMPARE_SCALAR_FIELD(type);

    switch (a->type) {
        case T_Integer:
            COMPARE_SCALAR_FIELD(val.ival);
            break;
        case T_Float:
        case T_String:
        case T_BitString:
            COMPARE_STRING_FIELD(val.str);
            break;
        case T_Null:
            /* nothing to do */
            break;
        default:
            ereport(
                ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)a->type)));
            break;
    }

    return true;
}

#ifdef PGXC
/*
 * stuff from barrier.h
 */
static bool _equalBarrierStmt(const BarrierStmt* a, const BarrierStmt* b)
{
    COMPARE_STRING_FIELD(id);
    return true;
}

/*
 * stuff from nodemgr.h
 */
static bool _equalAlterNodeStmt(const AlterNodeStmt* a, const AlterNodeStmt* b)
{
    COMPARE_STRING_FIELD(node_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalCreateNodeStmt(const CreateNodeStmt* a, const CreateNodeStmt* b)
{
    COMPARE_STRING_FIELD(node_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalDropNodeStmt(const DropNodeStmt* a, const DropNodeStmt* b)
{
    COMPARE_STRING_FIELD(node_name);
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_NODE_FIELD(remote_nodes);

    return true;
}

/*
 * stuff from groupmgr.h
 */
static bool _equalCreateGroupStmt(const CreateGroupStmt* a, const CreateGroupStmt* b)
{
    COMPARE_STRING_FIELD(group_name);
    COMPARE_STRING_FIELD(src_group_name);
    COMPARE_NODE_FIELD(nodes);
    COMPARE_NODE_FIELD(buckets);
    COMPARE_SCALAR_FIELD(vcgroup);
    return true;
}

static bool _equalAlterGroupStmt(const AlterGroupStmt* a, const AlterGroupStmt* b)
{
    COMPARE_STRING_FIELD(group_name);
    COMPARE_STRING_FIELD(install_name);
    COMPARE_SCALAR_FIELD(alter_type);
    COMPARE_NODE_FIELD(nodes);
    return true;
}

static bool _equalDropGroupStmt(const DropGroupStmt* a, const DropGroupStmt* b)
{
    COMPARE_STRING_FIELD(group_name);
    COMPARE_STRING_FIELD(src_group_name);
    COMPARE_SCALAR_FIELD(to_elastic_group);
    return true;
}

/*
 * stuff from workload.h
 */
static bool _equalCreateResourcePoolStmt(const CreateResourcePoolStmt* a, const CreateResourcePoolStmt* b)
{
    COMPARE_STRING_FIELD(pool_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalAlterResourcePoolStmt(const AlterResourcePoolStmt* a, const AlterResourcePoolStmt* b)
{
    COMPARE_STRING_FIELD(pool_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalDropResourcePoolStmt(const DropResourcePoolStmt* a, const DropResourcePoolStmt* b)
{
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_STRING_FIELD(pool_name);
    return true;
}

static bool _equalCreateWorkloadGroupStmt(const CreateWorkloadGroupStmt* a, const CreateWorkloadGroupStmt* b)
{
    COMPARE_STRING_FIELD(group_name);
    COMPARE_STRING_FIELD(pool_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalAlterWorkloadGroupStmt(const AlterWorkloadGroupStmt* a, const AlterWorkloadGroupStmt* b)
{
    COMPARE_STRING_FIELD(group_name);
    COMPARE_STRING_FIELD(pool_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalDropWorkloadGroupStmt(const DropWorkloadGroupStmt* a, const DropWorkloadGroupStmt* b)
{
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_STRING_FIELD(group_name);
    return true;
}

static bool _equalCreateAppWorkloadGroupMappingStmt(
    const CreateAppWorkloadGroupMappingStmt* a, const CreateAppWorkloadGroupMappingStmt* b)
{
    COMPARE_STRING_FIELD(app_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalAlterAppWorkloadGroupMappingStmt(
    const AlterAppWorkloadGroupMappingStmt* a, const AlterAppWorkloadGroupMappingStmt* b)
{
    COMPARE_STRING_FIELD(app_name);
    COMPARE_NODE_FIELD(options);
    return true;
}

static bool _equalDropAppWorkloadGroupMappingStmt(
    const DropAppWorkloadGroupMappingStmt* a, const DropAppWorkloadGroupMappingStmt* b)
{
    COMPARE_SCALAR_FIELD(missing_ok);
    COMPARE_STRING_FIELD(app_name);
    return true;
}

/*
 * stuff from poolutils.h
 */
static bool _equalCleanConnStmt(const CleanConnStmt* a, const CleanConnStmt* b)
{
    COMPARE_NODE_FIELD(nodes);
    COMPARE_STRING_FIELD(dbname);
    COMPARE_STRING_FIELD(username);
    COMPARE_SCALAR_FIELD(is_coord);
    COMPARE_SCALAR_FIELD(is_force);
    COMPARE_SCALAR_FIELD(is_check);
    return true;
}

#endif

bool _equalSimpleVar(void* va, void* vb)
{
    if (!IsA(va, Var) || !IsA(vb, Var)) {
        return false;
    }
    Var* a = (Var*)va;
    Var* b = (Var*)vb;
    COMPARE_SCALAR_FIELD(varno);
    COMPARE_SCALAR_FIELD(varattno);
    COMPARE_SCALAR_FIELD(vartype);
    COMPARE_SCALAR_FIELD(vartypmod);
    COMPARE_SCALAR_FIELD(varcollid);

    return true;
}

static bool _equalGroupingId(const GroupingId* a, const GroupingId* b)
{
    return true;
}

static bool _equalShutDown(const ShutdownStmt* a, const ShutdownStmt* b)
{
    COMPARE_STRING_FIELD(mode);
    return true;
}

/*
 * equal
 *	  returns whether two nodes are equal
 */
bool equal(const void* a, const void* b)
{
    bool retval = false;

    if (a == b) {
        return true;
    }

    /*
     * note that a!=b, so only one of them can be NULL
     */
    if (a == NULL || b == NULL) {
        return false;
    }

    /*
     * are they the same type of nodes?
     */
    if (nodeTag(a) != nodeTag(b)) {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(a)) {
            /*
             * PRIMITIVE NODES
             */
        case T_Alias:
            retval = _equalAlias((Alias*)a, (Alias*)b);
            break;
        case T_RangeVar:
            retval = _equalRangeVar((RangeVar*)a, (RangeVar*)b);
            break;
        case T_IntoClause:
            retval = _equalIntoClause((IntoClause*)a, (IntoClause*)b);
            break;
        case T_Var:
            retval = _equalVar((Var*)a, (Var*)b);
            break;
        case T_Const:
            retval = _equalConst((Const*)a, (Const*)b);
            break;
        case T_Param:
            retval = _equalParam((Param*)a, (Param*)b);
            break;
        case T_Aggref:
            retval = _equalAggref((Aggref*)a, (Aggref*)b);
            break;
        case T_GroupingFunc:
            retval = _equalGroupingFunc((GroupingFunc*)a, (GroupingFunc*)b);
            break;
        case T_WindowFunc:
            retval = _equalWindowFunc((WindowFunc*)a, (WindowFunc*)b);
            break;
        case T_ArrayRef:
            retval = _equalArrayRef((ArrayRef*)a, (ArrayRef*)b);
            break;
        case T_FuncExpr:
            retval = _equalFuncExpr((FuncExpr*)a, (FuncExpr*)b);
            break;
        case T_NamedArgExpr:
            retval = _equalNamedArgExpr((NamedArgExpr*)a, (NamedArgExpr*)b);
            break;
        case T_OpExpr:
            retval = _equalOpExpr((OpExpr*)a, (OpExpr*)b);
            break;
        case T_DistinctExpr:
            retval = _equalDistinctExpr((DistinctExpr*)a, (DistinctExpr*)b);
            break;
        case T_NullIfExpr:
            retval = _equalNullIfExpr((NullIfExpr*)a, (NullIfExpr*)b);
            break;
        case T_ScalarArrayOpExpr:
            retval = _equalScalarArrayOpExpr((ScalarArrayOpExpr*)a, (ScalarArrayOpExpr*)b);
            break;
        case T_BoolExpr:
            retval = _equalBoolExpr((BoolExpr*)a, (BoolExpr*)b);
            break;
        case T_SubLink:
            retval = _equalSubLink((SubLink*)a, (SubLink*)b);
            break;
        case T_SubPlan:
            retval = _equalSubPlan((SubPlan*)a, (SubPlan*)b);
            break;
        case T_AlternativeSubPlan:
            retval = _equalAlternativeSubPlan((AlternativeSubPlan*)a, (AlternativeSubPlan*)b);
            break;
        case T_FieldSelect:
            retval = _equalFieldSelect((FieldSelect*)a, (FieldSelect*)b);
            break;
        case T_FieldStore:
            retval = _equalFieldStore((FieldStore*)a, (FieldStore*)b);
            break;
        case T_RelabelType:
            retval = _equalRelabelType((RelabelType*)a, (RelabelType*)b);
            break;
        case T_CoerceViaIO:
            retval = _equalCoerceViaIO((CoerceViaIO*)a, (CoerceViaIO*)b);
            break;
        case T_ArrayCoerceExpr:
            retval = _equalArrayCoerceExpr((ArrayCoerceExpr*)a, (ArrayCoerceExpr*)b);
            break;
        case T_ConvertRowtypeExpr:
            retval = _equalConvertRowtypeExpr((ConvertRowtypeExpr*)a, (ConvertRowtypeExpr*)b);
            break;
        case T_CollateExpr:
            retval = _equalCollateExpr((CollateExpr*)a, (CollateExpr*)b);
            break;
        case T_CaseExpr:
            retval = _equalCaseExpr((CaseExpr*)a, (CaseExpr*)b);
            break;
        case T_CaseWhen:
            retval = _equalCaseWhen((CaseWhen*)a, (CaseWhen*)b);
            break;
        case T_CaseTestExpr:
            retval = _equalCaseTestExpr((CaseTestExpr*)a, (CaseTestExpr*)b);
            break;
        case T_ArrayExpr:
            retval = _equalArrayExpr((ArrayExpr*)a, (ArrayExpr*)b);
            break;
        case T_RowExpr:
            retval = _equalRowExpr((RowExpr*)a, (RowExpr*)b);
            break;
        case T_RowCompareExpr:
            retval = _equalRowCompareExpr((RowCompareExpr*)a, (RowCompareExpr*)b);
            break;
        case T_CoalesceExpr:
            retval = _equalCoalesceExpr((CoalesceExpr*)a, (CoalesceExpr*)b);
            break;
        case T_MinMaxExpr:
            retval = _equalMinMaxExpr((MinMaxExpr*)a, (MinMaxExpr*)b);
            break;
        case T_XmlExpr:
            retval = _equalXmlExpr((XmlExpr*)a, (XmlExpr*)b);
            break;
        case T_NullTest:
            retval = _equalNullTest((NullTest*)a, (NullTest*)b);
            break;
        case T_HashFilter:
            retval = _equalHashFilter((HashFilter*)a, (HashFilter*)b);
            break;
        case T_BooleanTest:
            retval = _equalBooleanTest((BooleanTest*)a, (BooleanTest*)b);
            break;
        case T_CoerceToDomain:
            retval = _equalCoerceToDomain((CoerceToDomain*)a, (CoerceToDomain*)b);
            break;
        case T_CoerceToDomainValue:
            retval = _equalCoerceToDomainValue((CoerceToDomainValue*)a, (CoerceToDomainValue*)b);
            break;
        case T_SetToDefault:
            retval = _equalSetToDefault((SetToDefault*)a, (SetToDefault*)b);
            break;
        case T_CurrentOfExpr:
            retval = _equalCurrentOfExpr((CurrentOfExpr*)a, (CurrentOfExpr*)b);
            break;
        case T_TargetEntry:
            retval = _equalTargetEntry((TargetEntry*)a, (TargetEntry*)b);
            break;
        case T_PseudoTargetEntry:
            retval = _equalPseudoTargetEntry((PseudoTargetEntry*) a, (PseudoTargetEntry*) b);
            break;
        case T_RangeTblRef:
            retval = _equalRangeTblRef((RangeTblRef*)a, (RangeTblRef*)b);
            break;
        case T_FromExpr:
            retval = _equalFromExpr((FromExpr*)a, (FromExpr*)b);
            break;
        case T_UpsertExpr:
            retval = _equalUpsertExpr((UpsertExpr*)a, (UpsertExpr*)b);
            break;
        case T_JoinExpr:
            retval = _equalJoinExpr((JoinExpr*)a, (JoinExpr*)b);
            break;
        case T_MergeAction:
            retval = _equalMergeAction((MergeAction*)a, (MergeAction*)b);
            break;

            /*
             * RELATION NODES
             */
        case T_PathKey:
            retval = _equalPathKey((PathKey*)a, (PathKey*)b);
            break;
        case T_RestrictInfo:
            retval = _equalRestrictInfo((RestrictInfo*)a, (RestrictInfo*)b);
            break;
        case T_PlaceHolderVar:
            retval = _equalPlaceHolderVar((PlaceHolderVar*)a, (PlaceHolderVar*)b);
            break;
        case T_SpecialJoinInfo:
            retval = _equalSpecialJoinInfo((SpecialJoinInfo*)a, (SpecialJoinInfo*)b);
            break;
        case T_LateralJoinInfo:
            retval = _equalLateralJoinInfo((LateralJoinInfo*)a, (LateralJoinInfo*)b);
            break;
        case T_AppendRelInfo:
            retval = _equalAppendRelInfo((AppendRelInfo*)a, (AppendRelInfo*)b);
            break;
        case T_PlaceHolderInfo:
            retval = _equalPlaceHolderInfo((PlaceHolderInfo*)a, (PlaceHolderInfo*)b);
            break;

        case T_List:
        case T_IntList:
        case T_OidList:
            retval = _equalList((List*)a, (List*)b);
            break;

        case T_Integer:
        case T_Float:
        case T_String:
        case T_BitString:
        case T_Null:
            retval = _equalValue((Value*)a, (Value*)b);
            break;

            /*
             * PARSE NODES
             */
        case T_Query:
            retval = _equalQuery((Query*)a, (Query*)b);
            break;
        case T_InsertStmt:
            retval = _equalInsertStmt((InsertStmt*)a, (InsertStmt*)b);
            break;
        case T_DeleteStmt:
            retval = _equalDeleteStmt((DeleteStmt*)a, (DeleteStmt*)b);
            break;
        case T_UpdateStmt:
            retval = _equalUpdateStmt((UpdateStmt*)a, (UpdateStmt*)b);
            break;
        case T_MergeStmt:
            retval = _equalMergeStmt((MergeStmt*)a, (MergeStmt*)b);
            break;
        case T_MergeWhenClause:
            retval = _equalMergeWhenClause((MergeWhenClause*)a, (MergeWhenClause*)b);
            break;
        case T_SelectStmt:
            retval = _equalSelectStmt((SelectStmt*)a, (SelectStmt*)b);
            break;
        case T_SetOperationStmt:
            retval = _equalSetOperationStmt((SetOperationStmt*)a, (SetOperationStmt*)b);
            break;
        case T_AlterTableStmt:
            retval = _equalAlterTableStmt((AlterTableStmt*)a, (AlterTableStmt*)b);
            break;
        case T_AlterTableCmd:
            retval = _equalAlterTableCmd((AlterTableCmd*)a, (AlterTableCmd*)b);
            break;
        case T_AlterDomainStmt:
            retval = _equalAlterDomainStmt((AlterDomainStmt*)a, (AlterDomainStmt*)b);
            break;
        case T_GrantStmt:
            retval = _equalGrantStmt((GrantStmt*)a, (GrantStmt*)b);
            break;
        case T_GrantRoleStmt:
            retval = _equalGrantRoleStmt((GrantRoleStmt*)a, (GrantRoleStmt*)b);
            break;
        case T_GrantDbStmt:
            retval = _equalGrantDbStmt((GrantDbStmt*)a, (GrantDbStmt*)b);
            break;
        case T_AlterDefaultPrivilegesStmt:
            retval = _equalAlterDefaultPrivilegesStmt((AlterDefaultPrivilegesStmt*)a, (AlterDefaultPrivilegesStmt*)b);
            break;
        case T_DeclareCursorStmt:
            retval = _equalDeclareCursorStmt((DeclareCursorStmt*)a, (DeclareCursorStmt*)b);
            break;
        case T_ClosePortalStmt:
            retval = _equalClosePortalStmt((ClosePortalStmt*)a, (ClosePortalStmt*)b);
            break;
        case T_ClusterStmt:
            retval = _equalClusterStmt((ClusterStmt*)a, (ClusterStmt*)b);
            break;
        case T_CopyStmt:
            retval = _equalCopyStmt((CopyStmt*)a, (CopyStmt*)b);
            break;
        case T_CreateStmt:
            retval = _equalCreateStmt((CreateStmt*)a, (CreateStmt*)b);
            break;
        case T_PartitionState:
            retval = _equalPartitionState((PartitionState*)a, (PartitionState*)b);
            break;
        case T_RangePartitionindexDefState:
            retval =
                _equalRangePartitionindexDefState((RangePartitionindexDefState*)a, (RangePartitionindexDefState*)b);
            break;
        case T_RangePartitionDefState:
            retval = _equalRangePartitionDefState((RangePartitionDefState*)a, (RangePartitionDefState*)b);
            break;
        case T_ListPartitionDefState:
            retval = _equalListPartitionDefState((ListPartitionDefState*)a, (ListPartitionDefState*)b);
            break;
        case T_HashPartitionDefState:
            retval = _equalHashPartitionDefState((HashPartitionDefState*)a, (HashPartitionDefState*)b);
            break;
        case T_IntervalPartitionDefState:
            retval = _equalIntervalPartitionDefState((IntervalPartitionDefState*)a, (IntervalPartitionDefState*)b);
            break;
        case T_RangePartitionStartEndDefState:
            retval = _equalRangePartitionStartEndDefState(
                (RangePartitionStartEndDefState*)a, (RangePartitionStartEndDefState*)b);
            break;
        case T_AddPartitionState:
            retval = _equalAddPartitionState((AddPartitionState*)a, (AddPartitionState*)b);
            break;
        case T_AddSubPartitionState:
            retval = _equalAddSubPartitionState((AddSubPartitionState*)a, (AddSubPartitionState*)b);
            break;
        case T_SplitInfo:
            retval = _equalSplitInfo((SplitInfo*)a, (SplitInfo*)b);
            break;
        case T_DistFdwDataNodeTask:
            retval = _equalDistFdwDataNodeTask((DistFdwDataNodeTask*)a, (DistFdwDataNodeTask*)b);
            break;
        case T_DistFdwFileSegment:
            retval = _equalDistFdwFileSegment((DistFdwFileSegment*)a, (DistFdwFileSegment*)b);
            break;
        case T_SplitPartitionState:
            retval = _equalSplitPartitionState((SplitPartitionState*)a, (SplitPartitionState*)b);
            break;
        case T_TableLikeClause:
            retval = _equalTableLikeClause((TableLikeClause*)a, (TableLikeClause*)b);
            break;
        case T_DefineStmt:
            retval = _equalDefineStmt((DefineStmt*)a, (DefineStmt*)b);
            break;
        case T_DropStmt:
            retval = _equalDropStmt((DropStmt*)a, (DropStmt*)b);
            break;
        case T_TruncateStmt:
            retval = _equalTruncateStmt((TruncateStmt*)a, (TruncateStmt*)b);
            break;
        case T_PurgeStmt:
            retval = EqualPurgeStmt((PurgeStmt*)a, (PurgeStmt*)b);
            break;
        case T_TimeCapsuleStmt:
            retval = EqualTimeCapsuleStmt((TimeCapsuleStmt*)a, (TimeCapsuleStmt*)b);
            break;
        case T_CommentStmt:
            retval = _equalCommentStmt((CommentStmt*)a, (CommentStmt*)b);
            break;
        case T_SecLabelStmt:
            retval = _equalSecLabelStmt((SecLabelStmt*)a, (SecLabelStmt*)b);
            break;
        case T_FetchStmt:
            retval = _equalFetchStmt((FetchStmt*)a, (FetchStmt*)b);
            break;
        case T_IndexStmt:
            retval = _equalIndexStmt((IndexStmt*)a, (IndexStmt*)b);
            break;
        case T_CreateFunctionStmt:
            retval = _equalCreateFunctionStmt((CreateFunctionStmt*)a, (CreateFunctionStmt*)b);
            break;
        case T_FunctionParameter:
            retval = _equalFunctionParameter((FunctionParameter*)a, (FunctionParameter*)b);
            break;
        case T_AlterFunctionStmt:
            retval = _equalAlterFunctionStmt((AlterFunctionStmt*)a, (AlterFunctionStmt*)b);
            break;
        case T_DoStmt:
            retval = _equalDoStmt((DoStmt*)a, (DoStmt*)b);
            break;
        case T_RenameStmt:
            retval = _equalRenameStmt((RenameStmt*)a, (RenameStmt*)b);
            break;
        case T_AlterObjectSchemaStmt:
            retval = _equalAlterObjectSchemaStmt((AlterObjectSchemaStmt*)a, (AlterObjectSchemaStmt*)b);
            break;
        case T_AlterOwnerStmt:
            retval = _equalAlterOwnerStmt((AlterOwnerStmt*)a, (AlterOwnerStmt*)b);
            break;
        case T_RuleStmt:
            retval = _equalRuleStmt((RuleStmt*)a, (RuleStmt*)b);
            break;
        case T_NotifyStmt:
            retval = _equalNotifyStmt((NotifyStmt*)a, (NotifyStmt*)b);
            break;
        case T_ListenStmt:
            retval = _equalListenStmt((ListenStmt*)a, (ListenStmt*)b);
            break;
        case T_UnlistenStmt:
            retval = _equalUnlistenStmt((UnlistenStmt*)a, (UnlistenStmt*)b);
            break;
        case T_TransactionStmt:
            retval = _equalTransactionStmt((TransactionStmt*)a, (TransactionStmt*)b);
            break;
        case T_CompositeTypeStmt:
            retval = _equalCompositeTypeStmt((CompositeTypeStmt*)a, (CompositeTypeStmt*)b);
            break;
        case T_TableOfTypeStmt:
            retval = _equalTableOfTypeStmt((TableOfTypeStmt*)a, (TableOfTypeStmt*)b);
            break;
        case T_CreateEnumStmt:
            retval = _equalCreateEnumStmt((CreateEnumStmt*)a, (CreateEnumStmt*)b);
            break;
        case T_CreateRangeStmt:
            retval = _equalCreateRangeStmt((CreateRangeStmt*)a, (CreateRangeStmt*)b);
            break;
        case T_AlterEnumStmt:
            retval = _equalAlterEnumStmt((AlterEnumStmt*)a, (AlterEnumStmt*)b);
            break;
        case T_ViewStmt:
            retval = _equalViewStmt((ViewStmt*)a, (ViewStmt*)b);
            break;
        case T_LoadStmt:
            retval = _equalLoadStmt((LoadStmt*)a, (LoadStmt*)b);
            break;
        case T_CreateDomainStmt:
            retval = _equalCreateDomainStmt((CreateDomainStmt*)a, (CreateDomainStmt*)b);
            break;
        case T_CreateOpClassStmt:
            retval = _equalCreateOpClassStmt((CreateOpClassStmt*)a, (CreateOpClassStmt*)b);
            break;
        case T_CreateOpClassItem:
            retval = _equalCreateOpClassItem((CreateOpClassItem*)a, (CreateOpClassItem*)b);
            break;
        case T_CreateOpFamilyStmt:
            retval = _equalCreateOpFamilyStmt((CreateOpFamilyStmt*)a, (CreateOpFamilyStmt*)b);
            break;
        case T_AlterOpFamilyStmt:
            retval = _equalAlterOpFamilyStmt((AlterOpFamilyStmt*)a, (AlterOpFamilyStmt*)b);
            break;
        case T_CreatedbStmt:
            retval = _equalCreatedbStmt((CreatedbStmt*)a, (CreatedbStmt*)b);
            break;
        case T_AlterDatabaseStmt:
            retval = _equalAlterDatabaseStmt((AlterDatabaseStmt*)a, (AlterDatabaseStmt*)b);
            break;
        case T_AlterDatabaseSetStmt:
            retval = _equalAlterDatabaseSetStmt((AlterDatabaseSetStmt*)a, (AlterDatabaseSetStmt*)b);
            break;
        case T_DropdbStmt:
            retval = _equalDropdbStmt((DropdbStmt*)a, (DropdbStmt*)b);
            break;
        case T_VacuumStmt:
            retval = _equalVacuumStmt((VacuumStmt*)a, (VacuumStmt*)b);
            break;
        case T_ExplainStmt:
            retval = _equalExplainStmt((ExplainStmt*)a, (ExplainStmt*)b);
            break;
        case T_CreateTableAsStmt:
            retval = _equalCreateTableAsStmt((CreateTableAsStmt*)a, (CreateTableAsStmt*)b);
            break;
        case T_RefreshMatViewStmt:
            retval = _equalRefreshMatViewStmt((RefreshMatViewStmt *)a, (RefreshMatViewStmt *)b);
            break;
        case T_CreateSeqStmt:
            retval = _equalCreateSeqStmt((CreateSeqStmt*)a, (CreateSeqStmt*)b);
            break;
        case T_ReplicaIdentityStmt:
            retval = _equalReplicaIdentityStmt((ReplicaIdentityStmt*)a, (ReplicaIdentityStmt*)b);
            break;
#ifndef ENABLE_MULTIPLE_NODES
        case T_AlterSystemStmt:
            retval = _equalAlterSystemStmt((AlterSystemStmt*)a, (AlterSystemStmt*)b);
            break;
#endif
        case T_AlterSeqStmt:
            retval = _equalAlterSeqStmt((AlterSeqStmt*)a, (AlterSeqStmt*)b);
            break;
        case T_VariableSetStmt:
            retval = _equalVariableSetStmt((VariableSetStmt*)a, (VariableSetStmt*)b);
            break;
        case T_VariableShowStmt:
            retval = _equalVariableShowStmt((VariableShowStmt*)a, (VariableShowStmt*)b);
            break;
        case T_DiscardStmt:
            retval = _equalDiscardStmt((DiscardStmt*)a, (DiscardStmt*)b);
            break;
        case T_CreateTableSpaceStmt:
            retval = _equalCreateTableSpaceStmt((CreateTableSpaceStmt*)a, (CreateTableSpaceStmt*)b);
            break;
        case T_DropTableSpaceStmt:
            retval = _equalDropTableSpaceStmt((DropTableSpaceStmt*)a, (DropTableSpaceStmt*)b);
            break;
        case T_AlterTableSpaceOptionsStmt:
            retval = _equalAlterTableSpaceOptionsStmt((AlterTableSpaceOptionsStmt*)a, (AlterTableSpaceOptionsStmt*)b);
            break;
        case T_CreateExtensionStmt:
            retval = _equalCreateExtensionStmt((CreateExtensionStmt*)a, (CreateExtensionStmt*)b);
            break;
        case T_AlterExtensionStmt:
            retval = _equalAlterExtensionStmt((AlterExtensionStmt*)a, (AlterExtensionStmt*)b);
            break;
        case T_AlterExtensionContentsStmt:
            retval = _equalAlterExtensionContentsStmt((AlterExtensionContentsStmt*)a, (AlterExtensionContentsStmt*)b);
            break;
        case T_CreateFdwStmt:
            retval = _equalCreateFdwStmt((CreateFdwStmt*)a, (CreateFdwStmt*)b);
            break;
        case T_AlterFdwStmt:
            retval = _equalAlterFdwStmt((AlterFdwStmt*)a, (AlterFdwStmt*)b);
            break;
        case T_CreateForeignServerStmt:
            retval = _equalCreateForeignServerStmt((CreateForeignServerStmt*)a, (CreateForeignServerStmt*)b);
            break;
        case T_AlterForeignServerStmt:
            retval = _equalAlterForeignServerStmt((AlterForeignServerStmt*)a, (AlterForeignServerStmt*)b);
            break;
        case T_CreateUserMappingStmt:
            retval = _equalCreateUserMappingStmt((CreateUserMappingStmt*)a, (CreateUserMappingStmt*)b);
            break;
        case T_AlterUserMappingStmt:
            retval = _equalAlterUserMappingStmt((AlterUserMappingStmt*)a, (AlterUserMappingStmt*)b);
            break;
        case T_DropUserMappingStmt:
            retval = _equalDropUserMappingStmt((DropUserMappingStmt*)a, (DropUserMappingStmt*)b);
            break;
        case T_CreateForeignTableStmt:
            retval = _equalCreateForeignTableStmt((CreateForeignTableStmt*)a, (CreateForeignTableStmt*)b);
            break;
        case T_CreateDataSourceStmt:
            retval = _equalCreateDataSourceStmt((CreateDataSourceStmt*)a, (CreateDataSourceStmt*)b);
            break;
        case T_AlterSchemaStmt:
            retval = _equalAlterSchemaStmt((AlterSchemaStmt*)a, (AlterSchemaStmt*)b);
            break;
        case T_AlterDataSourceStmt:
            retval = _equalAlterDataSourceStmt((AlterDataSourceStmt*)a, (AlterDataSourceStmt*)b);
            break;
        case T_CreateRlsPolicyStmt:
            retval = _equalCreateRlsPolicyStmt((CreateRlsPolicyStmt*)a, (CreateRlsPolicyStmt*)b);
            break;
        case T_AlterRlsPolicyStmt:
            retval = _equalAlterRlsPolicyStmt((AlterRlsPolicyStmt*)a, (AlterRlsPolicyStmt*)b);
            break;
        case T_CreateWeakPasswordDictionaryStmt:
            retval = _equalCreateWeakPasswordDictionaryStmt((CreateWeakPasswordDictionaryStmt*)a, (CreateWeakPasswordDictionaryStmt*)b);
            break;
        case T_DropWeakPasswordDictionaryStmt:
            retval = _equalDropWeakPasswordDictionaryStmt((DropWeakPasswordDictionaryStmt*)a, (DropWeakPasswordDictionaryStmt*)b);
            break;
        case T_CreatePolicyLabelStmt:
            retval = _equalCreatePolicyLabelStmt((CreatePolicyLabelStmt*)a, (CreatePolicyLabelStmt*)b);
            break;
        case T_AlterPolicyLabelStmt:
            retval = _equalAlterPolicyLabelStmt((AlterPolicyLabelStmt*)a, (AlterPolicyLabelStmt*)b);
            break;
        case T_DropPolicyLabelStmt:
            retval = _equalDropPolicyLabelStmt((DropPolicyLabelStmt*)a, (DropPolicyLabelStmt*)b);
            break;
        case T_CreateAuditPolicyStmt:
            retval = _equalCreateAuditPolicyStmt((CreateAuditPolicyStmt*)a, (CreateAuditPolicyStmt*)b);
            break;
        case T_AlterAuditPolicyStmt:
            retval = _equalAlterAuditPolicyStmt((AlterAuditPolicyStmt*)a, (AlterAuditPolicyStmt*)b);
            break;
        case T_DropAuditPolicyStmt:
            retval = _equalDropAuditPolicyStmt((DropAuditPolicyStmt*)a, (DropAuditPolicyStmt*)b);
            break;
        case T_CreateMaskingPolicyStmt:
            retval = _equalCreateMaskingPolicyStmt((CreateMaskingPolicyStmt*)a, (CreateMaskingPolicyStmt*)b);
            break;
        case T_AlterMaskingPolicyStmt:
            retval = _equalAlterMaskingPolicyStmt((AlterMaskingPolicyStmt*)a, (AlterMaskingPolicyStmt*)b);
            break;
        case T_DropMaskingPolicyStmt:
            retval = _equalDropMaskingPolicyStmt((DropMaskingPolicyStmt*)a, (DropMaskingPolicyStmt*)b);
            break;
        case T_MaskingPolicyCondition:
            retval = _equalMaskingPolicyCondition((MaskingPolicyCondition*)a, (MaskingPolicyCondition*)b);
            break;
        case T_PolicyFilterNode:
            retval = _equalPolicyFilterNode((PolicyFilterNode*)a, (PolicyFilterNode*)b);
            break;
        case T_CreateTrigStmt:
            retval = _equalCreateTrigStmt((CreateTrigStmt*)a, (CreateTrigStmt*)b);
            break;
        case T_CreatePLangStmt:
            retval = _equalCreatePLangStmt((CreatePLangStmt*)a, (CreatePLangStmt*)b);
            break;
        case T_CreateRoleStmt:
            retval = _equalCreateRoleStmt((CreateRoleStmt*)a, (CreateRoleStmt*)b);
            break;
        case T_AlterRoleStmt:
            retval = _equalAlterRoleStmt((AlterRoleStmt*)a, (AlterRoleStmt*)b);
            break;
        case T_AlterRoleSetStmt:
            retval = _equalAlterRoleSetStmt((AlterRoleSetStmt*)a, (AlterRoleSetStmt*)b);
            break;
        case T_DropRoleStmt:
            retval = _equalDropRoleStmt((DropRoleStmt*)a, (DropRoleStmt*)b);
            break;
        case T_LockStmt:
            retval = _equalLockStmt((LockStmt*)a, (LockStmt*)b);
            break;
        case T_ConstraintsSetStmt:
            retval = _equalConstraintsSetStmt((ConstraintsSetStmt*)a, (ConstraintsSetStmt*)b);
            break;
        case T_ReindexStmt:
            retval = _equalReindexStmt((ReindexStmt*)a, (ReindexStmt*)b);
            break;
        case T_CheckPointStmt:
            retval = true;
            break;
#ifdef PGXC
        case T_BarrierStmt:
            retval = _equalBarrierStmt((BarrierStmt*)a, (BarrierStmt*)b);
            break;
        case T_AlterNodeStmt:
            retval = _equalAlterNodeStmt((AlterNodeStmt*)a, (AlterNodeStmt*)b);
            break;
        case T_CreateNodeStmt:
            retval = _equalCreateNodeStmt((CreateNodeStmt*)a, (CreateNodeStmt*)b);
            break;
        case T_DropNodeStmt:
            retval = _equalDropNodeStmt((DropNodeStmt*)a, (DropNodeStmt*)b);
            break;
        case T_CreateGroupStmt:
            retval = _equalCreateGroupStmt((CreateGroupStmt*)a, (CreateGroupStmt*)b);
            break;
        case T_AlterGroupStmt:
            retval = _equalAlterGroupStmt((AlterGroupStmt*)a, (AlterGroupStmt*)b);
            break;
        case T_DropGroupStmt:
            retval = _equalDropGroupStmt((DropGroupStmt*)a, (DropGroupStmt*)b);
            break;
        case T_CreateResourcePoolStmt:
            retval = _equalCreateResourcePoolStmt((CreateResourcePoolStmt*)a, (CreateResourcePoolStmt*)b);
            break;
        case T_AlterResourcePoolStmt:
            retval = _equalAlterResourcePoolStmt((AlterResourcePoolStmt*)a, (AlterResourcePoolStmt*)b);
            break;
        case T_DropResourcePoolStmt:
            retval = _equalDropResourcePoolStmt((DropResourcePoolStmt*)a, (DropResourcePoolStmt*)b);
            break;
        case T_CreateWorkloadGroupStmt:
            retval = _equalCreateWorkloadGroupStmt((CreateWorkloadGroupStmt*)a, (CreateWorkloadGroupStmt*)b);
            break;
        case T_AlterWorkloadGroupStmt:
            retval = _equalAlterWorkloadGroupStmt((AlterWorkloadGroupStmt*)a, (AlterWorkloadGroupStmt*)b);
            break;
        case T_DropWorkloadGroupStmt:
            retval = _equalDropWorkloadGroupStmt((DropWorkloadGroupStmt*)a, (DropWorkloadGroupStmt*)b);
            break;
        case T_CreateAppWorkloadGroupMappingStmt:
            retval = _equalCreateAppWorkloadGroupMappingStmt(
                (CreateAppWorkloadGroupMappingStmt*)a, (CreateAppWorkloadGroupMappingStmt*)b);
            break;
        case T_AlterAppWorkloadGroupMappingStmt:
            retval = _equalAlterAppWorkloadGroupMappingStmt(
                (AlterAppWorkloadGroupMappingStmt*)a, (AlterAppWorkloadGroupMappingStmt*)b);
            break;
        case T_DropAppWorkloadGroupMappingStmt:
            retval = _equalDropAppWorkloadGroupMappingStmt(
                (DropAppWorkloadGroupMappingStmt*)a, (DropAppWorkloadGroupMappingStmt*)b);
            break;
        case T_CleanConnStmt:
            retval = _equalCleanConnStmt((CleanConnStmt*)a, (CleanConnStmt*)b);
            break;
#endif
        case T_CreateSchemaStmt:
            retval = _equalCreateSchemaStmt((CreateSchemaStmt*)a, (CreateSchemaStmt*)b);
            break;
        case T_CreateConversionStmt:
            retval = _equalCreateConversionStmt((CreateConversionStmt*)a, (CreateConversionStmt*)b);
            break;
        case T_CreateCastStmt:
            retval = _equalCreateCastStmt((CreateCastStmt*)a, (CreateCastStmt*)b);
            break;
        case T_PrepareStmt:
            retval = _equalPrepareStmt((PrepareStmt*)a, (PrepareStmt*)b);
            break;
        case T_ExecuteStmt:
            retval = _equalExecuteStmt((ExecuteStmt*)a, (ExecuteStmt*)b);
            break;
        case T_DeallocateStmt:
            retval = _equalDeallocateStmt((DeallocateStmt*)a, (DeallocateStmt*)b);
            break;
        case T_DropOwnedStmt:
            retval = _equalDropOwnedStmt((DropOwnedStmt*)a, (DropOwnedStmt*)b);
            break;
        case T_ReassignOwnedStmt:
            retval = _equalReassignOwnedStmt((ReassignOwnedStmt*)a, (ReassignOwnedStmt*)b);
            break;
        case T_AlterTSDictionaryStmt:
            retval = _equalAlterTSDictionaryStmt((AlterTSDictionaryStmt*)a, (AlterTSDictionaryStmt*)b);
            break;
        case T_AlterTSConfigurationStmt:
            retval = _equalAlterTSConfigurationStmt((AlterTSConfigurationStmt*)a, (AlterTSConfigurationStmt*)b);
            break;
        case T_ShutdownStmt:
            retval = _equalShutDown((ShutdownStmt*)a, (ShutdownStmt*)b);
            break;

        case T_A_Expr:
            retval = _equalAExpr((A_Expr*)a, (A_Expr*)b);
            break;
        case T_ColumnRef:
            retval = _equalColumnRef((ColumnRef*)a, (ColumnRef*)b);
            break;
        case T_ParamRef:
            retval = _equalParamRef((ParamRef*)a, (ParamRef*)b);
            break;
        case T_A_Const:
            retval = _equalAConst((A_Const*)a, (A_Const*)b);
            break;
        case T_FuncCall:
            retval = _equalFuncCall((FuncCall*)a, (FuncCall*)b);
            break;
        case T_A_Star:
            retval = _equalAStar((A_Star*)a, (A_Star*)b);
            break;
        case T_A_Indices:
            retval = _equalAIndices((A_Indices*)a, (A_Indices*)b);
            break;
        case T_A_Indirection:
            retval = _equalA_Indirection((A_Indirection*)a, (A_Indirection*)b);
            break;
        case T_A_ArrayExpr:
            retval = _equalA_ArrayExpr((A_ArrayExpr*)a, (A_ArrayExpr*)b);
            break;
        case T_ResTarget:
            retval = _equalResTarget((ResTarget*)a, (ResTarget*)b);
            break;
        case T_TypeCast:
            retval = _equalTypeCast((TypeCast*)a, (TypeCast*)b);
            break;
        case T_CollateClause:
            retval = _equalCollateClause((CollateClause*)a, (CollateClause*)b);
            break;
        case T_ClientLogicGlobalParam:
            retval = _equalGlobalParam((ClientLogicGlobalParam*)a, (ClientLogicGlobalParam*)b);
            break;
        case T_ClientLogicColumnParam:
            retval = _equalColumnParam((ClientLogicColumnParam*)a, (ClientLogicColumnParam*)b);
            break;
        case T_CreateClientLogicGlobal:
            retval = _equalGlobalSetting((CreateClientLogicGlobal*)a, (CreateClientLogicGlobal*)b);
            break;
        case T_CreateClientLogicColumn:
            retval = _equalColumnSetting((CreateClientLogicColumn*)a, (CreateClientLogicColumn*)b);
            break;
        case T_ClientLogicColumnRef:
            retval = _equalEncryptedColumn((ClientLogicColumnRef*)a, (ClientLogicColumnRef*)b);
            break;
        case T_SortBy:
            retval = _equalSortBy((SortBy*)a, (SortBy*)b);
            break;
        case T_WindowDef:
            retval = _equalWindowDef((WindowDef*)a, (WindowDef*)b);
            break;
        case T_RangeSubselect:
            retval = _equalRangeSubselect((RangeSubselect*)a, (RangeSubselect*)b);
            break;
        case T_RangeFunction:
            retval = _equalRangeFunction((RangeFunction*)a, (RangeFunction*)b);
            break;
        case T_RangeTableSample:
            retval = _equalRangeTableSample((RangeTableSample*)a, (RangeTableSample*)b);
            break;
        case T_RangeTimeCapsule:
            retval = EqualRangeTimeCapsule((RangeTimeCapsule*)a, (RangeTimeCapsule*)b);
            break;
        case T_TypeName:
            retval = _equalTypeName((TypeName*)a, (TypeName*)b);
            break;
        case T_IndexElem:
            retval = _equalIndexElem((IndexElem*)a, (IndexElem*)b);
            break;
        case T_ColumnDef:
            retval = _equalColumnDef((ColumnDef*)a, (ColumnDef*)b);
            break;
        case T_Constraint:
            retval = _equalConstraint((Constraint*)a, (Constraint*)b);
            break;
        case T_DefElem:
            retval = _equalDefElem((DefElem*)a, (DefElem*)b);
            break;
        case T_LockingClause:
            retval = _equalLockingClause((LockingClause*)a, (LockingClause*)b);
            break;
        case T_RangeTblEntry:
            retval = _equalRangeTblEntry((RangeTblEntry*)a, (RangeTblEntry*)b);
            break;
        case T_TableSampleClause:
            retval = _equalTableSampleClause((TableSampleClause*)a, (TableSampleClause*)b);
            break;
        case T_TimeCapsuleClause:
            retval = EqualTimeCapsuleClause((TimeCapsuleClause*)a, (TimeCapsuleClause*)b);
            break;
        case T_SortGroupClause:
            retval = _equalSortGroupClause((SortGroupClause*)a, (SortGroupClause*)b);
            break;
        case T_GroupingSet:
            retval = _equalGroupingSet((GroupingSet*)a, (GroupingSet*)b);
            break;
        case T_WindowClause:
            retval = _equalWindowClause((WindowClause*)a, (WindowClause*)b);
            break;
        case T_RowMarkClause:
            retval = _equalRowMarkClause((RowMarkClause*)a, (RowMarkClause*)b);
            break;
        case T_WithClause:
            retval = _equalWithClause((WithClause*)a, (WithClause*)b);
            break;
        case T_StartWithClause:
            retval = _equalStartWithClause((StartWithClause*) a, (StartWithClause*) b);
            break;
        case T_UpsertClause:
            retval = _equalUpsertClause((UpsertClause*)a, (UpsertClause*)b);
            break;
        case T_StartWithTargetRelInfo:
            retval = _equalStartWithTargetRelInfo((StartWithTargetRelInfo *)a,
                                                  (StartWithTargetRelInfo *)b);
            break;
        case T_CommonTableExpr:
            retval = _equalCommonTableExpr((CommonTableExpr*)a, (CommonTableExpr*)b);
            break;
        case T_StartWithOptions:
            retval = _equalStartWithOptions((StartWithOptions*) a, (StartWithOptions*) b);
            break;
        case T_PrivGrantee:
            retval = _equalPrivGrantee((PrivGrantee*)a, (PrivGrantee*)b);
            break;
        case T_FuncWithArgs:
            retval = _equalFuncWithArgs((FuncWithArgs*)a, (FuncWithArgs*)b);
            break;
        case T_AccessPriv:
            retval = _equalAccessPriv((AccessPriv*)a, (AccessPriv*)b);
            break;
        case T_DbPriv:
            retval = _equalDbPriv((DbPriv*)a, (DbPriv*)b);
            break;
        case T_XmlSerialize:
            retval = _equalXmlSerialize((XmlSerialize*)a, (XmlSerialize*)b);
            break;
        case T_GroupingId:
            retval = _equalGroupingId((GroupingId*)a, (GroupingId*)b);
            break;
        case T_CreateDirectoryStmt:
            retval = _equalCreateDirectoryStmt((CreateDirectoryStmt*)a, (CreateDirectoryStmt*)b);
            break;
        case T_DropDirectoryStmt:
            retval = _equalDropDirectoryStmt((DropDirectoryStmt*)a, (DropDirectoryStmt*)b);
            break;
        case T_CreateSynonymStmt:
            retval = _equalCreateSynonymStmt((CreateSynonymStmt*)a, (CreateSynonymStmt*)b);
            break;
        case T_DropSynonymStmt:
            retval = _equalDropSynonymStmt((DropSynonymStmt*)a, (DropSynonymStmt*)b);
            break;
        case T_Rownum:
            retval = _equalRownum((Rownum*)a, (Rownum*)b);
            break;
        case T_CreatePublicationStmt:
            retval = _equalCreatePublicationStmt((CreatePublicationStmt *)a, (CreatePublicationStmt *)b);
            break;
        case T_AlterPublicationStmt:
            retval = _equalAlterPublicationStmt((AlterPublicationStmt *)a, (AlterPublicationStmt *)b);
            break;
        case T_CreateSubscriptionStmt:
            retval = _equalCreateSubscriptionStmt((CreateSubscriptionStmt *)a, (CreateSubscriptionStmt *)b);
            break;
        case T_AlterSubscriptionStmt:
            retval = _equalAlterSubscriptionStmt((AlterSubscriptionStmt *)a, (AlterSubscriptionStmt *)b);
            break;
        case T_DropSubscriptionStmt:
            retval = _equalDropSubscriptionStmt((DropSubscriptionStmt *)a, (DropSubscriptionStmt *)b);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(a))));
            retval = false; /* keep compiler quiet */
            break;
    }

    return retval;
}
