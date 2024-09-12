/* -------------------------------------------------------------------------
 *
 * nodeFuncs.cpp
 *		Various general-purpose manipulations of Node trees
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/nodeFuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifdef FRONTEND_PARSER
#include "postgres_fe.h"
#include "nodes/parsenodes_common.h"
#else
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "optimizer/streamplan.h"
#include "parser/parse_expr.h"
#endif /* FRONTEND_PARSER */
#include "storage/tcap.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"

static bool query_check_no_flt_walker(Node* node, void* context);
static bool query_check_srf_walker(Node* node, void* context);
static bool expression_returns_set_walker(Node* node, void* context);
static bool expression_rownum_walker(Node* node, void* context);
static int leftmostLoc(int loc1, int loc2);
static void AssertExprCollation(const Node* expr, Oid collation);
Oid userSetElemTypeCollInfo(const Node* expr, Oid (*exprFunc)(const Node*));

/*
 *	exprType -
 *	  returns the Oid of the type of the expression's result.
 */
Oid exprType(const Node* expr)
{
    Oid type;

    if (NULL == expr) {
        return InvalidOid;
    }

    switch (nodeTag(expr)) {
        case T_BoolExpr:
        case T_BooleanTest:
        case T_CurrentOfExpr:
        case T_HashFilter:
        case T_NullTest:
        case T_NanTest:
        case T_InfiniteTest:
        case T_ScalarArrayOpExpr:
        case T_RowCompareExpr:
            type = BOOLOID;
            break;
        case T_GroupingFunc:
        case T_GroupingId:
            type = INT4OID;
            break;
        case T_Var:
            type = ((const Var*)expr)->vartype;
            break;
        case T_Const:
            type = ((const Const*)expr)->consttype;
            break;
        case T_UserVar:
            type = exprType((const Node*)(((UserVar*)expr)->value));
            break;
        case T_Param:
            type = ((const Param*)expr)->paramtype;
            break;
        case T_Aggref:
            type = ((const Aggref*)expr)->aggtype;
            break;
        case T_WindowFunc:
            type = ((const WindowFunc*)expr)->wintype;
            break;
        case T_ArrayRef: {
            const ArrayRef* arrayref = (const ArrayRef*)expr;

            /* slice and/or store operations yield the array type */
            if (arrayref->reflowerindexpr || arrayref->refassgnexpr) {
                type = arrayref->refarraytype;
            } else {
                type = arrayref->refelemtype;
            }
        } break;
        case T_FuncExpr:
            type = ((const FuncExpr*)expr)->funcresulttype;
            break;
        case T_NamedArgExpr:
            type = exprType((Node*)((const NamedArgExpr*)expr)->arg);
            break;
        case T_OpExpr:
            type = ((const OpExpr*)expr)->opresulttype;
            break;
        case T_DistinctExpr:
            type = ((const DistinctExpr*)expr)->opresulttype;
            break;
        case T_NullIfExpr:
            type = ((const NullIfExpr*)expr)->opresulttype;
            break;
        case T_SubLink: {
            const SubLink* sublink = (const SubLink*)expr;

            if (sublink->subLinkType == EXPR_SUBLINK || sublink->subLinkType == ARRAY_SUBLINK) {
                /* get the type of the subselect's first target column */
                Query* qtree = (Query*)sublink->subselect;
                TargetEntry* tent = NULL;

                if (qtree == NULL || !IsA(qtree, Query)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("cannot get type for untransformed sublink")));
                }
                tent = (TargetEntry*)linitial(qtree->targetList);
                Assert(IsA(tent, TargetEntry));
                Assert(!tent->resjunk);
                type = exprType((Node*)tent->expr);
                if (sublink->subLinkType == ARRAY_SUBLINK) {
                    type = get_array_type(type);
                    if (!OidIsValid(type)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("could not find array type for data type %s",
                                    format_type_be(exprType((Node*)tent->expr)))));
                    }
                }
            } else {
                /* for all other sublink types, result is boolean */
                type = BOOLOID;
            }
        } break;
        case T_SubPlan: {
            const SubPlan* subplan = (const SubPlan*)expr;

            if (subplan->subLinkType == EXPR_SUBLINK || subplan->subLinkType == ARRAY_SUBLINK) {
                /* get the type of the subselect's first target column */
                type = subplan->firstColType;
                if (subplan->subLinkType == ARRAY_SUBLINK) {
                    type = get_array_type(type);
                    if (!OidIsValid(type)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("could not find array type for data type %s",
                                    format_type_be(subplan->firstColType))));
                    }
                }
            } else {
                /* for all other subplan types, result is boolean */
                type = BOOLOID;
            }
        } break;
        case T_AlternativeSubPlan: {
            const AlternativeSubPlan* asplan = (const AlternativeSubPlan*)expr;

            /* subplans should all return the same thing */
            type = exprType((Node*)linitial(asplan->subplans));
        } break;
        case T_FieldSelect:
            type = ((const FieldSelect*)expr)->resulttype;
            break;
        case T_FieldStore:
            type = ((const FieldStore*)expr)->resulttype;
            break;
        case T_RelabelType:
            type = ((const RelabelType*)expr)->resulttype;
            break;
        case T_CoerceViaIO:
            type = ((const CoerceViaIO*)expr)->resulttype;
            break;
        case T_ArrayCoerceExpr:
            type = ((const ArrayCoerceExpr*)expr)->resulttype;
            break;
        case T_ConvertRowtypeExpr:
            type = ((const ConvertRowtypeExpr*)expr)->resulttype;
            break;
        case T_CollateExpr:
            type = exprType((Node*)((const CollateExpr*)expr)->arg);
            break;
        case T_CaseExpr:
            type = ((const CaseExpr*)expr)->casetype;
            break;
        case T_CaseTestExpr:
            type = ((const CaseTestExpr*)expr)->typeId;
            break;
        case T_ArrayExpr:
            type = ((const ArrayExpr*)expr)->array_typeid;
            break;
        case T_RowExpr:
            type = ((const RowExpr*)expr)->row_typeid;
            break;
        case T_CoalesceExpr:
            type = ((const CoalesceExpr*)expr)->coalescetype;
            break;
        case T_MinMaxExpr:
            type = ((const MinMaxExpr*)expr)->minmaxtype;
            break;
        case T_XmlExpr:
            if (((const XmlExpr*)expr)->op == IS_DOCUMENT) {
                type = BOOLOID;
            } else if (((const XmlExpr*)expr)->op == IS_XMLSERIALIZE) {
                type = TEXTOID;
            } else {
                type = XMLOID;
            }
            break;
        case T_CoerceToDomain:
            type = ((const CoerceToDomain*)expr)->resulttype;
            break;
        case T_CoerceToDomainValue:
            type = ((const CoerceToDomainValue*)expr)->typeId;
            break;
        case T_SetToDefault:
            type = ((const SetToDefault*)expr)->typeId;
            break;
        case T_PlaceHolderVar:
            type = exprType((Node*)((const PlaceHolderVar*)expr)->phexpr);
            break;
        case T_Rownum:
            if (ROWNUM_TYPE_COMPAT) {
                type = NUMERICOID;
            } else {
                type = INT8OID;
            }
            break;
        case T_PrefixKey:
            type = exprType((Node*)((PrefixKey*)expr)->arg);
            break;
        case T_SetVariableExpr:
            type = ((const Const*)(((SetVariableExpr*)expr)->value))->consttype;
            break;
        case T_UserSetElem:
            type = userSetElemTypeCollInfo(expr, exprType);
            break;
#ifdef USE_SPQ
        case T_DMLActionExpr:
            type = INT4OID;
            break;
#endif
        case T_PriorExpr:
            type = exprType(((const PriorExpr*)expr)->node);
            break;

        case T_CursorExpression:
             type = REFCURSOROID;
             break;
        case T_TypeCast:
            {
                TypeCast *tc = (TypeCast*)expr;
                if (tc->typname == NULL || !OidIsValid(tc->typname->typeOid)) {
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid typecast node")));
                }
                type = tc->typname->typeOid;
                break;
            }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(expr))));
            type = InvalidOid; /* keep compiler quiet */
            break;
    }
    return type;
}

/*
 *	exprTypmod -
 *	  returns the type-specific modifier of the expression's result type,
 *	  if it can be determined.	In many cases, it can't and we return -1.
 */
int32 exprTypmod(const Node* expr)
{
    if (NULL == expr) {
        return -1;
    }

    switch (nodeTag(expr)) {
        case T_Var:
            return ((const Var*)expr)->vartypmod;
        case T_Const:
            return ((const Const*)expr)->consttypmod;
        case T_UserVar:
            return ((const Const*)(((UserVar*)expr)->value))->consttypmod;
        case T_Param:
            return ((const Param*)expr)->paramtypmod;
        case T_ArrayRef:
            /* typmod is the same for array or element */
            return ((const ArrayRef*)expr)->reftypmod;
        case T_FuncExpr: {
            int32 coercedTypmod;

            /* Be smart about length-coercion functions... */
            if (exprIsLengthCoercion(expr, &coercedTypmod)) {
                return coercedTypmod;
            }
            FuncExpr *fexpr = (FuncExpr *)expr;
            if (IsClientLogicType(fexpr->funcresulttype)) {
                return fexpr->funcresulttype_orig;
            }
        } break;
        case T_NamedArgExpr:
            return exprTypmod((Node*)((const NamedArgExpr*)expr)->arg);
        case T_NullIfExpr: {
            /*
             * Result is either first argument or NULL, so we can report
             * first argument's typmod if known.
             */
            const NullIfExpr* nexpr = (const NullIfExpr*)expr;

            return exprTypmod((Node*)linitial(nexpr->args));
        } break;
        case T_SubLink: {
            const SubLink* sublink = (const SubLink*)expr;

            if (sublink->subLinkType == EXPR_SUBLINK || sublink->subLinkType == ARRAY_SUBLINK) {
                /* get the typmod of the subselect's first target column */
                Query* qtree = (Query*)sublink->subselect;
                TargetEntry* tent = NULL;

                if (qtree == NULL || !IsA(qtree, Query)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("cannot get type for untransformed sublink")));
                }
                tent = (TargetEntry*)linitial(qtree->targetList);
                Assert(IsA(tent, TargetEntry));
                Assert(!tent->resjunk);
                return exprTypmod((Node*)tent->expr);
                /* note we don't need to care if it's an array */
            }
        } break;
        case T_SubPlan: {
            const SubPlan* subplan = (const SubPlan*)expr;

            if (subplan->subLinkType == EXPR_SUBLINK || subplan->subLinkType == ARRAY_SUBLINK) {
                /* get the typmod of the subselect's first target column */
                /* note we don't need to care if it's an array */
                return subplan->firstColTypmod;
            } else {
                /* for all other subplan types, result is boolean */
                return -1;
            }
        } break;
        case T_AlternativeSubPlan: {
            const AlternativeSubPlan* asplan = (const AlternativeSubPlan*)expr;

            /* subplans should all return the same thing */
            return exprTypmod((Node*)linitial(asplan->subplans));
        } break;
        case T_FieldSelect:
            return ((const FieldSelect*)expr)->resulttypmod;
        case T_RelabelType:
            return ((const RelabelType*)expr)->resulttypmod;
        case T_ArrayCoerceExpr:
            return ((const ArrayCoerceExpr*)expr)->resulttypmod;
        case T_CollateExpr:
            return exprTypmod((Node*)((const CollateExpr*)expr)->arg);
        case T_CaseExpr: {
            /*
             * If all the alternatives agree on type/typmod, return that
             * typmod, else use -1
             */
            const CaseExpr* cexpr = (const CaseExpr*)expr;
            Oid casetype = cexpr->casetype;
            int32 typmod;
            ListCell* arg = NULL;

            if (!cexpr->defresult) {
                return -1;
            }
            if (exprType((Node*)cexpr->defresult) != casetype) {
                return -1;
            }
            typmod = exprTypmod((Node*)cexpr->defresult);
            if (typmod < 0) {
                return -1; /* no point in trying harder */
            }
            foreach (arg, cexpr->args) {
                CaseWhen* w = (CaseWhen*)lfirst(arg);

                Assert(IsA(w, CaseWhen));
                if (exprType((Node*)w->result) != casetype) {
                    return -1;
                }
                if (exprTypmod((Node*)w->result) != typmod) {
                    return -1;
                }
            }
            return typmod;
        } break;
        case T_CaseTestExpr:
            return ((const CaseTestExpr*)expr)->typeMod;
        case T_ArrayExpr: {
            /*
             * If all the elements agree on type/typmod, return that
             * typmod, else use -1
             */
            const ArrayExpr* arrayexpr = (const ArrayExpr*)expr;
            Oid commontype;
            int32 typmod;
            ListCell* elem = NULL;

            if (arrayexpr->elements == NIL) {
                return -1;
            }
            typmod = exprTypmod((Node*)linitial(arrayexpr->elements));
            if (typmod < 0) {
                return -1; /* no point in trying harder */
            }
            if (arrayexpr->multidims) {
                commontype = arrayexpr->array_typeid;
            } else {
                commontype = arrayexpr->element_typeid;
            }
            foreach (elem, arrayexpr->elements) {
                Node* e = (Node*)lfirst(elem);

                if (exprType(e) != commontype) {
                    return -1;
                }
                if (exprTypmod(e) != typmod) {
                    return -1;
                }
            }
            return typmod;
        } break;
        case T_CoalesceExpr: {
            /*
             * If all the alternatives agree on type/typmod, return that
             * typmod, else use -1
             */
            const CoalesceExpr* cexpr = (const CoalesceExpr*)expr;
            Oid coalescetype = cexpr->coalescetype;
            int32 typmod;
            ListCell* arg = NULL;

            if (exprType((Node*)linitial(cexpr->args)) != coalescetype) {
                return -1;
            }
            typmod = exprTypmod((Node*)linitial(cexpr->args));
            if (typmod < 0) {
                return -1; /* no point in trying harder */
            }
            for_each_cell(arg, lnext(list_head(cexpr->args)))
            {
                Node* e = (Node*)lfirst(arg);

                if (exprType(e) != coalescetype) {
                    return -1;
                }
                if (exprTypmod(e) != typmod) {
                    return -1;
                }
            }
            return typmod;
        } break;
        case T_MinMaxExpr: {
            /*
             * If all the alternatives agree on type/typmod, return that
             * typmod, else use -1
             */
            const MinMaxExpr* mexpr = (const MinMaxExpr*)expr;
            Oid minmaxtype = mexpr->minmaxtype;
            int32 typmod;
            ListCell* arg = NULL;

            if (exprType((Node*)linitial(mexpr->args)) != minmaxtype) {
                return -1;
            }
            typmod = exprTypmod((Node*)linitial(mexpr->args));
            if (typmod < 0) {
                return -1; /* no point in trying harder */
            }
            for_each_cell(arg, lnext(list_head(mexpr->args)))
            {
                Node* e = (Node*)lfirst(arg);

                if (exprType(e) != minmaxtype) {
                    return -1;
                }
                if (exprTypmod(e) != typmod) {
                    return -1;
                }
            }
            return typmod;
        } break;
        case T_CoerceToDomain:
            return ((const CoerceToDomain*)expr)->resulttypmod;
        case T_CoerceToDomainValue:
            return ((const CoerceToDomainValue*)expr)->typeMod;
        case T_SetToDefault:
            return ((const SetToDefault*)expr)->typeMod;
        case T_PlaceHolderVar:
            return exprTypmod((Node*)((const PlaceHolderVar*)expr)->phexpr);
        case T_PrefixKey:
            return exprTypmod((Node*)((const PrefixKey*)expr)->arg);
        case T_SetVariableExpr:
            return ((const Const*)(((SetVariableExpr*)expr)->value))->consttypmod;
        case T_PriorExpr:
            return exprTypmod((Node*)((const PriorExpr*)expr)->node);
        default:
            break;
    }
    return -1;
}

/*
 * exprIsLengthCoercion
 *		Detect whether an expression tree is an application of a datatype's
 *		typmod-coercion function.  Optionally extract the result's typmod.
 *
 * If coercedTypmod is not NULL, the typmod is stored there if the expression
 * is a length-coercion function, else -1 is stored there.
 *
 * Note that a combined type-and-length coercion will be treated as a
 * length coercion by this routine.
 */
bool exprIsLengthCoercion(const Node* expr, int32* coercedTypmod)
{
    if (coercedTypmod != NULL) {
        *coercedTypmod = -1; /* default result on failure */
    }
    /*
     * Scalar-type length coercions are FuncExprs, array-type length coercions
     * are ArrayCoerceExprs
     */
    if (expr && IsA(expr, FuncExpr)) {
        const FuncExpr* func = (const FuncExpr*)expr;
        int nargs;
        Const* second_arg = NULL;

        /*
         * If it didn't come from a coercion context, reject.
         */
        if (func->funcformat != COERCE_EXPLICIT_CAST && func->funcformat != COERCE_IMPLICIT_CAST) {
            return false;
        }

        /*
         * If it's not a two-argument or three-argument function with the
         * second argument being an int4 constant, it can't have been created
         * from a length coercion (it must be a type coercion, instead).
         */
        nargs = list_length(func->args);
        if (nargs < 2 || nargs > 3) {
            return false;
        }

        second_arg = (Const*)lsecond(func->args);
        if (!IsA(second_arg, Const) || second_arg->consttype != INT4OID || second_arg->constisnull) {
            return false;
        }

        /*
         * OK, it is indeed a length-coercion function.
         */
        if (coercedTypmod != NULL) {
            *coercedTypmod = DatumGetInt32(second_arg->constvalue);
        }

        return true;
    }

    if (expr && IsA(expr, ArrayCoerceExpr)) {
        const ArrayCoerceExpr* acoerce = (const ArrayCoerceExpr*)expr;

        /* It's not a length coercion unless there's a nondefault typmod */
        if (acoerce->resulttypmod < 0) {
            return false;
        }

        /*
         * OK, it is indeed a length-coercion expression.
         */
        if (coercedTypmod != NULL) {
            *coercedTypmod = acoerce->resulttypmod;
        }

        return true;
    }

    return false;
}

/*
 * relabel_to_typmod
 *		Add a RelabelType node that changes just the typmod of the expression.
 *
 * This is primarily intended to be used during planning.  Therefore, it
 * strips any existing RelabelType nodes to maintain the planner's invariant
 * that there are not adjacent RelabelTypes, and it uses COERCE_DONTCARE
 * which would typically be inappropriate earlier.
 */
Node* relabel_to_typmod(Node* expr, int32 typmod)
{
    Oid type = exprType(expr);
    Oid coll = exprCollation(expr);

    /* Strip any existing RelabelType node(s) */
    while (expr && IsA(expr, RelabelType)) {
        expr = (Node*)((RelabelType*)expr)->arg;
    }

    /* Apply new typmod, preserving the previous exposed type and collation */
    return (Node*)makeRelabelType((Expr*)expr, type, typmod, coll, COERCE_DONTCARE);
}

/*
 * expression_returns_set
 *	  Test whether an expression returns a set result.
 *
 * Because we use expression_tree_walker(), this can also be applied to
 * whole targetlists; it'll produce TRUE if any one of the tlist items
 * returns a set.
 */
bool expression_returns_set(Node* clause)
{
    return expression_returns_set_walker(clause, NULL);
}

static bool expression_returns_set_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }
    
    if (IsA(node, FuncExpr)) {
        FuncExpr* expr = (FuncExpr*)node;

        if (expr->funcretset) {
            return true;
        }
        /* else fall through to check args */
    }
    if (IsA(node, OpExpr)) {
        OpExpr* expr = (OpExpr*)node;

        if (expr->opretset) {
            return true;
        }
        /* else fall through to check args */
    }

    /* Avoid recursion for some cases that can't return a set */
    if (IsA(node, Aggref)) {
        return false;
    }
    if (IsA(node, WindowFunc)) {
        return false;
    }
    if (IsA(node, DistinctExpr)) {
        return false;
    }
    if (IsA(node, NullIfExpr)) {
        return false;
    }
    if (IsA(node, ScalarArrayOpExpr)) {
        return false;
    }
    if (IsA(node, BoolExpr)) {
        return false;
    }
    if (IsA(node, SubLink)) {
        return false;
    }
    if (IsA(node, SubPlan)) {
        return false;
    }
    if (IsA(node, AlternativeSubPlan)) {
        return false;
    }
    if (IsA(node, ArrayExpr)) {
        return false;
    }
    if (IsA(node, RowExpr)) {
        return false;
    }
    if (IsA(node, RowCompareExpr)) {
        return false;
    }
    if (IsA(node, CoalesceExpr)) {
        return false;
    }
    if (IsA(node, MinMaxExpr)) {
        return false;
    }
    if (IsA(node, XmlExpr)) {
        return false;
    }
    if (IsA(node, UserSetElem)) {
        return false;
    }

    return expression_tree_walker(node, (bool (*)())expression_returns_set_walker, context);
}

/*
 * node_query_check_no_flt
 *
 * It will check if we need a revert.
 */
bool query_check_no_flt(Query* qry)
{
    if (IsA(qry, Query)) {
        /* if we find a query need execute as old expression framework, return true imediately */
        if (!qry->is_flt_frame) {
            return true;
        }
    }
    return query_or_expression_tree_walker((Node*)qry, (bool (*)())query_check_no_flt_walker, (void*)NULL, 0);
}

static bool query_check_no_flt_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Query)) {
        Query* qry = (Query*)node;
        /* if we find a query need execute as old expression framework, return true imediately */
        if (!qry->is_flt_frame) {
            return true;
        }
        return query_tree_walker((Query*)node, (bool (*)())query_check_no_flt_walker, (void*)NULL, 0);
    }
    return expression_tree_walker(node, (bool (*)())query_check_no_flt_walker, (void*)NULL);
}

/*
 * query_check_srf
 *
 * query_check_srf will try to check SRFs in qry->targetList bt
 * function query_check_srf_walker
 */
void query_check_srf(Query* qry)
{
    qry->hasTargetSRFs = expression_returns_set((Node*)qry->targetList);
    /* if the rule_action has SRFs we need revert to old expression framework */
    if (qry->hasTargetSRFs) {
        qry->is_flt_frame = false;
    }
    query_or_expression_tree_walker((Node*)qry, (bool (*)())query_check_srf_walker, (void*)NULL, 0);
    return;
}

static bool query_check_srf_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Query)) {
        Query* qry = (Query*)node;
        qry->hasTargetSRFs = expression_returns_set((Node*)qry->targetList);
        /* if the rule_action has SRFs we need revert to old expression framework */
        if (qry->hasTargetSRFs) {
            qry->is_flt_frame = false;
        }
        return query_tree_walker((Query*)node, (bool (*)())query_check_srf_walker, (void*)NULL, 0);
    }
    return expression_tree_walker(node, (bool (*)())query_check_srf_walker, (void*)NULL);
}

/*
 * expression_contains_rownum
 *	  Test whether an expression contains rownum.
 *
 * Because we use expression_tree_walker(), this can also be applied to
 * whole targetlists; it'll produce TRUE if any one of the tlist items
 * contain rownum.
 */
bool expression_contains_rownum(Node* node)
{
    return expression_rownum_walker(node, NULL);
}

static bool expression_rownum_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Rownum)) {
        return true;
    }

    return expression_tree_walker(node, (bool (*)())expression_rownum_walker, context);
}

/*
 *	exprCollation -
 *	  returns the Oid of the collation of the expression's result.
 *
 * Note: expression nodes that can invoke functions generally have an
 * "inputcollid" field, which is what the function should use as collation.
 * That is the resolved common collation of the node's inputs.  It is often
 * but not always the same as the result collation; in particular, if the
 * function produces a non-collatable result type from collatable inputs
 * or vice versa, the two are different.
 */
Oid exprCollation(const Node* expr)
{
    Oid coll;

    if (NULL == expr) {
        return InvalidOid;
    }

    switch (nodeTag(expr)) {
        case T_Var:
            coll = ((const Var*)expr)->varcollid;
            break;
        case T_Const:
            coll = ((const Const*)expr)->constcollid;
            break;
        case T_Rownum:
            coll = ((const Rownum*)expr)->rownumcollid;
            break;
        case T_Param:
            coll = ((const Param*)expr)->paramcollid;
            break;
        case T_Aggref:
            coll = ((const Aggref*)expr)->aggcollid;
            break;
        case T_GroupingFunc:
            coll = InvalidOid;
            break;
        case T_GroupingId:
            coll = InvalidOid;
            break;
        case T_WindowFunc:
            coll = ((const WindowFunc*)expr)->wincollid;
            break;
        case T_ArrayRef:
            coll = ((const ArrayRef*)expr)->refcollid;
            break;
        case T_FuncExpr:
            coll = ((const FuncExpr*)expr)->funccollid;
            break;
        case T_NamedArgExpr:
            coll = exprCollation((Node*)((const NamedArgExpr*)expr)->arg);
            break;
        case T_UserVar:
            if (IsA(((UserVar*)expr)->value, FuncExpr)) {
                coll = ((const FuncExpr*)(((UserVar*)expr)->value))->funccollid;
            } else {
                coll = ((const Const*)(((UserVar*)expr)->value))->constcollid;
            }
            if (!OidIsValid(coll)) {
                coll = get_typcollation(exprType((const Node*)(((UserVar*)expr)->value)));
            }
            break;
        case T_OpExpr:
            coll = ((const OpExpr*)expr)->opcollid;
            break;
        case T_DistinctExpr:
            coll = ((const DistinctExpr*)expr)->opcollid;
            break;
        case T_NullIfExpr:
            coll = ((const NullIfExpr*)expr)->opcollid;
            break;
        case T_ScalarArrayOpExpr:
            coll = InvalidOid; /* result is always boolean */
            break;
        case T_BoolExpr:
            coll = InvalidOid; /* result is always boolean */
            break;
        case T_SubLink: {
            const SubLink* sublink = (const SubLink*)expr;

            if (sublink->subLinkType == EXPR_SUBLINK || sublink->subLinkType == ARRAY_SUBLINK) {
                /* get the collation of subselect's first target column */
                Query* qtree = (Query*)sublink->subselect;
                TargetEntry* tent = NULL;

                if (qtree == NULL || !IsA(qtree, Query)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("cannot get collation for untransformed sublink")));
                }
                tent = (TargetEntry*)linitial(qtree->targetList);
                Assert(IsA(tent, TargetEntry));
                Assert(!tent->resjunk);
                coll = exprCollation((Node*)tent->expr);
                /* collation doesn't change if it's converted to array */
            } else {
                /* for all other sublink types, result is boolean */
                coll = InvalidOid;
            }
        } break;
        case T_SubPlan: {
            const SubPlan* subplan = (const SubPlan*)expr;

            if (subplan->subLinkType == EXPR_SUBLINK || subplan->subLinkType == ARRAY_SUBLINK) {
                /* get the collation of subselect's first target column */
                coll = subplan->firstColCollation;
                /* collation doesn't change if it's converted to array */
            } else {
                /* for all other subplan types, result is boolean */
                coll = InvalidOid;
            }
        } break;
        case T_AlternativeSubPlan: {
            const AlternativeSubPlan* asplan = (const AlternativeSubPlan*)expr;

            /* subplans should all return the same thing */
            coll = exprCollation((Node*)linitial(asplan->subplans));
        } break;
        case T_FieldSelect:
            coll = ((const FieldSelect*)expr)->resultcollid;
            break;
        case T_FieldStore:
            coll = InvalidOid; /* result is always composite */
            break;
        case T_RelabelType:
            coll = ((const RelabelType*)expr)->resultcollid;
            break;
        case T_CoerceViaIO:
            coll = ((const CoerceViaIO*)expr)->resultcollid;
            break;
        case T_ArrayCoerceExpr:
            coll = ((const ArrayCoerceExpr*)expr)->resultcollid;
            break;
        case T_ConvertRowtypeExpr:
            coll = InvalidOid; /* result is always composite */
            break;
        case T_CollateExpr:
            coll = ((const CollateExpr*)expr)->collOid;
            break;
        case T_CaseExpr:
            coll = ((const CaseExpr*)expr)->casecollid;
            break;
        case T_CaseTestExpr:
            coll = ((const CaseTestExpr*)expr)->collation;
            break;
        case T_ArrayExpr:
            coll = ((const ArrayExpr*)expr)->array_collid;
            break;
        case T_RowExpr:
            coll = InvalidOid; /* result is always composite */
            break;
        case T_RowCompareExpr:
            coll = InvalidOid; /* result is always boolean */
            break;
        case T_CoalesceExpr:
            coll = ((const CoalesceExpr*)expr)->coalescecollid;
            break;
        case T_MinMaxExpr:
            coll = ((const MinMaxExpr*)expr)->minmaxcollid;
            break;
        case T_XmlExpr:

            /*
             * XMLSERIALIZE returns text from non-collatable inputs, so its
             * collation is always default.  The other cases return boolean or
             * XML, which are non-collatable.
             */
            if (((const XmlExpr*)expr)->op == IS_XMLSERIALIZE) {
                coll = DEFAULT_COLLATION_OID;
            } else {
                coll = InvalidOid;
            }
            break;
        case T_NullTest:
        case T_NanTest:
        case T_InfiniteTest:
        case T_HashFilter:
        case T_TypeCast:
            coll = InvalidOid; /* result is always boolean */
            break;
        case T_BooleanTest:
            coll = InvalidOid; /* result is always boolean */
            break;
        case T_CoerceToDomain:
            coll = ((const CoerceToDomain*)expr)->resultcollid;
            break;
        case T_CoerceToDomainValue:
            coll = ((const CoerceToDomainValue*)expr)->collation;
            break;
        case T_SetToDefault:
            coll = ((const SetToDefault*)expr)->collation;
            break;
        case T_CurrentOfExpr:
            coll = InvalidOid; /* result is always boolean */
            break;
        case T_PlaceHolderVar:
            coll = exprCollation((Node*)((const PlaceHolderVar*)expr)->phexpr);
            break;
        case T_PrefixKey:
            coll = exprCollation((Node*)((const PrefixKey*)expr)->arg);
            break;
        case T_SetVariableExpr:
            coll = ((const Const*)(((SetVariableExpr*)expr)->value))->constcollid;
            break;
        case T_UserSetElem:
            coll = userSetElemTypeCollInfo(expr, exprCollation); 
            break;
#ifdef USE_SPQ
        case T_DMLActionExpr:
            coll = InvalidOid;
            break;
#endif
        case T_PriorExpr:
            coll = exprCollation((Node*)((const PriorExpr*)expr)->node);
            break;

        case T_CursorExpression:
            coll = InvalidOid;
            break;
            
        default:
            ereport(
                ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unrecognized node type: %d", (int)nodeTag(expr))));
            coll = InvalidOid; /* keep compiler quiet */
            break;
    }
    return coll;
}

/*
 *	exprCharset -
 *	  returns the character set of the expression's result.
 */
int exprCharset(const Node* expr)
{
    return get_charset_by_collation(exprCollation(expr));
}

/*
 *	exprInputCollation -
 *	  returns the Oid of the collation a function should use, if available.
 *
 * Result is InvalidOid if the node type doesn't store this information.
 */
Oid exprInputCollation(const Node* expr)
{
    Oid coll;

    if (NULL == expr) {
        return InvalidOid;
    }

    switch (nodeTag(expr)) {
        case T_Aggref:
            coll = ((const Aggref*)expr)->inputcollid;
            break;
        case T_WindowFunc:
            coll = ((const WindowFunc*)expr)->inputcollid;
            break;
        case T_FuncExpr:
            coll = ((const FuncExpr*)expr)->inputcollid;
            break;
        case T_OpExpr:
            coll = ((const OpExpr*)expr)->inputcollid;
            break;
        case T_DistinctExpr:
            coll = ((const DistinctExpr*)expr)->inputcollid;
            break;
        case T_NullIfExpr:
            coll = ((const NullIfExpr*)expr)->inputcollid;
            break;
        case T_ScalarArrayOpExpr:
            coll = ((const ScalarArrayOpExpr*)expr)->inputcollid;
            break;
        case T_MinMaxExpr:
            coll = ((const MinMaxExpr*)expr)->inputcollid;
            break;
        default:
            coll = InvalidOid;
            break;
    }
    return coll;
}

static void AssertExprCollation(const Node* expr, Oid collation)
{
    Oid expr_collation = exprCollation(expr);
    if (DB_IS_CMPT(B_FORMAT) && ENABLE_MULTI_CHARSET && IsBinaryType(exprType(expr))) {
        expr_collation = BINARY_COLLATION_OID;
    }
    Assert(collation == expr_collation);
}

/*
 *	exprSetCollation -
 *	  Assign collation information to an expression tree node.
 *
 * Note: since this is only used during parse analysis, we don't need to
 * worry about subplans or PlaceHolderVars.
 */
void exprSetCollation(Node* expr, Oid collation)
{
    switch (nodeTag(expr)) {
        case T_Var:
            ((Var*)expr)->varcollid = collation;
            break;
        case T_Const:
            ((Const*)expr)->constcollid = collation;
            break;
        case T_UserVar:
            ((Const*)(((UserVar*)expr)->value))->constcollid = collation;
            break;
        case T_Rownum:
            ((Rownum*)expr)->rownumcollid = collation;
            break;
        case T_Param:
            ((Param*)expr)->paramcollid = collation;
            break;
        case T_Aggref:
            ((Aggref*)expr)->aggcollid = collation;
            break;
        case T_GroupingFunc:
            Assert(!OidIsValid(collation));
            break;
        case T_GroupingId:
            Assert(!OidIsValid(collation));
            break;
        case T_WindowFunc:
            ((WindowFunc*)expr)->wincollid = collation;
            break;
        case T_ArrayRef:
            ((ArrayRef*)expr)->refcollid = collation;
            break;
        case T_FuncExpr:
            ((FuncExpr*)expr)->funccollid = collation;
            break;
        case T_NamedArgExpr:
            AssertExprCollation((Node*)((NamedArgExpr*)expr)->arg, collation);
            break;
        case T_OpExpr:
            ((OpExpr*)expr)->opcollid = collation;
            break;
        case T_DistinctExpr:
            ((DistinctExpr*)expr)->opcollid = collation;
            break;
        case T_NullIfExpr:
            ((NullIfExpr*)expr)->opcollid = collation;
            break;
        case T_ScalarArrayOpExpr:
            Assert(!OidIsValid(collation)); /* result is always boolean */
            break;
        case T_BoolExpr:
            Assert(!OidIsValid(collation)); /* result is always boolean */
            break;
        case T_SubLink:
#ifdef USE_ASSERT_CHECKING
        {
            SubLink* sublink = (SubLink*)expr;

            if (sublink->subLinkType == EXPR_SUBLINK || sublink->subLinkType == ARRAY_SUBLINK) {
                /* get the collation of subselect's first target column */
                Query* qtree = (Query*)sublink->subselect;
                TargetEntry* tent = NULL;

                if (qtree == NULL || !IsA(qtree, Query)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION), errmsg("cannot set collation for untransformed sublink")));
                }
                tent = (TargetEntry*)linitial(qtree->targetList);
                Assert(IsA(tent, TargetEntry));
                Assert(!tent->resjunk);
                AssertExprCollation((Node*)tent->expr, collation);
            } else {
                /* for all other sublink types, result is boolean */
                Assert(!OidIsValid(collation));
            }
        }
#endif /* USE_ASSERT_CHECKING */
        break;
        case T_FieldSelect:
            ((FieldSelect*)expr)->resultcollid = collation;
            break;
        case T_FieldStore:
            Assert(!OidIsValid(collation)); /* result is always composite */
            break;
        case T_RelabelType:
            ((RelabelType*)expr)->resultcollid = collation;
            break;
        case T_CoerceViaIO:
            ((CoerceViaIO*)expr)->resultcollid = collation;
            break;
        case T_ArrayCoerceExpr:
            ((ArrayCoerceExpr*)expr)->resultcollid = collation;
            break;
        case T_ConvertRowtypeExpr:
            Assert(!OidIsValid(collation)); /* result is always composite */
            break;
        case T_CaseExpr:
            ((CaseExpr*)expr)->casecollid = collation;
            break;
        case T_ArrayExpr:
            ((ArrayExpr*)expr)->array_collid = collation;
            break;
        case T_RowExpr:
            Assert(!OidIsValid(collation)); /* result is always composite */
            break;
        case T_RowCompareExpr:
            Assert(!OidIsValid(collation)); /* result is always boolean */
            break;
        case T_CoalesceExpr:
            ((CoalesceExpr*)expr)->coalescecollid = collation;
            break;
        case T_MinMaxExpr:
            ((MinMaxExpr*)expr)->minmaxcollid = collation;
            break;
        case T_XmlExpr:
            Assert((((XmlExpr*)expr)->op == IS_XMLSERIALIZE) ? (collation == DEFAULT_COLLATION_OID) 
                : (collation == InvalidOid));
            break;
        case T_NullTest:
        case T_NanTest:
        case T_InfiniteTest:
        case T_HashFilter:
            Assert(!OidIsValid(collation)); /* result is always boolean */
            break;
        case T_BooleanTest:
            Assert(!OidIsValid(collation)); /* result is always boolean */
            break;
        case T_CoerceToDomain:
            ((CoerceToDomain*)expr)->resultcollid = collation;
            break;
        case T_CoerceToDomainValue:
            ((CoerceToDomainValue*)expr)->collation = collation;
            break;
        case T_SetToDefault:
            ((SetToDefault*)expr)->collation = collation;
            break;
        case T_CurrentOfExpr:
            Assert(!OidIsValid(collation)); /* result is always boolean */
            break;
        case T_PrefixKey:
            return exprSetCollation((Node*)((const PrefixKey*)expr)->arg, collation);
        case T_SetVariableExpr:
            ((Const*)(((SetVariableExpr*)expr)->value))->constcollid = collation;
            break;
        case T_UserSetElem:
            break;
        case T_PriorExpr:
            return exprSetCollation((Node*)((const PriorExpr*)expr)->node, collation);
        case T_CursorExpression:
        case T_TypeCast:
            break;
        default:
            ereport(
                ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unrecognized node type: %d", (int)nodeTag(expr))));
            break;
    }
}

/*
 *	exprSetInputCollation -
 *	  Assign input-collation information to an expression tree node.
 *
 * This is a no-op for node types that don't store their input collation.
 * Note we omit RowCompareExpr, which needs special treatment since it
 * contains multiple input collation OIDs.
 */
void exprSetInputCollation(Node* expr, Oid inputcollation)
{
    switch (nodeTag(expr)) {
        case T_Aggref:
            ((Aggref*)expr)->inputcollid = inputcollation;
            break;
        case T_WindowFunc:
            ((WindowFunc*)expr)->inputcollid = inputcollation;
            break;
        case T_FuncExpr:
            ((FuncExpr*)expr)->inputcollid = inputcollation;
            break;
        case T_OpExpr:
            ((OpExpr*)expr)->inputcollid = inputcollation;
            break;
        case T_DistinctExpr:
            ((DistinctExpr*)expr)->inputcollid = inputcollation;
            break;
        case T_NullIfExpr:
            ((NullIfExpr*)expr)->inputcollid = inputcollation;
            break;
        case T_ScalarArrayOpExpr:
            ((ScalarArrayOpExpr*)expr)->inputcollid = inputcollation;
            break;
        case T_MinMaxExpr:
            ((MinMaxExpr*)expr)->inputcollid = inputcollation;
            break;
        default:
            break;
    }
}

/*
 *	exprLocation -
 *	  returns the parse location of an expression tree, for error reports
 *
 * -1 is returned if the location can't be determined.
 *
 * For expressions larger than a single token, the intent here is to
 * return the location of the expression's leftmost token, not necessarily
 * the topmost Node's location field.  For example, an OpExpr's location
 * field will point at the operator name, but if it is not a prefix operator
 * then we should return the location of the left-hand operand instead.
 * The reason is that we want to reference the entire expression not just
 * that operator, and pointing to its start seems to be the most natural way.
 *
 * The location is not perfect --- for example, since the grammar doesn't
 * explicitly represent parentheses in the parsetree, given something that
 * had been written "(a + b) * c" we are going to point at "a" not "(".
 * But it should be plenty good enough for error reporting purposes.
 *
 * You might think that this code is overly general, for instance why check
 * the operands of a FuncExpr node, when the function name can be expected
 * to be to the left of them?  There are a couple of reasons.  The grammar
 * sometimes builds expressions that aren't quite what the user wrote;
 * for instance x IS NOT BETWEEN ... becomes a NOT-expression whose keyword
 * pointer is to the right of its leftmost argument.  Also, nodes that were
 * inserted implicitly by parse analysis (such as FuncExprs for implicit
 * coercions) will have location -1, and so we can have odd combinations of
 * known and unknown locations in a tree.
 */
int exprLocation(const Node* expr)
{
    int loc;

    if (expr == NULL) {
        return -1;
    }
    switch (nodeTag(expr)) {
        case T_RangeVar:
            loc = ((const RangeVar*)expr)->location;
            break;
        case T_Var:
            loc = ((const Var*)expr)->location;
            break;
        case T_Const:
            loc = ((const Const*)expr)->location;
            break;
        case T_Param:
            loc = ((const Param*)expr)->location;
            break;
        case T_Aggref:
            /* function name should always be the first thing */
            loc = ((const Aggref*)expr)->location;
            break;
        case T_GroupingFunc:
            loc = ((const GroupingFunc*)expr)->location;
            break;
        case T_WindowFunc:
            /* function name should always be the first thing */
            loc = ((const WindowFunc*)expr)->location;
            break;
        case T_ArrayRef:
            /* just use array argument's location */
            loc = exprLocation((Node*)((const ArrayRef*)expr)->refexpr);
            break;
        case T_FuncExpr: {
            const FuncExpr* fexpr = (const FuncExpr*)expr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc(fexpr->location, exprLocation((Node*)fexpr->args));
        } break;
        case T_NamedArgExpr: {
            const NamedArgExpr* na = (const NamedArgExpr*)expr;

            /* consider both argument name and value */
            loc = leftmostLoc(na->location, exprLocation((Node*)na->arg));
        } break;
        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        case T_NullIfExpr:   /* struct-equivalent to OpExpr */
        {
            const OpExpr* opexpr = (const OpExpr*)expr;

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(opexpr->location, exprLocation((Node*)opexpr->args));
        } break;
        case T_ScalarArrayOpExpr: {
            const ScalarArrayOpExpr* saopexpr = (const ScalarArrayOpExpr*)expr;

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(saopexpr->location, exprLocation((Node*)saopexpr->args));
        } break;
        case T_BoolExpr: {
            const BoolExpr* bexpr = (const BoolExpr*)expr;

            /*
             * Same as above, to handle either NOT or AND/OR.  We can't
             * special-case NOT because of the way that it's used for
             * things like IS NOT BETWEEN.
             */
            loc = leftmostLoc(bexpr->location, exprLocation((Node*)bexpr->args));
        } break;
        case T_SubLink: {
            const SubLink* sublink = (const SubLink*)expr;

            /* check the testexpr, if any, and the operator/keyword */
            loc = leftmostLoc(exprLocation(sublink->testexpr), sublink->location);
        } break;
        case T_FieldSelect:
            /* just use argument's location */
            loc = exprLocation((Node*)((const FieldSelect*)expr)->arg);
            break;
        case T_FieldStore:
            /* just use argument's location */
            loc = exprLocation((Node*)((const FieldStore*)expr)->arg);
            break;
        case T_RelabelType: {
            const RelabelType* rexpr = (const RelabelType*)expr;

            /* Much as above */
            loc = leftmostLoc(rexpr->location, exprLocation((Node*)rexpr->arg));
        } break;
        case T_CoerceViaIO: {
            const CoerceViaIO* cexpr = (const CoerceViaIO*)expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*)cexpr->arg));
        } break;
        case T_ArrayCoerceExpr: {
            const ArrayCoerceExpr* cexpr = (const ArrayCoerceExpr*)expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*)cexpr->arg));
        } break;
        case T_ConvertRowtypeExpr: {
            const ConvertRowtypeExpr* cexpr = (const ConvertRowtypeExpr*)expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*)cexpr->arg));
        } break;
        case T_CollateExpr:
            /* just use argument's location */
            loc = exprLocation((Node*)((const CollateExpr*)expr)->arg);
            break;
        case T_CaseExpr:
            /* CASE keyword should always be the first thing */
            loc = ((const CaseExpr*)expr)->location;
            break;
        case T_CaseWhen:
            /* WHEN keyword should always be the first thing */
            loc = ((const CaseWhen*)expr)->location;
            break;
        case T_ArrayExpr:
            /* the location points at ARRAY or [, which must be leftmost */
            loc = ((const ArrayExpr*)expr)->location;
            break;
        case T_RowExpr:
            /* the location points at ROW or (, which must be leftmost */
            loc = ((const RowExpr*)expr)->location;
            break;
        case T_RowCompareExpr:
            /* just use leftmost argument's location */
            loc = exprLocation((Node*)((const RowCompareExpr*)expr)->largs);
            break;
        case T_CoalesceExpr:
            /* COALESCE keyword should always be the first thing */
            loc = ((const CoalesceExpr*)expr)->location;
            break;
        case T_MinMaxExpr:
            /* GREATEST/LEAST keyword should always be the first thing */
            loc = ((const MinMaxExpr*)expr)->location;
            break;
        case T_XmlExpr: {
            const XmlExpr* xexpr = (const XmlExpr*)expr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc(xexpr->location, exprLocation((Node*)xexpr->args));
        } break;
        case T_GroupingSet:
            loc = ((const GroupingSet*)expr)->location;
            break;
        case T_NullTest:
            /* just use argument's location */
            loc = exprLocation((Node*)((const NullTest*)expr)->arg);
            break;
        case T_NanTest:
            /* just use argument's location */
            loc = exprLocation((Node*)((const NanTest*)expr)->arg);
            break;
        case T_InfiniteTest:
            /* just use argument's location */
            loc = exprLocation((Node*)((const InfiniteTest*)expr)->arg);
            break;
        case T_HashFilter:
            /* just use argument's location */
            loc = exprLocation((Node*)((const HashFilter*)expr)->arg);
            break;
        case T_BooleanTest:
            /* just use argument's location */
            loc = exprLocation((Node*)((const BooleanTest*)expr)->arg);
            break;
        case T_CoerceToDomain: {
            const CoerceToDomain* cexpr = (const CoerceToDomain*)expr;

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation((Node*)cexpr->arg));
        } break;
        case T_CoerceToDomainValue:
            loc = ((const CoerceToDomainValue*)expr)->location;
            break;
        case T_SetToDefault:
            loc = ((const SetToDefault*)expr)->location;
            break;
        case T_TargetEntry:
            /* just use argument's location */
            loc = exprLocation((Node*)((const TargetEntry*)expr)->expr);
            break;
        case T_IntoClause:
            /* use the contained RangeVar's location --- close enough */
            loc = exprLocation((Node*)((const IntoClause*)expr)->rel);
            break;
        case T_List: {
            /* report location of first list member that has a location */
            ListCell* lc = NULL;

            loc = -1; /* just to suppress compiler warning */
            foreach (lc, (const List*)expr) {
                loc = exprLocation((Node*)lfirst(lc));
                if (loc >= 0) {
                    break;
                }
            }
        } break;
        case T_A_Expr: {
            const A_Expr* aexpr = (const A_Expr*)expr;

            /* use leftmost of operator or left operand (if any) */
            /* we assume right operand can't be to left of operator */
            loc = leftmostLoc(aexpr->location, exprLocation(aexpr->lexpr));
        } break;
        case T_ColumnRef:
            loc = ((const ColumnRef*)expr)->location;
            break;
        case T_ParamRef:
            loc = ((const ParamRef*)expr)->location;
            break;
        case T_A_Const:
            loc = ((const A_Const*)expr)->location;
            break;
        case T_FuncCall: {
            const FuncCall* fc = (const FuncCall*)expr;

            /* consider both function name and leftmost arg */
            /* (we assume any ORDER BY nodes must be to right of name) */
            loc = leftmostLoc(fc->location, exprLocation((Node*)fc->args));
        } break;
        case T_A_ArrayExpr:
            /* the location points at ARRAY or [, which must be leftmost */
            loc = ((const A_ArrayExpr*)expr)->location;
            break;
        case T_ResTarget:
            /* we need not examine the contained expression (if any) */
            loc = ((const ResTarget*)expr)->location;
            break;
        case T_TypeCast: {
            const TypeCast* tc = (const TypeCast*)expr;

            /*
             * This could represent CAST(), ::, or TypeName 'literal', so
             * any of the components might be leftmost.
             */
            loc = exprLocation(tc->arg);
            loc = leftmostLoc(loc, tc->typname->location);
            loc = leftmostLoc(loc, tc->location);
        } break;
        case T_CollateClause:
            /* just use argument's location */
            loc = exprLocation(((const CollateClause*)expr)->arg);
            break;
        case T_SortBy:
            /* just use argument's location (ignore operator, if any) */
            loc = exprLocation(((const SortBy*)expr)->node);
            break;
        case T_WindowDef:
            loc = ((const WindowDef*)expr)->location;
            break;
        case T_RangeTableSample:
            loc = ((const RangeTableSample*)expr)->location;
            break;
        case T_RangeTimeCapsule:
            loc = ((const RangeTimeCapsule*)expr)->location;
            break;
        case T_TypeName:
            loc = ((const TypeName*)expr)->location;
            break;
        case T_Constraint:
            loc = ((const Constraint*)expr)->location;
            break;
        case T_XmlSerialize:
            /* XMLSERIALIZE keyword should always be the first thing */
            loc = ((const XmlSerialize*)expr)->location;
            break;
        case T_WithClause:
            loc = ((const WithClause*)expr)->location;
            break;
        case T_UpsertClause:
            loc = ((const UpsertClause*)expr)->location;
            break;
        case T_CommonTableExpr:
            loc = ((const CommonTableExpr*)expr)->location;
            break;
        case T_PlaceHolderVar:
            /* just use argument's location */
            loc = exprLocation((Node*)((const PlaceHolderVar*)expr)->phexpr);
            break;
        case T_FunctionParameter:
            /* just use typename's location */
            loc = exprLocation((Node*)((const FunctionParameter*)expr)->argType);
            break;
        case T_Rownum:
            loc = ((const Rownum*)expr)->location;
            break;
        case T_PrefixKey:
            loc = exprLocation((Node*)((const PrefixKey*)expr)->arg);
            break;
        default:
            /* for any other node type it's just unknown... */
            loc = -1;
            break;
    }
    return loc;
}

/*
 * leftmostLoc - support for exprLocation
 *
 * Take the minimum of two parse location values, but ignore unknowns
 */
static int leftmostLoc(int loc1, int loc2)
{
    if (loc1 < 0) {
        return loc2;
    } else if (loc2 < 0) {
        return loc1;
    } else {
        return Min(loc1, loc2);
    }
}

/*
 * Standard expression-tree walking support
 *
 * We used to have near-duplicate code in many different routines that
 * understood how to recurse through an expression node tree.  That was
 * a pain to maintain, and we frequently had bugs due to some particular
 * routine neglecting to support a particular node type.  In most cases,
 * these routines only actually care about certain node types, and don't
 * care about other types except insofar as they have to recurse through
 * non-primitive node types.  Therefore, we now provide generic tree-walking
 * logic to consolidate the redundant "boilerplate" code.  There are
 * two versions: expression_tree_walker() and expression_tree_mutator().
 */

/*
 * expression_tree_walker() is designed to support routines that traverse
 * a tree in a read-only fashion (although it will also work for routines
 * that modify nodes in-place but never add/delete/replace nodes).
 * A walker routine should look like this:
 *
 * bool my_walker (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return false;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... do special actions for Var nodes
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special actions for other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_walker(node, my_walker, (void *) context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the walker routine needs --- it can be used to return data
 * gathered by the walker, too.  This argument is not touched by
 * expression_tree_walker, but it is passed down to recursive sub-invocations
 * of my_walker.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_walker with the top-level
 * node of the tree, and then examines the results.
 *
 * The walker routine should return "false" to continue the tree walk, or
 * "true" to abort the walk and immediately return "true" to the top-level
 * caller.	This can be used to short-circuit the traversal if the walker
 * has found what it came for.	"false" is returned to the top-level caller
 * iff no invocation of the walker returned "true".
 *
 * The node types handled by expression_tree_walker include all those
 * normally found in target lists and qualifier clauses during the planning
 * stage.  In particular, it handles List nodes since a cnf-ified qual clause
 * will have List structure at the top level, and it handles TargetEntry nodes
 * so that a scan of a target list can be handled without additional code.
 * Also, RangeTblRef, FromExpr, JoinExpr, and SetOperationStmt nodes are
 * handled, so that query jointrees and setOperation trees can be processed
 * without additional code.
 *
 * expression_tree_walker will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the walker on the sub-Query node; however, when
 * expression_tree_walker itself is called on a Query node, it does nothing
 * and returns "false".  The net effect is that unless the walker does
 * something special at a Query node, sub-selects will not be visited during
 * an expression tree walk. This is exactly the behavior wanted in many cases
 * --- and for those walkers that do want to recurse into sub-selects, special
 * behavior is typically needed anyway at the entry to a sub-select (such as
 * incrementing a depth counter). A walker that wants to examine sub-selects
 * should include code along the lines of:
 *
 *		if (IsA(node, Query))
 *		{
 *			adjust context for subquery;
 *			result = query_tree_walker((Query *) node, my_walker, context,
 *									   0); // adjust flags as needed
 *			restore context if needed;
 *			return result;
 *		}
 *
 * query_tree_walker is a convenience routine (see below) that calls the
 * walker on all the expression subtrees of the given Query node.
 *
 * expression_tree_walker will handle SubPlan nodes by recursing normally
 * into the "testexpr" and the "args" list (which are expressions belonging to
 * the outer plan).  It will not touch the completed subplan, however.	Since
 * there is no link to the original Query, it is not possible to recurse into
 * subselects of an already-planned expression tree.  This is OK for current
 * uses, but may need to be revisited in future.
 */

bool expression_tree_walker(Node* node, bool (*walker)(), void* context)
{
    ListCell* temp = NULL;
    bool (*p2walker)(void*, void*) = (bool (*)(void*, void*))walker;

    /*
     * The walker has already visited the current node, and so we need only
     * recurse into any sub-nodes it has.
     *
     * We assume that the walker is not interested in List nodes per se, so
     * when we expect a List we just recurse directly to self without
     * bothering to call the walker.
     */
    if (node == NULL) {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(node)) {
        case T_Var:
        case T_Const:
        case T_Param:
        case T_CoerceToDomainValue:
        case T_CaseTestExpr:
        case T_SetToDefault:
        case T_CurrentOfExpr:
        case T_RangeTblRef:
        case T_SortGroupClause:
        case T_GroupingId:
        case T_Value:
        case T_Integer:
        case T_Float:
        case T_String:
        case T_BitString:
        case T_Null:
        case T_PgFdwRemoteInfo:
        case T_Rownum:
        case T_UserVar:
        case T_SetVariableExpr:
        case T_TypeCast:
#ifdef USE_SPQ
        case T_DMLActionExpr:
#endif
            /* primitive node types with no expression subnodes */
            break;
        case T_WithCheckOption:
            return p2walker(((WithCheckOption*)node)->qual, context);
        case T_Aggref: {
            Aggref* expr = (Aggref*)node;

            /* recurse directly on List */
            if (expression_tree_walker((Node*)expr->aggdirectargs, walker, context)) {
                return true;
            }
            if (expression_tree_walker((Node*)expr->args, walker, context)) {
                return true;
            }
            if (expression_tree_walker((Node*)expr->aggorder, walker, context)) {
                return true;
            }
            if (expression_tree_walker((Node*)expr->aggdistinct, walker, context)) {
                return true;
            }
            if (expression_tree_walker((Node*)expr->aggfilter, walker, context)) {
                return true;
            }
        } break;
        case T_GroupingFunc: {
            GroupingFunc* grouping = (GroupingFunc*)node;
            if (expression_tree_walker((Node*)grouping->args, walker, context)) {
                return true;
            }
        } break;
        case T_WindowFunc: {
            WindowFunc* expr = (WindowFunc*)node;

            /* recurse directly on List */
            if (expression_tree_walker((Node*)expr->args, walker, context)) {
                return true;
            }
        } break;
        case T_ArrayRef: {
            ArrayRef* aref = (ArrayRef*)node;

            /* recurse directly for upper/lower array index lists */
            if (expression_tree_walker((Node*)aref->refupperindexpr, walker, context)) {
                return true;
            }
            if (expression_tree_walker((Node*)aref->reflowerindexpr, walker, context)) {
                return true;
            }
            /* walker must see the refexpr and refassgnexpr, however */
            if (p2walker(aref->refexpr, context)) {
                return true;
            }
            if (p2walker(aref->refassgnexpr, context)) {
                return true;
            }
        } break;
        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;

            if (expression_tree_walker((Node*)expr->args, walker, context)) {
                return true;
            }
        } break;
        case T_NamedArgExpr:
            return p2walker(((NamedArgExpr*)node)->arg, context);
        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        case T_NullIfExpr:   /* struct-equivalent to OpExpr */
        {
            OpExpr* expr = (OpExpr*)node;

            if (expression_tree_walker((Node*)expr->args, walker, context)) {
                return true;
            }
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;

            if (expression_tree_walker((Node*)expr->args, walker, context)) {
                return true;
            }
        } break;
        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;

            if (expression_tree_walker((Node*)expr->args, walker, context)) {
                return true;
            }
        } break;
        case T_SubLink: {
            SubLink* sublink = (SubLink*)node;

            if (p2walker(sublink->testexpr, context)) {
                return true;
            }

            /*
             * Also invoke the walker on the sublink's Query node, so it
             * can recurse into the sub-query if it wants to.
             */
            return p2walker(sublink->subselect, context);
        } break;
        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;

            /* recurse into the testexpr, but not into the Plan */
            if (p2walker(subplan->testexpr, context)) {
                return true;
            }
            /* also examine args list */
            if (expression_tree_walker((Node*)subplan->args, walker, context)) {
                return true;
            }
        } break;
        case T_AlternativeSubPlan:
            return p2walker(((AlternativeSubPlan*)node)->subplans, context);
        case T_FieldSelect:
            return p2walker(((FieldSelect*)node)->arg, context);
        case T_FieldStore: {
            FieldStore* fstore = (FieldStore*)node;

            if (p2walker(fstore->arg, context)) {
                return true;
            }
            if (p2walker(fstore->newvals, context)) {
                return true;
            }
        } break;
        case T_RelabelType:
            return p2walker(((RelabelType*)node)->arg, context);
        case T_CoerceViaIO:
            return p2walker(((CoerceViaIO*)node)->arg, context);
        case T_ArrayCoerceExpr:
            return p2walker(((ArrayCoerceExpr*)node)->arg, context);
        case T_ConvertRowtypeExpr:
            return p2walker(((ConvertRowtypeExpr*)node)->arg, context);
        case T_CollateExpr:
            return p2walker(((CollateExpr*)node)->arg, context);
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;

            if (p2walker(caseexpr->arg, context)) {
                return true;
            }
            /* we assume walker doesn't care about CaseWhens, either */
            foreach (temp, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(temp);

                Assert(IsA(when, CaseWhen));
                if (p2walker(when->expr, context)) {
                    return true;
                }
                if (p2walker(when->result, context)) {
                    return true;
                }
            }
            if (p2walker(caseexpr->defresult, context)) {
                return true;
            }
        } break;
        case T_ArrayExpr:
            return p2walker(((ArrayExpr*)node)->elements, context);
        case T_RowExpr:
            /* Assume colnames isn't interesting */
            return p2walker(((RowExpr*)node)->args, context);
        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;

            if (p2walker(rcexpr->largs, context)) {
                return true;
            }
            if (p2walker(rcexpr->rargs, context)) {
                return true;
            }
        } break;
        case T_CoalesceExpr:
            return p2walker(((CoalesceExpr*)node)->args, context);
        case T_MinMaxExpr:
            return p2walker(((MinMaxExpr*)node)->args, context);
        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;

            if (p2walker(xexpr->named_args, context)) {
                return true;
            }
            /* we assume walker doesn't care about arg_names */
            if (p2walker(xexpr->args, context)) {
                return true;
            }
        } break;
        case T_NullTest:
            return p2walker(((NullTest*)node)->arg, context);
        case T_NanTest:
            return p2walker(((NanTest*)node)->arg, context);
        case T_InfiniteTest:
            return p2walker(((InfiniteTest*)node)->arg, context);
        case T_HashFilter:
            return p2walker(((HashFilter*)node)->arg, context);
        case T_BooleanTest:
            return p2walker(((BooleanTest*)node)->arg, context);
        case T_CoerceToDomain:
            return p2walker(((CoerceToDomain*)node)->arg, context);
        case T_TargetEntry:
            return p2walker(((TargetEntry*)node)->expr, context);
        case T_Query:
            /* Do nothing with a sub-Query, per discussion above */
            break;
        case T_WindowClause: {
            WindowClause* wc = (WindowClause*)node;

            if (p2walker(wc->partitionClause, context)) {
                return true;
            }
            if (p2walker(wc->orderClause, context)) {
                return true;
            }
            if (p2walker(wc->startOffset, context)) {
                return true;
            }
            if (p2walker(wc->endOffset, context)) {
                return true;
            }
        } break;
        case T_CommonTableExpr: {
            CommonTableExpr* cte = (CommonTableExpr*)node;

            /*
             * Invoke the walker on the CTE's Query node, so it can
             * recurse into the sub-query if it wants to.
             */
            return p2walker(cte->ctequery, context);
        } break;
        case T_List:
            foreach (temp, (List*)node) {
                if (p2walker((Node*)lfirst(temp), context)) {
                    return true;
                }
            }
            break;
        case T_FromExpr: {
            FromExpr* from = (FromExpr*)node;

            if (p2walker(from->fromlist, context)) {
                return true;
            }
            if (p2walker(from->quals, context)) {
                return true;
			}
        } break;
        case T_UpsertExpr: {
            UpsertExpr* upsertClause = (UpsertExpr*)node;
            if (p2walker(upsertClause->updateTlist, context))
                return true;
            if (p2walker(upsertClause->upsertWhere, context))
                return true;
        } break;
        case T_JoinExpr: {
            JoinExpr* join = (JoinExpr*)node;

            if (p2walker(join->larg, context)) {
                return true;
            }
            if (p2walker(join->rarg, context)) {
                return true;
            }
            if (p2walker(join->quals, context)) {
                return true;
            }

            /*
             * alias clause, using list are deemed uninteresting.
             */
        } break;
        case T_MergeAction: {
            MergeAction* action = (MergeAction*)node;

            if (p2walker(action->targetList, context)) {
                return true;
            }
            if (p2walker(action->qual, context)) {
                return true;
            }
        } break;
        case T_SetOperationStmt: {
            SetOperationStmt* setop = (SetOperationStmt*)node;

            if (p2walker(setop->larg, context)) {
                return true;
            }
            if (p2walker(setop->rarg, context)) {
                return true;
            }

            /* groupClauses are deemed uninteresting */
        } break;
        case T_PlaceHolderVar:
            return p2walker(((PlaceHolderVar*)node)->phexpr, context);
        case T_AppendRelInfo: {
            AppendRelInfo* appinfo = (AppendRelInfo*)node;

            if (expression_tree_walker((Node*)appinfo->translated_vars, walker, context)) {
                return true;
            }
        } break;
        case T_TableSampleClause: {
            TableSampleClause* tsc = (TableSampleClause*)node;

            if (expression_tree_walker((Node*)tsc->args, walker, context)) {
                return true;
            }
            if (p2walker((Node*)tsc->repeatable, context)) {
                return true;
            }
        } break;
        case T_TimeCapsuleClause: {
            TimeCapsuleClause* tcc = (TimeCapsuleClause*)node;

            if (p2walker(tcc->tvver, context)) {
                return true;
            }
        } break;
        case T_PlaceHolderInfo:
            return p2walker(((PlaceHolderInfo*)node)->ph_var, context);
        case T_AutoIncrement:
            return p2walker(((AutoIncrement*)node)->expr, context);
        case T_PrefixKey:
            return p2walker(((PrefixKey*)node)->arg, context);
        case T_UserSetElem: {
            return p2walker(((UserSetElem*)node)->val, context);
        }
        case T_PriorExpr:
            return p2walker(((PriorExpr*)node)->node, context);

        case T_CursorExpression:
            return false;

        default:
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("expression_tree_walker:unrecognized node type: %d", (int)nodeTag(node))));
            break;
    }
    return false;
}

/*
 * query_tree_walker --- initiate a walk of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * walker intends to descend into subqueries.  It is also useful for
 * descending into subqueries within a walker.
 *
 * Some callers want to suppress visitation of certain items in the sub-Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to suppress visitation of
 * indicated items.  (More flag bits may be added as needed.)
 */
bool query_tree_walker(Query* query, bool (*walker)(), void* context, int flags)
{
    bool (*p2walker)(Node*, void*) = (bool (*)(Node*, void*))walker;
    Assert(query != NULL && IsA(query, Query));

    CHECK_FOR_INTERRUPTS();

    if (p2walker((Node*)query->targetList, context)) {
        return true;
    }
    if (p2walker((Node*)query->withCheckOptions, context))
        return true;
    if (p2walker((Node*)query->mergeSourceTargetList, context)) {
        return true;
    }
    if (p2walker((Node*)query->mergeActionList, context)) {
        return true;
    }
    if (p2walker((Node*)query->upsertClause, context)) {
	return true;
    }
    if (p2walker((Node*)query->returningList, context)) {
        return true;
    }
    if (p2walker((Node*)query->jointree, context)) {
        return true;
    }
    if (p2walker(query->setOperations, context)) {
        return true;
    }
    if (p2walker(query->havingQual, context)) {
        return true;
    }
    if (p2walker(query->limitOffset, context)) {
        return true;
    }
    if (p2walker(query->limitCount, context)) {
        return true;
    }
    if (!(flags & QTW_IGNORE_CTE_SUBQUERIES)) {
        if (p2walker((Node*)query->cteList, context)) {
            return true;
        }
    }
    if (!(flags & QTW_IGNORE_RANGE_TABLE)) {
        if (range_table_walker(query->rtable, walker, context, flags)) {
            return true;
        }
    }

    return false;
}

/*
 * range_table_walker is just the part of query_tree_walker that scans
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */
bool range_table_walker(List* rtable, bool (*walker)(), void* context, int flags)
{
    ListCell* rt = NULL;
    bool (*p2walker)(Node*, void*) = (bool (*)(Node*, void*))walker;

    foreach (rt, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(rt);

        /* For historical reasons, visiting RTEs is not the default */
        if (flags & QTW_EXAMINE_RTES) {
            if (p2walker((Node*)rte, context)) {
                return true;
            }
        }

        switch (rte->rtekind) {
            case RTE_RELATION:
                if (p2walker((Node *)rte->tablesample, context) || p2walker((Node *)rte->timecapsule, context)) {
                    return true;
                }
                /* fall through */
            case RTE_CTE:
            case RTE_RESULT:
                /* nothing to do */
                break;
            case RTE_SUBQUERY:
                if (!(flags & QTW_IGNORE_RT_SUBQUERIES)) {
                    if (p2walker((Node*)rte->subquery, context)) {
                        return true;
                    }
                }
                break;
            case RTE_JOIN:
                if (!(flags & QTW_IGNORE_JOINALIASES)) {
                    if (p2walker((Node*)rte->joinaliasvars, context)) {
                        return true;
                    }
                }
                break;
            case RTE_FUNCTION:
                if (p2walker(rte->funcexpr, context)) {
                    return true;
                }
                break;
            case RTE_VALUES:
                if (p2walker((Node*)rte->values_lists, context)) {
                    return true;
                }
                break;
#ifdef PGXC
            case RTE_REMOTE_DUMMY:
                if (!(flags & QTW_IGNORE_DUMMY)) {
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Invalid RTE found.")));
                }
                break;
#endif /* PGXC */
            default:
                break;
        }
        /* walk the security quals in range table entry */
        if (p2walker((Node*)rte->securityQuals, context)) {
            return true;
        }
    }
    return false;
}

/*
 * expression_tree_mutator() is designed to support routines that make a
 * modified copy of an expression tree, with some nodes being added,
 * removed, or replaced by new subtrees.  The original tree is (normally)
 * not changed.  Each recursion level is responsible for returning a copy of
 * (or appropriately modified substitute for) the subtree it is handed.
 * A mutator routine should look like this:
 *
 * Node * my_mutator (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return NULL;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... create and return modified copy of Var node
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special transformations of other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_mutator(node, my_mutator, (void *) context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the mutator routine needs --- it can be used to return extra
 * data gathered by the mutator, too.  This argument is not touched by
 * expression_tree_mutator, but it is passed down to recursive sub-invocations
 * of my_mutator.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_mutator with the
 * top-level node of the tree, and does any required post-processing.
 *
 * Each level of recursion must return an appropriately modified Node.
 * If expression_tree_mutator() is called, it will make an exact copy
 * of the given Node, but invoke my_mutator() to copy the sub-node(s)
 * of that Node.  In this way, my_mutator() has full control over the
 * copying process but need not directly deal with expression trees
 * that it has no interest in.
 *
 * Just as for expression_tree_walker, the node types handled by
 * expression_tree_mutator include all those normally found in target lists
 * and qualifier clauses during the planning stage.
 *
 * expression_tree_mutator will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the mutator on the sub-Query node; however, when
 * expression_tree_mutator itself is called on a Query node, it does nothing
 * and returns the unmodified Query node.  The net effect is that unless the
 * mutator does something special at a Query node, sub-selects will not be
 * visited or modified; the original sub-select will be linked to by the new
 * SubLink node.  Mutators that want to descend into sub-selects will usually
 * do so by recognizing Query nodes and calling query_tree_mutator (below).
 *
 * expression_tree_mutator will handle a SubPlan node by recursing into the
 * "testexpr" and the "args" list (which belong to the outer plan), but it
 * will simply copy the link to the inner plan, since that's typically what
 * expression tree mutators want.  A mutator that wants to modify the subplan
 * can force appropriate behavior by recognizing SubPlan expression nodes
 * and doing the right thing.
 */

Node* expression_tree_mutator(Node* node, Node* (*mutator)(Node*, void*), void* context, bool isCopy)
{
    /*
     * The mutator has already decided not to modify the current node, but we
     * must call the mutator for any sub-nodes.
     */
#define FLATCOPY(newnode, node, nodetype, isCopy)                                     \
    if (isCopy) {                                                                     \
        (newnode) = (nodetype*)palloc(sizeof(nodetype));                              \
        errno_t rc = memcpy_s((newnode), sizeof(nodetype), (node), sizeof(nodetype)); \
        securec_check(rc, "\0", "\0");                                                \
    } else {                                                                          \
        ((newnode) = (node));                                                         \
    }

#define CHECKFLATCOPY(newnode, node, nodetype)                                        \
    do {                                                                              \
        AssertMacro(IsA((node), nodetype));                                           \
        (newnode) = (nodetype*)palloc(sizeof(nodetype));                              \
        errno_t rc = memcpy_s((newnode), sizeof(nodetype), (node), sizeof(nodetype)); \
        securec_check(rc, "\0", "\0");                                                \
    } while (0)

#define MUTATE(newfield, oldfield, fieldtype) ((newfield) = (fieldtype)mutator((Node*)(oldfield), context))

    if (node == NULL)
        return NULL;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(node)) {
            /*
             * Primitive node types with no expression subnodes.  Var and
             * Const are frequent enough to deserve special cases, the others
             * we just use copyObject for.
             */
        case T_Var: {
            Var* var = (Var*)node;
            Var* newnode = NULL;

            FLATCOPY(newnode, var, Var, isCopy);

            return (Node*)newnode;
        } break;
        case T_Const: {
            Const* oldnode = (Const*)node;
            Const* newnode = NULL;

            FLATCOPY(newnode, oldnode, Const, isCopy);

            /* XXX we don't bother with datumCopy; should we? */
            return (Node*)newnode;
        } break;
        case T_Rownum: {
            Rownum* oldnode = (Rownum*)node;
            Rownum* newnode = NULL;
            FLATCOPY(newnode, oldnode, Rownum, isCopy);
            return (Node*)newnode;
        } break;
        case T_Param:
        case T_CoerceToDomainValue:
        case T_CaseTestExpr:
        case T_SetToDefault:
        case T_CurrentOfExpr:
        case T_RangeTblRef:
        case T_SortGroupClause:
        case T_GroupingId:
            if (isCopy) {
                return (Node*)copyObject(node);
            } else {
                return node;
            }
        case T_WithCheckOption: {
                WithCheckOption* wco = (WithCheckOption*)node;
                WithCheckOption* newnode;

                FLATCOPY(newnode, wco, WithCheckOption, isCopy);
                MUTATE(newnode->qual, wco->qual, Node*);
                return (Node*)newnode;
            }
        case T_Aggref: {
            Aggref* aggref = (Aggref*)node;
            Aggref* newnode = NULL;

            FLATCOPY(newnode, aggref, Aggref, isCopy);
            MUTATE(newnode->aggdirectargs, aggref->aggdirectargs, List*);
            MUTATE(newnode->args, aggref->args, List*);
            MUTATE(newnode->aggorder, aggref->aggorder, List*);
            MUTATE(newnode->aggdistinct, aggref->aggdistinct, List*);
            MUTATE(newnode->aggfilter, aggref->aggfilter, Expr*);
            return (Node*)newnode;
        } break;
        case T_WindowFunc: {
            WindowFunc* wfunc = (WindowFunc*)node;
            WindowFunc* newnode = NULL;

            FLATCOPY(newnode, wfunc, WindowFunc, isCopy);
            MUTATE(newnode->args, wfunc->args, List*);
            return (Node*)newnode;
        } break;
        case T_ArrayRef: {
            ArrayRef* arrayref = (ArrayRef*)node;
            ArrayRef* newnode = NULL;

            FLATCOPY(newnode, arrayref, ArrayRef, isCopy);
            MUTATE(newnode->refupperindexpr, arrayref->refupperindexpr, List*);
            MUTATE(newnode->reflowerindexpr, arrayref->reflowerindexpr, List*);
            MUTATE(newnode->refexpr, arrayref->refexpr, Expr*);
            MUTATE(newnode->refassgnexpr, arrayref->refassgnexpr, Expr*);
            return (Node*)newnode;
        } break;
        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;
            FuncExpr* newnode = NULL;

            FLATCOPY(newnode, expr, FuncExpr, isCopy);
            MUTATE(newnode->args, expr->args, List*);
            return (Node*)newnode;
        } break;
        case T_NamedArgExpr: {
            NamedArgExpr* nexpr = (NamedArgExpr*)node;
            NamedArgExpr* newnode = NULL;

            FLATCOPY(newnode, nexpr, NamedArgExpr, isCopy);
            MUTATE(newnode->arg, nexpr->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_UserVar: {
            UserVar* oldnode = (UserVar *)node;
            UserVar* newnode = NULL;

            FLATCOPY(newnode, oldnode, UserVar, isCopy);
            MUTATE(newnode->value, oldnode->value, Expr*);
            return (Node *)newnode;
        } break;
        case T_OpExpr: {
            OpExpr* expr = (OpExpr*)node;
            OpExpr* newnode = NULL;

            FLATCOPY(newnode, expr, OpExpr, isCopy);
            MUTATE(newnode->args, expr->args, List*);
            return (Node*)newnode;
        } break;
        case T_DistinctExpr: {
            DistinctExpr* expr = (DistinctExpr*)node;
            DistinctExpr* newnode = NULL;

            FLATCOPY(newnode, expr, DistinctExpr, isCopy);
            MUTATE(newnode->args, expr->args, List*);
            return (Node*)newnode;
        } break;
        case T_NullIfExpr: {
            NullIfExpr* expr = (NullIfExpr*)node;
            NullIfExpr* newnode = NULL;

            FLATCOPY(newnode, expr, NullIfExpr, isCopy);
            MUTATE(newnode->args, expr->args, List*);
            return (Node*)newnode;
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;
            ScalarArrayOpExpr* newnode = NULL;

            FLATCOPY(newnode, expr, ScalarArrayOpExpr, isCopy);
            MUTATE(newnode->args, expr->args, List*);
            return (Node*)newnode;
        } break;
        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            BoolExpr* newnode = NULL;

            FLATCOPY(newnode, expr, BoolExpr, isCopy);
            MUTATE(newnode->args, expr->args, List*);
            return (Node*)newnode;
        } break;
        case T_SubLink: {
            SubLink* sublink = (SubLink*)node;
            SubLink* newnode = NULL;

            FLATCOPY(newnode, sublink, SubLink, isCopy);
            MUTATE(newnode->testexpr, sublink->testexpr, Node*);

            /*
             * Also invoke the mutator on the sublink's Query node, so it
             * can recurse into the sub-query if it wants to.
             */
            MUTATE(newnode->subselect, sublink->subselect, Node*);
            return (Node*)newnode;
        } break;
        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            SubPlan* newnode = NULL;

            FLATCOPY(newnode, subplan, SubPlan, isCopy);
            /* transform testexpr */
            MUTATE(newnode->testexpr, subplan->testexpr, Node*);
            /* transform args list (params to be passed to subplan) */
            MUTATE(newnode->args, subplan->args, List*);
            /* but not the sub-Plan itself, which is referenced as-is */
            return (Node*)newnode;
        } break;
        case T_AlternativeSubPlan: {
            AlternativeSubPlan* asplan = (AlternativeSubPlan*)node;
            AlternativeSubPlan* newnode = NULL;

            FLATCOPY(newnode, asplan, AlternativeSubPlan, isCopy);
            MUTATE(newnode->subplans, asplan->subplans, List*);
            return (Node*)newnode;
        } break;
        case T_FieldSelect: {
            FieldSelect* fselect = (FieldSelect*)node;
            FieldSelect* newnode = NULL;

            FLATCOPY(newnode, fselect, FieldSelect, isCopy);
            MUTATE(newnode->arg, fselect->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_FieldStore: {
            FieldStore* fstore = (FieldStore*)node;
            FieldStore* newnode = NULL;

            FLATCOPY(newnode, fstore, FieldStore, isCopy);
            MUTATE(newnode->arg, fstore->arg, Expr*);
            MUTATE(newnode->newvals, fstore->newvals, List*);
            newnode->fieldnums = list_copy(fstore->fieldnums);
            return (Node*)newnode;
        } break;
        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            RelabelType* newnode = NULL;

            FLATCOPY(newnode, relabel, RelabelType, isCopy);
            MUTATE(newnode->arg, relabel->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_CoerceViaIO: {
            CoerceViaIO* iocoerce = (CoerceViaIO*)node;
            CoerceViaIO* newnode = NULL;

            FLATCOPY(newnode, iocoerce, CoerceViaIO, isCopy);
            MUTATE(newnode->arg, iocoerce->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_ArrayCoerceExpr: {
            ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
            ArrayCoerceExpr* newnode = NULL;

            FLATCOPY(newnode, acoerce, ArrayCoerceExpr, isCopy);
            MUTATE(newnode->arg, acoerce->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_ConvertRowtypeExpr: {
            ConvertRowtypeExpr* convexpr = (ConvertRowtypeExpr*)node;
            ConvertRowtypeExpr* newnode = NULL;

            FLATCOPY(newnode, convexpr, ConvertRowtypeExpr, isCopy);
            MUTATE(newnode->arg, convexpr->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_CollateExpr: {
            CollateExpr* collate = (CollateExpr*)node;
            CollateExpr* newnode = NULL;

            FLATCOPY(newnode, collate, CollateExpr, isCopy);
            MUTATE(newnode->arg, collate->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            CaseExpr* newnode = NULL;

            FLATCOPY(newnode, caseexpr, CaseExpr, isCopy);
            MUTATE(newnode->arg, caseexpr->arg, Expr*);
            MUTATE(newnode->args, caseexpr->args, List*);
            MUTATE(newnode->defresult, caseexpr->defresult, Expr*);
            return (Node*)newnode;
        } break;
        case T_CaseWhen: {
            CaseWhen* casewhen = (CaseWhen*)node;
            CaseWhen* newnode = NULL;

            FLATCOPY(newnode, casewhen, CaseWhen, isCopy);
            MUTATE(newnode->expr, casewhen->expr, Expr*);
            MUTATE(newnode->result, casewhen->result, Expr*);
            return (Node*)newnode;
        } break;
        case T_ArrayExpr: {
            ArrayExpr* arrayexpr = (ArrayExpr*)node;
            ArrayExpr* newnode = NULL;

            FLATCOPY(newnode, arrayexpr, ArrayExpr, isCopy);
            MUTATE(newnode->elements, arrayexpr->elements, List*);
            return (Node*)newnode;
        } break;
        case T_RowExpr: {
            RowExpr* rowexpr = (RowExpr*)node;
            RowExpr* newnode = NULL;

            FLATCOPY(newnode, rowexpr, RowExpr, isCopy);
            MUTATE(newnode->args, rowexpr->args, List*);
            /* Assume colnames needn't be duplicated */
            return (Node*)newnode;
        } break;
        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            RowCompareExpr* newnode = NULL;

            FLATCOPY(newnode, rcexpr, RowCompareExpr, isCopy);
            MUTATE(newnode->largs, rcexpr->largs, List*);
            MUTATE(newnode->rargs, rcexpr->rargs, List*);
            return (Node*)newnode;
        } break;
        case T_CoalesceExpr: {
            CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;
            CoalesceExpr* newnode = NULL;

            FLATCOPY(newnode, coalesceexpr, CoalesceExpr, isCopy);
            MUTATE(newnode->args, coalesceexpr->args, List*);
            return (Node*)newnode;
        } break;
        case T_MinMaxExpr: {
            MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;
            MinMaxExpr* newnode = NULL;

            FLATCOPY(newnode, minmaxexpr, MinMaxExpr, isCopy);
            MUTATE(newnode->args, minmaxexpr->args, List*);
            return (Node*)newnode;
        } break;
        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;
            XmlExpr* newnode = NULL;

            FLATCOPY(newnode, xexpr, XmlExpr, isCopy);
            MUTATE(newnode->named_args, xexpr->named_args, List*);
            /* assume mutator does not care about arg_names */
            MUTATE(newnode->args, xexpr->args, List*);
            return (Node*)newnode;
        } break;
        case T_NullTest: {
            NullTest* ntest = (NullTest*)node;
            NullTest* newnode = NULL;

            FLATCOPY(newnode, ntest, NullTest, isCopy);
            MUTATE(newnode->arg, ntest->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_NanTest: {
            NanTest* ntest = (NanTest*)node;
            NanTest* newnode = NULL;

            FLATCOPY(newnode, ntest, NanTest, isCopy);
            MUTATE(newnode->arg, ntest->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_InfiniteTest: {
            InfiniteTest* ntest = (InfiniteTest*)node;
            InfiniteTest* newnode = NULL;

            FLATCOPY(newnode, ntest, InfiniteTest, isCopy);
            MUTATE(newnode->arg, ntest->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_HashFilter: {
            HashFilter* htest = (HashFilter*)node;
            HashFilter* newnode = NULL;

            FLATCOPY(newnode, htest, HashFilter, isCopy);
            MUTATE(newnode->arg, htest->arg, List*);
            return (Node*)newnode;
        } break;
        case T_BooleanTest: {
            BooleanTest* btest = (BooleanTest*)node;
            BooleanTest* newnode = NULL;

            FLATCOPY(newnode, btest, BooleanTest, isCopy);
            MUTATE(newnode->arg, btest->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_CoerceToDomain: {
            CoerceToDomain* ctest = (CoerceToDomain*)node;
            CoerceToDomain* newnode = NULL;

            FLATCOPY(newnode, ctest, CoerceToDomain, isCopy);
            MUTATE(newnode->arg, ctest->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_TargetEntry: {
            TargetEntry* targetentry = (TargetEntry*)node;
            TargetEntry* newnode = NULL;

            FLATCOPY(newnode, targetentry, TargetEntry, isCopy);
            MUTATE(newnode->expr, targetentry->expr, Expr*);
            return (Node*)newnode;
        } break;
        case T_Query:
            /* Do nothing with a sub-Query, per discussion above */
            return node;
        case T_GroupingFunc: {
            GroupingFunc* grouping = (GroupingFunc*)node;
            GroupingFunc* newnode = NULL;

            FLATCOPY(newnode, grouping, GroupingFunc, isCopy);
            MUTATE(newnode->args, grouping->args, List*);

            /*
             * We assume here that mutating the arguments does not change
             * the semantics, i.e. that the arguments are not mutated in a
             * way that makes them semantically different from their
             * previously matching expressions in the GROUP BY clause.
             *
             * If a mutator somehow wanted to do this, it would have to
             * handle the refs and cols lists itself as appropriate.
             */
            newnode->refs = list_copy(grouping->refs);
            newnode->cols = list_copy(grouping->cols);

            return (Node*)newnode;
        } break;
        case T_WindowClause: {
            WindowClause* wc = (WindowClause*)node;
            WindowClause* newnode = NULL;

            FLATCOPY(newnode, wc, WindowClause, isCopy);
            MUTATE(newnode->partitionClause, wc->partitionClause, List*);
            MUTATE(newnode->orderClause, wc->orderClause, List*);
            MUTATE(newnode->startOffset, wc->startOffset, Node*);
            MUTATE(newnode->endOffset, wc->endOffset, Node*);
            return (Node*)newnode;
        } break;
        case T_CommonTableExpr: {
            CommonTableExpr* cte = (CommonTableExpr*)node;
            CommonTableExpr* newnode = NULL;

            FLATCOPY(newnode, cte, CommonTableExpr, isCopy);

            /*
             * Also invoke the mutator on the CTE's Query node, so it can
             * recurse into the sub-query if it wants to.
             */
            MUTATE(newnode->ctequery, cte->ctequery, Node*);
            return (Node*)newnode;
        } break;
        case T_List: {
            /*
             * We assume the mutator isn't interested in the list nodes
             * per se, so just invoke it on each list element. NOTE: this
             * would fail badly on a list with integer elements!
             */
            List* resultlist = NIL;
            ListCell* temp = NULL;

            foreach (temp, (List*)node) {
                resultlist = (List*)lappend(resultlist, mutator((Node*)lfirst(temp), context));
            }
            return (Node*)resultlist;
        } break;
        case T_UpsertExpr: {
            UpsertExpr* upsertClause = (UpsertExpr*)node;
            UpsertExpr* newnode = NULL;

            FLATCOPY(newnode, upsertClause, UpsertExpr, isCopy);
            MUTATE(newnode->updateTlist, upsertClause->updateTlist, List*);
            MUTATE(newnode->upsertWhere, upsertClause->upsertWhere, Node*);
            return (Node*)newnode;
        } break;
        case T_FromExpr: {
            FromExpr* from = (FromExpr*)node;
            FromExpr* newnode = NULL;

            FLATCOPY(newnode, from, FromExpr, isCopy);
            MUTATE(newnode->fromlist, from->fromlist, List*);
            MUTATE(newnode->quals, from->quals, Node*);
            return (Node*)newnode;
        } break;
        case T_JoinExpr: {
            JoinExpr* join = (JoinExpr*)node;
            JoinExpr* newnode = NULL;

            FLATCOPY(newnode, join, JoinExpr, isCopy);
            MUTATE(newnode->larg, join->larg, Node*);
            MUTATE(newnode->rarg, join->rarg, Node*);
            MUTATE(newnode->quals, join->quals, Node*);
            /* We do not mutate alias or using by default */
            return (Node*)newnode;
        } break;
        case T_MergeAction: {
            MergeAction* action = (MergeAction*)node;
            MergeAction* newnode = NULL;

            FLATCOPY(newnode, action, MergeAction, isCopy);
            MUTATE(newnode->qual, action->qual, Node*);
            MUTATE(newnode->targetList, action->targetList, List*);
            MUTATE(newnode->pulluped_targetList, action->pulluped_targetList, List*);

            return (Node*)newnode;
        } break;
        case T_SetOperationStmt: {
            SetOperationStmt* setop = (SetOperationStmt*)node;
            SetOperationStmt* newnode = NULL;

            FLATCOPY(newnode, setop, SetOperationStmt, isCopy);
            MUTATE(newnode->larg, setop->larg, Node*);
            MUTATE(newnode->rarg, setop->rarg, Node*);
            /* We do not mutate groupClauses by default */
            return (Node*)newnode;
        } break;
        case T_PlaceHolderVar: {
            PlaceHolderVar* phv = (PlaceHolderVar*)node;
            PlaceHolderVar* newnode = NULL;

            FLATCOPY(newnode, phv, PlaceHolderVar, isCopy);
            MUTATE(newnode->phexpr, phv->phexpr, Expr*);
            /* Assume we need not copy the relids bitmapset */
            return (Node*)newnode;
        } break;
        case T_AppendRelInfo: {
            AppendRelInfo* appinfo = (AppendRelInfo*)node;
            AppendRelInfo* newnode = NULL;

            FLATCOPY(newnode, appinfo, AppendRelInfo, isCopy);
            MUTATE(newnode->translated_vars, appinfo->translated_vars, List*);
            return (Node*)newnode;
        } break;
        case T_PlaceHolderInfo: {
            PlaceHolderInfo* phinfo = (PlaceHolderInfo*)node;
            PlaceHolderInfo* newnode = NULL;

            FLATCOPY(newnode, phinfo, PlaceHolderInfo, isCopy);
            MUTATE(newnode->ph_var, phinfo->ph_var, PlaceHolderVar*);
            /* Assume we need not copy the relids bitmapsets */
            return (Node*)newnode;
        } break;
        case T_TableSampleClause: {
            TableSampleClause* tsc = (TableSampleClause*)node;
            TableSampleClause* newnode = NULL;

            FLATCOPY(newnode, tsc, TableSampleClause, isCopy);
            MUTATE(newnode->args, tsc->args, List*);
            MUTATE(newnode->repeatable, tsc->repeatable, Expr*);
            return (Node*)newnode;
        } break;
        case T_TimeCapsuleClause: {
            TimeCapsuleClause* tcc = (TimeCapsuleClause*)node;
            TimeCapsuleClause* newnode = NULL;

            FLATCOPY(newnode, tcc, TimeCapsuleClause, isCopy);
            MUTATE(newnode->tvver, tcc->tvver, Node*);
            return (Node*)newnode;
        } break;
        case T_PrefixKey: {
            PrefixKey* pkey = (PrefixKey*)node;
            PrefixKey* newnode = NULL;

            FLATCOPY(newnode, pkey, PrefixKey, isCopy);
            MUTATE(newnode->arg, pkey->arg, Expr*);
            return (Node*)newnode;
        } break;
        case T_SetVariableExpr: {
            SetVariableExpr* oldnode = (SetVariableExpr*)node;
            SetVariableExpr* newnode = NULL;
            FLATCOPY(newnode, oldnode, SetVariableExpr, isCopy);
            MUTATE(newnode->value, oldnode->value, Expr*);
            return (Node*)newnode;
        } break;
        case T_UserSetElem: {
            UserSetElem* use = (UserSetElem*)node;
            UserSetElem* newnode = NULL;
            FLATCOPY(newnode, use, UserSetElem, isCopy);
            MUTATE(newnode->val, use->val, Expr*);
            return (Node*)newnode;
        } break;
#ifdef USE_SPQ
        case T_DMLActionExpr: {
            DMLActionExpr *action_expr = (DMLActionExpr *) node;
            DMLActionExpr *newnode = NULL;
            FLATCOPY(newnode, action_expr, DMLActionExpr, isCopy);
            return (Node *)newnode;
        } break;
#endif
        case T_PriorExpr: {
            PriorExpr* p_expr = (PriorExpr*)node;
            PriorExpr* newnode = NULL;
            FLATCOPY(newnode, p_expr, PriorExpr, isCopy);
            MUTATE(newnode->node, p_expr->node, Node*);
            return (Node*)newnode;
        } break;

        case T_CursorExpression: {
            CursorExpression* cursor_expression = (CursorExpression*)node;
            CursorExpression* newnode = NULL;
            FLATCOPY(newnode, cursor_expression, CursorExpression, isCopy);
            MUTATE(newnode->param, cursor_expression->param, List*);
            return (Node*)newnode;
        } break;
        
        case T_TypeCast: {
            TypeCast *tc = (TypeCast*)node;
            TypeCast *newnode = NULL;
            FLATCOPY(newnode, tc, TypeCast, isCopy);
            MUTATE(newnode->arg, tc->arg, Node*);
            return (Node*)newnode;
        } break;
        
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(node))));
            break;
    }
    /* can't get here, but keep compiler happy */
    return NULL;
}

/*
 * query_tree_mutator --- initiate modification of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * mutator intends to descend into subqueries.	It is also useful for
 * descending into subqueries within a mutator.
 *
 * Some callers want to suppress mutating of certain items in the Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to suppress mutating of
 * indicated items.  (More flag bits may be added as needed.)
 *
 * Normally the Query node itself is copied, but some callers want it to be
 * modified in-place; they must pass QTW_DONT_COPY_QUERY in flags.	All
 * modified substructure is safely copied in any case.
 */
Query* query_tree_mutator(Query* query, Node* (*mutator)(Node*, void*), void* context, int flags)
{
    Assert(query != NULL && IsA(query, Query));

    if (!(flags & QTW_DONT_COPY_QUERY)) {
        Query* newquery = NULL;

        FLATCOPY(newquery, query, Query, true);
        if (newquery->resultRelations)
            newquery->resultRelations = (List*)copyObject(query->resultRelations);
        query = newquery;
    }

    MUTATE(query->targetList, query->targetList, List*);
    MUTATE(query->withCheckOptions, query->withCheckOptions, List *);
    MUTATE(query->mergeSourceTargetList, query->mergeSourceTargetList, List*);
    MUTATE(query->mergeActionList, query->mergeActionList, List*);
    MUTATE(query->upsertClause, query->upsertClause, UpsertExpr*);
    MUTATE(query->returningList, query->returningList, List*);
    MUTATE(query->jointree, query->jointree, FromExpr*);
    MUTATE(query->setOperations, query->setOperations, Node*);
    MUTATE(query->havingQual, query->havingQual, Node*);
    MUTATE(query->limitOffset, query->limitOffset, Node*);
    MUTATE(query->limitCount, query->limitCount, Node*);
    if (!(flags & QTW_IGNORE_CTE_SUBQUERIES))
        MUTATE(query->cteList, query->cteList, List*);
    else /* else copy CTE list as-is */
        query->cteList = (List*)copyObject(query->cteList);
    query->rtable = range_table_mutator(query->rtable, (Node * (*)(Node*, void*)) mutator, context, flags);
    return query;
}

/*
 * range_table_mutator is just the part of query_tree_mutator that processes
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */
List* range_table_mutator(List* rtable, Node* (*mutator)(Node*, void*), void* context, int flags)
{
    List* newrt = NIL;
    ListCell* rt = NULL;

    foreach (rt, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(rt);
        RangeTblEntry* newrte = NULL;

        FLATCOPY(newrte, rte, RangeTblEntry, true);
        switch (rte->rtekind) {
            case RTE_RELATION:
                MUTATE(newrte->tablesample, rte->tablesample, TableSampleClause*);
                MUTATE(newrte->timecapsule, rte->timecapsule, TimeCapsuleClause*);
                break;
            case RTE_CTE:
            case RTE_RESULT:
#ifdef PGXC
            case RTE_REMOTE_DUMMY:
#endif /* PGXC */
                /* we don't bother to copy eref, aliases, etc; OK? */
                break;
            case RTE_SUBQUERY:
                if (!((unsigned int)flags & QTW_IGNORE_RT_SUBQUERIES)) {
                    CHECKFLATCOPY(newrte->subquery, rte->subquery, Query);
                    MUTATE(newrte->subquery, newrte->subquery, Query*);
                } else {
                    /* else, copy RT subqueries as-is */
                    newrte->subquery = (Query*)copyObject(rte->subquery);
                }
                break;
            case RTE_JOIN:
                if (!((unsigned int)flags & QTW_IGNORE_JOINALIASES))
                    MUTATE(newrte->joinaliasvars, rte->joinaliasvars, List*);
                else {
                    /* else, copy join aliases as-is */
                    newrte->joinaliasvars = (List*)copyObject(rte->joinaliasvars);
                }
                break;
            case RTE_FUNCTION:
                MUTATE(newrte->funcexpr, rte->funcexpr, Node*);
                break;
            case RTE_VALUES:
                MUTATE(newrte->values_lists, rte->values_lists, List*);
                break;
            default:
                break;
        }
        MUTATE(newrte->securityQuals, rte->securityQuals, List*);
        newrt = lappend(newrt, newrte);
    }
    return newrt;
}

/*
 * query_or_expression_tree_walker --- hybrid form
 *
 * This routine will invoke query_tree_walker if called on a Query node,
 * else will invoke the walker directly.  This is a useful way of starting
 * the recursion when the walker's normal change of state is not appropriate
 * for the outermost Query node.
 */
bool query_or_expression_tree_walker(Node* node, bool (*walker)(), void* context, int flags)
{
    bool (*p2walker)(Node*, void*) = (bool (*)(Node*, void*))walker;
    if (node && IsA(node, Query))
        return query_tree_walker((Query*)node, (bool (*)())walker, context, flags);
    else
        return p2walker(node, context);
}

/*
 * query_or_expression_tree_mutator --- hybrid form
 *
 * This routine will invoke query_tree_mutator if called on a Query node,
 * else will invoke the mutator directly.  This is a useful way of starting
 * the recursion when the mutator's normal change of state is not appropriate
 * for the outermost Query node.
 */
Node* query_or_expression_tree_mutator(Node* node, Node* (*mutator)(Node*, void*), void* context, int flags)
{
    Node* (*p2mutator)(Node*, void*) = (Node * (*)(Node*, void*)) mutator;
    if (node && IsA(node, Query)) {
        return (Node*)query_tree_mutator((Query*)node, mutator, context, flags);
    }
    else {
        return p2mutator(node, context);
    }
}

/*
 * raw_expression_tree_walker --- walk raw parse trees
 *
 * This has exactly the same API as expression_tree_walker, but instead of
 * walking post-analysis parse trees, it knows how to walk the node types
 * found in raw grammar output.  (There is not currently any need for a
 * combined walker, so we keep them separate in the name of efficiency.)
 * Unlike expression_tree_walker, there is no special rule about query
 * boundaries: we descend to everything that's possibly interesting.
 *
 * Currently, the node type coverage extends to SelectStmt and everything
 * that could appear under it, but not other statement types.
 */
bool raw_expression_tree_walker(Node* node, bool (*walker)(), void* context)
{
    ListCell* temp = NULL;
    bool (*p2walker)(void*, void*) = (bool (*)(void*, void*))walker;

    /*
     * The walker has already visited the current node, and so we need only
     * recurse into any sub-nodes it has.
     */
    if (node == NULL) {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(node)) {
        case T_SetToDefault:
        case T_CurrentOfExpr:
        case T_Integer:
        case T_Float:
        case T_String:
        case T_BitString:
        case T_Null:
        case T_ParamRef:
        case T_A_Const:
        case T_A_Star:
        case T_Rownum:
            /* primitive node types with no subnodes */
            break;
        case T_Alias:
            /* we assume the colnames list isn't interesting */
            break;
        case T_RangeVar:
            return p2walker(((RangeVar*)node)->alias, context);
        case T_GroupingFunc:
            return p2walker(((GroupingFunc*)node)->args, context);
        case T_GroupingSet:
            return p2walker(((GroupingSet*)node)->content, context);
        case T_SubLink: {
            SubLink* sublink = (SubLink*)node;

            if (p2walker(sublink->testexpr, context)) {
                return true;
            }
            /* we assume the operName is not interesting */
            if (p2walker(sublink->subselect, context)) {
                return true;
            }
        } break;
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;

            if (p2walker(caseexpr->arg, context))
                return true;
            /* we assume walker doesn't care about CaseWhens, either */
            foreach (temp, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(temp);

                Assert(IsA(when, CaseWhen));
                if (p2walker(when->expr, context)) {
                    return true;
                }
                if (p2walker(when->result, context)) {
                    return true;
                }
            }
            if (p2walker(caseexpr->defresult, context)){
                return true;
            }
        } break;
        case T_RowExpr:
            /* Assume colnames isn't interesting */
            return p2walker(((RowExpr*)node)->args, context);
        case T_CoalesceExpr:
            return p2walker(((CoalesceExpr*)node)->args, context);
        case T_MinMaxExpr:
            return p2walker(((MinMaxExpr*)node)->args, context);
        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;

            if (p2walker(xexpr->named_args, context)) {
                return true;
            }
            /* we assume walker doesn't care about arg_names */
            if (p2walker(xexpr->args, context)) {
                return true;
            }
        } break;
        case T_NullTest:
            return p2walker(((NullTest*)node)->arg, context);
        case T_NanTest:
            return p2walker(((NanTest*)node)->arg, context);
        case T_InfiniteTest:
            return p2walker(((InfiniteTest*)node)->arg, context);
        case T_BooleanTest:
            return p2walker(((BooleanTest*)node)->arg, context);
        case T_HashFilter:
            return p2walker(((HashFilter*)node)->arg, context);
        case T_JoinExpr: {
            JoinExpr* join = (JoinExpr*)node;

            if (p2walker(join->larg, context)) {
                return true;
            }
            if (p2walker(join->rarg, context)) {
                return true;
            }
            if (p2walker(join->quals, context)) {
                return true;
            }
            if (p2walker(join->alias, context)) {
                return true;
            }
            /* using list is deemed uninteresting */
        } break;
        case T_IntoClause: {
            IntoClause* into = (IntoClause*)node;

            if (p2walker(into->rel, context)) {
                return true;
            }
            /* colNames, options are deemed uninteresting */
        } break;
        case T_List:
            foreach (temp, (List*)node) {
                if (p2walker((Node*)lfirst(temp), context)) {
                    return true;
                }
            }
            break;
        case T_InsertStmt: {
            InsertStmt* stmt = (InsertStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->cols, context)) {
                return true;
            }
            if (p2walker(stmt->selectStmt, context)) {
                return true;
            }
            if (p2walker(stmt->returningList, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
        } break;
        case T_DeleteStmt: {
            DeleteStmt* stmt = (DeleteStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->usingClause, context)) {
                return true;
            }
            if (p2walker(stmt->whereClause, context)) {
                return true;
            }
            if (p2walker(stmt->returningList, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if (p2walker(stmt->limitClause, context)) {
                return true;
            }
            if (p2walker(stmt->relations, context)) {
                return true;
            }
        } break;
        case T_UpdateStmt: {
            UpdateStmt* stmt = (UpdateStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->targetList, context)) {
                return true;
            }
            if (p2walker(stmt->whereClause, context)) {
                return true;
            }
            if (p2walker(stmt->fromClause, context)) {
                return true;
            }
            if (p2walker(stmt->returningList, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if (p2walker(stmt->relationClause, context)) {
                return true;
            }
        } break;
        case T_MergeStmt: {
            MergeStmt* stmt = (MergeStmt*)node;

            if (p2walker(stmt->relation, context)) {
                return true;
            }
            if (p2walker(stmt->source_relation, context)) {
                return true;
            }
            if (p2walker(stmt->join_condition, context)) {
                return true;
            }
            if (p2walker(stmt->mergeWhenClauses, context)) {
                return true;
            }
        } break;
        case T_MergeWhenClause: {
            MergeWhenClause* mergeWhenClause = (MergeWhenClause*)node;

            if (p2walker(mergeWhenClause->condition, context)) {
                return true;
            }
            if (p2walker(mergeWhenClause->targetList, context)) {
                return true;
            }
            if (p2walker(mergeWhenClause->cols, context)) {
                return true;
            }
            if (p2walker(mergeWhenClause->values, context)) {
                return true;
            }
        } break;
        case T_SelectStmt: {
            SelectStmt* stmt = (SelectStmt*)node;

            if (p2walker(stmt->distinctClause, context)) {
                return true;
            }
            if (p2walker(stmt->intoClause, context)) {
                return true;
            }
            if (p2walker(stmt->targetList, context)) {
                return true;
            }
            if (p2walker(stmt->fromClause, context)) {
                return true;
            }
            if (p2walker(stmt->unrotateInfo, context)) {
                return true;
            }
            if (p2walker(stmt->whereClause, context)) {
                return true;
            }
            if (p2walker(stmt->groupClause, context)) {
                return true;
            }
            if (p2walker(stmt->havingClause, context)) {
                return true;
            }
            if (p2walker(stmt->windowClause, context)) {
                return true;
            }
            if (p2walker(stmt->withClause, context)) {
                return true;
            }
            if (p2walker(stmt->valuesLists, context)) {
                return true;
            }
            if (p2walker(stmt->sortClause, context)) {
                return true;
            }
            if (p2walker(stmt->limitOffset, context)) {
                return true;
            }
            if (p2walker(stmt->limitCount, context)) {
                return true;
            }
            if (p2walker(stmt->lockingClause, context)) {
                return true;
            }
            if (p2walker(stmt->larg, context)) {
                return true;
            }
            if (p2walker(stmt->rarg, context)) {
                return true;
            }
        } break;
        case T_A_Expr: {
            A_Expr* expr = (A_Expr*)node;

            if (p2walker(expr->lexpr, context)) {
                return true;
            }
            if (p2walker(expr->rexpr, context)) {
                return true;
            }
            /* operator name is deemed uninteresting */
        } break;
        case T_ColumnRef:
            /* we assume the fields contain nothing interesting */
            break;
        case T_FuncCall: {
            FuncCall* fcall = (FuncCall*)node;

            if (p2walker(fcall->args, context)) {
                return true;
            }
            if (p2walker(fcall->agg_order, context)) {
                return true;
            }
            if (p2walker(fcall->agg_filter, context)) {
                return true;
            }
            if (p2walker(fcall->over, context)) {
                return true;
            }
            /* function name is deemed uninteresting */
        } break;
        case T_NamedArgExpr:
            return p2walker(((NamedArgExpr*)node)->arg, context);
        case T_A_Indices: {
            A_Indices* indices = (A_Indices*)node;

            if (p2walker(indices->lidx, context)) {
                return true;
            }
            if (p2walker(indices->uidx, context)) {
                return true;
            }
        } break;
        case T_A_Indirection: {
            A_Indirection* indir = (A_Indirection*)node;

            if (p2walker(indir->arg, context)) {
                return true;
            }
            if (p2walker(indir->indirection, context)) {
                return true;
            }
        } break;
        case T_A_ArrayExpr:
            return p2walker(((A_ArrayExpr*)node)->elements, context);
        case T_ResTarget: {
            ResTarget* rt = (ResTarget*)node;

            if (p2walker(rt->indirection, context)) {
                return true;
            }
            if (p2walker(rt->val, context)) {
                return true;
            }
        } break;
        case T_TypeCast: {
            TypeCast* tc = (TypeCast*)node;

            if (p2walker(tc->arg, context)) {
                return true;
            }
            if (p2walker(tc->typname, context)) {
                return true;
            }
        } break;
        case T_CollateClause:
            return p2walker(((CollateClause*)node)->arg, context);
        case T_SortBy:
            return p2walker(((SortBy*)node)->node, context);
        case T_WindowDef: {
            WindowDef* wd = (WindowDef*)node;

            if (p2walker(wd->partitionClause, context)) {
                return true;
            }
            if (p2walker(wd->orderClause, context)) {
                return true;
            }
            if (p2walker(wd->startOffset, context)) {
                return true;
            }
            if (p2walker(wd->endOffset, context)) {
                return true;
            }
        } break;
        case T_RangeSubselect: {
            RangeSubselect* rs = (RangeSubselect*)node;

            if (p2walker(rs->subquery, context)) {
                return true;
            }
            if (p2walker(rs->alias, context)) {
                return true;
            }
            if (p2walker(rs->rotate, context)) {
                return true;
            }
        } break;
        case T_RangeFunction: {
            RangeFunction* rf = (RangeFunction*)node;

            if (p2walker(rf->funccallnode, context)) {
                return true;
            }
            if (p2walker(rf->alias, context)) {
                return true;
            }
        } break;
        case T_RangeTableSample: {
            RangeTableSample* rts = (RangeTableSample*)node;

            if (p2walker(rts->relation, context)) {
                return true;
            }
            /* method name is deemed uninteresting */
            if (p2walker(rts->args, context)) {
                return true;
            }
            if (p2walker(rts->repeatable, context)) {
                return true;
            }
        } break;
        case T_RangeTimeCapsule: {
            RangeTimeCapsule* rtc = (RangeTimeCapsule*)node;

            if (p2walker(rtc->relation, context)) {
                return true;
            }
            /* method name is deemed uninteresting */
            if (p2walker(rtc->tvver, context)) {
                return true;
            }
        } break;
        case T_TypeName: {
            TypeName* tn = (TypeName*)node;

            if (p2walker(tn->typmods, context)) {
                return true;
            }
            if (p2walker(tn->arrayBounds, context)) {
                return true;
            }
            /* type name itself is deemed uninteresting */
        } break;
        case T_ColumnDef: {
            ColumnDef* coldef = (ColumnDef*)node;

            if (p2walker(coldef->typname, context)) {
                return true;
            }
            if (p2walker(coldef->raw_default, context)) {
                return true;
            }
            if (p2walker(coldef->collClause, context)) {
                return true;
            }
            /* for now, constraints are ignored */
        } break;
        case T_LockingClause:
            return p2walker(((LockingClause*)node)->lockedRels, context);
        case T_XmlSerialize: {
            XmlSerialize* xs = (XmlSerialize*)node;

            if (p2walker(xs->expr, context)) {
                return true;
            }
            if (p2walker(xs->typname, context)) {
                return true;
            }
        } break;
        case T_WithClause:
            return p2walker(((WithClause*)node)->ctes, context);
        case T_RotateClause:  {
            RotateClause *stmt = (RotateClause*)node;

            if (p2walker(stmt->forColName, context))
                return true;
            if (p2walker(stmt->inExprList, context))
                return true;
            if (p2walker(stmt->aggregateFuncCallList, context))
                return true;
        } break;
        case T_UnrotateClause: {
            UnrotateClause *stmt = (UnrotateClause*)node;

            if (p2walker(stmt->forColName, context))
                return true;
            if (p2walker(stmt->inExprList, context))
                return true;
        } break;
        case T_RotateInCell: {
            RotateInCell *stmt = (RotateInCell*)node;

            if (p2walker(stmt->rotateInExpr, context))
                return true;
        } break;
        case T_UnrotateInCell: {
            UnrotateInCell *stmt = (UnrotateInCell*)node;

            if (p2walker(stmt->aliaList, context))
                return true;
            if (p2walker(stmt->unrotateInExpr, context))
                return true;
        } break;
        case T_UpsertClause:
            return p2walker(((UpsertClause*)node)->targetList, context);
        case T_CommonTableExpr:
            return p2walker(((CommonTableExpr*)node)->ctequery, context);
        case T_AutoIncrement:
            return p2walker(((AutoIncrement*)node)->expr, context);
        case T_UserVar:
            /* @var do not need recursion */
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(node))));
            break;
    }
    return false;
}

bool lockNextvalWalker(Node* node, void* context)
{
    /* lock nextval on cn when select nextval to avoid dead lock with alter sequence */
    lockSeqForNextvalFunc(node);
    return expression_tree_walker(node, (bool (*)())lockNextvalWalker, context);
}

void find_nextval_seqoid_walker(Node* node, Oid* seqoid)
{
    if (node != NULL && IsA(node, FuncExpr) && ((FuncExpr*)node)->funcid == NEXTVALFUNCOID) {
        FuncExpr* funcexpr = (FuncExpr*)node;
        Assert(funcexpr->args->length == 1);
        if (IsA(linitial(funcexpr->args), Const)) {
            Const* con = (Const*)linitial(funcexpr->args);
            *seqoid = DatumGetObjectId(con->constvalue);
            return;
        }
    }
    (void)expression_tree_walker(node, (bool (*)())find_nextval_seqoid_walker, (void*)seqoid);
}

Oid userSetElemTypeCollInfo(const Node* expr, Oid (*exprFunc)(const Node*))
{
    Oid coll = InvalidOid;
    UserSetElem* use_node = (UserSetElem*)expr;
    UserVar* uv = (UserVar*)linitial(use_node->name);
    if (uv != NULL) {
        if (uv->value != NULL) {
            coll = exprFunc((Node*)uv->value);
        } else if (use_node->val != NULL) {
            coll = exprFunc((Node*)use_node->val);
        }
    }
    return coll;
}
