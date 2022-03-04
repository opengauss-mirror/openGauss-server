/* -------------------------------------------------------------------------
 *
 * parse_expr.cpp
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/commmon/backend/parser/parse_expr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "db4ai/predict_by.h"
#include "executor/node/nodeCtescan.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_agg.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/plpgsql.h"
#include "utils/xml.h"
#include "funcapi.h"

extern Node* makeAConst(Value* v, int location);
extern Value* makeStringValue(char* str);
static Node* transformParamRef(ParseState* pstate, ParamRef* pref);
static Node* transformAExprOp(ParseState* pstate, A_Expr* a);
static Node* transformAExprAnd(ParseState* pstate, A_Expr* a);
static Node* transformAExprOr(ParseState* pstate, A_Expr* a);
static Node* transformAExprNot(ParseState* pstate, A_Expr* a);
static Node* transformAExprOpAny(ParseState* pstate, A_Expr* a);
static Node* transformAExprOpAll(ParseState* pstate, A_Expr* a);
static Node* transformAExprDistinct(ParseState* pstate, A_Expr* a);
static Node* transformAExprNullIf(ParseState* pstate, A_Expr* a);
static Node* transformAExprOf(ParseState* pstate, A_Expr* a);
static Node* transformAExprIn(ParseState* pstate, A_Expr* a);
static Node* transformFuncCall(ParseState* pstate, FuncCall* fn);
static Node* transformCaseExpr(ParseState* pstate, CaseExpr* c);
static Node* transformSubLink(ParseState* pstate, SubLink* sublink);
static Node* transformArrayExpr(ParseState* pstate, A_ArrayExpr* a, Oid array_type, Oid element_type, int32 typmod);
static Node* transformRowExpr(ParseState* pstate, RowExpr* r);
static Node* transformCoalesceExpr(ParseState* pstate, CoalesceExpr* c);
static Node* transformMinMaxExpr(ParseState* pstate, MinMaxExpr* m);
static Node* transformXmlExpr(ParseState* pstate, XmlExpr* x);
static Node* transformXmlSerialize(ParseState* pstate, XmlSerialize* xs);
static Node* transformBooleanTest(ParseState* pstate, BooleanTest* b);
static Node* transformCurrentOfExpr(ParseState* pstate, CurrentOfExpr* cexpr);
static Node* transformPredictByFunction(ParseState* pstate, PredictByFunction* cexpr);
static Node* transformWholeRowRef(ParseState* pstate, RangeTblEntry* rte, int location);
static Node* transformIndirection(ParseState* pstate, Node* basenode, List* indirection);
static Node* transformTypeCast(ParseState* pstate, TypeCast* tc);
static Node* transformCollateClause(ParseState* pstate, CollateClause* c);
static Node* make_row_comparison_op(ParseState* pstate, List* opname, List* largs, List* rargs, int location);
static Node* make_row_distinct_op(ParseState* pstate, List* opname, RowExpr* lrow, RowExpr* rrow, int location);
static Node* convertStarToCRef(RangeTblEntry* rte, char* catname, char* nspname, char* relname, int location);
static bool IsSequenceFuncCall(Node* filed1, Node* filed2, Node* filed3);
static Node* transformSequenceFuncCall(ParseState* pstate, Node* field1, Node* field2, Node* field3, int location);
static Node* transformConnectByRootFuncCall(ParseState* pstate, Node* funcNameVal, ColumnRef *cref);
static char *ColumnRefFindRelname(ParseState *pstate, const char *colname);
static Node *transformStartWithColumnRef(ParseState *pstate, ColumnRef *cref, char **colname);
static Node* tryTransformFunc(ParseState* pstate, List* fields, int location);

#define OrientedIsCOLorPAX(rte) ((rte)->orientation == REL_COL_ORIENTED || (rte)->orientation == REL_PAX_ORIENTED)

/*
 * transformExpr -
 *	  Analyze and transform expressions. Type checking and type casting is
 *	  done here. The optimizer and the executor cannot handle the original
 *	  (raw) expressions collected by the parse tree. Hence the transformation
 *	  here.
 *
 * NOTE: there are various cases in which this routine will get applied to
 * an already-transformed expression.  Some examples:
 *	1. At least one construct (BETWEEN/AND) puts the same nodes
 *	into two branches of the parse tree; hence, some nodes
 *	are transformed twice.
 *	2. Another way it can happen is that coercion of an operator or
 *	function argument to the required type (via coerce_type())
 *	can apply transformExpr to an already-transformed subexpression.
 *	An example here is "SELECT count(*) + 1.0 FROM table".
 *	3. CREATE TABLE t1 (LIKE t2 INCLUDING INDEXES) can pass in
 *	already-transformed index expressions.
 * While it might be possible to eliminate these cases, the path of
 * least resistance so far has been to ensure that transformExpr() does
 * no damage if applied to an already-transformed tree.  This is pretty
 * easy for cases where the transformation replaces one node type with
 * another, such as A_Const => Const; we just do nothing when handed
 * a Const.  More care is needed for node types that are used as both
 * input and output of transformExpr; see SubLink for example.
 */
Node* transformExpr(ParseState* pstate, Node* expr)
{
    Node* result = NULL;

    if (expr == NULL) {
        return NULL;
    }
    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    switch (nodeTag(expr)) {
        case T_ColumnRef:
            result = transformColumnRef(pstate, (ColumnRef*)expr);
            break;

        case T_ParamRef:
            result = transformParamRef(pstate, (ParamRef*)expr);
            break;

        case T_A_Const: {
            A_Const* con = (A_Const*)expr;
            Value* val = &con->val;

            result = (Node*)make_const(pstate, val, con->location);
            break;
        }

        case T_A_Indirection: {
            A_Indirection* ind = (A_Indirection*)expr;

            result = transformExpr(pstate, ind->arg);
            result = transformIndirection(pstate, result, ind->indirection);
            break;
        }

        case T_A_ArrayExpr:
            result = transformArrayExpr(pstate, (A_ArrayExpr*)expr, InvalidOid, InvalidOid, -1);
            break;

        case T_TypeCast: {
            TypeCast* tc = (TypeCast*)expr;

            /*
             * If the subject of the typecast is an ARRAY[] construct and
             * the target type is an array type, we invoke
             * transformArrayExpr() directly so that we can pass down the
             * type information.  This avoids some cases where
             * transformArrayExpr() might not infer the correct type.
             */
            if (IsA(tc->arg, A_ArrayExpr)) {
                Oid targetType;
                Oid elementType;
                int32 targetTypmod;

                typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);

                /*
                 * If target is a domain over array, work with the base
                 * array type here.  transformTypeCast below will cast the
                 * array type to the domain.  In the usual case that the
                 * target is not a domain, transformTypeCast is a no-op.
                 */
                targetType = getBaseTypeAndTypmod(targetType, &targetTypmod);
                elementType = get_element_type(targetType);
                if (OidIsValid(elementType)) {
                    tc = (TypeCast*)copyObject(tc);
                    tc->arg = transformArrayExpr(pstate, (A_ArrayExpr*)tc->arg, targetType, elementType, targetTypmod);
                }
            }

            result = transformTypeCast(pstate, tc);
            break;
        }

        case T_CollateClause:
            result = transformCollateClause(pstate, (CollateClause*)expr);
            break;

        case T_A_Expr: {
            A_Expr* a = (A_Expr*)expr;

            switch (a->kind) {
                case AEXPR_OP:
                    result = transformAExprOp(pstate, a);
                    break;
                case AEXPR_AND:
                    result = transformAExprAnd(pstate, a);
                    break;
                case AEXPR_OR:
                    result = transformAExprOr(pstate, a);
                    break;
                case AEXPR_NOT:
                    result = transformAExprNot(pstate, a);
                    break;
                case AEXPR_OP_ANY:
                    result = transformAExprOpAny(pstate, a);
                    break;
                case AEXPR_OP_ALL:
                    result = transformAExprOpAll(pstate, a);
                    break;
                case AEXPR_DISTINCT:
                    result = transformAExprDistinct(pstate, a);
                    break;
                case AEXPR_NULLIF:
                    result = transformAExprNullIf(pstate, a);
                    break;
                case AEXPR_OF:
                    result = transformAExprOf(pstate, a);
                    break;
                case AEXPR_IN:
                    result = transformAExprIn(pstate, a);
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized A_Expr kind: %d", a->kind)));
            }
            break;
        }

        case T_FuncCall:
            result = transformFuncCall(pstate, (FuncCall*)expr);
            break;

        case T_NamedArgExpr: {
            NamedArgExpr* na = (NamedArgExpr*)expr;

            na->arg = (Expr*)transformExpr(pstate, (Node*)na->arg);
            result = expr;
            break;
        }

        case T_SubLink:
            result = transformSubLink(pstate, (SubLink*)expr);
            break;

        case T_CaseExpr:
            result = transformCaseExpr(pstate, (CaseExpr*)expr);
            break;

        case T_RowExpr:
            result = transformRowExpr(pstate, (RowExpr*)expr);
            break;

        case T_CoalesceExpr:
            result = transformCoalesceExpr(pstate, (CoalesceExpr*)expr);
            break;

        case T_MinMaxExpr:
            result = transformMinMaxExpr(pstate, (MinMaxExpr*)expr);
            break;
        case T_GroupingFunc:
            result = transformGroupingFunc(pstate, (GroupingFunc*)expr);
            break;

        case T_XmlExpr:
            result = transformXmlExpr(pstate, (XmlExpr*)expr);
            break;

        case T_XmlSerialize:
            result = transformXmlSerialize(pstate, (XmlSerialize*)expr);
            break;

        case T_NullTest: {
            NullTest* n = (NullTest*)expr;

            n->arg = (Expr*)transformExpr(pstate, (Node*)n->arg);
            /* the argument can be any type, so don't coerce it */
            n->argisrow = type_is_rowtype(exprType((Node*)n->arg));
            result = expr;
            break;
        }

        case T_BooleanTest:
            result = transformBooleanTest(pstate, (BooleanTest*)expr);
            break;

        case T_CurrentOfExpr:
            result = transformCurrentOfExpr(pstate, (CurrentOfExpr*)expr);
            break;

        case T_PredictByFunction:
            result = transformPredictByFunction(pstate, (PredictByFunction*) expr);
            break;

            /*********************************************
             * Quietly accept node types that may be presented when we are
             * called on an already-transformed tree.
             *
             * Do any other node types need to be accepted?  For now we are
             * taking a conservative approach, and only accepting node
             * types that are demonstrably necessary to accept.
             *********************************************/
        case T_Var:
        case T_Const:
        case T_Param:
        case T_Aggref:
        case T_WindowFunc:
        case T_ArrayRef:
        case T_FuncExpr:
        case T_Rownum:
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
        case T_ScalarArrayOpExpr:
        case T_BoolExpr:
        case T_FieldSelect:
        case T_FieldStore:
        case T_RelabelType:
        case T_CoerceViaIO:
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_CollateExpr:
        case T_CaseTestExpr:
        case T_ArrayExpr:
        case T_CoerceToDomain:
        case T_CoerceToDomainValue:
        case T_SetToDefault: {
            result = (Node*)expr;
            break;
        }

        default:
            /* should not reach here */
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(expr))));
            break;
    }

    return result;
}

/*
 * helper routine for delivering "column does not exist" error message
 *
 * (Usually we don't have to work this hard, but the general case of field
 * selection from an arbitrary node needs it.)
 */
static void unknown_attribute(ParseState* pstate, Node* relref, char* attname, int location)
{
    RangeTblEntry* rte = NULL;

    if (IsA(relref, Var) && ((Var*)relref)->varattno == InvalidAttrNumber) {
        /* Reference the RTE by alias not by actual table name */
        rte = GetRTEByRangeTablePosn(pstate, ((Var*)relref)->varno, ((Var*)relref)->varlevelsup);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("column %s.%s does not exist", rte->eref->aliasname, attname),
                parser_errposition(pstate, location)));
    } else {
        /* Have to do it by reference to the type of the expression */
        Oid relTypeId = exprType(relref);

        if (ISCOMPLEX(relTypeId)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" not found in data type %s", attname, format_type_be(relTypeId)),
                    parser_errposition(pstate, location)));
        } else if (relTypeId == RECORDOID) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("could not identify column \"%s\" in record data type", attname),
                    parser_errposition(pstate, location)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("column notation .%s applied to type %s, "
                           "which is not a composite type",
                        attname,
                        format_type_be(relTypeId)),
                    parser_errposition(pstate, location)));
        }
    }
}

static Node* transformIndirection(ParseState* pstate, Node* basenode, List* indirection)
{
    Node* result = basenode;
    List* subscripts = NIL;
    int location = exprLocation(basenode);
    ListCell* i = NULL;

    /*
     * We have to split any field-selection operations apart from
     * subscripting.  Adjacent A_Indices nodes have to be treated as a single
     * multidimensional subscript operation.
     */
    foreach (i, indirection) {
        Node* n = (Node*)lfirst(i);

        if (IsA(n, A_Indices)) {
            subscripts = lappend(subscripts, n);
        } else if (IsA(n, A_Star)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("row expansion via \"*\" is not supported here"),
                    parser_errposition(pstate, location)));
        } else {
            Node* newresult = NULL;

            AssertEreport(IsA(n, String), MOD_OPT, "");

            /* process subscripts before this field selection */
            if (subscripts != NIL) {
                result = (Node*)transformArraySubscripts(
                    pstate, result, exprType(result), InvalidOid, exprTypmod(result), subscripts, NULL);
            }
            subscripts = NIL;

            newresult = ParseFuncOrColumn(pstate, list_make1(n), list_make1(result), NULL, location);
            if (newresult == NULL) {
                unknown_attribute(pstate, result, strVal(n), location);
            }
            result = newresult;
        }
    }
    /* process trailing subscripts, if any */
    if (subscripts != NIL) {
        result = (Node*)transformArraySubscripts(
            pstate, result, exprType(result), InvalidOid, exprTypmod(result), subscripts, NULL);
    }
    return result;
}

static Node* replaceExprAliasIfNecessary(ParseState* pstate, char* colname, ColumnRef* cref)
{
    ListCell* lc = NULL;
    bool isFind = false;
    Expr* matchExpr = NULL;
    TargetEntry* tle = NULL;
    foreach (lc, pstate->p_target_list) {
        tle = (TargetEntry*)lfirst(lc);
        /*
         * 1. in a select stmt in stored procudre, a columnref may be a param(e.g. a declared var or the stored
         *    procedure's arg), which is not a alias, so can not be matched here.
         * 2. in a select stmt in stored procudre such like a[1],a[2],a[3], they have same name,
         *    so, we should pass this target.
         */
        bool isArrayParam = IsA(tle->expr, ArrayRef) && ((ArrayRef*)tle->expr)->refexpr != NULL &&
                            IsA(((ArrayRef*)tle->expr)->refexpr, Param);
        if (tle->resname != NULL && !IsA(tle->expr, Param) && !isArrayParam &&
            strncmp(tle->resname, colname, strlen(colname) + 1) == 0) {
            if (checkExprHasWindowFuncs((Node*)tle->expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("Alias \"%s\" reference with window function included is not supported.", colname),
                        parser_errposition(pstate, cref->location)));
#ifndef ENABLE_MULTIPLE_NODES
            } else if (ContainRownumExpr((Node*)tle->expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("Alias \"%s\" reference with ROWNUM included is invalid.", colname),
                     parser_errposition(pstate, cref->location)));
#endif					 
            } else if (contain_volatile_functions((Node*)tle->expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("Alias \"%s\" reference with volatile function included is not supported.", colname),
                        parser_errposition(pstate, cref->location)));
            } else {
                if (!isFind) {
                    matchExpr = tle->expr;
                    isFind = true;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("Alias \"%s\" is ambiguous.", colname),
                            parser_errposition(pstate, cref->location)));
                    return NULL;
                }
            }
        }
    }
    return (Node*)copyObject(matchExpr);
}

static Node* ParseColumnRef(ParseState* pstate, RangeTblEntry* rte, char* colname, ColumnRef* cref)
{
    Node* node = NULL;

    /* Try to identify as a column of the RTE */
    node = scanRTEForColumn(pstate, rte, colname, cref->location);
    if (node == NULL) {
        /* Try it as a function call on the whole row */
        node = transformWholeRowRef(pstate, rte, cref->location);
        node = ParseFuncOrColumn(pstate, list_make1(makeString(colname)), list_make1(node), NULL, cref->location);
    }

    return node;
}

static ColumnRef *fixSWNameSubLevel(RangeTblEntry *rte, char *rname, char **colname)
{
    ListCell *lc = NULL;

    foreach(lc, rte->eref->colnames) {
        Value *val = (Value *)lfirst(lc);

        if (strstr(strVal(val), *colname)) {
            *colname = pstrdup(strVal(val));
            break;
        }
    }

    ColumnRef* c = makeNode(ColumnRef);
    c->fields = list_make2((Node*)makeString(rname), (Node*)makeString(*colname));
    c->location = -1;

    return c;
}

/*
 * Transform a ColumnRef.
 *
 * If you find yourself changing this code, see also ExpandColumnRefStar.
 */
Node* transformColumnRef(ParseState* pstate, ColumnRef* cref)
{
    Node* node = NULL;
    char* nspname = NULL;
    char* relname = NULL;
    char* colname = NULL;
    RangeTblEntry* rte = NULL;
    int levels_up;
    bool hasplus = false;
    enum { CRERR_NO_COLUMN, CRERR_NO_RTE, CRERR_WRONG_DB, CRERR_TOO_MANY } crerr = CRERR_NO_COLUMN;

    /*
     * Give the PreParseColumnRefHook, if any, first shot.	If it returns
     * non-null then that's all, folks.
     */
    if (pstate->p_pre_columnref_hook != NULL) {
        node = (*pstate->p_pre_columnref_hook)(pstate, cref);
        if (node != NULL) {
            return node;
        }
    }

    /*
     * First to check if last cref->fields is "(+)" we have to evaluate the previous
     * elements can costructe a
     */
    int fields_len = list_length(cref->fields);
    hasplus = IsColumnRefPlusOuterJoin(cref);
    if (hasplus) {
        /*
         * Report error when found operator "(+)" and ignoreplus is false.
         * Actually we only set ignoreplus be true in WhereClause of Select Satatement for now.
         */
        if (!pstate->ignoreplus) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Operator \"(+)\" can only be used in WhereClause of Select-Statement or Subquery."),
                    parser_errposition(pstate, cref->location)));
        }

        /* If identify it is a "(+)" we need minus the length first */
        fields_len--;
    }

    /* ----------
     * The allowed syntaxes are:
     *
     * A		First try to resolve as unqualified column name;
     *			if no luck, try to resolve as unqualified table name (A.*).
     * A.B		A is an unqualified table name; B is either a
     *			column or function name (trying column name first).
     * A.B.C	schema A, table B, col or func name C.
     * A.B.C.D	catalog A, schema B, table C, col or func D.
     * A.*		A is an unqualified table name; means whole-row value.
     * A.B.*	whole-row value of table B in schema A.
     * A.B.C.*	whole-row value of table C in schema B in catalog A.
     *
     * We do not need to cope with bare "*"; that will only be accepted by
     * the grammar at the top level of a SELECT list, and transformTargetList
     * will take care of it before it ever gets here.  Also, "A.*" etc will
     * be expanded by transformTargetList if they appear at SELECT top level,
     * so here we are only going to see them as function or operator inputs.
     *
     * Currently, if a catalog name is given then it must equal the current
     * database name; we check it here and then discard it.
     * ----------
     */
    switch (fields_len) {
        case 1: {
            Node* field1 = (Node*)linitial(cref->fields);

            AssertEreport(IsA(field1, String), MOD_OPT, "");
            colname = strVal(field1);

            if (pstate->p_hasStartWith) {
                Node *expr = transformStartWithColumnRef(pstate, cref, &colname);

                /* function case, return directly */
                if (expr != NULL) {
                    return expr;
                }
            }

            /*
             * Try to identify as an unqualified column
             *
             * if hasplus, only consider current pstate level
             */
            node = colNameToVar(pstate, colname, hasplus, cref->location, &rte);

            if (hasplus && node == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                        errmsg("column reference \"%s\" is ambiguous.", colname),
                        errhint("\"%s\" with \"(+)\" can only reference relation in current query level.", colname),
                        parser_errposition(pstate, cref->location)));
            }

            if (node == NULL) {
                /*
                 * Not known as a column of any range-table entry.
                 *
                 * Consider the possibility that it's VALUE in a domain
                 * check expression.  (We handle VALUE as a name, not a
                 * keyword, to avoid breaking a lot of applications that
                 * have used VALUE as a column name in the past.)
                 */
                if (pstate->p_value_substitute != NULL && strcmp(colname, "value") == 0) {
                    node = (Node*)copyObject(pstate->p_value_substitute);

                    /*
                     * Try to propagate location knowledge.  This should
                     * be extended if p_value_substitute can ever take on
                     * other node types.
                     */
                    if (IsA(node, CoerceToDomainValue)) {
                        ((CoerceToDomainValue*)node)->location = cref->location;
                    }
                    break;
                }

                /*
                 * Try to find the name as a relation.	Note that only
                 * relations already entered into the rangetable will be
                 * recognized.
                 *
                 * This is a hack for backwards compatibility with
                 * PostQUEL-inspired syntax.  The preferred form now is
                 * "rel.*".
                 */
                rte = refnameRangeTblEntry(pstate, NULL, colname, cref->location, &levels_up);
                if (rte != NULL) {
                    if ((OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) &&
                        ((rte->alias && (strcmp(rte->alias->aliasname, colname) == 0)) ||
                            (strcmp(rte->relname, colname) == 0))) {
                        Node* row_expr = convertStarToCRef(rte, NULL, NULL, colname, cref->location);
                        node = transformExpr(pstate, row_expr);
                    } else {
                        node = transformWholeRowRef(pstate, rte, cref->location);
                    }
                    break;
                }

                /*expr of target_list replace of node*/
                node = replaceExprAliasIfNecessary(pstate, colname, cref);

                if (!pstate->isAliasReplace && contain_subplans(node)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg(
                                "Alias \"%s\" contains subplan, which is not supported to use in grouping() function",
                                colname)));
                }
                /* 
                 * Now give the p_bind_variable_columnref_hook, check column index. only DBE_SQL can get here.
                 */
                if (node == NULL) {
                    if (pstate->p_bind_variable_columnref_hook != NULL) {
                        node = (*pstate->p_bind_variable_columnref_hook)(pstate, cref);
                        if (node != NULL) {
                            return node;
                        }
                    }
                    if (pstate->p_bind_describe_hook != NULL) {
                        node = (*pstate->p_bind_describe_hook)(pstate, cref);
                        return node;
                    }
                }
            }
            break;
        }
        case 2: {
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);

            AssertEreport(IsA(field1, String), MOD_OPT, "");
            relname = strVal(field1);

            /* Locate the referenced RTE */
            if (hasplus) {
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            /* check if it's sequence function call, like: sequence1.nextval */
            if (rte == NULL && IsSequenceFuncCall(NULL, field1, field2)) {
                return transformSequenceFuncCall(pstate, NULL, field1, field2, cref->location);

            } else if (rte == NULL) {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field2, A_Star)) {
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    Node* row_expr = convertStarToCRef(rte, NULL, NULL, relname, cref->location);
                    node = transformExpr(pstate, row_expr);
                } else {
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            AssertEreport(IsA(field2, String), MOD_OPT, "");
            colname = strVal(field2);

            if (rte->rtekind == RTE_SUBQUERY && rte->swSubExist) {
                cref = fixSWNameSubLevel(rte, relname, &colname);
            }

            if (pstate->p_hasStartWith || rte->swConverted) {
                Node *expr = transformStartWithColumnRef(pstate, cref, &colname);

                /* function case, return directly */
                if (expr != NULL) {
                    return expr;
                }

                if (strstr(colname, "@")) {
                    ListCell *lc = NULL;

                    foreach(lc, pstate->p_rtable) {
                        RangeTblEntry *tbl = (RangeTblEntry *)lfirst(lc);

                        if (tbl->relname != NULL &&
                            strcmp(tbl->relname, "tmp_reuslt") == 0) {
                            rte = tbl;
                            break;
                        }
                    }
                }
            }

            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }
        case 3: {
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);

            AssertEreport(IsA(field1, String), MOD_OPT, "");
            nspname = strVal(field1);
            AssertEreport(IsA(field2, String), MOD_OPT, "");
            relname = strVal(field2);

            /* Locate the referenced RTE */
            if (hasplus) {
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            /* check if it's sequence function call, like: nsp.sequence.nextval */
            if (rte == NULL && IsSequenceFuncCall(field1, field2, field3)) {
                return transformSequenceFuncCall(pstate, field1, field2, field3, cref->location);

            } else if (rte == NULL) {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field3, A_Star)) {
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    Node* row_expr = convertStarToCRef(rte, NULL, nspname, relname, cref->location);
                    node = transformExpr(pstate, row_expr);
                } else {
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            AssertEreport(IsA(field3, String), MOD_OPT, "");
            colname = strVal(field3);

            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }
        case 4: {
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);
            Node* field4 = (Node*)lfourth(cref->fields);
            char* catname = NULL;

            AssertEreport(IsA(field1, String), MOD_OPT, "");
            catname = strVal(field1);
            AssertEreport(IsA(field2, String), MOD_OPT, "");
            nspname = strVal(field2);
            AssertEreport(IsA(field3, String), MOD_OPT, "");
            relname = strVal(field3);

            /*
             * We check the catalog name and then ignore it.
             */
            if (strcmp(catname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)) != 0) {
                crerr = CRERR_WRONG_DB;
                break;
            }

            /* Locate the referenced RTE */
            if (hasplus) {
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            /* check if it's sequence function call, like: nsp.sequence.nextval */
            if (rte == NULL && IsSequenceFuncCall(field2, field3, field4)) {
                return transformSequenceFuncCall(pstate, field2, field3, field4, cref->location);

            } else if (rte == NULL) {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field4, A_Star)) {
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    Node* row_expr = convertStarToCRef(rte, catname, nspname, relname, cref->location);
                    node = transformExpr(pstate, row_expr);
                } else {
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            AssertEreport(IsA(field4, String), MOD_OPT, "");
            colname = strVal(field4);

            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }
        default:
            crerr = CRERR_TOO_MANY; /* too many dotted names */
            break;
    }

    /*
     * Now give the PostParseColumnRefHook, if any, a chance.  We pass the
     * translation-so-far so that it can throw an error if it wishes in the
     * case that it has a conflicting interpretation of the ColumnRef. (If it
     * just translates anyway, we'll throw an error, because we can't undo
     * whatever effects the preceding steps may have had on the pstate.) If it
     * returns NULL, use the standard translation, or throw a suitable error
     * if there is none.
     */
    if (pstate->p_post_columnref_hook != NULL) {
        Node* hookresult = NULL;

        /*
         * if node is not a table column, we should pass a null to the hook,
         * or it will misjudge the columnref as ambiguous.
         */
        hookresult = (*pstate->p_post_columnref_hook)(pstate, cref, node);
        if (node == NULL) {
            node = hookresult;
        } else if (hookresult != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                    errmsg("column reference \"%s\" is ambiguous", NameListToString(cref->fields)),
                    parser_errposition(pstate, cref->location)));
        }
    }
    
    if (node == NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        node = tryTransformFunc(pstate, cref->fields, cref->location);
        if (node) {
            return node;
        }
    }
    /* check current column match request index or not, use value in func in A_FORMAT */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        Node* check = plpgsql_check_match_var(node, pstate, cref);
        if (check) {
            return check;
        }
    }

    /*
     * Throw error if no translation found.
     */
    if (node == NULL) {
        switch (crerr) {
            case CRERR_NO_COLUMN:
                errorMissingColumn(pstate, relname, colname, cref->location);
                break;
            case CRERR_NO_RTE:
                errorMissingRTE(pstate, makeRangeVar(nspname, relname, cref->location), hasplus);
                break;
            case CRERR_WRONG_DB:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cross-database references are not implemented: %s", NameListToString(cref->fields)),
                        parser_errposition(pstate, cref->location)));
                break;
            case CRERR_TOO_MANY:
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("improper qualified name (too many dotted names): %s", NameListToString(cref->fields)),
                        parser_errposition(pstate, cref->location)));
                break;
            default:
                break;
        }
    }

    /* Only when hanlde operator "(+)" in WhereClause need recode the RTE info */
    if (pstate->p_plusjoin_rte_info != NULL && pstate->p_plusjoin_rte_info->needrecord && rte != NULL) {
        pstate->p_plusjoin_rte_info->info =
            lappend(pstate->p_plusjoin_rte_info->info, makePlusJoinRTEItem(rte, hasplus));
    }

    return node;
}

static bool isCol2Function(List* fields)
{
    char* schemaname = NULL;
    char* pkgname = NULL;
    char* funcname = NULL;
    DeconstructQualifiedName(fields, &schemaname, &funcname, &pkgname);
    Oid npsOid = InvalidOid;
    Oid pkgOid = InvalidOid;
    if (schemaname != NULL) {
        npsOid = get_namespace_oid(schemaname, true);
    }
    else {
        npsOid = getCurrentNamespace();
    }

    if (pkgname != NULL) {
        pkgOid = PackageNameListGetOid(fields, true);
    }

    bool is_found = false;
    /* check args and if have return arg*/
    CatCList   *catlist = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
#else
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif
    for (int i = 0; i < catlist->n_members; i++) {
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
        /* get function all args */
        Oid *p_argtypes = NULL;
        char **p_argnames = NULL;
        char *p_argmodes = NULL;
        int allArgs = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
        if (allArgs > 0 || !OidIsValid(procform->prorettype)) {
            continue;
        }

        if (OidIsValid(npsOid)) {
            /* Consider only procs in specified namespace */
            if (procform->pronamespace != npsOid) {
                continue;
            }
	    }

        if (OidIsValid(pkgOid)) {
            bool isNull = false;
            Datum packageid_datum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isNull);
            Oid packageid = ObjectIdGetDatum(packageid_datum);
            if (packageid != pkgOid) {
                continue;
            }
        }
        is_found = true;
    }
    ReleaseSysCacheList(catlist);
    return is_found;
}

static Node* tryTransformFunc(ParseState* pstate, List* fields, int location)
{
    if (!isCol2Function(fields)) {
        return NULL;
    }

    Node* result = NULL;
    FuncCall *fn = makeNode(FuncCall);
    fn->funcname = fields;
    fn->args = NIL;
    fn->agg_order = NIL;
    fn->agg_star = FALSE;
    fn->agg_distinct = FALSE;
    fn->func_variadic = FALSE;
    fn->over = NULL;
    fn->location = location;
    fn->call_func = false;
    /* function must have 0 args */
    List* targs = NIL;

    /* ... and hand off to ParseFuncOrColumn */
    result = ParseFuncOrColumn(pstate, fn->funcname, targs, fn, fn->location, fn->call_func);

    /* extract out parameter for package function */
    if (IsPackageFunction(fn->funcname) && result != NULL && nodeTag(result) == T_FuncExpr && fn->call_func) {
        FuncExpr* funcexpr = (FuncExpr*)result;
        int funcoid = funcexpr->funcid;
        funcexpr->args = extract_function_outarguments(funcoid, funcexpr->args, fn->funcname);
    }

    return result;
}

static Node* transformParamRef(ParseState* pstate, ParamRef* pref)
{
    Node* result = NULL;

    /*
     * The core parser knows nothing about Params.	If a hook is supplied,
     * call it.  If not, or if the hook returns NULL, throw a generic error.
     */
    if (pstate->p_paramref_hook != NULL) {
        result = (*pstate->p_paramref_hook)(pstate, pref);
    } else {
        result = NULL;
    }
    if (result == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PARAMETER),
                errmsg("there is no parameter $%d", pref->number),
                parser_errposition(pstate, pref->location)));
    }
    return result;
}

/* Test whether an a_expr is a plain NULL constant or not */
static bool exprIsNullConstant(Node* arg)
{
    if (arg && IsA(arg, A_Const)) {
        A_Const* con = (A_Const*)arg;

        if (con->val.type == T_Null) {
            return true;
        }
    }
    return false;
}

static Node* transformAExprOp(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = a->lexpr;
    Node* rexpr = a->rexpr;
    Node* result = NULL;

    /*
     * Special-case "foo = NULL" and "NULL = foo" for compatibility with
     * standards-broken products (like Microsoft's).  Turn these into IS NULL
     * exprs. (If either side is a CaseTestExpr, then the expression was
     * generated internally from a CASE-WHEN expression, and
     * transform_null_equals does not apply.)
     */
    if (u_sess->attr.attr_sql.Transform_null_equals && list_length(a->name) == 1 &&
        strcmp(strVal(linitial(a->name)), "=") == 0 && (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)) &&
        (!IsA(lexpr, CaseTestExpr) && !IsA(rexpr, CaseTestExpr))) {
        NullTest* n = makeNode(NullTest);

        n->nulltesttype = IS_NULL;

        n->arg = exprIsNullConstant(lexpr) ? (Expr *)rexpr : (Expr *)lexpr;

        result = transformExpr(pstate, (Node*)n);
    } else if (lexpr && IsA(lexpr, RowExpr) && rexpr && IsA(rexpr, SubLink) &&
               ((SubLink*)rexpr)->subLinkType == EXPR_SUBLINK) {
        /*
         * Convert "row op subselect" into a ROWCOMPARE sublink. Formerly the
         * grammar did this, but now that a row construct is allowed anywhere
         * in expressions, it's easier to do it here.
         */
        SubLink* s = (SubLink*)rexpr;

        s->subLinkType = ROWCOMPARE_SUBLINK;
        s->testexpr = lexpr;
        s->operName = a->name;
        s->location = a->location;
        result = transformExpr(pstate, (Node*)s);
    } else if (lexpr && IsA(lexpr, RowExpr) && rexpr && IsA(rexpr, RowExpr)) {
        /* "row op row" */
        lexpr = transformExpr(pstate, lexpr);
        rexpr = transformExpr(pstate, rexpr);
        AssertEreport(IsA(lexpr, RowExpr), MOD_OPT, "");
        AssertEreport(IsA(rexpr, RowExpr), MOD_OPT, "");

        result = make_row_comparison_op(pstate, a->name, ((RowExpr*)lexpr)->args, ((RowExpr*)rexpr)->args, a->location);
    } else {
        /* Ordinary scalar operator */
        lexpr = transformExpr(pstate, lexpr);
        rexpr = transformExpr(pstate, rexpr);

        result = (Node*)make_op(pstate, a->name, lexpr, rexpr, a->location);
    }

    return result;
}

static Node* transformAExprAnd(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Node* rexpr = transformExpr(pstate, a->rexpr);

    lexpr = coerce_to_boolean(pstate, lexpr, "AND");
    rexpr = coerce_to_boolean(pstate, rexpr, "AND");

    return (Node*)makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), a->location);
}

static Node* transformAExprOr(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Node* rexpr = transformExpr(pstate, a->rexpr);

    lexpr = coerce_to_boolean(pstate, lexpr, "OR");
    rexpr = coerce_to_boolean(pstate, rexpr, "OR");

    return (Node*)makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), a->location);
}

static Node* transformAExprNot(ParseState* pstate, A_Expr* a)
{
    Node* rexpr = transformExpr(pstate, a->rexpr);

    rexpr = coerce_to_boolean(pstate, rexpr, "NOT");

    return (Node*)makeBoolExpr(NOT_EXPR, list_make1(rexpr), a->location);
}

static Node* transformAExprOpAny(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Node* rexpr = transformExpr(pstate, a->rexpr);

    return (Node*)make_scalar_array_op(pstate, a->name, true, lexpr, rexpr, a->location);
}

static Node* transformAExprOpAll(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Node* rexpr = transformExpr(pstate, a->rexpr);

    return (Node*)make_scalar_array_op(pstate, a->name, false, lexpr, rexpr, a->location);
}

static Node* transformAExprDistinct(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Node* rexpr = transformExpr(pstate, a->rexpr);

    if (lexpr && IsA(lexpr, RowExpr) && rexpr && IsA(rexpr, RowExpr)) {
        /* "row op row" */
        return make_row_distinct_op(pstate, a->name, (RowExpr*)lexpr, (RowExpr*)rexpr, a->location);
    } else {
        /* Ordinary scalar operator */
        return (Node*)make_distinct_op(pstate, a->name, lexpr, rexpr, a->location);
    }
}

static Node* transformAExprNullIf(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Node* rexpr = transformExpr(pstate, a->rexpr);
    OpExpr* result = NULL;

    result = (OpExpr*)make_op(pstate, a->name, lexpr, rexpr, a->location);

    /*
     * The comparison operator itself should yield boolean ...
     */
    if (result->opresulttype != BOOLOID) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("NULLIF requires = operator to yield boolean"),
                parser_errposition(pstate, a->location)));
    }
    /*
     * ... but the NullIfExpr will yield the first operand's type.
     */
    result->opresulttype = exprType((Node*)linitial(result->args));

    /*
     * We rely on NullIfExpr and OpExpr being the same struct
     */
    NodeSetTag(result, T_NullIfExpr);

    return (Node*)result;
}

static Node* transformAExprOf(ParseState* pstate, A_Expr* a)
{
    /*
     * Checking an expression for match to a list of type names. Will result
     * in a boolean constant node.
     */
    Node* lexpr = transformExpr(pstate, a->lexpr);
    Const* result = NULL;
    ListCell* telem = NULL;
    Oid ltype, rtype;
    bool matched = false;

    ltype = exprType(lexpr);
    foreach (telem, (List*)a->rexpr) {
        rtype = typenameTypeId(pstate, (const TypeName*)lfirst(telem));
        matched = (rtype == ltype);
        if (matched) {
            break;
        }
    }

    /*
     * We have two forms: equals or not equals. Flip the sense of the result
     * for not equals.
     */
    if (strcmp(strVal(linitial(a->name)), "<>") == 0) {
        matched = (!matched);
    }
    result = (Const*)makeBoolConst(matched, false);

    /* Make the result have the original input's parse location */
    result->location = exprLocation((Node*)a);

    return (Node*)result;
}

static Node* transformAExprIn(ParseState* pstate, A_Expr* a)
{
    Node* result = NULL;
    Node* lexpr = NULL;
    List* rexprs = NIL;
    List* rvars = NIL;
    List* rnonvars = NIL;
    bool useOr = false;
    bool haveRowExpr = false;
    ListCell* l = NULL;

    /*
     * If the operator is <>, combine with AND not OR.
     */
    if (strcmp(strVal(linitial(a->name)), "<>") == 0) {
        useOr = false;
    } else {
        useOr = true;
    }
    /*
     * We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is only
     * possible if the inputs are all scalars (no RowExprs) and there is a
     * suitable array type available.  If not, we fall back to a boolean
     * condition tree with multiple copies of the lefthand expression. Also,
     * any IN-list items that contain Vars are handled as separate boolean
     * conditions, because that gives the planner more scope for optimization
     * on such clauses.
     *
     * First step: transform all the inputs, and detect whether any are
     * RowExprs or contain Vars.
     */
    lexpr = transformExpr(pstate, a->lexpr);
    haveRowExpr = (lexpr && IsA(lexpr, RowExpr));
    rexprs = rvars = rnonvars = NIL;
    foreach (l, (List*)a->rexpr) {
        Node* rexpr = (Node*)transformExpr(pstate, (Node*)lfirst(l));

        haveRowExpr = haveRowExpr || (rexpr && IsA(rexpr, RowExpr));
        rexprs = lappend(rexprs, rexpr);
        if (contain_vars_of_level(rexpr, 0)) {
            rvars = lappend(rvars, rexpr);
        } else {
            rnonvars = lappend(rnonvars, rexpr);
        }
    }

    /*
     * ScalarArrayOpExpr is only going to be useful if there's more than one
     * non-Var righthand item.	Also, it won't work for RowExprs.
     */
    if (!haveRowExpr && list_length(rnonvars) > 1) {
        List* allexprs = NIL;
        Oid scalar_type;
        Oid array_type;

        /*
         * Try to select a common type for the array elements.	Note that
         * since the LHS' type is first in the list, it will be preferred when
         * there is doubt (eg, when all the RHS items are unknown literals).
         *
         * Note: use list_concat here not lcons, to avoid damaging rnonvars.
         */
        allexprs = list_concat(list_make1(lexpr), rnonvars);
        scalar_type = select_common_type(pstate, allexprs, NULL, NULL);

        /* Do we have an array type to use? */
        if (OidIsValid(scalar_type)) {
            array_type = get_array_type(scalar_type);
        } else {
            array_type = InvalidOid;
        }
        if (array_type != InvalidOid) {
            /*
             * OK: coerce all the right-hand non-Var inputs to the common type
             * and build an ArrayExpr for them.
             */
            List* aexprs = NIL;
            ArrayExpr* newa = NULL;

            aexprs = NIL;
            foreach (l, rnonvars) {
                Node* rexpr = (Node*)lfirst(l);

                rexpr = coerce_to_common_type(pstate, rexpr, scalar_type, "IN");
                aexprs = lappend(aexprs, rexpr);
            }
            newa = makeNode(ArrayExpr);
            newa->array_typeid = array_type;
            /* array_collid will be set by parse_collate.c */
            newa->element_typeid = scalar_type;
            newa->elements = aexprs;
            newa->multidims = false;
            newa->location = -1;

            result = (Node*)make_scalar_array_op(pstate, a->name, useOr, lexpr, (Node*)newa, a->location);

            /* Consider only the Vars (if any) in the loop below */
            rexprs = rvars;
        }
    }

    /*
     * Must do it the hard way, ie, with a boolean expression tree.
     */
    foreach (l, rexprs) {
        Node* rexpr = (Node*)lfirst(l);
        Node* cmp = NULL;

        if (haveRowExpr) {
            if (!IsA(lexpr, RowExpr) || !IsA(rexpr, RowExpr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("arguments of row IN must all be row expressions"),
                        parser_errposition(pstate, a->location)));
            }
            cmp = make_row_comparison_op(
                pstate, a->name, (List*)copyObject(((RowExpr*)lexpr)->args), ((RowExpr*)rexpr)->args, a->location);
        } else {
            cmp = (Node*)make_op(pstate, a->name, (Node*)copyObject(lexpr), rexpr, a->location);
        }
        cmp = coerce_to_boolean(pstate, cmp, "IN");
        if (result == NULL) {
            result = cmp;
        } else {
            result = (Node*)makeBoolExpr(useOr ? OR_EXPR : AND_EXPR, list_make2(result, cmp), a->location);
        }
    }

    return result;
}

static bool IsStartWithFunction(FuncExpr* result)
{
    return result->funcid == SYS_CONNECT_BY_PATH_FUNCOID ||
           result->funcid == CONNECT_BY_ROOT_FUNCOID;
}

static bool NeedExtractOutParam(FuncCall* fn, Node* result)
{
    if (result == NULL || nodeTag(result) != T_FuncExpr) {
        return false;
    }

    /* Always extract for called package functions */
    if (IsPackageFunction(fn->funcname) && fn->call_func) {
        return true;
    }

    /*
     * When proc_outparam_override is on, extract all but select func
     */
    FuncExpr* funcexpr = (FuncExpr*)result;
    if (is_function_with_plpgsql_language_and_outparam(funcexpr->funcid) && !fn->call_func) {
        return true;
    }
    char prokind = get_func_prokind(funcexpr->funcid);
    if (!PROC_IS_PRO(prokind) && !fn->call_func) {
        return false;
    }
#ifndef ENABLE_MULTIPLE_NODES
    return enable_out_param_override();
#else
    return false;
#endif
}

static Node* transformFuncCall(ParseState* pstate, FuncCall* fn)
{
    List* targs = NIL;
    ListCell* args = NULL;
    Node* result = NULL;

    /* Transform the list of arguments ... */
    targs = NIL;
    foreach (args, fn->args) {
        targs = lappend(targs, transformExpr(pstate, (Node*)lfirst(args)));
    }

    if (fn->agg_within_group) {
        Assert(fn->agg_order != NIL);
        foreach (args, fn->agg_order) {
            SortBy* arg = (SortBy*)lfirst(args);

            targs = lappend(targs, transformExpr(pstate, arg->node));
        }
    }

    /* ... and hand off to ParseFuncOrColumn */
    result = ParseFuncOrColumn(pstate, fn->funcname, targs, fn, fn->location, fn->call_func);

    if (IsStartWithFunction((FuncExpr*)result) && !pstate->p_hasStartWith) {
        ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OPT),
                errmsg("Invalid function call."),
                errdetail("START WITH CONNECT BY function found in non-hierarchical query."),
                errcause("Incorrect query input"),
                erraction("Please check and revise your query")));
    }

    /* extract out parameter for package function */
    if (NeedExtractOutParam(fn, result)) {
        FuncExpr* funcexpr = (FuncExpr*)result;
        int funcoid = funcexpr->funcid;
        funcexpr->args = extract_function_outarguments(funcoid, funcexpr->args, fn->funcname);
    }

    /* lock nextval on cn when transformFuncExpr to avoid dead lock with alter sequence */
    lockSeqForNextvalFunc(result);
    return result;
}

/*
 * @Description: We should lock sequence on CN when use nextval to avoid deadlock on dn
 * with alter sequence.
 * @in node - FuncExpr node
 */
void lockSeqForNextvalFunc(Node* node)
{
    if (node != NULL && IsA(node, FuncExpr) && ((FuncExpr*)node)->funcid == NEXTVALFUNCOID) {
        FuncExpr* funcexpr = (FuncExpr*)node;
        Assert(funcexpr->args->length == 1);
        if (IsA(linitial(funcexpr->args), Const)) {
            Const* con = (Const*)linitial(funcexpr->args);
            /* lock sequence and hold the lock */
            Oid relid = DatumGetObjectId(con->constvalue);
            lockNextvalOnCn(relid);
        }
    }
}

/*
 * @Description: get function's oid for overlaod function
 * @in fun_expr - function call statement
 * @in expr - plpgsql expr
 * @return - function's oid
 */
Oid getMultiFuncInfo(char* fun_expr, PLpgSQL_expr* expr)
{
    List* raw_parser_list = raw_parser(fun_expr);
    ListCell* parsetree_item = NULL;

    foreach (parsetree_item, raw_parser_list) {
        Node* parsetree = (Node*)lfirst(parsetree_item);
        ParseState* pstate = make_parsestate(NULL);

        plpgsql_parser_setup(pstate, expr);
        if (nodeTag(parsetree) == T_SelectStmt) {
            SelectStmt* stmt = (SelectStmt*)parsetree;
            List* frmList = stmt->fromClause;
            ListCell* fl = NULL;
            foreach (fl, frmList) {
                Node* n = (Node*)lfirst(fl);
                if (IsA(n, RangeFunction)) {
                    RangeFunction* r = (RangeFunction*)n;
                    if (r->funccallnode != NULL && nodeTag(r->funccallnode) == T_FuncCall) {
                        List* targs = NIL;
                        ListCell* args = NULL;
                        FuncCall* fn = (FuncCall*)(r->funccallnode);

                        foreach (args, fn->args) {
                            targs = lappend(targs, transformExpr(pstate, (Node*)lfirst(args)));
                        }

                        Node* result = ParseFuncOrColumn(pstate, fn->funcname, targs, fn, fn->location, true);

                        if (result != NULL && nodeTag(result) == T_FuncExpr) {
                            FuncExpr* funcexpr = (FuncExpr*)result;
                            return funcexpr->funcid;
                        }
                    }
                }
            }
        }
    }

    return InvalidOid;
}

static Node* transformCaseExpr(ParseState* pstate, CaseExpr* c)
{
    CaseExpr* newc = NULL;
    Node* arg = NULL;
    CaseTestExpr* placeholder = NULL;
    List* newargs = NIL;
    List* resultexprs = NIL;
    ListCell* l = NULL;
    Node* defresult = NULL;
    Oid ptype;

    /* If we already transformed this node, do nothing */
    if (OidIsValid(c->casetype)) {
        return (Node*)c;
    }
    bool saved_is_case_when = pstate->p_is_case_when;
    pstate->p_is_case_when = true;
    newc = makeNode(CaseExpr);

    /* transform the test expression, if any */
    arg = transformExpr(pstate, (Node*)c->arg);

    /* generate placeholder for test expression */
    if (arg != NULL) {
        /*
         * If test expression is an untyped literal, force it to text. We have
         * to do something now because we won't be able to do this coercion on
         * the placeholder.  This is not as flexible as what was done in 7.4
         * and before, but it's good enough to handle the sort of silly coding
         * commonly seen.
         */
        if (exprType(arg) == UNKNOWNOID) {
            arg = coerce_to_common_type(pstate, arg, TEXTOID, "CASE");
        }
        /*
         * Run collation assignment on the test expression so that we know
         * what collation to mark the placeholder with.  In principle we could
         * leave it to parse_collate.c to do that later, but propagating the
         * result to the CaseTestExpr would be unnecessarily complicated.
         */
        assign_expr_collations(pstate, arg);

        placeholder = makeNode(CaseTestExpr);
        placeholder->typeId = exprType(arg);
        placeholder->typeMod = exprTypmod(arg);
        placeholder->collation = exprCollation(arg);
    } else {
        placeholder = NULL;
    }
    newc->arg = (Expr*)arg;

    /* transform the list of arguments */
    newargs = NIL;
    resultexprs = NIL;
    foreach (l, c->args) {
        CaseWhen* w = (CaseWhen*)lfirst(l);
        CaseWhen* neww = makeNode(CaseWhen);
        Node* warg = NULL;

        AssertEreport(IsA(w, CaseWhen), MOD_OPT, "");

        warg = (Node*)w->expr;
        if (placeholder != NULL) {
            /* shorthand form was specified, so expand... */
            warg = (Node*)makeSimpleA_Expr(AEXPR_OP, "=", (Node*)placeholder, warg, w->location);
        }
        neww->expr = (Expr*)transformExpr(pstate, warg);

        neww->expr = (Expr*)coerce_to_boolean(pstate, (Node*)neww->expr, "CASE/WHEN");

        warg = (Node*)w->result;
        neww->result = (Expr*)transformExpr(pstate, warg);
        neww->location = w->location;

        newargs = lappend(newargs, neww);
        resultexprs = lappend(resultexprs, neww->result);
    }

    newc->args = newargs;

    /* transform the default clause */
    defresult = (Node*)c->defresult;
    if (defresult == NULL) {
        A_Const* n = makeNode(A_Const);

        n->val.type = T_Null;
        n->location = -1;
        defresult = (Node*)n;
    }
    newc->defresult = (Expr*)transformExpr(pstate, defresult);

    /*
     * Note: default result is considered the most significant type in
     * determining preferred type. This is how the code worked before, but it
     * seems a little bogus to me --- tgl
     */
    resultexprs = lcons(newc->defresult, resultexprs);

    ptype = select_common_type(pstate, resultexprs, "CASE", NULL);
    AssertEreport(OidIsValid(ptype), MOD_OPT, "");
    newc->casetype = ptype;
    /* casecollid will be set by parse_collate.c */

    /* Convert default result clause, if necessary */
    newc->defresult = (Expr*)coerce_to_common_type(pstate, (Node*)newc->defresult, ptype, "CASE/ELSE");

    /* Convert when-clause results, if necessary */
    foreach (l, newc->args) {
        CaseWhen* w = (CaseWhen*)lfirst(l);

        w->result = (Expr*)coerce_to_common_type(pstate, (Node*)w->result, ptype, "CASE/WHEN");
    }

    newc->location = c->location;
    pstate->p_is_case_when = saved_is_case_when;
    return (Node*)newc;
}

static Node* transformSubLink(ParseState* pstate, SubLink* sublink)
{
    Node* result = (Node*)sublink;
    Query* qtree = NULL;

    /* If we already transformed this node, do nothing */
    if (IsA(sublink->subselect, Query)) {
        return result;
    }
    pstate->p_hasSubLinks = true;
    qtree = parse_sub_analyze(sublink->subselect, pstate, NULL, false, true);

    /*
     * Check that we got something reasonable.	Many of these conditions are
     * impossible given restrictions of the grammar, but check 'em anyway.
     */
    if (!IsA(qtree, Query) || qtree->commandType != CMD_SELECT || qtree->utilityStmt != NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected non-SELECT command in SubLink")));
    }
    sublink->subselect = (Node*)qtree;

    if (sublink->subLinkType == EXISTS_SUBLINK) {
        /*
         * EXISTS needs no test expression or combining operator. These fields
         * should be null already, but make sure.
         */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    } else if (sublink->subLinkType == EXPR_SUBLINK || sublink->subLinkType == ARRAY_SUBLINK) {
        ListCell* tlist_item = list_head(qtree->targetList);

        /*
         * Make sure the subselect delivers a single column (ignoring resjunk
         * targets).
         */
        if (tlist_item == NULL || ((TargetEntry*)lfirst(tlist_item))->resjunk) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("subquery must return a column"),
                    parser_errposition(pstate, sublink->location)));
        }
        while ((tlist_item = lnext(tlist_item)) != NULL) {
            if (!((TargetEntry*)lfirst(tlist_item))->resjunk) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("subquery must return only one column"),
                        parser_errposition(pstate, sublink->location)));
            }
        }

        /*
         * EXPR and ARRAY need no test expression or combining operator. These
         * fields should be null already, but make sure.
         */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    } else {
        /* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
        Node* lefthand = NULL;
        List* left_list = NIL;
        List* right_list = NIL;
        ListCell* l = NULL;

        /*
         * Transform lefthand expression, and convert to a list
         */
        lefthand = transformExpr(pstate, sublink->testexpr);
        if (lefthand && IsA(lefthand, RowExpr)) {
            left_list = ((RowExpr*)lefthand)->args;
        } else {
            left_list = list_make1(lefthand);
        }
        /*
         * Build a list of PARAM_SUBLINK nodes representing the output columns
         * of the subquery.
         */
        right_list = NIL;
        foreach (l, qtree->targetList) {
            TargetEntry* tent = (TargetEntry*)lfirst(l);
            Param* param = NULL;

            if (tent->resjunk) {
                continue;
            }
            param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = tent->resno;
            param->paramtype = exprType((Node*)tent->expr);
            param->paramtypmod = exprTypmod((Node*)tent->expr);
            param->paramcollid = exprCollation((Node*)tent->expr);
            param->location = -1;
            param->tableOfIndexType = InvalidOid;

            right_list = lappend(right_list, param);
        }

        /*
         * We could rely on make_row_comparison_op to complain if the list
         * lengths differ, but we prefer to generate a more specific error
         * message.
         */
        if (list_length(left_list) < list_length(right_list)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("subquery has too many columns"),
                    parser_errposition(pstate, sublink->location)));
        }
        if (list_length(left_list) > list_length(right_list)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("subquery has too few columns"),
                    parser_errposition(pstate, sublink->location)));
        }

        /*
         * Identify the combining operator(s) and generate a suitable
         * row-comparison expression.
         */
        sublink->testexpr = make_row_comparison_op(pstate, sublink->operName, left_list, right_list, sublink->location);
    }

    return result;
}

/*
 * transformArrayExpr
 *
 * If the caller specifies the target type, the resulting array will
 * be of exactly that type.  Otherwise we try to infer a common type
 * for the elements using select_common_type().
 */
static Node* transformArrayExpr(ParseState* pstate, A_ArrayExpr* a, Oid array_type, Oid element_type, int32 typmod)
{
    ArrayExpr* newa = makeNode(ArrayExpr);
    List* newelems = NIL;
    List* newcoercedelems = NIL;
    ListCell* element = NULL;
    Oid coerce_type;
    bool coerce_hard = false;

    /*
     * Transform the element expressions
     *
     * Assume that the array is one-dimensional unless we find an array-type
     * element expression.
     */
    newa->multidims = false;
    foreach (element, a->elements) {
        Node* e = (Node*)lfirst(element);
        Node* newe = NULL;

        /*
         * If an element is itself an A_ArrayExpr, recurse directly so that we
         * can pass down any target type we were given.
         */
        if (IsA(e, A_ArrayExpr)) {
            newe = transformArrayExpr(pstate, (A_ArrayExpr*)e, array_type, element_type, typmod);
            /* we certainly have an array here */
            AssertEreport(array_type == InvalidOid || array_type == exprType(newe), MOD_OPT, "");
            newa->multidims = true;
        } else {
            newe = transformExpr(pstate, e);

            /*
             * Check for sub-array expressions, if we haven't already found
             * one.
             */
            if (!newa->multidims && type_is_array(exprType(newe))) {
                newa->multidims = true;
            }
        }

        newelems = lappend(newelems, newe);
    }

    /*
     * Select a target type for the elements.
     *
     * If we haven't been given a target array type, we must try to deduce a
     * common type based on the types of the individual elements present.
     */
    if (OidIsValid(array_type)) {
        /* Caller must ensure array_type matches element_type */
        AssertEreport(OidIsValid(element_type), MOD_OPT, "");
        coerce_type = (newa->multidims ? array_type : element_type);
        coerce_hard = true;
    } else {
        /* Can't handle an empty array without a target type */
        if (newelems == NIL) {
            ereport(ERROR,
                (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                    errmsg("cannot determine type of empty array"),
                    errhint("Explicitly cast to the desired type, "
                            "for example ARRAY[]::integer[]."),
                    parser_errposition(pstate, a->location)));
        }
        /* Select a common type for the elements */
        coerce_type = select_common_type(pstate, newelems, "ARRAY", NULL);

        if (newa->multidims) {
            array_type = coerce_type;
            element_type = get_element_type(array_type);
            if (!OidIsValid(element_type)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find element type for data type %s", format_type_be(array_type)),
                        parser_errposition(pstate, a->location)));
            }
        } else {
            element_type = coerce_type;
            array_type = get_array_type(element_type);
            if (!OidIsValid(array_type)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find array type for data type %s", format_type_be(element_type)),
                        parser_errposition(pstate, a->location)));
            }
        }
        coerce_hard = false;
    }

    /*
     * Coerce elements to target type
     *
     * If the array has been explicitly cast, then the elements are in turn
     * explicitly coerced.
     *
     * If the array's type was merely derived from the common type of its
     * elements, then the elements are implicitly coerced to the common type.
     * This is consistent with other uses of select_common_type().
     */
    foreach (element, newelems) {
        Node* e = (Node*)lfirst(element);
        Node* newe = NULL;

        if (coerce_hard) {
            newe = coerce_to_target_type(
                pstate, e, exprType(e), coerce_type, typmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
            if (newe == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_CANNOT_COERCE),
                        errmsg("cannot cast type %s to %s", format_type_be(exprType(e)), format_type_be(coerce_type)),
                        parser_errposition(pstate, exprLocation(e))));
            }
        } else {
            newe = coerce_to_common_type(pstate, e, coerce_type, "ARRAY");
        }
        newcoercedelems = lappend(newcoercedelems, newe);
    }

    newa->array_typeid = array_type;
    /* array_collid will be set by parse_collate.c */
    newa->element_typeid = element_type;
    newa->elements = newcoercedelems;
    newa->location = a->location;

    return (Node*)newa;
}

static Node* transformRowExpr(ParseState* pstate, RowExpr* r)
{
    RowExpr* newr = NULL;
    char fname[16];
    int fnum;
    ListCell* lc = NULL;

    /* If we already transformed this node, do nothing */
    if (OidIsValid(r->row_typeid)) {
        return (Node*)r;
    }
    newr = makeNode(RowExpr);

    /* Transform the field expressions */
    newr->args = transformExpressionList(pstate, r->args);

    /* Barring later casting, we consider the type RECORD */
    newr->row_typeid = RECORDOID;
    newr->row_format = COERCE_IMPLICIT_CAST;

    /* ROW() has anonymous columns, so invent some field names */
    newr->colnames = NIL;
    fnum = 1;
    foreach (lc, newr->args) {
        int rcs;
        rcs = snprintf_s(fname, sizeof(fname), sizeof(fname) - 1, "f%d", fnum++);
        securec_check_ss_c(rcs, "\0", "\0");
        newr->colnames = lappend(newr->colnames, makeString(pstrdup(fname)));
    }

    newr->location = r->location;

    return (Node*)newr;
}

static Node* transformCoalesceExpr(ParseState* pstate, CoalesceExpr* c)
{
    CoalesceExpr* newc = makeNode(CoalesceExpr);
    List* newargs = NIL;
    List* newcoercedargs = NIL;
    ListCell* args = NULL;

    foreach (args, c->args) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        newe = transformExpr(pstate, e);
        newargs = lappend(newargs, newe);
    }

    // supports implicit convert
    if (c->isnvl) {
        newc->coalescetype = select_common_type(pstate, newargs, "NVL", NULL);
    } else {
        newc->coalescetype = select_common_type(pstate, newargs, "COALESCE", NULL);
    }
    /* coalescecollid will be set by parse_collate.c */

    /* Convert arguments if necessary */
    foreach (args, newargs) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        newe = coerce_to_common_type(pstate, e, newc->coalescetype, "COALESCE");
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    newc->args = newcoercedargs;
    newc->location = c->location;
    return (Node*)newc;
}

static Node* transformMinMaxExpr(ParseState* pstate, MinMaxExpr* m)
{
    MinMaxExpr* newm = makeNode(MinMaxExpr);
    List* newargs = NIL;
    List* newcoercedargs = NIL;
    const char* funcname = (m->op == IS_GREATEST) ? "GREATEST" : "LEAST";
    ListCell* args = NULL;

    newm->op = m->op;
    foreach (args, m->args) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        newe = transformExpr(pstate, e);
        newargs = lappend(newargs, newe);
    }

    newm->minmaxtype = select_common_type(pstate, newargs, funcname, NULL);
    /* minmaxcollid and inputcollid will be set by parse_collate.c */

    /* Convert arguments if necessary */
    foreach (args, newargs) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        newe = coerce_to_common_type(pstate, e, newm->minmaxtype, funcname);
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    newm->args = newcoercedargs;
    newm->location = m->location;
    return (Node*)newm;
}

static Node* transformXmlExpr(ParseState* pstate, XmlExpr* x)
{
    XmlExpr* newx = NULL;
    ListCell* lc = NULL;
    int i;

    /* If we already transformed this node, do nothing */
    if (OidIsValid(x->type)) {
        return (Node*)x;
    }
    newx = makeNode(XmlExpr);
    newx->op = x->op;
    if (x->name) {
        newx->name = map_sql_identifier_to_xml_name(x->name, false, false);
    } else {
        newx->name = NULL;
    }
    newx->xmloption = x->xmloption;
    newx->type = XMLOID; /* this just marks the node as transformed */
    newx->typmod = -1;
    newx->location = x->location;

    /*
     * gram.y built the named args as a list of ResTarget.	Transform each,
     * and break the names out as a separate list.
     */
    newx->named_args = NIL;
    newx->arg_names = NIL;

    foreach (lc, x->named_args) {
        ResTarget* r = (ResTarget*)lfirst(lc);
        Node* expr = NULL;
        char* argname = NULL;

        AssertEreport(IsA(r, ResTarget), MOD_OPT, "");

        expr = transformExpr(pstate, r->val);

        if (r->name) {
            argname = map_sql_identifier_to_xml_name(r->name, false, false);
        } else if (IsA(r->val, ColumnRef)) {
            argname = map_sql_identifier_to_xml_name(FigureColname(r->val), true, false);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    x->op == IS_XMLELEMENT ? errmsg("unnamed XML attribute value must be a column reference")
                                           : errmsg("unnamed XML element value must be a column reference"),
                    parser_errposition(pstate, r->location)));
            argname = NULL; /* keep compiler quiet */
        }

        /* reject duplicate argnames in XMLELEMENT only */
        if (x->op == IS_XMLELEMENT) {
            ListCell* lc2 = NULL;

            foreach (lc2, newx->arg_names) {
                if (strcmp(argname, strVal(lfirst(lc2))) == 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("XML attribute name \"%s\" appears more than once", argname),
                            parser_errposition(pstate, r->location)));
                }
            }
        }

        newx->named_args = lappend(newx->named_args, expr);
        newx->arg_names = lappend(newx->arg_names, makeString(argname));
    }

    /* The other arguments are of varying types depending on the function */
    newx->args = NIL;
    i = 0;
    foreach (lc, x->args) {
        Node* e = (Node*)lfirst(lc);
        Node* newe = NULL;

        newe = transformExpr(pstate, e);
        switch (x->op) {
            case IS_XMLCONCAT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLCONCAT");
                break;
            case IS_XMLELEMENT:
                /* no coercion necessary */
                break;
            case IS_XMLFOREST:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLFOREST");
                break;
            case IS_XMLPARSE:
                if (i == 0) {
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLPARSE");
                } else {
                    newe = coerce_to_boolean(pstate, newe, "XMLPARSE");
                }
                break;
            case IS_XMLPI:
                newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLPI");
                break;
            case IS_XMLROOT:
                if (i == 0) {
                    newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLROOT");
                } else if (i == 1) {
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLROOT");
                } else {
                    newe = coerce_to_specific_type(pstate, newe, INT4OID, "XMLROOT");
                }
                break;
            case IS_XMLSERIALIZE:
                /* not handled here */
                Assert(false);
                break;
            case IS_DOCUMENT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "IS DOCUMENT");
                break;
            default:
                break;
        }
        newx->args = lappend(newx->args, newe);
        i++;
    }

    return (Node*)newx;
}

static Node* transformXmlSerialize(ParseState* pstate, XmlSerialize* xs)
{
    Node* result = NULL;
    XmlExpr* xexpr = NULL;
    Oid targetType;
    int32 targetTypmod;

    xexpr = makeNode(XmlExpr);
    xexpr->op = IS_XMLSERIALIZE;
    xexpr->args = list_make1(coerce_to_specific_type(pstate, transformExpr(pstate, xs->expr), XMLOID, "XMLSERIALIZE"));

    typenameTypeIdAndMod(pstate, xs->typname, &targetType, &targetTypmod);

    xexpr->xmloption = xs->xmloption;
    xexpr->location = xs->location;
    /* We actually only need these to be able to parse back the expression. */
    xexpr->type = targetType;
    xexpr->typmod = targetTypmod;

    /*
     * The actual target type is determined this way.  SQL allows char and
     * varchar as target types.  We allow anything that can be cast implicitly
     * from text.  This way, user-defined text-like data types automatically
     * fit in.
     */
    result = coerce_to_target_type(
        pstate, (Node*)xexpr, TEXTOID, targetType, targetTypmod, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    if (result == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast XMLSERIALIZE result to %s", format_type_be(targetType)),
                parser_errposition(pstate, xexpr->location)));
    }
    return result;
}

static Node* transformBooleanTest(ParseState* pstate, BooleanTest* b)
{
    const char* clausename = NULL;

    switch (b->booltesttype) {
        case IS_TRUE:
            clausename = "IS TRUE";
            break;
        case IS_NOT_TRUE:
            clausename = "IS NOT TRUE";
            break;
        case IS_FALSE:
            clausename = "IS FALSE";
            break;
        case IS_NOT_FALSE:
            clausename = "IS NOT FALSE";
            break;
        case IS_UNKNOWN:
            clausename = "IS UNKNOWN";
            break;
        case IS_NOT_UNKNOWN:
            clausename = "IS NOT UNKNOWN";
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized booltesttype: %d", (int)b->booltesttype)));
            clausename = NULL; /* keep compiler quiet */
    }

    b->arg = (Expr*)transformExpr(pstate, (Node*)b->arg);

    b->arg = (Expr*)coerce_to_boolean(pstate, (Node*)b->arg, clausename);

    return (Node*)b;
}

static Node* transformCurrentOfExpr(ParseState* pstate, CurrentOfExpr* cexpr)
{
    int sublevels_up;

#ifdef PGXC
    ereport(ERROR, (errcode(ERRCODE_STATEMENT_TOO_COMPLEX), (errmsg("WHERE CURRENT OF clause not yet supported"))));
#endif

    /* CURRENT OF can only appear at top level of UPDATE/DELETE */
    AssertEreport(pstate->p_target_rangetblentry != NULL, MOD_OPT, "");
    cexpr->cvarno = RTERangeTablePosn(pstate, pstate->p_target_rangetblentry, &sublevels_up);
    AssertEreport(sublevels_up == 0, MOD_OPT, "");

    /*
     * Check to see if the cursor name matches a parameter of type REFCURSOR.
     * If so, replace the raw name reference with a parameter reference. (This
     * is a hack for the convenience of plpgsql.)
     */
    if (cexpr->cursor_name != NULL) { /* in case already transformed */
        ColumnRef* cref = makeNode(ColumnRef);
        Node* node = NULL;

        /* Build an unqualified ColumnRef with the given name */
        cref->fields = list_make1(makeString(cexpr->cursor_name));
        cref->location = -1;

        /* See if there is a translation available from a parser hook */
        if (pstate->p_pre_columnref_hook != NULL) {
            node = (*pstate->p_pre_columnref_hook)(pstate, cref);
        }
        if (node == NULL && pstate->p_post_columnref_hook != NULL) {
            node = (*pstate->p_post_columnref_hook)(pstate, cref, NULL);
        }
        /*
         * XXX Should we throw an error if we get a translation that isn't a
         * refcursor Param?  For now it seems best to silently ignore false
         * matches.
         */
        if (node != NULL && IsA(node, Param)) {
            Param* p = (Param*)node;

            if (p->paramkind == PARAM_EXTERN && p->paramtype == REFCURSOROID) {
                /* Matches, so convert CURRENT OF to a param reference */
                cexpr->cursor_name = NULL;
                cexpr->cursor_param = p->paramid;
            }
        }
    }

    return (Node*)cexpr;
}

// Locate in the system catalog the information for a model name
static char* select_prediction_function(const Model* model){

    char* result;
    switch(model->return_type){
        case BOOLOID:
            result = "db4ai_predict_by_bool";
            break;
        case FLOAT4OID:
            result = "db4ai_predict_by_float4";
            break;
        case FLOAT8OID:
            result = "db4ai_predict_by_float8";
            break;
        case FLOAT8ARRAYOID:
            result = "db4ai_predict_by_float8_array";
            break;
        case INT1OID:
        case INT2OID:
        case INT4OID:
            result = "db4ai_predict_by_int32";
            break;
        case INT8OID:
            result = "db4ai_predict_by_int64";
            break;
        case NUMERICOID:
            result = "db4ai_predict_by_numeric";
            break;
        case VARCHAROID:
        case BPCHAROID:
        case CHAROID:
        case TEXTOID:
            result = "db4ai_predict_by_text";
            break;

        default:
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot trigger prediction for model with oid %u",  model->return_type)));
            result = NULL;
            break;
    }

    return result;
}

// Convert the PredictByFunction created during parsing phase into the function
// call that computes the prediction of the model. We cannot do this during parsing
// because at that moment we do not know the return type of the model, which is obtained
// from the catalog
static Node* transformPredictByFunction(ParseState* pstate, PredictByFunction* p)
{
    FuncCall* n = makeNode(FuncCall);

    if (p->model_name == NULL) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Model name for prediction cannot be null")));
    }

    const Model* model = get_model(p->model_name, true);
    if (model == NULL) {
        ereport(ERROR, (errmsg(
            "No model found with name %s", p->model_name)));
    }

    // Locate the proper function according to the model name
    char* function_name = select_prediction_function(model);
    ereport(DEBUG1, (errmsg(
            "Selecting prediction function %s for model %s",
            function_name, p->model_name)));
    n->funcname = list_make1(makeString(function_name));
    n->colname  = p->model_name;

    // Fill model name parameter
    A_Const* model_name_aconst      = makeNode(A_Const);
    model_name_aconst->val.type     = T_String;
    model_name_aconst->val.val.str  = p->model_name;
    model_name_aconst->location     = p->model_name_location;

    // Copy other parameters
    n->args = list_make1(model_name_aconst);
    if (list_length(p->model_args) > 0) {
        n->args = lappend3(n->args, p->model_args);
    }else{
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Innput features for the model not specified")));
    }

    n->agg_order        = NULL;
    n->agg_star         = FALSE;
    n->agg_distinct     = FALSE;
    n->func_variadic    = FALSE;
    n->over             = NULL;
    n->location         = p->model_args_location;
    n->call_func        = false;

    return  transformExpr(pstate, (Node*)n);
}


/*
 * Construct a whole-row reference to represent the notation "relation.*".
 */
static Node* transformWholeRowRef(ParseState* pstate, RangeTblEntry* rte, int location)
{
    Var* result = NULL;
    int vnum;
    int sublevels_up;

    /* Find the RTE's rangetable location */
    vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);

    /*
     * Build the appropriate referencing node.	Note that if the RTE is a
     * function returning scalar, we create just a plain reference to the
     * function value, not a composite containing a single column.	This is
     * pretty inconsistent at first sight, but it's what we've done
     * historically.  One argument for it is that "rel" and "rel.*" mean the
     * same thing for composite relations, so why not for scalar functions...
     */
    result = makeWholeRowVar(rte, vnum, sublevels_up, true);
    if (result == NULL) {
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_FILE),errmsg("Fail to build a referencing node.")));
        return NULL;
    }

    /* location is not filled in by makeWholeRowVar */
    result->location = location;

    /* mark relation as requiring whole-row SELECT access */
    markVarForSelectPriv(pstate, result, rte);

    return (Node*)result;
}

/*
 * Handle an explicit CAST construct.
 *
 * Transform the argument, then look up the type name and apply any necessary
 * coercion function(s).
 */
static Node* transformTypeCast(ParseState* pstate, TypeCast* tc)
{
    Node* result = NULL;
    Node* expr = transformExpr(pstate, tc->arg);
    Oid inputType = exprType(expr);
    Oid targetType;
    int32 targetTypmod;
    int location;

    typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);

    if (inputType == InvalidOid) {
        return expr; /* do nothing if NULL input */
    }
    /*
     * Location of the coercion is preferentially the location of the :: or
     * CAST symbol, but if there is none then use the location of the type
     * name (this can happen in TypeName 'string' syntax, for instance).
     */
    location = tc->location;
    if (location < 0) {
        location = tc->typname->location;
    }
    result = coerce_to_target_type(
        pstate, expr, inputType, targetType, targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, location);
    if (result == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(inputType), format_type_be(targetType)),
                parser_coercion_errposition(pstate, location, expr)));
    }
    return result;
}

/*
 * Handle an explicit COLLATE clause.
 *
 * Transform the argument, and look up the collation name.
 */
static Node* transformCollateClause(ParseState* pstate, CollateClause* c)
{
    CollateExpr* newc = NULL;
    Oid argtype;

    newc = makeNode(CollateExpr);
    newc->arg = (Expr*)transformExpr(pstate, c->arg);

    argtype = exprType((Node*)newc->arg);

    /*
     * The unknown type is not collatable, but coerce_type() takes care of it
     * separately, so we'll let it go here.
     */
    if (!type_is_collatable(argtype) && argtype != UNKNOWNOID) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("collations are not supported by type %s", format_type_be(argtype)),
                parser_errposition(pstate, c->location)));
    }
    newc->collOid = LookupCollation(pstate, c->collname, c->location);
    newc->location = c->location;

    return (Node*)newc;
}

static Node* transformSequenceFuncCall(ParseState* pstate, Node* field1, Node* field2, Node* field3, int location)
{
    Value* arg = NULL;
    List* funcname = list_make1((Value*)field3);
    if (field1 != NULL) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "%s.%s", strVal(field1), strVal(field2));
        arg = makeString(buf.data);
    } else {
        arg = (Value*)field2;
    }

    List* args = list_make1(makeAConst(arg, location));
    FuncCall* fn = makeFuncCall(funcname, args, location);

    return transformFuncCall(pstate, fn);
}

/*
 * Transform a "row compare-op row" construct
 *
 * The inputs are lists of already-transformed expressions.
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 *
 * The output may be a single OpExpr, an AND or OR combination of OpExprs,
 * or a RowCompareExpr.  In all cases it is guaranteed to return boolean.
 * The AND, OR, and RowCompareExpr cases further imply things about the
 * behavior of the operators (ie, they behave as =, <>, or < <= > >=).
 */
static Node* make_row_comparison_op(ParseState* pstate, List* opname, List* largs, List* rargs, int location)
{
    RowCompareExpr* rcexpr = NULL;
    RowCompareType rctype;
    List* opexprs = NIL;
    List* opnos = NIL;
    List* opfamilies = NIL;
    ListCell *l = NULL, *r = NULL;
    List** opinfo_lists = NULL;
    Bitmapset* strats = NULL;
    int nopers;
    int i;

    nopers = list_length(largs);
    if (nopers != list_length(rargs)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unequal number of entries in row expressions"),
                parser_errposition(pstate, location)));
    }
    /*
     * We can't compare zero-length rows because there is no principled basis
     * for figuring out what the operator is.
     */
    if (nopers == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot compare rows of zero length"),
                parser_errposition(pstate, location)));
    }
    /*
     * Identify all the pairwise operators, using make_op so that behavior is
     * the same as in the simple scalar case.
     */
    opexprs = NIL;
    forboth(l, largs, r, rargs) {
        Node* larg = (Node*)lfirst(l);
        Node* rarg = (Node*)lfirst(r);
        OpExpr* cmp = NULL;

        cmp = (OpExpr*)make_op(pstate, opname, larg, rarg, location);
        AssertEreport(IsA(cmp, OpExpr), MOD_OPT, "");

        /*
         * We don't use coerce_to_boolean here because we insist on the
         * operator yielding boolean directly, not via coercion.  If it
         * doesn't yield bool it won't be in any index opfamilies...
         */
        if (cmp->opresulttype != BOOLOID) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("row comparison operator must yield type boolean, "
                           "not type %s",
                        format_type_be(cmp->opresulttype)),
                    parser_errposition(pstate, location)));
        }
        if (expression_returns_set((Node*)cmp)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("row comparison operator must not return a set"),
                    parser_errposition(pstate, location)));
        }
        opexprs = lappend(opexprs, cmp);
    }

    /*
     * If rows are length 1, just return the single operator.  In this case we
     * don't insist on identifying btree semantics for the operator (but we
     * still require it to return boolean).
     */
    if (nopers == 1) {
        return (Node*)linitial(opexprs);
    }
    /*
     * Now we must determine which row comparison semantics (= <> < <= > >=)
     * apply to this set of operators.	We look for btree opfamilies
     * containing the operators, and see which interpretations (strategy
     * numbers) exist for each operator.
     */
    opinfo_lists = (List**)palloc(nopers * sizeof(List*));
    strats = NULL;
    i = 0;
    foreach (l, opexprs) {
        Oid opno = ((OpExpr*)lfirst(l))->opno;
        Bitmapset* this_strats = NULL;
        ListCell* j = NULL;

        opinfo_lists[i] = get_op_btree_interpretation(opno);

        /*
         * convert strategy numbers into a Bitmapset to make the intersection
         * calculation easy.
         */
        this_strats = NULL;
        foreach (j, opinfo_lists[i]) {
            OpBtreeInterpretation* opinfo = (OpBtreeInterpretation*)lfirst(j);

            this_strats = bms_add_member(this_strats, opinfo->strategy);
        }
        if (i == 0) {
            strats = this_strats;
        } else {
            strats = bms_int_members(strats, this_strats);
            bms_free_ext(this_strats);
        }
        i++;
    }

    /*
     * If there are multiple common interpretations, we may use any one of
     * them ... this coding arbitrarily picks the lowest btree strategy
     * number.
     */
    i = bms_first_member(strats);
    if (i < 0) {
        /* No common interpretation, so fail */
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                errhint("Row comparison operators must be associated with btree operator families."),
                parser_errposition(pstate, location)));
    }
    rctype = (RowCompareType)i;

    /*
     * For = and <> cases, we just combine the pairwise operators with AND or
     * OR respectively.
     *
     * Note: this is presently the only place where the parser generates
     * BoolExpr with more than two arguments.  Should be OK since the rest of
     * the system thinks BoolExpr is N-argument anyway.
     */
    if (rctype == ROWCOMPARE_EQ) {
        return (Node*)makeBoolExpr(AND_EXPR, opexprs, location);
    }
    if (rctype == ROWCOMPARE_NE) {
        return (Node*)makeBoolExpr(OR_EXPR, opexprs, location);
    }
    /*
     * Otherwise we need to choose exactly which opfamily to associate with
     * each operator.
     */
    opfamilies = NIL;
    for (i = 0; i < nopers; i++) {
        Oid opfamily = InvalidOid;
        ListCell* j = NULL;

        foreach (j, opinfo_lists[i]) {
            OpBtreeInterpretation* opinfo = (OpBtreeInterpretation*)lfirst(j);

            if (opinfo->strategy == rctype) {
                opfamily = opinfo->opfamily_id;
                break;
            }
        }
        if (OidIsValid(opfamily)) {
            opfamilies = lappend_oid(opfamilies, opfamily);
        } else { /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                    errdetail("There are multiple equally-plausible candidates."),
                    parser_errposition(pstate, location)));
        }
    }

    /*
     * Now deconstruct the OpExprs and create a RowCompareExpr.
     *
     * Note: can't just reuse the passed largs/rargs lists, because of
     * possibility that make_op inserted coercion operations.
     */
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach (l, opexprs) {
        OpExpr* cmp = (OpExpr*)lfirst(l);

        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

    rcexpr = makeNode(RowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opfamilies = opfamilies;
    rcexpr->inputcollids = NIL; /* assign_expr_collations will fix this */
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (Node*)rcexpr;
}

/*
 * Transform a "row IS DISTINCT FROM row" construct
 *
 * The input RowExprs are already transformed
 */
static Node* make_row_distinct_op(ParseState* pstate, List* opname, RowExpr* lrow, RowExpr* rrow, int location)
{
    Node* result = NULL;
    List* largs = lrow->args;
    List* rargs = rrow->args;
    ListCell *l = NULL, *r = NULL;

    if (list_length(largs) != list_length(rargs)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unequal number of entries in row expressions"),
                parser_errposition(pstate, location)));
    }
    forboth(l, largs, r, rargs) {
        Node* larg = (Node*)lfirst(l);
        Node* rarg = (Node*)lfirst(r);
        Node* cmp = NULL;

        cmp = (Node*)make_distinct_op(pstate, opname, larg, rarg, location);
        if (result == NULL) {
            result = cmp;
        } else {
            result = (Node*)makeBoolExpr(OR_EXPR, list_make2(result, cmp), location);
        }
    }

    if (result == NULL) {
        /* zero-length rows?  Generate constant FALSE */
        result = makeBoolConst(false, false);
    }

    return result;
}

/*
 * make the node for an IS DISTINCT FROM operator
 */
Expr* make_distinct_op(ParseState* pstate, List* opname, Node* ltree, Node* rtree, int location)
{
    Expr* result = NULL;

    result = make_op(pstate, opname, ltree, rtree, location);
    if (((OpExpr*)result)->opresulttype != BOOLOID) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("IS DISTINCT FROM requires = operator to yield boolean"),
                parser_errposition(pstate, location)));
    }
    /*
     * We rely on DistinctExpr and OpExpr being same struct
     */
    NodeSetTag(result, T_DistinctExpr);

    return result;
}

/*
 * Convert table.* to table.col1, table.col2, ...., table.coln.
 *
 * Only the following three cases are considered
 * A.*		A is an unqualified table name; means whole-row value.
 * A.B.*		whole-row value of table B in schema A.
 * A.B.C.*	whole-row value of table C in schema B in catalog A.
 *
 * Currently, if a catalog name is given then it must equal the current
 * database name; we check it here and then discard it.
 *
 */
static Node* convertStarToCRef(RangeTblEntry* rte, char* catname, char* nspname, char* relname, int location)
{
    RowExpr* re = makeNode(RowExpr);
    ListCell* c = NULL;

    foreach (c, rte->eref->colnames) {
        ColumnRef* column = makeNode(ColumnRef);
        char* theField = strVal(lfirst(c));

        /* skip the dropped column */
        if (NULL != theField && (*theField) == '\0') {
            continue;
        }
        column->fields = list_make1(makeString(pstrdup(theField)));

        /* deal with A.* */
        if (relname != NULL) {
            column->fields = lcons(makeString(relname), column->fields);
        }

        /* deal with A.B.* */
        if (nspname != NULL) {
            AssertEreport(relname, MOD_OPT, "");
            column->fields = lcons(makeString(nspname), column->fields);
        }

        /* deal with A.B.C.* */
        if (catname != NULL) {
            AssertEreport(relname, MOD_OPT, "");
            AssertEreport(nspname, MOD_OPT, "");
            column->fields = lcons(makeString(catname), column->fields);
        }

        column->location = -1;
        re->args = lappend(re->args, column);
    }

    re->row_typeid = InvalidOid; /* not analyzed yet */
    re->colnames = NIL;          /* to be filled in during analysis */
    re->location = location;

    return (Node*)re;
}

/*
 * Check if the
 *
 */
static bool IsSequenceFuncCall(Node* filed1, Node* filed2, Node* filed3)
{
    if (!IsA(filed3, String)) {
        return false;
    }

    char* relname = strVal(filed2);
    char* funcname = strVal(filed3);
    char* nspname = NULL;
    Oid nspid = InvalidOid;
    Assert(relname != NULL && funcname != NULL);

    if (filed1 != NULL) {
        nspname = strVal(filed1);
        nspid = get_namespace_oid(nspname, true);
        if (nspid == InvalidOid) {
            return false;
        }
    }

    if (strcmp(funcname, "nextval") == 0 || strcmp(funcname, "currval") == 0) {
        if (filed1 != NULL && RELKIND_IS_SEQUENCE(get_rel_relkind(get_relname_relid(relname, nspid)))) {
            return true;
        } else if (filed1 == NULL && RELKIND_IS_SEQUENCE(get_rel_relkind(RelnameGetRelid(relname)))) {
            return true;
        }
    }

    return false;
}

/*
 * start with transform support
 */
static Node *transformStartWithColumnRef(ParseState *pstate, ColumnRef *cref, char **colname)
{
    Assert (*colname != NULL);

    Node *field1 = NULL;
    Node *field2 = NULL;
    char *local_column_ref = *colname;

    int len = list_length(cref->fields);
    switch (len) {
        case 1: {
            field1 = (Node*)linitial(cref->fields);

            if (pg_strcasecmp(local_column_ref, "connect_by_root") == 0 ) {
                Node *funexpr = transformConnectByRootFuncCall(pstate, field1, cref);

                /*
                 * Return function funexpr, otherwise process
                 * connect_by_root as regular case
                 */
                if (funexpr != NULL) {
                    return funexpr;
                }
            }

            /* for pseudo column, we don't do column name replacement */
            if (IsPseudoReturnColumn(local_column_ref) ||
                pg_strcasecmp(local_column_ref, "connect_by_root") == 0) {
                return NULL;
            }

            char *relname = ColumnRefFindRelname(pstate, local_column_ref);
            if (relname == NULL) {
                elog(LOG, "do not find colname %s in sw-aborted RTE, maybe it's a normal column", *colname);
                return NULL;
            }

            *colname = makeStartWithDummayColname(relname, local_column_ref);
            field1 = (Node*)makeString(pstrdup(*colname));
            cref->fields = list_make1(field1);

            break;
        }
        case 2: {
            field1 = (Node*)linitial(cref->fields);
            field2 = (Node*)lsecond(cref->fields);

            char *relname = strVal(field1);

            /* Already rewrite column once relname equal tmp_reuslt */
            if (pg_strcasecmp(relname, "tmp_reuslt") == 0) {
                return NULL;
            }

            *colname = makeStartWithDummayColname(relname, local_column_ref);
            field1 = (Node *)makeString("tmp_reuslt");
            field2 = (Node*)makeString(pstrdup(*colname));
            cref->fields = list_make2(field1, field2);

            break;
        }

        default:
            break;
    }

    return NULL;
}

static Node* transformConnectByRootFuncCall(ParseState* pstate, Node* funcNameVal, ColumnRef *cref)
{
    Assert (pg_strcasecmp(strVal(funcNameVal), "connect_by_root") == 0 &&
            IsA(pstate->p_sw_selectstmt, SelectStmt));

    /* find the targe column name */
    List *targetlist = pstate->p_sw_selectstmt->targetList;
    ListCell *lc = NULL;

    char *relname = NULL;
    char *colname = NULL;

    /* scan SelectStmt's targetlist to find corresponding ColumnRef */
    foreach (lc, targetlist) {
        ResTarget *rt = (ResTarget *)lfirst(lc);

        if (equal(rt->val, cref)) {
            colname = rt->name;

            /*
             * Indicate we do not find a case "connect_by_root column", so return to
             * transformColumnRef and treat conect_by_column as a regular RTE's entry
             */
            if (colname == NULL) {
                return NULL;
            }

            relname = ColumnRefFindRelname(pstate, colname);
            if (relname == NULL) {
                elog(ERROR, "do not find colname %s in sw-aborted RTE", colname);
                return NULL;
            }

            colname = makeStartWithDummayColname(relname, colname);

            break;
        }
    }

    /* construct function call expr */
    Value* argExpr = (Value *)makeColumnRef("tmp_reuslt", colname, -1);
    List* args = list_make1(argExpr);
    List* funcExpr = list_make1((Value*)funcNameVal);
    FuncCall* fn = makeFuncCall(funcExpr, args, cref->location);

    return transformFuncCall(pstate, fn);
}

/*
 * Note, currently, only support 1 fild ColumnRef, will expand to support multiple case
 */
static char *ColumnRefFindRelname(ParseState *pstate, const char *colname)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    char *relname = NULL;
    int count = 0;

    while (pstate != NULL) {
        foreach(lc1, pstate->p_rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc1);

            if (!rte->swAborted) {
                continue;
            }

            if (rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_CTE) {
                List *colnames = rte->eref->colnames;

                foreach(lc2, colnames) {
                    Value *col = (Value *)lfirst(lc2);
                    if (strcmp(colname, strVal(col)) == 0) {
                        if (rte->rtekind == RTE_RELATION) {
                            relname = (rte->alias && rte->alias->aliasname) ?
                                       rte->alias->aliasname : rte->relname;
                        } else if (rte->rtekind == RTE_SUBQUERY) {
                            relname = rte->alias->aliasname;
                        } else if (rte->rtekind == RTE_CTE) {
                            relname = (rte->alias && rte->alias->aliasname) ?
                                       rte->alias->aliasname : rte->ctename;
                        }

                        count++;
                    }
                }
            }
        }

        /* If we found just break */
        if (count > 1) {
            ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                    errmsg("column reference \"%s\" is ambiguous", colname)));
        }

        if (count == 1) {
            break;
        }

        pstate = pstate->parentParseState;
    }

    return relname;
}
