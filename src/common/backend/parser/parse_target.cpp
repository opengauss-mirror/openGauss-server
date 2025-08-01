/* -------------------------------------------------------------------------
 *
 * parse_target.cpp
 *	  handle target lists
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_target.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_collate.h"
#include "nodes/parsenodes_common.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/typcache.h"
#include "executor/executor.h"
#include "gs_ledger/ledger_utils.h"
#include "mb/pg_wchar.h"
#include "parser/parse_utilcmd.h"
#include "commands/sequence.h"

static void markTargetListOrigin(ParseState* pstate, TargetEntry* tle, Var* var, int levelsup);
static Node* transformAssignmentIndirection(ParseState* pstate, Node* basenode, const char* targetName,
    bool targetIsArray, Oid targetTypeId, int32 targetTypMod, Oid targetCollation, ListCell* indirection, Node* rhs,
    int location);
static Node* transformAssignmentSubscripts(ParseState* pstate, Node* basenode, const char* targetName, Oid targetTypeId,
    int32 targetTypMod, Oid targetCollation, List* subscripts, bool isSlice, ListCell* next_indirection, Node* rhs,
    int location);
static List* ExpandColumnRefStar(ParseState* pstate, ColumnRef* cref, bool targetlist);
static List* ExpandAllTables(ParseState* pstate, int location);
static List* ExpandIndirectionStar(ParseState* pstate, A_Indirection* ind, bool targetlist, ParseExprKind exprKind);
static List* ExpandSingleTable(ParseState* pstate, RangeTblEntry* rte, int location, bool targetlist);
static List* ExpandRowReference(ParseState* pstate, Node* expr, bool targetlist);
static int FigureColnameInternal(Node* node, char** name);

extern void checkArrayTypeInsert(ParseState* pstate, Expr* expr);
extern Oid pg_get_serial_sequence_internal(Oid tableOid, AttrNumber attnum, bool find_identity, char** out_seq_name);

/*
 * @Description: return the last filed's name and ignore * or subscrpts
 * @in field: list of field to search
 * @return the found field's name
 */
static char* find_last_field_name(List* field)
{
    char* fname = NULL;
    ListCell* l = NULL;
    Node* i = NULL;
    /* find last field name, if any, ignoring "*" and subscripts */
    foreach (l, field) {
        i = (Node*)lfirst(l);
        if (IsA(i, String)) {
            fname = strVal(i);
        }
    }
    return fname;
}

/*
 * transformTargetEntry()
 *	Transform any ordinary "expression-type" node into a targetlist entry.
 *	This is exported so that parse_clause.c can generate targetlist entries
 *	for ORDER/GROUP BY items that are not already in the targetlist.
 *
 * node		the (untransformed) parse tree for the value expression.
 * expr		the transformed expression, or NULL if caller didn't do it yet.
 * colname	the column name to be assigned, or NULL if none yet set.
 * resjunk	true if the target should be marked resjunk, ie, it is not
 *			wanted in the final projected tuple.
 */
TargetEntry* transformTargetEntry(ParseState* pstate, Node* node, Node* expr, ParseExprKind exprKind, char* colname, bool resjunk)
{
    /* Generate a suitable name for column shown in error case */
    if (colname == NULL && !resjunk) {
        colname = FigureIndexColname(node);
    }
    ELOG_FIELD_NAME_START(colname);

    /* Transform the node if caller didn't do it already */
    if (expr == NULL) {
        expr = transformExpr(pstate, node, exprKind);
    }
    ELOG_FIELD_NAME_END;

    if (colname == NULL && !resjunk) {
        /*
         * Generate a suitable column name for a column without any explicit
         * 'AS ColumnName' clause, may be different from above call due to
         * change of node expression
         */
        colname = FigureColname(node);
    }

    return makeTargetEntry((Expr*)expr, (AttrNumber)pstate->p_next_resno++, colname, resjunk);
}

/*
 * transformTargetList()
 * Turns a list of ResTarget's into a list of TargetEntry's.
 *
 * At this point, we don't care whether we are doing SELECT, INSERT,
 * or UPDATE; we just transform the given expressions (the "val" fields).
 */
List* transformTargetList(ParseState* pstate, List* targetlist, ParseExprKind exprKind)
{
    List* p_target = NIL;
    ListCell* o_target = NULL;

    foreach (o_target, targetlist) {
        ResTarget* res = (ResTarget*)lfirst(o_target);

        /*
         * Check for "something.*".  Depending on the complexity of the
         * "something", the star could appear as the last field in ColumnRef,
         * or as the last indirection item in A_Indirection.
         */
        if (IsA(res->val, ColumnRef)) {
            ColumnRef* cref = (ColumnRef*)res->val;

            if (cref->prior && t_thrd.proc->workingVersionNum < PRIOR_EXPR_VERSION_NUM) {
                ereport(ERROR,
                        (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                         errmsg("Not Support prior column in TargetList in case swcb.")));
            }

            if (IsA(llast(cref->fields), A_Star)) {
                /* It is something.*, expand into multiple items */
                p_target = list_concat(p_target, ExpandColumnRefStar(pstate, cref, true));
                continue;
            }
        } else if (IsA(res->val, A_Indirection)) {
            A_Indirection* ind = (A_Indirection*)res->val;

            if (IsA(llast(ind->indirection), A_Star)) {
                /* It is something.*, expand into multiple items */
                p_target = list_concat(p_target, ExpandIndirectionStar(pstate, ind, true, exprKind));
                continue;
            }
        }

        /*
         * Not "something.*", so transform as a single expression
         */
        p_target = lappend(p_target, transformTargetEntry(pstate, res->val, NULL, exprKind, res->name, false));
        pstate->p_target_list = p_target;
    }

    return p_target;
}

/*
 * transformExpressionList()
 *
 * This is the identical transformation to transformTargetList, except that
 * the input list elements are bare expressions without ResTarget decoration,
 * and the output elements are likewise just expressions without TargetEntry
 * decoration.	We use this for ROW() and VALUES() constructs.
 */
List* transformExpressionList(ParseState* pstate, List* exprlist, ParseExprKind exprKind)
{
    List* result = NIL;
    ListCell* lc = NULL;

    foreach (lc, exprlist) {
        Node* e = (Node*)lfirst(lc);

        /*
         * Check for "something.*".  Depending on the complexity of the
         * "something", the star could appear as the last field in ColumnRef,
         * or as the last indirection item in A_Indirection.
         */
        if (IsA(e, ColumnRef)) {
            ColumnRef* cref = (ColumnRef*)e;

            if (IsA(llast(cref->fields), A_Star)) {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandColumnRefStar(pstate, cref, false));
                continue;
            }
        } else if (IsA(e, A_Indirection)) {
            A_Indirection* ind = (A_Indirection*)e;

            if (IsA(llast(ind->indirection), A_Star)) {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandIndirectionStar(pstate, ind, false, exprKind));
                continue;
            }
        }

        /*
         * Not "something.*", so transform as a single expression
         */
        result = lappend(result, transformExpr(pstate, e, exprKind));
    }

    return result;
}

/*
 * resolveTargetListUnknowns()
 *		Convert any unknown-type targetlist entries to type TEXT.
 *
 * We do this after we've exhausted all other ways of identifying the output
 * column types of a query.
 */
void resolveTargetListUnknowns(ParseState* pstate, List* targetlist)
{
    ListCell* l = NULL;

    foreach (l, targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        Oid restype = exprType((Node*)tle->expr);
        if (UNKNOWNOID == restype) {
            tle->expr = (Expr*)coerce_type(
                pstate, (Node*)tle->expr, restype, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
                NULL, NULL, -1);
        }
    }
}

/*
 * markTargetListOrigins()
 *		Mark targetlist columns that are simple Vars with the source
 *		table's OID and column number.
 *
 * Currently, this is done only for SELECT targetlists, since we only
 * need the info if we are going to send it to the frontend.
 */
void markTargetListOrigins(ParseState* pstate, List* targetlist)
{
    ListCell* l = NULL;

    foreach (l, targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        markTargetListOrigin(pstate, tle, (Var*)tle->expr, 0);
    }
}

/*
 * markTargetListOrigin()
 *		If 'var' is a Var of a plain relation, mark 'tle' with its origin
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 *
 * This is split out so it can recurse for join references.  Note that we
 * do not drill down into views, but report the view as the column owner.
 */
static void markTargetListOrigin(ParseState* pstate, TargetEntry* tle, Var* var, int levelsup)
{
    int netlevelsup;
    RangeTblEntry* rte = NULL;
    AttrNumber attnum;

    if (var == NULL || !IsA(var, Var)) {
        return;
    }
    netlevelsup = var->varlevelsup + levelsup;
    rte = GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
    attnum = var->varattno;

    switch (rte->rtekind) {
        case RTE_RELATION:
            /* It's a table or view, report it */
            tle->resorigtbl = rte->relid;
            tle->resorigcol = attnum;
            break;
        case RTE_SUBQUERY:
            /* Subselect-in-FROM: copy up from the subselect */
            if (attnum != InvalidAttrNumber) {
                TargetEntry* ste = get_tle_by_resno(rte->subquery->targetList, attnum);

                if (ste == NULL || ste->resjunk) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            errmsg("subquery %s does not have attribute %d", rte->eref->aliasname, attnum)));
                }
                tle->resorigtbl = ste->resorigtbl;
                tle->resorigcol = ste->resorigcol;
            }
            break;
        case RTE_JOIN:
            /* Join RTE --- recursively inspect the alias variable */
            if (attnum != InvalidAttrNumber) {
                Var* aliasvar = NULL;

                AssertEreport(attnum > 0, MOD_OPT, "");
                AssertEreport(attnum <= list_length(rte->joinaliasvars), MOD_OPT, "");
                aliasvar = (Var*)list_nth(rte->joinaliasvars, attnum - 1);
                markTargetListOrigin(pstate, tle, aliasvar, netlevelsup);
            }
            break;
        case RTE_FUNCTION:
        case RTE_VALUES:
        case RTE_RESULT:
            /* not a simple relation, leave it unmarked */
            break;
        case RTE_CTE:

            /*
             * CTE reference: copy up from the subquery, if possible. If the
             * RTE is a recursive self-reference then we can't do anything
             * because we haven't finished analyzing it yet. However, it's no
             * big loss because we must be down inside the recursive term of a
             * recursive CTE, and so any markings on the current targetlist
             * are not going to affect the results anyway.
             */
            if (attnum != InvalidAttrNumber && !rte->self_reference) {
                CommonTableExpr* cte = GetCTEForRTE(pstate, rte, netlevelsup);
                TargetEntry* ste = NULL;

                ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
                if (ste == NULL || ste->resjunk) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("subquery %s does not have attribute %d", rte->eref->aliasname, attnum)));
                }
                tle->resorigtbl = ste->resorigtbl;
                tle->resorigcol = ste->resorigcol;
            }
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Invalid RTE found")));
            break;
#endif /* PGXC */
        default:
            break;
    }
}

/*
 * transformAssignedExpr()
 *	This is used in INSERT and UPDATE (including DUPLICATE KEY UPDATE)
 *	statements only. It prepares an expression for assignment to a column
 *	of the target table. This includes coercing the given value
 *	to the target column's type (if necessary), and dealing with
 *	any subfield names or subscripts attached to the target column itself
 *	The input expression has already been through transformExpr().
 *
 * pstate		parse state
 * expr			expression to be modified
 * colname		target column name (ie, name of attribute to be assigned to)
 * attrno		target attribute number
 * indirection	subscripts/field names for target column, if any
 * location		error cursor position for the target column, or -1
 *
 * Returns the modified expression.
 *
 * Note: location points at the target column name (SET target or INSERT
 * column name list entry), and must therefore be -1 in an INSERT that
 * omits the column name list.	So we should usually prefer to use
 * exprLocation(expr) for errors that can happen in a default INSERT.
 */
Expr* transformAssignedExpr(ParseState* pstate, Expr* expr, ParseExprKind exprKind, char* colname, int attrno,
                            List* indirection, int location, Relation rd, RangeTblEntry* rte)
{
    Oid type_id;    /* type of value provided */
    int32 type_mod; /* typmod of value provided */
    Oid attrtype;   /* type of target column */
    int32 attrtypmod;
    Oid attrcollation; /* collation of target column */
    int attrcharset = PG_INVALID_ENCODING;
    ParseExprKind sv_expr_kind;

    /*
    * Save and restore identity of expression type we're parsing.  We must
    * set p_expr_kind here because we can parse subscripts without going
    * through transformExpr().
    */
    Assert(exprKind != EXPR_KIND_NONE);
    sv_expr_kind = pstate->p_expr_kind;
    pstate->p_expr_kind = exprKind;

    AssertEreport(rd != NULL, MOD_OPT, "");
    /*
     * for relation not in ledger schema, only attrno is system column,
     * for relation in ledger schema, the "hash" column is reserved for system.
     * for "hash" column of table in ledger schema, we forbid insert and update
     */
    bool is_system_column = (attrno <= 0 || (rd->rd_isblockchain && strcmp(colname, "hash") == 0));
    if (is_system_column) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot assign to system column \"%s\"", colname),
                parser_errposition(pstate, location)));
    }
    attrtype = attnumTypeId(rd, attrno);
    attrtypmod = rd->rd_att->attrs[attrno - 1].atttypmod;
    attrcollation = rd->rd_att->attrs[attrno - 1].attcollation;
    if (DB_IS_CMPT_BD && OidIsValid(attrcollation)) {
        attrcharset = get_valid_charset_by_collation(attrcollation);
    }

    /*
     * If the expression is a DEFAULT placeholder, insert the attribute's
     * type/typmod/collation into it so that exprType etc will report the
     * right things.  (We expect that the eventually substituted default
     * expression will in fact have this type and typmod.  The collation
     * likely doesn't matter, but let's set it correctly anyway.)  Also,
     * reject trying to update a subfield or array element with DEFAULT, since
     * there can't be any default for portions of a column.
     */
    if (expr && IsA(expr, SetToDefault)) {
        SetToDefault* def = (SetToDefault*)expr;

        def->typeId = attrtype;
        def->typeMod = attrtypmod;
        def->collation = attrcollation;
        if (indirection != NULL) {
            if (IsA(linitial(indirection), A_Indices)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot set an array element to DEFAULT"),
                        parser_errposition(pstate, location)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot set a subfield to DEFAULT"),
                        parser_errposition(pstate, location)));
            }
        }
    }

    /* Now we can use exprType() safely. */
    type_id = exprType((Node*)expr);
    type_mod = exprTypmod((Node*)expr);

    if (IsA(expr, Param) || (IsA(expr, ArrayRef) && ((ArrayRef*)expr)->refexpr != NULL)) {
        checkArrayTypeInsert(pstate, expr);
    }

    if (IsA(expr, Param) && DISABLE_RECORD_TYPE_IN_DML && type_id == RECORDOID) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), 
                           errmsg("The record type variable cannot be used as an insertion value.")));
    }
    ELOG_FIELD_NAME_START(colname);

    /*
     * If there is indirection on the target column, prepare an array or
     * subfield assignment expression.	This will generate a new column value
     * that the source value has been inserted into, which can then be placed
     * in the new tuple constructed by INSERT or UPDATE.
     */
    if (indirection != NULL) {
        Node* colVar = NULL;

        if (pstate->p_is_insert) {
            /*
             * The command is INSERT INTO table (col.something) ... so there
             * is not really a source value to work with. Insert a NULL
             * constant as the source value.
             */
            colVar = (Node*)makeNullConst(attrtype, attrtypmod, attrcollation);
        } else {
            /*
             * Build a Var for the column to be updated.
             */
            colVar = (Node*)make_var(pstate, rte, attrno, location);
        }

        expr = (Expr*)transformAssignmentIndirection(pstate,
            colVar,
            colname,
            false,
            attrtype,
            attrtypmod,
            attrcollation,
            list_head(indirection),
            (Node*)expr,
            location);
    } else {
        /*
         * For normal non-qualified target column, do type checking and
         * coercion.
         */
        Node* orig_expr = (Node*)expr;

        /*
         * For the insert statement, we may do auto truncation.
         * When (1) p_is_td_compatible_truncation is true;
         * 		(2) target column type is char and varchar type;
         * 		(3) the target column length is smaller than source column length;
         * 				a) the type_mod <= 0, it because the source maybe a const,
         * 					then we all add cast
         * 				b) the source column length and target column length both exist,
         *					then we only add explict cast when the target length is smaller
         *					than source length
         * all satisfied. then we will add an explict cast for source column data to let it
         * could be insert to target column successful.
         * For nvarchar2 type is not support to be truncated automaticlly.
         */
        bool is_all_satisfied = pstate->p_is_td_compatible_truncation &&
            (attrtype == BPCHAROID || attrtype == VARCHAROID) &&
            ((type_mod > 0 && attrtypmod < type_mod) || type_mod < 0);

        if (type_is_set(attrtype)) {
            Node* orig_expr = (Node*)expr;
            expr = (Expr*)coerce_to_settype(
                pstate, orig_expr, type_id, attrtype, attrtypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
                    -1, attrcollation);
        } else if (is_all_satisfied) {
            expr = (Expr*)coerce_to_target_type(
                pstate, orig_expr, type_id, attrtype, attrtypmod, COERCION_ASSIGNMENT, COERCE_EXPLICIT_CAST,
                NULL, NULL, -1);
            pstate->tdTruncCastStatus = TRUNC_CAST_QUERY;
        } else {
            expr = (Expr*)coerce_to_target_type(
                pstate, orig_expr, type_id, attrtype, attrtypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
                NULL, NULL, -1);
        }
        if (expr == NULL) {
            /* if we are in create proc - change input parameter type  */
            if (IsClientLogicType(attrtype) && IsA(orig_expr, Param) && pstate->p_create_proc_insert_hook != NULL) {
                int param_no = ((Param*)orig_expr)->paramid - 1;
                pstate->p_create_proc_insert_hook(pstate, param_no, attrtype, RelationGetRelid(rd), colname);
                type_id = attrtype;
                expr = (Expr*)coerce_to_target_type(pstate, orig_expr, type_id, attrtype, attrtypmod,
                    COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
            }
        }
        if (expr == NULL) {
            bool invalid_encrypted_column = 
                ((attrtype == BYTEAWITHOUTORDERWITHEQUALCOLOID || attrtype == BYTEAWITHOUTORDERCOLOID) &&
                coerce_to_target_type(pstate, orig_expr, type_id, attrtypmod, -1, COERCION_ASSIGNMENT,
                    COERCE_IMPLICIT_CAST, NULL, NULL, -1));
            if (invalid_encrypted_column) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_ENCRYPTED_COLUMN_DATA),
                    errmsg("column \"%s\" is of type %s"
                    " but expression is of type %s",
                    colname, format_type_be(attrtype), format_type_be(type_id)),
                    errhint("You will need to rewrite or cast the expression."),
                    parser_errposition(pstate, exprLocation(orig_expr))));
            } else {
                if (!pstate->p_has_ignore) {
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("column \"%s\" is of type %s"
                               " but expression is of type %s",
                               colname, format_type_be(attrtype), format_type_be(type_id)),
                               errhint("You will need to rewrite or cast the expression."),
                               parser_errposition(pstate, exprLocation(orig_expr))));
                }
                expr = (Expr*)makeConst(attrtype, attrtypmod, attrcollation, rd->rd_att->attrs[attrno - 1].attlen,
                                        GetTypeZeroValue(&rd->rd_att->attrs[attrno - 1]), false,
                                        rd->rd_att->attrs[attrno - 1].attbyval);
                ereport(WARNING, (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("column \"%s\" is of type %s"
                                       " but expression is of type %s. Data truncated automatically.",
                                       colname, format_type_be(attrtype), format_type_be(type_id))));
            }
        }
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (attrcharset != PG_INVALID_ENCODING) {
        assign_expr_collations(pstate, (Node*)expr);
        expr = (Expr*)coerce_to_target_charset((Node*)expr, attrcharset, attrtype, attrtypmod, attrcollation);
    }
#endif

    ELOG_FIELD_NAME_END;

    pstate->p_expr_kind = sv_expr_kind;

    return expr;
}

/*
 * updateTargetListEntry()
 *	This is used in UPDATE (and DUPLICATE KEY UPDATE) statements only.
 *	It prepares an UPDATE TargetEntry for assignment to a column of
 *	the target table. This includes coercing the given value to the
 *	target column's type (if necessary), and dealing with any subfield
 *	names or subscripts attached to the target column itself.
 *
 * pstate		parse state
 * tle			target list entry to be modified
 * colname		target column name (ie, name of attribute to be assigned to)
 * attrno		target attribute number
 * indirection	subscripts/field names for target column, if any
 * location		error cursor position (should point at column name), or -1
 */
void updateTargetListEntry(ParseState* pstate, TargetEntry* tle, char* colname, int attrno,
    List* indirection, int location, Relation rd, RangeTblEntry* rte)
{
    /* Fix up expression as needed */
    tle->expr = transformAssignedExpr(pstate, tle->expr, EXPR_KIND_UPDATE_TARGET, colname, attrno,
                                      indirection, location, rd, rte);

    /*
     * Set the resno to identify the target column --- the rewriter and
     * planner depend on this.	We also set the resname to identify the target
     * column, but this is only for debugging purposes; it should not be
     * relied on.  (In particular, it might be out of date in a stored rule.)
     */
    tle->resno = (AttrNumber)attrno;
    tle->resname = colname;
}

/*
 * Process indirection (field selection or subscripting) of the target
 * column in INSERT/UPDATE.  This routine recurses for multiple levels
 * of indirection --- but note that several adjacent A_Indices nodes in
 * the indirection list are treated as a single multidimensional subscript
 * operation.
 *
 * In the initial call, basenode is a Var for the target column in UPDATE,
 * or a null Const of the target's type in INSERT.  In recursive calls,
 * basenode is NULL, indicating that a substitute node should be consed up if
 * needed.
 *
 * targetName is the name of the field or subfield we're assigning to, and
 * targetIsArray is true if we're subscripting it.  These are just for
 * error reporting.
 *
 * targetTypeId, targetTypMod, targetCollation indicate the datatype and
 * collation of the object to be assigned to (initially the target column,
 * later some subobject).
 *
 * indirection is the sublist remaining to process.  When it's NULL, we're
 * done recursing and can just coerce and return the RHS.
 *
 * rhs is the already-transformed value to be assigned; note it has not been
 * coerced to any particular type.
 *
 * location is the cursor error position for any errors.  (Note: this points
 * to the head of the target clause, eg "foo" in "foo.bar[baz]".  Later we
 * might want to decorate indirection cells with their own location info,
 * in which case the location argument could probably be dropped.)
 */
static Node* transformAssignmentIndirection(ParseState* pstate, Node* basenode, const char* targetName,
    bool targetIsArray, Oid targetTypeId, int32 targetTypMod, Oid targetCollation, ListCell* indirection, Node* rhs,
    int location)
{
    Node* result = NULL;
    List* subscripts = NIL;
    bool isSlice = false;
    ListCell* i = NULL;

    if (indirection != NULL && basenode == NULL) {
        /* Set up a substitution.  We reuse CaseTestExpr for this. */
        CaseTestExpr* ctest = makeNode(CaseTestExpr);

        ctest->typeId = targetTypeId;
        ctest->typeMod = targetTypMod;
        ctest->collation = targetCollation;
        basenode = (Node*)ctest;
    }

    /*
     * We have to split any field-selection operations apart from
     * subscripting.  Adjacent A_Indices nodes have to be treated as a single
     * multidimensional subscript operation.
     */
    for_each_cell(i, indirection)
    {
        Node* n = (Node*)lfirst(i);

        if (IsA(n, A_Indices)) {
            subscripts = lappend(subscripts, n);
            if (((A_Indices*)n)->lidx != NULL) {
                isSlice = true;
            }
        } else if (IsA(n, A_Star)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("row expansion via \"*\" is not supported here"),
                    parser_errposition(pstate, location)));
        } else {
            FieldStore* fstore = NULL;
            Oid typrelid;
            AttrNumber attnum;
            Oid fieldTypeId;
            int32 fieldTypMod;
            Oid fieldCollation;

            AssertEreport(IsA(n, String), MOD_OPT, "");

            /* process subscripts before this field selection */
            if (subscripts != NULL) {
                /* recurse, and then return because we're done */
                return transformAssignmentSubscripts(pstate,
                    basenode,
                    targetName,
                    targetTypeId,
                    targetTypMod,
                    targetCollation,
                    subscripts,
                    isSlice,
                    i,
                    rhs,
                    location);
            }

            /* No subscripts, so can process field selection here */
            typrelid = typeidTypeRelid(targetTypeId);
            if (!typrelid) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("cannot assign to field \"%s\" of column \"%s\" because its type %s is not a composite "
                               "type",
                            strVal(n),
                            targetName,
                            format_type_be(targetTypeId)),
                        parser_errposition(pstate, location)));
            }

            attnum = get_attnum(typrelid, strVal(n));
            if (attnum == InvalidAttrNumber) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("cannot assign to field \"%s\" of column \"%s\" because there is no such column in data "
                               "type %s",
                            strVal(n),
                            targetName,
                            format_type_be(targetTypeId)),
                        parser_errposition(pstate, location)));
            }
            if (attnum < 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("cannot assign to system column \"%s\"", strVal(n)),
                        parser_errposition(pstate, location)));
            }
            /* for "hash" column of table in ledger schema, we forbid insert and update */
            if (strcmp(strVal(n), "hash") == 0 && is_ledger_usertable(typrelid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot assign to system column \"%s\"", strVal(n)),
                        parser_errposition(pstate, location)));
            }
            get_atttypetypmodcoll(typrelid, attnum, &fieldTypeId, &fieldTypMod, &fieldCollation);

            /* recurse to create appropriate RHS for field assign */
            rhs = transformAssignmentIndirection(
                pstate, NULL, strVal(n), false, fieldTypeId, fieldTypMod, fieldCollation, lnext(i), rhs, location);

            /* and build a FieldStore node */
            fstore = makeNode(FieldStore);
            fstore->arg = (Expr*)basenode;
            fstore->newvals = list_make1(rhs);
            fstore->fieldnums = list_make1_int(attnum);
            fstore->resulttype = targetTypeId;

            return (Node*)fstore;
        }
    }

    /* process trailing subscripts, if any */
    if (subscripts != NULL) {
        /* recurse, and then return because we're done */
        return transformAssignmentSubscripts(pstate,
            basenode,
            targetName,
            targetTypeId,
            targetTypMod,
            targetCollation,
            subscripts,
            isSlice,
            NULL,
            rhs,
            location);
    }

    /* base case: just coerce RHS to match target type ID */

    result = coerce_to_target_type(
        pstate, rhs, exprType(rhs), targetTypeId, targetTypMod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
        NULL, NULL, -1);
    if (result == NULL) {
        if (targetIsArray) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("array assignment to \"%s\" requires type %s"
                           " but expression is of type %s",
                        targetName,
                        format_type_be(targetTypeId),
                        format_type_be(exprType(rhs))),
                    errhint("You will need to rewrite or cast the expression."),
                    parser_errposition(pstate, location)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("subfield \"%s\" is of type %s"
                           " but expression is of type %s",
                        targetName,
                        format_type_be(targetTypeId),
                        format_type_be(exprType(rhs))),
                    errhint("You will need to rewrite or cast the expression."),
                    parser_errposition(pstate, location)));
        }
    }

    return result;
}

/*
 * helper for transformAssignmentIndirection: process array assignment
 */
static Node* transformAssignmentSubscripts(ParseState* pstate, Node* basenode, const char* targetName, Oid targetTypeId,
    int32 targetTypMod, Oid targetCollation, List* subscripts, bool isSlice, ListCell* next_indirection, Node* rhs,
    int location)
{
    Node* result = NULL;
    Oid arrayType;
    int32 arrayTypMod;
    Oid elementTypeId;
    Oid typeNeeded;
    Oid collationNeeded;

    AssertEreport(subscripts != NIL, MOD_OPT, "");

    /* Identify the actual array type and element type involved */
    arrayType = targetTypeId;
    arrayTypMod = targetTypMod;
    elementTypeId = transformArrayType(&arrayType, &arrayTypMod);

    /* Identify type that RHS must provide */
    typeNeeded = isSlice ? arrayType : elementTypeId;

    /*
     * Array normally has same collation as elements, but there's an
     * exception: we might be subscripting a domain over an array type. In
     * that case use collation of the base type.
     */
    if (arrayType == targetTypeId) {
        collationNeeded = targetCollation;
    } else {
        collationNeeded = get_typcollation(arrayType);
    }
    /* recurse to create appropriate RHS for array assign */
    rhs = transformAssignmentIndirection(
        pstate, NULL, targetName, true, typeNeeded, arrayTypMod, collationNeeded, next_indirection, rhs, location);

    /* process subscripts */
    result = (Node*)transformArraySubscripts(pstate, basenode, arrayType, elementTypeId, arrayTypMod, subscripts, rhs);

    /* If target was a domain over array, need to coerce up to the domain */
    if (arrayType != targetTypeId) {
        result = coerce_to_target_type(pstate,
            result,
            exprType(result),
            targetTypeId,
            targetTypMod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            NULL,
            NULL,
            -1);
        /* probably shouldn't fail, but check */
        if (result == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                    errmsg("cannot cast type %s to %s", format_type_be(exprType(result)), format_type_be(targetTypeId)),
                    parser_errposition(pstate, location)));
        }
    }

    return result;
}

/*
 * checkInsertTargets -
 *	  generate a list of INSERT column targets if not supplied, or
 *	  test supplied column names to make sure they are in target table.
 *	  Also return an integer list of the columns' attribute numbers.
 */
List* checkInsertTargets(ParseState* pstate, List* cols, List** attrnos)
{
    *attrnos = NIL;
    bool is_blockchain_rel = false;
    Relation targetrel = (Relation)linitial(pstate->p_target_relation);

    if (cols == NIL) {
        /*
         * Generate default column list for INSERT.
         */
        if (targetrel == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("targetrel is NULL unexpectedly")));
        }

        FormData_pg_attribute* attr = targetrel->rd_att->attrs;
        int numcol = RelationGetNumberOfAttributes(targetrel);
        int i;
        is_blockchain_rel = targetrel->rd_isblockchain;

        for (i = 0; i < numcol; i++) {
            ResTarget* col = NULL;

            if (attr[i].attisdropped) {
                continue;
            }
            /* If the hidden column in timeseries relation, skip it */
            if (TsRelWithImplDistColumn(attr, i) && RelationIsTsStore(targetrel)) {
                continue;
            }

            col = makeNode(ResTarget);
            col->name = pstrdup(NameStr(attr[i].attname));
            if (is_blockchain_rel && strcmp(col->name, "hash") == 0) {
                continue;
            }
            /* skip the identity default value in D format */
            if (DB_IS_CMPT(D_FORMAT) && OidIsValid(pg_get_serial_sequence_internal(RelationGetRelid(targetrel),
                                                                                   attr[i].attnum, true, NULL))) {
                continue;
            }

            col->indirection = NIL;
            col->val = NULL;
            col->location = -1;
            cols = lappend(cols, col);
            *attrnos = lappend_int(*attrnos, i + 1);
        }
    } else {
        /*
         * Do initial validation of user-supplied INSERT column list.
         */
        Bitmapset* wholecols = NULL;
        Bitmapset* partialcols = NULL;
        ListCell* tl = NULL;

        foreach (tl, cols) {
            ResTarget* col = (ResTarget*)lfirst(tl);
            char* name = col->name;
            int attrno;

            /* Lookup column name, ereport on failure */
            attrno = attnameAttNum(targetrel, name, false);
            if (attrno == InvalidAttrNumber) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist",
                            name,
                            RelationGetRelationName(targetrel)),
                        parser_errposition(pstate, col->location)));
            }
            /*
             * Check for duplicates, but only of whole columns --- we allow
             * INSERT INTO foo (col.subcol1, col.subcol2)
             */
            if (col->indirection == NIL) {
                /* whole column; must not have any other assignment */
                if (bms_is_member(attrno, wholecols) || bms_is_member(attrno, partialcols)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_COLUMN),
                            errmsg("column \"%s\" specified more than once", name),
                            parser_errposition(pstate, col->location)));
                }
                wholecols = bms_add_member(wholecols, attrno);
            } else {
                /* partial column; must not have any whole assignment */
                if (bms_is_member(attrno, wholecols)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_COLUMN),
                            errmsg("column \"%s\" specified more than once", name),
                            parser_errposition(pstate, col->location)));
                }
                partialcols = bms_add_member(partialcols, attrno);
            }

            *attrnos = lappend_int(*attrnos, attrno);
        }
    }

    return cols;
}

/*
 * ExpandColumnRefStar()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 *
 * This handles the case where '*' appears as the last or only item in a
 * ColumnRef.  The code is shared between the case of foo.* at the top level
 * in a SELECT target list (where we want TargetEntry nodes in the result)
 * and foo.* in a ROW() or VALUES() construct (where we want just bare
 * expressions).
 *
 * The referenced columns are marked as requiring SELECT access.
 */
static List* ExpandColumnRefStar(ParseState* pstate, ColumnRef* cref, bool targetlist)
{
    List* fields = cref->fields;
    int numnames = list_length(fields);

    if (numnames == 1) {
        /*
         * Target item is a bare '*', expand all tables
         *
         * (e.g., SELECT * FROM emp, dept)
         *
         * Since the grammar only accepts bare '*' at top level of SELECT, we
         * need not handle the targetlist==false case here.
         */
        AssertEreport(targetlist, MOD_OPT, "");
        return ExpandAllTables(pstate, cref->location);
    } else {
        /*
         * Target item is relation.*, expand that table
         *
         * (e.g., SELECT emp.*, dname FROM emp, dept)
         *
         * Note: this code is a lot like transformColumnRef; it's tempting to
         * call that instead and then replace the resulting whole-row Var with
         * a list of Vars.	However, that would leave us with the RTE's
         * selectedCols bitmap showing the whole row as needing select
         * permission, as well as the individual columns.  That would be
         * incorrect (since columns added later shouldn't need select
         * permissions).  We could try to remove the whole-row permission bit
         * after the fact, but duplicating code is less messy.
         */
        char* nspname = NULL;
        char* relname = NULL;
        RangeTblEntry* rte = NULL;
        int levels_up;
        enum { CRSERR_NO_RTE, CRSERR_WRONG_DB, CRSERR_TOO_MANY } crserr = CRSERR_NO_RTE;

        /*
         * Give the PreParseColumnRefHook, if any, first shot.	If it returns
         * non-null then we should use that expression.
         */
        if (pstate->p_pre_columnref_hook != NULL) {
            Node* node = NULL;

            node = (*pstate->p_pre_columnref_hook)(pstate, cref);
            if (node != NULL) {
                return ExpandRowReference(pstate, node, targetlist);
            }
        }

        switch (numnames) {
            case 2:
                relname = strVal(linitial(fields));
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
                break;
            case 3:
                nspname = strVal(linitial(fields));
                relname = strVal(lsecond(fields));
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
                break;
            case 4: {
                char* catname = strVal(linitial(fields));

                /*
                 * We check the catalog name and then ignore it.
                 */
                if (strcmp(catname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, false)) != 0) {
                    crserr = CRSERR_WRONG_DB;
                    break;
                }
                nspname = strVal(lsecond(fields));
                relname = strVal(lthird(fields));
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
                break;
            }
            default:
                crserr = CRSERR_TOO_MANY;
                break;
        }

        /*
         * Now give the PostParseColumnRefHook, if any, a chance. We cheat a
         * bit by passing the RangeTblEntry, not a Var, as the planned
         * translation.  (A single Var wouldn't be strictly correct anyway.
         * This convention allows hooks that really care to know what is
         * happening.)
         */
        if (pstate->p_post_columnref_hook != NULL) {
            Node* node = NULL;

            node = (*pstate->p_post_columnref_hook)(pstate, cref, (Node*)rte);
            if (node != NULL) {
                if (rte != NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                            errmsg("column reference \"%s\" is ambiguous", NameListToString(cref->fields)),
                            parser_errposition(pstate, cref->location)));
                }
                return ExpandRowReference(pstate, node, targetlist);
            }
        }

        /*
         * Throw error if no translation found.
         */
        if (rte == NULL) {
            switch (crserr) {
                case CRSERR_NO_RTE:
                    errorMissingRTE(pstate, makeRangeVar(nspname, relname, cref->location));
                    break;
                case CRSERR_WRONG_DB:
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cross-database references are not implemented: %s", NameListToString(cref->fields)),
                            parser_errposition(pstate, cref->location)));
                    break;
                case CRSERR_TOO_MANY:
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg(
                                "improper qualified name (too many dotted names): %s", NameListToString(cref->fields)),
                            parser_errposition(pstate, cref->location)));
                    break;
                default:
                    break;
            }
        }

        /*
         * OK, expand the RTE into fields.
         */
        return ExpandSingleTable(pstate, rte, cref->location, targetlist);
    }
}

/*
 * ExpandAllTables()
 *		Transforms '*' (in the target list) into a list of targetlist entries.
 *
 * tlist entries are generated for each relation appearing in the query's
 * varnamespace.  We do not consider relnamespace because that would include
 * input tables of aliasless JOINs, NEW/OLD pseudo-entries, etc.
 *
 * The referenced relations/columns are marked as requiring SELECT access.
 */
static List* ExpandAllTables(ParseState* pstate, int location)
{
    List* target = NIL;
    ListCell* l = NULL;

    /* Check for SELECT *; */
    if (!pstate->p_varnamespace) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("SELECT * with no tables specified is not valid"),
                parser_errposition(pstate, location)));
    }
    /* add star info(start, end, only) */
    pstate->p_star_start = lappend_int(pstate->p_star_start, pstate->p_next_resno);
    pstate->p_star_only = lappend_int(pstate->p_star_only, 1);
    foreach (l, pstate->p_varnamespace) {
        ParseNamespaceItem *nsitem = (ParseNamespaceItem *)lfirst(l);
        RangeTblEntry *rte = nsitem->p_rte;
        int rtindex = RTERangeTablePosn(pstate, rte, NULL);

        /* Should not have any lateral-only items when parsing targetlist */
        Assert(!nsitem->p_lateral_only);

        target = list_concat(target, expandRelAttrs(pstate, rte, rtindex, 0, location));
    }
    pstate->p_star_end = lappend_int(pstate->p_star_end, pstate->p_next_resno);

    return target;
}

/*
 * ExpandIndirectionStar()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 *
 * This handles the case where '*' appears as the last item in A_Indirection.
 * The code is shared between the case of foo.* at the top level in a SELECT
 * target list (where we want TargetEntry nodes in the result) and foo.* in
 * a ROW() or VALUES() construct (where we want just bare expressions).
 */
static List* ExpandIndirectionStar(ParseState* pstate, A_Indirection* ind, bool targetlist, ParseExprKind exprKind)
{
    Node* expr = NULL;

    /* Strip off the '*' to create a reference to the rowtype object */
    ind = (A_Indirection*)copyObject(ind);
    ind->indirection = list_truncate(ind->indirection, list_length(ind->indirection) - 1);

    /* And transform that */
    expr = transformExpr(pstate, (Node*)ind, exprKind);

    /* Expand the rowtype expression into individual fields */
    return ExpandRowReference(pstate, expr, targetlist);
}

/*
 * ExpandSingleTable()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 *
 * This handles the case where foo has been determined to be a simple
 * reference to an RTE, so we can just generate Vars for the expressions.
 *
 * The referenced columns are marked as requiring SELECT access.
 */
static List* ExpandSingleTable(ParseState* pstate, RangeTblEntry* rte, int location, bool targetlist)
{
    int sublevels_up;
    int rtindex;

    rtindex = RTERangeTablePosn(pstate, rte, &sublevels_up);

    if (targetlist) {
        List* te_list = NIL;

        /*mark flags for single table just like  foo.* */
        pstate->p_star_start = lappend_int(pstate->p_star_start, pstate->p_next_resno);
        pstate->p_star_only = lappend_int(pstate->p_star_only, -1);

        /* expandRelAttrs handles permissions marking */
        te_list = expandRelAttrs(pstate, rte, rtindex, sublevels_up, location);
        pstate->p_star_end = lappend_int(pstate->p_star_end, pstate->p_next_resno);

        return te_list;
    } else {
        List* vars = NIL;
        ListCell* l = NULL;

        expandRTE(rte, rtindex, sublevels_up, location, false, NULL, &vars);

        /*
         * Require read access to the table.  This is normally redundant with
         * the markVarForSelectPriv calls below, but not if the table has zero
         * columns.
         */
        rte->requiredPerms |= ACL_SELECT;

        /* Require read access to each column */
        foreach (l, vars) {
            Var* var = (Var*)lfirst(l);

            markVarForSelectPriv(pstate, var, rte);
        }

        return vars;
    }
}

/*
 * ExpandRowReference()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 *
 * This handles the case where foo is an arbitrary expression of composite
 * type.
 */
static List* ExpandRowReference(ParseState* pstate, Node* expr, bool targetlist)
{
    List* result = NIL;
    TupleDesc tupleDesc;
    int numAttrs;
    int i;

    /*
     * If the rowtype expression is a whole-row Var, we can expand the fields
     * as simple Vars.	Note: if the RTE is a relation, this case leaves us
     * with the RTE's selectedCols bitmap showing the whole row as needing
     * select permission, as well as the individual columns.  However, we can
     * only get here for weird notations like (table.*).*, so it's not worth
     * trying to clean up --- arguably, the permissions marking is correct
     * anyway for such cases.
     */
    if (IsA(expr, Var) && ((Var*)expr)->varattno == InvalidAttrNumber) {
        Var* var = (Var*)expr;
        RangeTblEntry* rte = NULL;

        rte = GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
        return ExpandSingleTable(pstate, rte, var->location, targetlist);
    }

    /*
     * Otherwise we have to do it the hard way.  Our current implementation is
     * to generate multiple copies of the expression and do FieldSelects.
     * (This can be pretty inefficient if the expression involves nontrivial
     * computation :-(.)
     *
     * Verify it's a composite type, and get the tupdesc.  We use
     * get_expr_result_type() because that can handle references to functions
     * returning anonymous record types.  If that fails, use
     * lookup_rowtype_tupdesc(), which will almost certainly fail as well, but
     * it will give an appropriate error message.
     *
     * If it's a Var of type RECORD, we have to work even harder: we have to
     * find what the Var refers to, and pass that to get_expr_result_type.
     * That task is handled by expandRecordVariable().
     */
    if (IsA(expr, Var) && ((Var*)expr)->vartype == RECORDOID) {
        tupleDesc = expandRecordVariable(pstate, (Var*)expr, 0);
    } else if (get_expr_result_type(expr, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE) {
        if (IsA(expr, RowExpr) && ((RowExpr*)expr)->row_typeid == RECORDOID) {
            RowExpr* rowexpr = (RowExpr*)expr;
            tupleDesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
            BlessTupleDesc(tupleDesc);
        } else {
            tupleDesc = lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));
        }
	}

    if (unlikely(tupleDesc == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("tupleDesc should not be null")));
    }

    /* Generate a list of references to the individual fields */
    numAttrs = tupleDesc->natts;
    for (i = 0; i < numAttrs; i++) {
        Form_pg_attribute att = &tupleDesc->attrs[i];
        FieldSelect* fselect = NULL;

        if (att->attisdropped) {
            continue;
        }
        fselect = makeNode(FieldSelect);
        fselect->arg = (Expr*)copyObject(expr);
        fselect->fieldnum = i + 1;
        fselect->resulttype = att->atttypid;
        fselect->resulttypmod = att->atttypmod;
        /* save attribute's collation for parse_collate.c */
        fselect->resultcollid = att->attcollation;

        if (targetlist) {
            /* add TargetEntry decoration */
            TargetEntry* te = NULL;

            te = makeTargetEntry(
                (Expr*)fselect, (AttrNumber)pstate->p_next_resno++, pstrdup(NameStr(att->attname)), false);
            result = lappend(result, te);
        } else {
            result = lappend(result, fselect);
        }
    }

    return result;
}

/*
 * expandRecordVariable
 *		Get the tuple descriptor for a Var of type RECORD, if possible.
 *
 * Since no actual table or view column is allowed to have type RECORD, such
 * a Var must refer to a JOIN or FUNCTION RTE or to a subquery output.	We
 * drill down to find the ultimate defining expression and attempt to infer
 * the tupdesc from it.  We ereport if we can't determine the tupdesc.
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 */
TupleDesc expandRecordVariable(ParseState* pstate, Var* var, int levelsup)
{
    TupleDesc tupleDesc;
    int netlevelsup;
    RangeTblEntry* rte = NULL;
    AttrNumber attnum;
    Node* expr = NULL;

    /* Check my caller didn't mess up */
    AssertEreport(IsA(var, Var), MOD_OPT, "");
    AssertEreport(var->vartype == RECORDOID, MOD_OPT, "");

    netlevelsup = var->varlevelsup + levelsup;
    rte = GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
    attnum = var->varattno;

    if (attnum == InvalidAttrNumber) {
        /* Whole-row reference to an RTE, so expand the known fields */
        List *names = NIL; 
        List *vars = NIL;
        ListCell *lname = NULL; 
        ListCell *lvar = NULL;
        int i;

        expandRTE(rte, var->varno, 0, var->location, false, &names, &vars);

        tupleDesc = CreateTemplateTupleDesc(list_length(vars), false);
        i = 1;
        forboth(lname, names, lvar, vars) {
            char* label = strVal(lfirst(lname));
            Node* varnode = (Node*)lfirst(lvar);

            TupleDescInitEntry(tupleDesc, i, label, exprType(varnode), exprTypmod(varnode), 0);
            TupleDescInitEntryCollation(tupleDesc, i, exprCollation(varnode));
            i++;
        }
        AssertEreport(lname == NULL && lvar == NULL, MOD_OPT, ""); /* lists same length? */

        return tupleDesc;
    }

    expr = (Node*)var; /* default if we can't drill down */

    switch (rte->rtekind) {
        case RTE_RELATION:
        case RTE_VALUES:
        case RTE_RESULT:
            /*
             * This case should not occur: a column of a table or values list
             * shouldn't have type RECORD.  Fall through and fail (most
             * likely) at the bottom.
             */
            break;
        case RTE_SUBQUERY: {
            /* Subselect-in-FROM: examine sub-select's output expr */
            TargetEntry* ste = get_tle_by_resno(rte->subquery->targetList, attnum);

            if (ste == NULL || ste->resjunk) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("subquery %s does not have attribute %d", rte->eref->aliasname, attnum)));
            }    
            expr = (Node*)ste->expr;
            if (IsA(expr, Var)) {
                /*
                 * Recurse into the sub-select to see what its Var refers
                 * to.	We have to build an additional level of ParseState
                 * to keep in step with varlevelsup in the subselect.
                 */
                ParseState mypstate;
                errno_t rc;

                rc = memset_s(&mypstate, sizeof(mypstate), 0, sizeof(mypstate));
                securec_check(rc, "\0", "\0");
                mypstate.parentParseState = pstate;
                mypstate.p_rtable = rte->subquery->rtable;
                /* don't bother filling the rest of the fake pstate */
                return expandRecordVariable(&mypstate, (Var*)expr, 0);
            }
            /* else fall through to inspect the expression */
        } break;
        case RTE_JOIN:
            /* Join RTE --- recursively inspect the alias variable */
            AssertEreport(attnum > 0 && attnum <= list_length(rte->joinaliasvars), MOD_OPT, "");
            expr = (Node*)list_nth(rte->joinaliasvars, attnum - 1);
            if (IsA(expr, Var)) {
                return expandRecordVariable(pstate, (Var*)expr, netlevelsup);
            }
            /* else fall through to inspect the expression */
            break;
        case RTE_FUNCTION:

            /*
             * We couldn't get here unless a function is declared with one of
             * its result columns as RECORD, which is not allowed.
             */
            break;
        case RTE_CTE:
            /* CTE reference: examine subquery's output expr */
            if (!rte->self_reference) {
                CommonTableExpr* cte = GetCTEForRTE(pstate, rte, netlevelsup);
                TargetEntry* ste = NULL;

                ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
                if (ste == NULL || ste->resjunk) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            errmsg("subquery %s does not have attribute %d", rte->eref->aliasname, attnum)));
                }    
                expr = (Node*)ste->expr;
                if (IsA(expr, Var)) {
                    /*
                     * Recurse into the CTE to see what its Var refers to. We
                     * have to build an additional level of ParseState to keep
                     * in step with varlevelsup in the CTE; furthermore it
                     * could be an outer CTE.
                     */
                    ParseState mypstate;
                    Index levelsup;
                    errno_t rc = EOK;

                    rc = memset_s(&mypstate, sizeof(mypstate), 0, sizeof(mypstate));
                    securec_check(rc, "\0", "\0");
                    /* this loop must work, since GetCTEForRTE did */
                    for (levelsup = 0; levelsup < rte->ctelevelsup + netlevelsup; levelsup++) {
                        pstate = pstate->parentParseState;
                    }
                    mypstate.parentParseState = pstate;
                    mypstate.p_rtable = ((Query*)cte->ctequery)->rtable;
                    /* don't bother filling the rest of the fake pstate */

                    return expandRecordVariable(&mypstate, (Var*)expr, 0);
                }
                /* else fall through to inspect the expression */
            }
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Invalid RTE found")));
            break;
#endif /* PGXC */
        default:
            break;
    }

    /*
     * We now have an expression we can't expand any more, so see if
     * get_expr_result_type() can do anything with it.	If not, pass to
     * lookup_rowtype_tupdesc() which will probably fail, but will give an
     * appropriate error message while failing.
     */
    if (get_expr_result_type(expr, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE) {
        tupleDesc = lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));
    }

    return tupleDesc;
}

/*
 * FigureColname -
 *	  if the name of the resulting column is not specified in the target
 *	  list, we have to guess a suitable name.  The SQL spec provides some
 *	  guidance, but not much...
 *
 * Note that the argument is the *untransformed* parse tree for the target
 * item.  This is a shade easier to work with than the transformed tree.
 */
char* FigureColname(Node* node)
{
    char* name = NULL;

    (void)FigureColnameInternal(node, &name);
    if (name != NULL) {
        return name;
    }
    /* default result if we can't guess anything */
    return (char*)"?column?";
}

/*
 * FigureIndexColname -
 *	  choose the name for an expression column in an index
 *
 * This is actually just like FigureColname, except we return NULL if
 * we can't pick a good name.
 */
char* FigureIndexColname(Node* node)
{
    char* name = NULL;

    (void)FigureColnameInternal(node, &name);
    return name;
}

/*
 * FigureColnameInternal -
 *	  internal workhorse for FigureColname
 *
 * Return value indicates strength of confidence in result:
 *		0 - no information
 *		1 - second-best name choice
 *		2 - good name choice
 * The return value is actually only used internally.
 * If the result isn't zero, *name is set to the chosen name.
 */
static int FigureColnameInternal(Node* node, char** name)
{
    int strength = 0;

    if (node == NULL) {
        return strength;
    }
    switch (nodeTag(node)) {
        case T_ColumnRef: {
            char* fname = find_last_field_name(((ColumnRef*)node)->fields);
            if (fname != NULL) {
                *name = fname;
                return 2;
            }
        } break;
        case T_A_Indirection: {
            A_Indirection* ind = (A_Indirection*)node;
            char* fname = find_last_field_name(ind->indirection);
            if (fname != NULL) {
                *name = fname;
                return 2;
            }
            return FigureColnameInternal(ind->arg, name);
        } break;
        case T_FuncCall:
            if (((FuncCall*)node)->colname != NULL) {
                *name = ((FuncCall*)node)->colname;
            } else {
                *name = strVal(llast(((FuncCall*)node)->funcname));
            }
            return 2;
        case T_A_Expr:
            /* make nullif() act like a regular function */
            if (((A_Expr*)node)->kind == AEXPR_NULLIF) {
                *name = "nullif";
                return 2;
            }
            break;
        case T_PredictByFunction: {
            size_t len = strlen(((PredictByFunction*)node)->model_name) + strlen("_pred") + 1;
            char* colname = (char*)palloc0(len);
            errno_t rc = snprintf_s(colname, len, len - 1, "%s_pred", ((PredictByFunction*)node)->model_name);
            securec_check_ss(rc, "\0", "\0");
            *name = colname;
            return 1;
        } break;    
        case T_TypeCast:
            strength = FigureColnameInternal(((TypeCast*)node)->arg, name);
            if (strength <= 1) {
                if (((TypeCast*)node)->typname != NULL) {
                    *name = strVal(llast(((TypeCast*)node)->typname->names));
                    return 1;
                }
            }
            if (!((TypeCast*)node)->default_expr) {
                break;
            }
            strength = FigureColnameInternal(((TypeCast*)node)->default_expr, name);
            if (strength <= 1) {
                if (((TypeCast*)node)->typname != NULL) {
                    *name = strVal(llast(((TypeCast*)node)->typname->names));
                    return 1;
                }
            }
            break;
        case T_CollateClause:
            return FigureColnameInternal(((CollateClause*)node)->arg, name);
        case T_GroupingFunc:
            /* make GROUPING() act like a regular function */
            *name = "grouping";
            return 2;
        case T_Rownum:
            *name = "rownum";
            return 2;
        case T_SubLink:
            switch (((SubLink*)node)->subLinkType) {
                case EXISTS_SUBLINK:
                    *name = "exists";
                    return 2;
                case ARRAY_SUBLINK:
                    *name = "array";
                    return 2;
                case EXPR_SUBLINK: {
                    /* Get column name of the subquery's single target */
                    SubLink* sublink = (SubLink*)node;
                    Query* query = (Query*)sublink->subselect;

                    /*
                     * The subquery has probably already been transformed,
                     * but let's be careful and check that.  (The reason
                     * we can see a transformed subquery here is that
                     * transformSubLink is lazy and modifies the SubLink
                     * node in-place.)
                     */
                    if (IsA(query, Query)) {
                        TargetEntry* te = (TargetEntry*)linitial(query->targetList);

                        if (te->resname) {
                            *name = te->resname;
                            return 2;
                        }
                    }
                } break;
                    /* As with other operator-like nodes, these have no names */
                case ALL_SUBLINK:
                case ANY_SUBLINK:
                case ROWCOMPARE_SUBLINK:
                case CTE_SUBLINK:
#ifdef USE_SPQ
                case NOT_EXISTS_SUBLINK:
#endif
                    break;
            }
            break;
        case T_CaseExpr:
            strength = FigureColnameInternal((Node*)((CaseExpr*)node)->defresult, name);
            if (strength <= 1) {
                *name = "case";
                return 1;
            }
            break;
        case T_A_ArrayExpr:
            /* make ARRAY[] act like a function */
            *name = "array";
            return 2;
        case T_RowExpr:
            /* make ROW() act like a function */
            *name = "row";
            return 2;
        case T_CoalesceExpr:
            /* make coalesce() act like a regular function */
            // modify NVL display to A db's style "NVL" instead of "COALESCE"
            if (((CoalesceExpr*)node)->isnvl) {
                *name = "nvl";
            } else {
                *name = "coalesce";
            }
            return 2;
        case T_MinMaxExpr:
            /* make greatest/least act like a regular function */
            switch (((MinMaxExpr*)node)->op) {
                case IS_GREATEST:
                    *name = "greatest";
                    return 2;
                case IS_LEAST:
                    *name = "least";
                    return 2;
            }
            break;
        case T_XmlExpr:
            /* make SQL/XML functions act like a regular function */
            switch (((XmlExpr*)node)->op) {
                case IS_XMLCONCAT:
                    *name = "xmlconcat";
                    return 2;
                case IS_XMLELEMENT:
                    *name = "xmlelement";
                    return 2;
                case IS_XMLFOREST:
                    *name = "xmlforest";
                    return 2;
                case IS_XMLPARSE:
                    *name = "xmlparse";
                    return 2;
                case IS_XMLPI:
                    *name = "xmlpi";
                    return 2;
                case IS_XMLROOT:
                    *name = "xmlroot";
                    return 2;
                case IS_XMLSERIALIZE:
                    *name = "xmlserialize";
                    return 2;
                case IS_DOCUMENT:
                    /* nothing */
                    break;
            }
            break;
        case T_XmlSerialize:
            *name = "xmlserialize";
            return 2;
        /* get name of user_defined variables. */
        case T_UserVar: {
            size_t len = strlen(((UserVar *)node)->name) + strlen("@") + 1;
            char *colname = (char *)palloc0(len);
            errno_t rc = snprintf_s(colname, len, len - 1, "@%s", ((UserVar *)node)->name);
            securec_check_ss(rc, "\0", "\0");
            *name = colname;
            return 1;
        } break;
        default:
            break;
    }

    return strength;
}
