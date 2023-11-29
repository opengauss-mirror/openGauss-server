/* -------------------------------------------------------------------------
 *
 * tlist.cpp
 *	  Target list manipulation routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/tlist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_constraint.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/streamplan.h"
#include "optimizer/tlist.h"
#include "optimizer/cost.h"

/* Test if an expression node represents a SRF call.  Beware multiple eval! */
#define IS_SRF_CALL(node) \
    ((IsA(node, FuncExpr) && ((FuncExpr *)(node))->funcretset) || (IsA(node, OpExpr) && ((OpExpr *)(node))->opretset))

/*
 * Data structures for split_pathtarget_at_srfs().  To preserve the identity
 * of sortgroupref items even if they are textually equal(), what we track is
 * not just bare expressions but expressions plus their sortgroupref indexes.
 */
typedef struct {
    Node *expr;         /* some subexpression of a PathTarget */
    Index sortgroupref; /* its sortgroupref, or 0 if none */
} split_pathtarget_item;

typedef struct {
    /* This is a List of bare expressions: */
    List *input_target_exprs; /* exprs available from input */
    /* These are Lists of Lists of split_pathtarget_items: */
    List *level_srfs;       /* SRF exprs to evaluate at each level */
    List *level_input_vars; /* input vars needed at each level */
    List *level_input_srfs; /* input SRFs needed at each level */
    /* These are Lists of split_pathtarget_items: */
    List *current_input_vars; /* vars needed in current subexpr */
    List *current_input_srfs; /* SRFs needed in current subexpr */
    /* Auxiliary data for current split_pathtarget_walker traversal: */
    int current_depth;   /* max SRF depth in current subexpr */
    Index current_sgref; /* current subexpr's sortgroupref, or 0 */
} split_pathtarget_context;

static bool split_pathtarget_walker(Node *node, split_pathtarget_context *context);
static void add_sp_item_to_pathtarget(PathTarget *target, split_pathtarget_item *item);
static void add_sp_items_to_pathtarget(PathTarget *target, List *items);

/*
 * Target list creation and searching utilities
 *
 * tlist_member
 *	  Finds the (first) member of the given tlist whose expression is
 *	  equal() to the given expression.	Result is NULL if no such member.
 */
TargetEntry* tlist_member(Node* node, List* targetlist)
{
    ListCell* temp = NULL;

    foreach (temp, targetlist) {
        TargetEntry* tlentry = (TargetEntry*)lfirst(temp);

        if (equal(node, tlentry->expr))
            return tlentry;
    }
    return NULL;
}

/*
 * tlist_member_ignore_relabel
 *	  Same as above, except that we ignore top-level RelabelType nodes
 *	  while checking for a match.  This is needed for some scenarios
 *	  involving binary-compatible sort operations.
 */
TargetEntry* tlist_member_ignore_relabel(Node* node, List* targetlist)
{
    ListCell* temp = NULL;

    while (node && IsA(node, RelabelType))
        node = (Node*)((RelabelType*)node)->arg;

    foreach (temp, targetlist) {
        TargetEntry* tlentry = (TargetEntry*)lfirst(temp);
        Expr* tlexpr = tlentry->expr;

        while (tlexpr && IsA(tlexpr, RelabelType))
            tlexpr = ((RelabelType*)tlexpr)->arg;

        if (equal(node, tlexpr))
            return tlentry;
    }
    return NULL;
}

#ifdef STREAMPLAN
static bool equal_node_except_aggref(const void* a, const void* b, bool* judge_nest)
{
    Aggref* node1 = (Aggref*)a;
    Aggref* node2 = (Aggref*)b;
    int saved_stage = -1;
    List* saved_aggdistinct = NIL;
    Oid saved_type = InvalidOid;
    bool matched = false;
    bool nested = false;

    if (node1->aggstage != node2->aggstage) {
        /* For Aggref, we allow aggstage 1 more than lower level node */
        if (node1->aggstage == node2->aggstage + 1)
            nested = true;
        saved_stage = node2->aggstage;
        node2->aggstage = node1->aggstage;
    }
    /*
     * For count(distinct) pull down, the top may not have aggdistinct node
     * while the low level has aggdistinct node
     */
    if (node1->aggdistinct == NIL && node2->aggdistinct != NIL) {
        saved_aggdistinct = node2->aggdistinct;
        node2->aggdistinct = NIL;
    }
    /* For aggref in middle level, we allow different agg_type, only in nested case */
    if (node1->aggtype != node2->aggtype && judge_nest) {
        saved_type = node2->aggtype;
        node2->aggtype = node1->aggtype;
    }
    if (equal(node1, node2))
        matched = true;
    if (saved_stage != -1)
        node2->aggstage = saved_stage;
    if (saved_aggdistinct != NIL)
        node2->aggdistinct = saved_aggdistinct;
    if (saved_type != InvalidOid)
        node2->aggtype = saved_type;
    if (matched && judge_nest != NULL)
        *judge_nest = nested;
    return matched;
}

/*
 * tlist_member_except_aggref
 *	  Same as tlist_member(), except that we treat the following scenario(s) as a match:
 *	  case 1:
 *		  (1) top-level aggref nodes same as low-level aggref nodes except aggdistinct
 *		  (2) aggref node in middle level can have different aggtype
 *		  (3) nested aggrefs can have adjacent aggstage number
 *		  This is needed for some stream scenarios when we consider push count(distinct)
 *		  operation to datanodes. The function can be extended in stream scenarios.
 *
 *	  case 2:
 *		  top-level relabeltype node and the low-level relabeltype node has same arg.
 */
TargetEntry* tlist_member_except_aggref(Node* node, List* targetlist, bool* nested_agg, bool* nested_relabeltype)
{
    ListCell* temp = NULL;

    foreach (temp, targetlist) {
        TargetEntry* tlentry = (TargetEntry*)lfirst(temp);
        Expr* tlexpr = tlentry->expr;

        if (IsA(node, Aggref) && IsA(tlexpr, Aggref) && equal_node_except_aggref(node, tlexpr, nested_agg))
            return tlentry;
        if (equal(node, tlentry->expr))
            return tlentry;
    }

    /*
     * when node is a relabeltype and not find in targetlist, then try to match relabeltype's arg.
     */
    foreach (temp, targetlist) {
        TargetEntry* tlentry = (TargetEntry*)lfirst(temp);
        Expr* tlexpr = tlentry->expr;

        /* Also check relabel type since it has the same type as node */
        if (IsA(tlexpr, RelabelType) && IsA(node, RelabelType)) {
            Expr* tlexpr_arg = ((RelabelType*)tlexpr)->arg;
            Expr* node_arg = ((RelabelType*)node)->arg;

            if (equal(node_arg, tlexpr_arg)) {
                *nested_relabeltype = true;
                return tlentry;
            }
        }
    }

    return NULL;
}
#endif

/*
 * flatten_tlist
 *	  Create a target list that only contains unique variables.
 *
 * Aggrefs and PlaceHolderVars in the input are treated according to
 * aggbehavior and phbehavior, for which see pull_var_clause().
 *
 * 'tlist' is the current target list
 *
 * Returns the "flattened" new target list.
 *
 * The result is entirely new structure sharing no nodes with the original.
 * Copying the Var nodes is probably overkill, but be safe for now.
 */
List* flatten_tlist(List* tlist, PVCAggregateBehavior aggbehavior, PVCPlaceHolderBehavior phbehavior)
{
    List* vlist = pull_var_clause((Node*)tlist, aggbehavior, phbehavior);
    List* new_tlist = NIL;

    new_tlist = add_to_flat_tlist(NIL, vlist);
    list_free_ext(vlist);
    return new_tlist;
}

/*
 * add_to_flat_tlist
 *		Add more items to a flattened tlist (if they're not already in it)
 *
 * 'tlist' is the flattened tlist
 * 'exprs' is a list of expressions (usually, but not necessarily, Vars)
 *
 * Returns the extended tlist.
 */
List* add_to_flat_tlist(List* tlist, List* exprs)
{
    int next_resno = list_length(tlist) + 1;
    ListCell* lc = NULL;

    foreach (lc, exprs) {
        Node* expr = (Node*)lfirst(lc);

        if (!tlist_member(expr, tlist)) {
            TargetEntry* tle = NULL;

            tle = makeTargetEntry((Expr*)copyObject(expr), /* copy needed?? */
                next_resno++,
                NULL,
                false);
            tlist = lappend(tlist, tle);
        }
    }
    return tlist;
}

/*
 * get_tlist_exprs
 *		Get just the expression subtrees of a tlist
 *
 * Resjunk columns are ignored unless includeJunk is true
 */
List* get_tlist_exprs(List* tlist, bool includeJunk)
{
    List* result = NIL;
    ListCell* l = NULL;

    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->resjunk && !includeJunk)
            continue;

        result = lappend(result, tle->expr);
    }
    return result;
}

/*
 * Does tlist have same output datatypes as listed in colTypes?
 *
 * Resjunk columns are ignored if junkOK is true; otherwise presence of
 * a resjunk column will always cause a 'false' result.
 *
 * Note: currently no callers care about comparing typmods.
 */
bool tlist_same_datatypes(List* tlist, List* colTypes, bool junkOK)
{
    ListCell* l = NULL;
    ListCell* curColType = list_head(colTypes);

    /* if there are correlated vars, we don't support append path */
    if (IS_STREAM_PLAN && contain_vars_of_level_or_above((Node*)tlist, 1))
        return false;

    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->resjunk) {
            if (!junkOK)
                return false;
        } else {
            if (curColType == NULL)
                return false; /* tlist longer than colTypes */
            if (exprType((Node*)tle->expr) != lfirst_oid(curColType))
                return false;
            curColType = lnext(curColType);
        }
    }
    if (curColType != NULL)
        return false; /* tlist shorter than colTypes */
    return true;
}

/*
 * Does tlist have same exposed collations as listed in colCollations?
 *
 * Identical logic to the above, but for collations.
 */
bool tlist_same_collations(List* tlist, List* colCollations, bool junkOK)
{
    ListCell* l = NULL;
    ListCell* curColColl = list_head(colCollations);

    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->resjunk) {
            if (!junkOK)
                return false;
        } else {
            if (curColColl == NULL)
                return false; /* tlist longer than colCollations */
            if (exprCollation((Node*)tle->expr) != lfirst_oid(curColColl))
                return false;
            curColColl = lnext(curColColl);
        }
    }
    if (curColColl != NULL)
        return false; /* tlist shorter than colCollations */
    return true;
}

/*
 * tlist_same_exprs
 *		Check whether two target lists contain the same expressions
 *
 * Note: this function is used to decide whether it's safe to jam a new tlist
 * into a non-projection-capable plan node.  Obviously we can't do that unless
 * the node's tlist shows it already returns the column values we want.
 * However, we can ignore the TargetEntry attributes resname, ressortgroupref,
 * resorigtbl, resorigcol, and resjunk, because those are only labelings that
 * don't affect the row values computed by the node.  (Moreover, if we didn't
 * ignore them, we'd frequently fail to make the desired optimization, since
 * the planner tends to not bother to make resname etc. valid in intermediate
 * plan nodes.)  Note that on success, the caller must still jam the desired
 * tlist into the plan node, else it won't have the desired labeling fields.
 */
bool tlist_same_exprs(List *tlist1, List *tlist2)
{
    ListCell *lc1, *lc2;

    if (list_length(tlist1) != list_length(tlist2))
        return false; /* not same length, so can't match */

    forboth(lc1, tlist1, lc2, tlist2)
    {
        TargetEntry *tle1 = (TargetEntry *)lfirst(lc1);
        TargetEntry *tle2 = (TargetEntry *)lfirst(lc2);

        if (!equal(tle1->expr, tle2->expr))
            return false;
    }

    return true;
}

/*
 * get_sortgroupref_tle
 *		Find the targetlist entry matching the given SortGroupRef index,
 *		and return it.
 */
TargetEntry* get_sortgroupref_tle(Index sortref, List* targetList, bool report_error)
{
    ListCell* l = NULL;

    foreach (l, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->ressortgroupref == sortref)
            return tle;
    }

    if (report_error)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_GROUPING_ERROR),
                errmsg("ORDER/GROUP BY expression not found in targetlist")));
    return NULL;
}

/*
 * get_sortgroupclause_tle
 *		Find the targetlist entry matching the given SortGroupClause
 *		by ressortgroupref, and return it.
 */
TargetEntry* get_sortgroupclause_tle(SortGroupClause* sgClause, List* targetList, bool report_error)
{
    return get_sortgroupref_tle(sgClause->tleSortGroupRef, targetList, report_error);
}

/*
 * get_sortgroupclause_expr
 *		Find the targetlist entry matching the given SortGroupClause
 *		by ressortgroupref, and return its expression.
 */
Node* get_sortgroupclause_expr(SortGroupClause* sgClause, List* targetList)
{
    TargetEntry* tle = get_sortgroupclause_tle(sgClause, targetList);
    if (tle == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                (errmsg("Failed to get sort group clause. "))));

    return (Node*)tle->expr;
}

/*
 * get_sortgrouplist_exprs
 *		Given a list of SortGroupClauses, build a list
 *		of the referenced targetlist expressions.
 */
List* get_sortgrouplist_exprs(List* sgClauses, List* targetList)
{
    List* result = NIL;
    ListCell* l = NULL;

    foreach (l, sgClauses) {
        SortGroupClause* sortcl = (SortGroupClause*)lfirst(l);
        Node* sortexpr = NULL;

        sortexpr = get_sortgroupclause_expr(sortcl, targetList);
        result = lappend(result, sortexpr);
    }
    return result;
}

/*
 * Functions to extract data from a list of SortGroupClauses
 *
 * These don't really belong in tlist.c, but they are sort of related to the
 * functions just above, and they don't seem to deserve their own file.
 *
 * extract_grouping_ops - make an array of the equality operator OIDs
 *		for a SortGroupClause list
 */
Oid* extract_grouping_ops(List* groupClause)
{
    int numCols = list_length(groupClause);
    int colno = 0;
    Oid* groupOperators = NULL;
    ListCell* glitem = NULL;

    groupOperators = (Oid*)palloc(sizeof(Oid) * numCols);

    foreach (glitem, groupClause) {
        SortGroupClause* groupcl = (SortGroupClause*)lfirst(glitem);

        groupOperators[colno] = groupcl->eqop;
        Assert(OidIsValid(groupOperators[colno]));
        colno++;
    }

    return groupOperators;
}

/*
 * extract_grouping_collations - make an array of the grouping column collations
 * for a SortGroupClause list
 */
Oid* extract_grouping_collations(List* group_clause, List* tlist)
{
    int num_cols = list_length(group_clause);
    int colno = 0;
    Oid* grp_collations;
    ListCell* glitem;

    grp_collations = (Oid *)palloc(sizeof(Oid) * num_cols);

    foreach(glitem, group_clause)
    {
        SortGroupClause* groupcl = (SortGroupClause *)lfirst(glitem);
        TargetEntry* tle = get_sortgroupclause_tle(groupcl, tlist);

        grp_collations[colno++] = exprCollation((Node *) tle->expr);
    }

    return grp_collations;
}

/*
 * get_sortgroupref_clause
 *		Find the SortGroupClause matching the given SortGroupRef index,
 *		and return it.
 */
SortGroupClause* get_sortgroupref_clause(Index sortref, List* clauses)
{
    ListCell* l = NULL;

    foreach (l, clauses) {
        SortGroupClause* cl = (SortGroupClause*)lfirst(l);

        if (cl->tleSortGroupRef == sortref)
            return cl;
    }

    ereport(ERROR,
        (errmodule(MOD_OPT), errcode(ERRCODE_GROUPING_ERROR), errmsg("ORDER/GROUP BY expression not found in list")));

    return NULL; /* keep compiler quiet */
}

/*
 * get_sortgroupref_clause_noerr
 *		As above, but return NULL rather than throwing an error if not found.
 */
SortGroupClause *get_sortgroupref_clause_noerr(Index sortref, List *clauses)
{
    ListCell *l;

    foreach (l, clauses) {
        SortGroupClause *cl = (SortGroupClause *)lfirst(l);

        if (cl->tleSortGroupRef == sortref)
            return cl;
    }

    return NULL;
}

/*
 * extract_grouping_cols - make an array of the grouping column resnos
 *		for a SortGroupClause list
 */
AttrNumber* extract_grouping_cols(List* groupClause, List* tlist)
{
    AttrNumber* grpColIdx = NULL;
    int numCols = list_length(groupClause);
    int colno = 0;
    ListCell* glitem = NULL;

    grpColIdx = (AttrNumber*)palloc(sizeof(AttrNumber) * numCols);

    foreach (glitem, groupClause) {
        SortGroupClause* groupcl = (SortGroupClause*)lfirst(glitem);
        TargetEntry* tle = get_sortgroupclause_tle(groupcl, tlist);

        grpColIdx[colno++] = tle->resno;
    }

    return grpColIdx;
}

/*
 * grouping_is_sortable - is it possible to implement grouping list by sorting?
 *
 * This is easy since the parser will have included a sortop if one exists.
 */
bool grouping_is_sortable(List* groupClause)
{
    ListCell* glitem = NULL;

    foreach (glitem, groupClause) {
        SortGroupClause* groupcl = (SortGroupClause*)lfirst(glitem);

        if (!OidIsValid(groupcl->sortop))
            return false;
    }
    return true;
}

/*
 * grouping_is_hashable - is it possible to implement grouping list by hashing?
 *
 * We rely on the parser to have set the hashable flag correctly.
 */
bool grouping_is_hashable(List* groupClause)
{
    ListCell* glitem = NULL;

    foreach (glitem, groupClause) {
        SortGroupClause* groupcl = (SortGroupClause*)lfirst(glitem);

        if (!groupcl->hashable)
            return false;
    }
    return true;
}

/*
 * grouping_is_distributable
 *		if any in groupClause is distributable, return true, else false
 */
bool grouping_is_distributable(List* groupClause, List* targetlist)
{
    ListCell* glitem = NULL;

    foreach (glitem, groupClause) {
        Node* expr = (Node*)lfirst(glitem);
        if (IsA(expr, SortGroupClause)) {
            expr = get_sortgroupclause_expr((SortGroupClause*)expr, targetlist);
        }

        Oid datatype = exprType((Node*)(expr));
        if (OidIsValid(datatype) && IsTypeDistributable(datatype)) {
            return true;
        }
    }

    return false;
}

/*
 * get_grouping_column_index
 *		Get the GROUP BY column position, if any, of a targetlist entry.
 *
 * Returns the index (counting from 0) of the TLE in the GROUP BY list, or -1
 * if it's not a grouping column.  Note: the result is unique because the
 * parser won't make multiple groupClause entries for the same TLE.
 */
int get_grouping_column_index(Query* parse, TargetEntry* tle, List* groupClause)
{
    int colno = 0;
    Index ressortgroupref = tle->ressortgroupref;
    ListCell* gl = NULL;

    /* No need to search groupClause if TLE hasn't got a sortgroupref */
    if (ressortgroupref == 0)
        return -1;

    foreach (gl, groupClause) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(gl);

        if (grpcl->tleSortGroupRef == ressortgroupref)
            return colno;
        colno++;
    }

    return -1;
}

/*
 * For the query with agg, pull out all the group by TargetEntry and aggref exprs
 */
List* make_agg_var_list(PlannerInfo* root, List* tlist, List** duplicate_tlist)
{
    Query* parse = root->parse;
    List* sub_tlist = NIL;
    List* non_group_cols = NIL;
    List* non_group_vars = NIL;
    ListCell* lc = NULL;

    /* TargetEntry(s) which are used in group clauses */
    sub_tlist = NIL;
    /* expr field of TargetEntry(s) which are not used in group clauses */
    non_group_cols = NIL;

    get_tlist_group_vars_split(parse, tlist, &sub_tlist, &non_group_cols);

    /*
     * Pull out all the Vars mentioned in non-group cols (plus HAVING), and
     * add them to the result tlist if not already present.  (A Var used
     * directly as a GROUP BY item will be present already.)  Note this
     * includes Vars used in resjunk items, so we are covering the needs of
     * ORDER BY and window specifications.	Vars used within Aggrefs will be
     * pulled out here, too.
     */
    non_group_vars = pull_var_clause(
        (Node*)non_group_cols, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR, true);

    if (parse->constraintDeps != NIL)
        sub_tlist = list_concat(sub_tlist, get_dependency_var_list(parse, sub_tlist, non_group_vars));

    /*
     * find all Aggref and Var from TargetEntry used in group clauses
     */
    foreach (lc, non_group_vars) {
        Node* non_group_var = (Node*)lfirst(lc);
        if (IsA(non_group_var, Var)) {
            if (((Var *) non_group_var)->varlevelsup != 0) {
                continue;
            }

            Index   varno = ((Var *) non_group_var)->varno;
            RangeTblEntry *tbl = (RangeTblEntry *) list_nth(root->parse->rtable, (int)(varno - 1));
            if (!tbl->sublink_pull_up)
                continue;
        }

        ListCell* target_lc = NULL;
        Expr* expr = NULL;
        foreach (target_lc, sub_tlist) {
            Node* node = (Node*)lfirst(target_lc);
            if (IsA(node, TargetEntry)) {
                expr = ((TargetEntry*)node)->expr;
            } else {
                expr = (Expr*)node;
            }
            if (equal(expr, lfirst(lc)))
                break;
        }
        /* This expr not exists in sub_tlist */
        if (target_lc == NULL) {
            sub_tlist = lappend(sub_tlist, lfirst(lc));
        } else if (duplicate_tlist != NULL)
            *duplicate_tlist = lappend(*duplicate_tlist, lfirst(lc));
    }
    list_free_ext(non_group_cols);
    list_free_ext(non_group_vars);

    return sub_tlist;
}

/*
 * Split the targetlist into group by cols and non-group by cols
 *	@in parse: parse tree.
 *	@in tlist: targetlist.
 *	@out group_cols: list that contains group by cols
 *	@out non_group_cols: list that contains non-group by cols
 */
void get_tlist_group_vars_split(Query* parse, List* tlist, List** group_cols, List** non_group_cols)
{
    int numCols = list_length(parse->groupClause);
    if (numCols > 0) {
        ListCell* tl = NULL;
        foreach (tl, tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(tl);
            int colno;

            colno = get_grouping_column_index(parse, tle, parse->groupClause);
            if (colno >= 0)
                *group_cols = lappend(*group_cols, tle);
            else
                *non_group_cols = lappend(*non_group_cols, tle->expr);
        }
    } else {
        /*
         * With no grouping columns, just pass whole tlist to non_group_cols
         */
        *non_group_cols = list_copy(tlist);
    }

    /*
     * If there's a HAVING clause, we'll need the Vars it uses, too.
     */
    if (parse->havingQual)
        *non_group_cols = lappend(*non_group_cols, parse->havingQual);
}

/*
 * Get var list that comes from rels with dependency columns
 *	@in parse: parse tree.
 *	@in group_cols: list that contains group by cols
 *	@in non_group_vars: list that contains non-group by cols (vars and aggs)
 */
List* get_dependency_var_list(Query* parse, List* group_cols, List* non_group_vars)
{
    ListCell* lc = NULL;
    List* dep_oids = get_parse_dependency_rel_list(parse->constraintDeps);
    List* result = NIL;

    foreach (lc, non_group_vars) {
        /* we only care about Aggref(s) */
        Node* non_group_var = (Node*)lfirst(lc);
        if (IsA(non_group_var, Var) && ((Var*)non_group_var)->varlevelsup == 0) {
            /*
             * If Vars from non-group-clause TargetEntry are not exists in group-clauses,
             *     we should add it to target list
             *
             * eg. CREATE TABLE t (pk int, b int, c int, d int);
             *     ALTER TABLE t ADD PRIMARY KEY (pk);
             *     SELECT pk, b, (c * sum(d))
             *     FROM t
             *     GROUP BY pk;
             * We need to add 'b' and 'c' to target list
             */
            if (var_from_dependency_rel(parse, (Var*)non_group_var, dep_oids) &&
                !tlist_member(non_group_var, group_cols)) {
                result = list_append_unique(result, non_group_var);
            }
        }
    }
    list_free_ext(dep_oids);

    return result;
}

/*
 * Check if var comes from rels that have dependency columns
 *	@in parse: parse tree.
 *	@in var: input var.
 *	@in dep_oids: relids that have dependency columns
 */
bool var_from_dependency_rel(Query* parse, Var* var, List* dep_oids)
{
    Index varno = var->varno;
    RangeTblEntry* tbl = rt_fetch(varno, parse->rtable);

    return list_member_oid(dep_oids, tbl->relid);
}

/*
 * Check if var comes from sublink pulled up
 *  @in parse: the parse tree contained the vars.
 *  @in var: the vars need be checked.
 */
bool var_from_sublink_pulluped(Query *parse, Var *var)
{
    Index varno = var->varno;
    RangeTblEntry *tbl = rt_fetch((int)varno, parse->rtable);

    return tbl->sublink_pull_up;
}

/*
 * Check if var comes from subquery pulled up
 *  @in parse: the parse tree contained the vars.
 *  @in var: the vars need be checked.
 */
bool var_from_subquery_pulluped(Query *parse, Var *var)
{
    Index varno = var->varno;
    RangeTblEntry *tbl = rt_fetch((int)varno, parse->rtable);

    return tbl->pulled_from_subquery;
}


/*
 * apply_tlist_labeling
 *		Apply the TargetEntry labeling attributes of src_tlist to dest_tlist
 *
 * This is useful for reattaching column names etc to a plan's final output
 * targetlist.
 */
void apply_tlist_labeling(List *dest_tlist, List *src_tlist)
{
    ListCell *ld, *ls;

    Assert(list_length(dest_tlist) == list_length(src_tlist));
    forboth(ld, dest_tlist, ls, src_tlist)
    {
        TargetEntry *dest_tle = (TargetEntry *)lfirst(ld);
        TargetEntry *src_tle = (TargetEntry *)lfirst(ls);

        Assert(dest_tle->resno == src_tle->resno);
        dest_tle->resname = src_tle->resname;
        dest_tle->ressortgroupref = src_tle->ressortgroupref;
        dest_tle->resorigtbl = src_tle->resorigtbl;
        dest_tle->resorigcol = src_tle->resorigcol;
        dest_tle->resjunk = src_tle->resjunk;
    }
}

/*****************************************************************************
 *		PathTarget manipulation functions
 *
 * PathTarget is a somewhat stripped-down version of a full targetlist; it
 * omits all the TargetEntry decoration except (optionally) sortgroupref data,
 * and it adds evaluation cost and output data width info.
 *****************************************************************************/

/*
 * make_pathtarget_from_tlist
 *	  Construct a PathTarget equivalent to the given targetlist.
 *
 * This leaves the cost and width fields as zeroes.  Most callers will want
 * to use create_pathtarget(), so as to get those set.
 */
PathTarget *make_pathtarget_from_tlist(List *tlist)
{
    PathTarget *target = makeNode(PathTarget);
    int i;
    ListCell *lc;

    target->sortgrouprefs = (Index *)palloc(list_length(tlist) * sizeof(Index));

    i = 0;
    foreach (lc, tlist) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);

        target->exprs = lappend(target->exprs, tle->expr);
        target->sortgrouprefs[i] = tle->ressortgroupref;
        i++;
    }

    return target;
}

/*
 * make_tlist_from_pathtarget
 *	  Construct a targetlist from a PathTarget.
 */
List *make_tlist_from_pathtarget(PathTarget *target)
{
    List *tlist = NIL;
    int i;
    ListCell *lc;

    i = 0;
    foreach (lc, target->exprs) {
        Expr *expr = (Expr *)lfirst(lc);
        TargetEntry *tle;

        tle = makeTargetEntry(expr, i + 1, NULL, false);
        if (target->sortgrouprefs)
            tle->ressortgroupref = target->sortgrouprefs[i];
        tlist = lappend(tlist, tle);
        i++;
    }

    return tlist;
}

/*
 * create_empty_pathtarget
 *	  Create an empty (zero columns, zero cost) PathTarget.
 */
PathTarget *create_empty_pathtarget(void)
{
    /* This is easy, but we don't want callers to hard-wire this ... */
    return makeNode(PathTarget);
}

/*
 * add_column_to_pathtarget
 *		Append a target column to the PathTarget.
 *
 * As with make_pathtarget_from_tlist, we leave it to the caller to update
 * the cost and width fields.
 */
void add_column_to_pathtarget(PathTarget *target, Expr *expr, Index sortgroupref)
{
    /* Updating the exprs list is easy ... */
    target->exprs = lappend(target->exprs, expr);
    /* ... the sortgroupref data, a bit less so */
    if (target->sortgrouprefs) {
        int nexprs = list_length(target->exprs);

        /* This might look inefficient, but actually it's usually cheap */
        target->sortgrouprefs = (Index *)repalloc(target->sortgrouprefs, nexprs * sizeof(Index));
        target->sortgrouprefs[nexprs - 1] = sortgroupref;
    } else if (sortgroupref) {
        /* Adding sortgroupref labeling to a previously unlabeled target */
        int nexprs = list_length(target->exprs);

        target->sortgrouprefs = (Index *)palloc0(nexprs * sizeof(Index));
        target->sortgrouprefs[nexprs - 1] = sortgroupref;
    }
}

/*
 * add_new_column_to_pathtarget
 *		Append a target column to the PathTarget, but only if it's not
 *		equal() to any pre-existing target expression.
 *
 * The caller cannot specify a sortgroupref, since it would be unclear how
 * to merge that with a pre-existing column.
 *
 * As with make_pathtarget_from_tlist, we leave it to the caller to update
 * the cost and width fields.
 */
void add_new_column_to_pathtarget(PathTarget *target, Expr *expr)
{
    if (!list_member(target->exprs, expr))
        add_column_to_pathtarget(target, expr, 0);
}

/*
 * add_new_columns_to_pathtarget
 *		Apply add_new_column_to_pathtarget() for each element of the list.
 */
void add_new_columns_to_pathtarget(PathTarget *target, List *exprs)
{
    ListCell *lc;

    foreach (lc, exprs) {
        Expr *expr = (Expr *)lfirst(lc);

        add_new_column_to_pathtarget(target, expr);
    }
}

/*
 * split_pathtarget_at_srfs
 *		Split given PathTarget into multiple levels to position SRFs safely
 *
 * The executor can only handle set-returning functions that appear at the
 * top level of the targetlist of a ProjectSet plan node.  If we have any SRFs
 * that are not at top level, we need to split up the evaluation into multiple
 * plan levels in which each level satisfies this constraint.  This function
 * creates appropriate PathTarget(s) for each level.
 *
 * As an example, consider the tlist expression
 *		x + srf1(srf2(y + z))
 * This expression should appear as-is in the top PathTarget, but below that
 * we must have a PathTarget containing
 *		x, srf1(srf2(y + z))
 * and below that, another PathTarget containing
 *		x, srf2(y + z)
 * and below that, another PathTarget containing
 *		x, y, z
 * When these tlists are processed by setrefs.c, subexpressions that match
 * output expressions of the next lower tlist will be replaced by Vars,
 * so that what the executor gets are tlists looking like
 *		Var1 + Var2
 *		Var1, srf1(Var2)
 *		Var1, srf2(Var2 + Var3)
 *		x, y, z
 * which satisfy the desired property.
 *
 * Another example is
 *		srf1(x), srf2(srf3(y))
 * That must appear as-is in the top PathTarget, but below that we need
 *		srf1(x), srf3(y)
 * That is, each SRF must be computed at a level corresponding to the nesting
 * depth of SRFs within its arguments.
 *
 * In some cases, a SRF has already been evaluated in some previous plan level
 * and we shouldn't expand it again (that is, what we see in the target is
 * already meant as a reference to a lower subexpression).  So, don't expand
 * any tlist expressions that appear in input_target, if that's not NULL.
 *
 * It's also important that we preserve any sortgroupref annotation appearing
 * in the given target, especially on expressions matching input_target items.
 *
 * The outputs of this function are two parallel lists, one a list of
 * PathTargets and the other an integer list of bool flags indicating
 * whether the corresponding PathTarget contains any evaluatable SRFs.
 * The lists are given in the order they'd need to be evaluated in, with
 * the "lowest" PathTarget first.  So the last list entry is always the
 * originally given PathTarget, and any entries before it indicate evaluation
 * levels that must be inserted below it.  The first list entry must not
 * contain any SRFs (other than ones duplicating input_target entries), since
 * it will typically be attached to a plan node that cannot evaluate SRFs.
 *
 * Note: using a list for the flags may seem like overkill, since there
 * are only a few possible patterns for which levels contain SRFs.
 * But this representation decouples callers from that knowledge.
 */
bool 
split_pathtarget_at_srfs(PlannerInfo *root, PathTarget *target, PathTarget *input_target,
                        List **targets, List **targets_contain_srfs)
{
    split_pathtarget_context context;
    int max_depth;
    bool need_extra_projection;
    List *prev_level_tlist;
    int lci;
    ListCell *lc, *lc1, *lc2, *lc3;
    bool contains_srfs = false;

    /*
     * It's not unusual for planner.c to pass us two physically identical
     * targets, in which case we can conclude without further ado that all
     * expressions are available from the input.  (The logic below would
     * arrive at the same conclusion, but much more tediously.)
     */
    if (target == input_target) {
        *targets = list_make1(target);
        *targets_contain_srfs = list_make1_int(false);
        return contains_srfs;
    }

    /* Pass any input_target exprs down to split_pathtarget_walker() */
    context.input_target_exprs = input_target ? input_target->exprs : NIL;

    /*
     * Initialize with empty level-zero lists, and no levels after that.
     * (Note: we could dispense with representing level zero explicitly, since
     * it will never receive any SRFs, but then we'd have to special-case that
     * level when we get to building result PathTargets.  Level zero describes
     * the SRF-free PathTarget that will be given to the input plan node.)
     */
    context.level_srfs = list_make1(NIL);
    context.level_input_vars = list_make1(NIL);
    context.level_input_srfs = list_make1(NIL);

    /* Initialize data we'll accumulate across all the target expressions */
    context.current_input_vars = NIL;
    context.current_input_srfs = NIL;
    max_depth = 0;
    need_extra_projection = false;

    /* Scan each expression in the PathTarget looking for SRFs */
    lci = 0;
    foreach (lc, target->exprs) {
        Node *node = (Node *)lfirst(lc);

        /* Tell split_pathtarget_walker about this expr's sortgroupref */
        context.current_sgref = get_pathtarget_sortgroupref(target, lci);
        lci++;

        /*
         * Find all SRFs and Vars (and Var-like nodes) in this expression, and
         * enter them into appropriate lists within the context struct.
         */
        context.current_depth = 0;
        split_pathtarget_walker(node, &context);

        /* An expression containing no SRFs is of no further interest */
        if (context.current_depth == 0)
            continue;

        /*
         * Track max SRF nesting depth over the whole PathTarget.  Also, if
         * this expression establishes a new max depth, we no longer care
         * whether previous expressions contained nested SRFs; we can handle
         * any required projection for them in the final ProjectSet node.
         */
        if (max_depth < context.current_depth) {
            max_depth = context.current_depth;
            need_extra_projection = false;
        }

        /*
         * If any maximum-depth SRF is not at the top level of its expression,
         * we'll need an extra Result node to compute the top-level scalar
         * expression.
         */
        if (max_depth == context.current_depth && !IS_SRF_CALL(node))
            need_extra_projection = true;
    }

    /*
     * If we found no SRFs needing evaluation (maybe they were all present in
     * input_target, or maybe they were all removed by const-simplification),
     * then no ProjectSet is needed; fall out.
     */
    if (max_depth == 0) {
        *targets = list_make1(target);
        *targets_contain_srfs = list_make1_int(false);
        return contains_srfs;
    }

    /*
     * The Vars and SRF outputs needed at top level can be added to the last
     * level_input lists if we don't need an extra projection step.  If we do
     * need one, add a SRF-free level to the lists.
     */
    if (need_extra_projection) {
        context.level_srfs = lappend(context.level_srfs, NIL);
        context.level_input_vars = lappend(context.level_input_vars, context.current_input_vars);
        context.level_input_srfs = lappend(context.level_input_srfs, context.current_input_srfs);
    } else {
        lc = list_nth_cell(context.level_input_vars, max_depth);
        lfirst(lc) = list_concat((List*)lfirst(lc), context.current_input_vars);
        lc = list_nth_cell(context.level_input_srfs, max_depth);
        lfirst(lc) = list_concat((List*)lfirst(lc), context.current_input_srfs);
    }

    /*
     * Now construct the output PathTargets.  The original target can be used
     * as-is for the last one, but we need to construct a new SRF-free target
     * representing what the preceding plan node has to emit, as well as a
     * target for each intermediate ProjectSet node.
     */
    *targets = *targets_contain_srfs = NIL;
    prev_level_tlist = NIL;

    forthree(lc1, context.level_srfs, lc2, context.level_input_vars, lc3, context.level_input_srfs)
    {
        List *level_srfs = (List *)lfirst(lc1);
        PathTarget *ntarget;

        if (lnext(lc1) == NULL) {
            ntarget = target;
        } else {
            ntarget = create_empty_pathtarget();

            /*
             * This target should actually evaluate any SRFs of the current
             * level, and it needs to propagate forward any Vars needed by
             * later levels, as well as SRFs computed earlier and needed by
             * later levels.
             */
            add_sp_items_to_pathtarget(ntarget, level_srfs);
            for_each_cell(lc, lnext(lc2))
            {
                List *input_vars = (List *)lfirst(lc);

                add_sp_items_to_pathtarget(ntarget, input_vars);
            }
            for_each_cell(lc, lnext(lc3))
            {
                List *input_srfs = (List *)lfirst(lc);
                ListCell *lcx;

                foreach (lcx, input_srfs) {
                    split_pathtarget_item *item = (split_pathtarget_item*)lfirst(lcx);

                    if (list_member(prev_level_tlist, item->expr))
                        add_sp_item_to_pathtarget(ntarget, item);
                }
            }
            set_pathtarget_cost_width(root, ntarget);
        }

        /*
         * Add current target and does-it-compute-SRFs flag to output lists.
         */
        *targets = lappend(*targets, ntarget);
        *targets_contain_srfs = lappend_int(*targets_contain_srfs, (level_srfs != NIL));
        if (level_srfs != NIL) {
            contains_srfs = true;
        }

        /* Remember this level's output for next pass */
        prev_level_tlist = ntarget->exprs;
    }
    return contains_srfs;
}

/* Recursively examine expressions for split_pathtarget_at_srfs */
static bool
split_pathtarget_walker(Node *node, split_pathtarget_context *context)
{
    if (node == NULL)
        return false;

    /*
     * A subexpression that matches an expression already computed in
     * input_target can be treated like a Var (which indeed it will be after
     * setrefs.c gets done with it), even if it's actually a SRF.  Record it
     * as being needed for the current expression, and ignore any
     * substructure.  (Note in particular that this preserves the identity of
     * any expressions that appear as sortgrouprefs in input_target.)
     */
    if (list_member(context->input_target_exprs, node)) {
        split_pathtarget_item *item = (split_pathtarget_item*)palloc(sizeof(split_pathtarget_item));

        item->expr = node;
        item->sortgroupref = context->current_sgref;
        context->current_input_vars = lappend(context->current_input_vars, item);
        return false;
    }

    /*
     * Vars and Var-like constructs are expected to be gotten from the input,
     * too.  We assume that these constructs cannot contain any SRFs (if one
     * does, there will be an executor failure from a misplaced SRF).
     */
    if (IsA(node, Var) 
        || IsA(node, PlaceHolderVar) 
        || IsA(node, Aggref) 
        || IsA(node, GroupingFunc) 
        || IsA(node, WindowFunc)) {
        split_pathtarget_item *item = (split_pathtarget_item*)palloc(sizeof(split_pathtarget_item));

        item->expr = node;
        item->sortgroupref = context->current_sgref;
        context->current_input_vars = lappend(context->current_input_vars, item);
        return false;
    }

    /*
     * If it's a SRF, recursively examine its inputs, determine its level, and
     * make appropriate entries in the output lists.
     */
    if (IS_SRF_CALL(node)) {
        split_pathtarget_item *item = (split_pathtarget_item*)palloc(sizeof(split_pathtarget_item));
        List *save_input_vars = context->current_input_vars;
        List *save_input_srfs = context->current_input_srfs;
        int save_current_depth = context->current_depth;
        int srf_depth;
        ListCell *lc;

        item->expr = node;
        item->sortgroupref = context->current_sgref;

        context->current_input_vars = NIL;
        context->current_input_srfs = NIL;
        context->current_depth = 0;
        context->current_sgref = 0; /* subexpressions are not sortgroup items */

        (void)expression_tree_walker(node, (bool (*)())split_pathtarget_walker, (void *)context);

        /* Depth is one more than any SRF below it */
        srf_depth = context->current_depth + 1;

        /* If new record depth, initialize another level of output lists */
        if (srf_depth >= list_length(context->level_srfs)) {
            context->level_srfs = lappend(context->level_srfs, NIL);
            context->level_input_vars = lappend(context->level_input_vars, NIL);
            context->level_input_srfs = lappend(context->level_input_srfs, NIL);
        }

        /* Record this SRF as needing to be evaluated at appropriate level */
        lc = list_nth_cell(context->level_srfs, srf_depth);
        lfirst(lc) = lappend((List*)lfirst(lc), item);

        /* Record its inputs as being needed at the same level */
        lc = list_nth_cell(context->level_input_vars, srf_depth);
        lfirst(lc) = list_concat((List*)lfirst(lc), context->current_input_vars);
        lc = list_nth_cell(context->level_input_srfs, srf_depth);
        lfirst(lc) = list_concat((List*)lfirst(lc), context->current_input_srfs);

        /*
         * Restore caller-level state and update it for presence of this SRF.
         * Notice we report the SRF itself as being needed for evaluation of
         * surrounding expression.
         */
        context->current_input_vars = save_input_vars;
        context->current_input_srfs = lappend(save_input_srfs, item);
        context->current_depth = Max(save_current_depth, srf_depth);

        /* We're done here */
        return false;
    }

    /*
     * Otherwise, the node is a scalar (non-set) expression, so recurse to
     * examine its inputs.
     */
    context->current_sgref = 0; /* subexpressions are not sortgroup items */
    return expression_tree_walker(node, (bool (*)())split_pathtarget_walker, (void *)context);
}

/*
 * Add a split_pathtarget_item to the PathTarget, unless a matching item is
 * already present.  This is like add_new_column_to_pathtarget, but allows
 * for sortgrouprefs to be handled.  An item having zero sortgroupref can
 * be merged with one that has a sortgroupref, acquiring the latter's
 * sortgroupref.
 *
 * Note that we don't worry about possibly adding duplicate sortgrouprefs
 * to the PathTarget.  That would be bad, but it should be impossible unless
 * the target passed to split_pathtarget_at_srfs already had duplicates.
 * As long as it didn't, we can have at most one split_pathtarget_item with
 * any particular nonzero sortgroupref.
 */
static void 
add_sp_item_to_pathtarget(PathTarget *target, split_pathtarget_item *item)
{
    int lci;
    ListCell *lc;

    /*
     * Look for a pre-existing entry that is equal() and does not have a
     * conflicting sortgroupref already.
     */
    lci = 0;
    foreach (lc, target->exprs) {
        Node *node = (Node *)lfirst(lc);
        Index sgref = get_pathtarget_sortgroupref(target, lci);

        if ((item->sortgroupref == sgref || item->sortgroupref == 0 || sgref == 0) 
            && equal(item->expr, node)) {
            /* Found a match.  Assign item's sortgroupref if it has one. */
            if (item->sortgroupref) {
                if (target->sortgrouprefs == NULL) {
                    target->sortgrouprefs = (Index *)palloc0(list_length(target->exprs) * sizeof(Index));
                }
                target->sortgrouprefs[lci] = item->sortgroupref;
            }
            return;
        }
        lci++;
    }

    /*
     * No match, so add item to PathTarget.  Copy the expr for safety.
     */
    add_column_to_pathtarget(target, (Expr *)copyObject(item->expr), item->sortgroupref);
}

/*
 * Apply add_sp_item_to_pathtarget to each element of list.
 */
static void 
add_sp_items_to_pathtarget(PathTarget *target, List *items)
{
    ListCell *lc;

    foreach (lc, items) {
        split_pathtarget_item *item = (split_pathtarget_item*)lfirst(lc);

        add_sp_item_to_pathtarget(target, item);
    }
}

#ifdef USE_SPQ
typedef struct maxSortGroupRef_context {
    Index maxsgr;
    bool include_orderedagg;
} maxSortGroupRef_context;
/*
 * tlist_members
 *	  Finds all members of the given tlist whose expression is
 *	  equal() to the given expression.	Result is NIL if no such member.
 *	  Note: We do not make a copy of the tlist entries that match.
 *	  The caller is responsible for cleaning up the memory allocated
 *	  to the List returned.
 */
List* tlist_members(Node *node, List *targetlist)
{
    List *tlist = NIL;
    ListCell *temp = NULL;
 
    foreach (temp, targetlist) {
        TargetEntry *tlentry = (TargetEntry *) lfirst(temp);
 
        Assert(IsA(tlentry, TargetEntry));
 
        if (equal(node, tlentry->expr)) {
            tlist = lappend(tlist, tlentry);
        }
    }
 
    return tlist;
}
 
static void get_sortgroupclauses_tles_recurse(List *clauses, List *targetList, List **tles, List **sortops,
    List **eqops)
{
    ListCell *lc;
    ListCell *lc_sortop;
    ListCell *lc_eqop;
    List *sub_grouping_tles = NIL;
    List *sub_grouping_sortops = NIL;
    List *sub_grouping_eqops = NIL;
 
    foreach (lc, clauses) {
        Node *node = (Node *)lfirst(lc);
 
        if (node == NULL)
            continue;
 
        if (IsA(node, SortGroupClause)) {
            SortGroupClause *sgc = (SortGroupClause *)node;
            TargetEntry *tle = get_sortgroupclause_tle(sgc, targetList);
 
            if (!list_member(*tles, tle)) {
                *tles = lappend(*tles, tle);
                *sortops = lappend_oid(*sortops, sgc->sortop);
                *eqops = lappend_oid(*eqops, sgc->eqop);
            }
        } else if (IsA(node, List)) {
            get_sortgroupclauses_tles_recurse((List *)node, targetList, tles, sortops, eqops);
        } else
            elog(ERROR, "unrecognized node type in list of sort/group clauses: %d", (int)nodeTag(node));
    }
 
    /*
     * Put SortGroupClauses before GroupingClauses.
     */
    forthree(lc, sub_grouping_tles, lc_sortop, sub_grouping_sortops, lc_eqop, sub_grouping_eqops)
    {
        if (!list_member(*tles, lfirst(lc))) {
            *tles = lappend(*tles, lfirst(lc));
            *sortops = lappend_oid(*sortops, lfirst_oid(lc_sortop));
            *eqops = lappend_oid(*eqops, lfirst_oid(lc_eqop));
        }
    }
}
 
void get_sortgroupclauses_tles(List *clauses, List *targetList, List **tles, List **sortops, List **eqops)
{
    *tles = NIL;
    *sortops = NIL;
    *eqops = NIL;
 
    get_sortgroupclauses_tles_recurse(clauses, targetList, tles, sortops, eqops);
}
 
bool maxSortGroupRef_walker(Node *node, maxSortGroupRef_context *cxt)
{
    if (node == NULL)
        return false;
 
    if (IsA(node, TargetEntry)) {
        TargetEntry *tle = (TargetEntry *)node;
        if (tle->ressortgroupref > cxt->maxsgr)
            cxt->maxsgr = tle->ressortgroupref;
 
        return maxSortGroupRef_walker((Node *)tle->expr, cxt);
    }
 
    /* Aggref nodes don't nest, so we can treat them here without recurring
     * further.
     */
 
    if (IsA(node, Aggref)) {
        Aggref *ref = (Aggref *)node;
 
        if (cxt->include_orderedagg) {
            ListCell *lc;
 
            foreach (lc, ref->aggorder) {
                SortGroupClause *sort = (SortGroupClause *)lfirst(lc);
                Assert(IsA(sort, SortGroupClause));
                Assert(sort->tleSortGroupRef != 0);
                if (sort->tleSortGroupRef > cxt->maxsgr)
                    cxt->maxsgr = sort->tleSortGroupRef;
            }
        }
        return false;
    }
 
    return expression_tree_walker(node, (bool (*)())maxSortGroupRef_walker, cxt);
}
 
/*
 * Return the largest sortgroupref value in use in the given
 * target list.
 *
 * If include_orderedagg is false, consider only the top-level
 * entries in the target list, i.e., those that might be occur
 * in a groupClause, distinctClause, or sortClause of the Query
 * node that immediately contains the target list.
 *
 * If include_orderedagg is true, also consider AggOrder entries
 * embedded in Aggref nodes within the target list.  Though
 * such entries will only occur in the aggregation sub_tlist
 * (input) they affect sortgroupref numbering for both sub_tlist
 * and tlist (aggregate).
 */
Index maxSortGroupRef(List *targetlist, bool include_orderedagg)
{
    maxSortGroupRef_context context;
    context.maxsgr = 0;
    context.include_orderedagg = include_orderedagg;
 
    if (targetlist != NIL) {
        if (!IsA(targetlist, List) || !IsA(linitial(targetlist), TargetEntry))
            elog(ERROR, "non-targetlist argument supplied");
 
        maxSortGroupRef_walker((Node *)targetlist, &context);
    }
 
    return context.maxsgr;
}

/*
 * get_sortgroupref_tle_spq
 *             Find the targetlist entry matching the given SortGroupRef index,
 *             and return it.
 */
TargetEntry* get_sortgroupref_tle_spq(Index sortref, List* targetList, bool report_error)
{
    ListCell* l = NULL;

    foreach (l, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (IS_SPQ_COORDINATOR && tle->resno == sortref && tle->ressortgroupref == 0) {
            return tle;
        }
    }

    if (report_error)
        ereport(ERROR,
                (errmodule(MOD_OPT),
                 errcode(ERRCODE_GROUPING_ERROR),
                 errmsg("ORDER/GROUP BY expression not found in targetlist")));
    return NULL;
}
#endif
