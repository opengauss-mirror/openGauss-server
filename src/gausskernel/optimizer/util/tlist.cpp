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
