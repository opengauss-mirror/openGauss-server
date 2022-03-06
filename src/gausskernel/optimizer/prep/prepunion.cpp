/* -------------------------------------------------------------------------
 *
 * prepunion.cpp
 *	  Routines to plan set-operation queries.  The filename is a leftover
 *	  from a time when only UNIONs were implemented.
 *
 * There are two code paths in the planner for set-operation queries.
 * If a subquery consists entirely of simple UNION ALL operations, it
 * is converted into an "append relation".	Otherwise, it is handled
 * by the general code in this module (plan_set_operations and its
 * subroutines).  There is some support code here for the append-relation
 * case, but most of the heavy lifting for that is done elsewhere,
 * notably in prepjointree.c and allpaths.c.
 *
 * There is also some code here to support planning of queries that use
 * inheritance (SELECT FROM foo*).	Inheritance trees are converted into
 * append relations, and thenceforth share code with the UNION ALL case.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/prep/prepunion.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_type.h"
#include "executor/node/nodeRecursiveunion.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/selfuncs.h"

/* the length of distinct groups */
#define DISTINCT_GROUPS_LEN    2
typedef struct {
    PlannerInfo* root;
    AppendRelInfo* appinfo;
} adjust_appendrel_attrs_context;

static Plan* recurse_set_operations(Node* setOp, PlannerInfo* root, double tuple_fraction, List* colTypes,
    List* colCollations, bool junkOK, int flag, List* refnames_tlist, List** sortClauses, double* pNumGroups);
static Plan* generate_recursion_plan(
    SetOperationStmt* setOp, PlannerInfo* root, double tuple_fraction, List* refnames_tlist, List** sortClauses);
static Plan* generate_union_plan(SetOperationStmt* op, PlannerInfo* root, double tuple_fraction, List* refnames_tlist,
    List** sortClauses, double* pNumGroups);
static Plan* generate_nonunion_plan(SetOperationStmt* op, PlannerInfo* root, double tuple_fraction,
    List* refnames_tlist, List** sortClauses, double* pNumGroups);
static List* recurse_union_children(
    Node* setOp, PlannerInfo* root, double tuple_fraction, SetOperationStmt* top_union, List* refnames_tlist);
static Plan* make_union_unique(
    SetOperationStmt* op, Plan* plan, PlannerInfo* root, double tuple_fraction, List** sortClauses);
static bool choose_hashed_setop(PlannerInfo* root, List* groupClauses, Plan* input_plan, double dNumGroups,
    double dNumOutputRows, double tuple_fraction, const char* construct, OpMemInfo* mem_info = NULL);
static List* generate_setop_tlist(List* colTypes, List* colCollations, int flag, Index varno, bool hack_constants,
    List* input_tlist, List* refnames_tlist);
static List* generate_append_tlist(
    List* colTypes, List* colCollations, bool flag, List* input_plans, List* refnames_tlist);
static List* generate_setop_grouplist(SetOperationStmt* op, List* targetlist);
static void expand_inherited_rtentry(PlannerInfo* root, RangeTblEntry* rte, Index rti);
static Node* adjust_appendrel_attrs_mutator(Node* node, adjust_appendrel_attrs_context* context);
static Relids adjust_relid_set(Relids relids, Index oldrelid, Index newrelid);
static List* adjust_inherited_tlist(List* tlist, AppendRelInfo* context);
static void parallel_setop(PlannerInfo* root, Plan* plan, bool isunionall);

/*
 * plan_set_operations
 *
 *	  Plans the queries for a tree of set operations (UNION/INTERSECT/EXCEPT)
 *
 * This routine only deals with the setOperations tree of the given query.
 * Any top-level ORDER BY requested in root->parse->sortClause will be added
 * when we return to grouping_planner.
 *
 * tuple_fraction is the fraction of tuples we expect will be retrieved.
 * tuple_fraction is interpreted as for grouping_planner(); in particular,
 * zero means "all the tuples will be fetched".  Any LIMIT present at the
 * top level has already been factored into tuple_fraction.
 *
 * *sortClauses is an output argument: it is set to a list of SortGroupClauses
 * representing the result ordering of the topmost set operation.  (This will
 * be NIL if the output isn't ordered.)
 */
Plan* plan_set_operations(PlannerInfo* root, double tuple_fraction, List** sortClauses)
{
    Query* parse = root->parse;
    SetOperationStmt* topop = (SetOperationStmt*)parse->setOperations;
    Node* node = NULL;
    RangeTblEntry* leftmostRTE = NULL;
    Query* leftmostQuery = NULL;

    AssertEreport(topop != NULL && IsA(topop, SetOperationStmt),
        MOD_OPT,
        "setOperations should not be NULL in plan_set_operations");

    /* check for unsupported stuff */
    AssertEreport(parse->jointree->fromlist == NIL, MOD_OPT, "fromlist should be NULL in plan_set_operations");
    AssertEreport(parse->jointree->quals == NULL, MOD_OPT, "quals should be NULL in plan_set_operations");
    AssertEreport(parse->groupClause == NIL, MOD_OPT, "groupClause should be NULL in plan_set_operations");
    AssertEreport(parse->havingQual == NULL, MOD_OPT, "havingQual should be NULL in plan_set_operations");
    AssertEreport(parse->windowClause == NIL, MOD_OPT, "windowClause should be NULL in plan_set_operations");
    AssertEreport(parse->distinctClause == NIL, MOD_OPT, "distinctClause should be NULL in plan_set_operations");

    /*
     * We'll need to build RelOptInfos for each of the leaf subqueries, which
     * are RTE_SUBQUERY rangetable entries in this Query.  Prepare the index
     * arrays for that.
     */
    setup_simple_rel_arrays(root);

    /*
     * Find the leftmost component Query.  We need to use its column names for
     * all generated tlists (else SELECT INTO won't work right).
     */
    node = topop->larg;
    while (node && IsA(node, SetOperationStmt))
        node = ((SetOperationStmt*)node)->larg;
    if (node != NULL && IsA(node, RangeTblRef)) {
        leftmostRTE = root->simple_rte_array[((RangeTblRef*)node)->rtindex];
        leftmostQuery = leftmostRTE->subquery;
        AssertEreport(leftmostQuery != NULL, MOD_OPT, "leftmostQuery should not be NULL in plan_set_operations");
    } else
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), (errmsg("RangeTblRef not found."))));
    /*
     * If the topmost node is a recursive union, it needs special processing.
     */
    if (root->hasRecursion) {
        /*
         * For distributed with-recursive processing we currently disable the SMP
         */
        int saved_query_dop = u_sess->opt_cxt.query_dop;
        Plan* result = NULL;
        if (STREAM_RECURSIVECTE_SUPPORTED) {
            u_sess->opt_cxt.query_dop = 1;
        }
        result = generate_recursion_plan(topop, root, tuple_fraction, leftmostQuery->targetList, sortClauses);

        if (STREAM_RECURSIVECTE_SUPPORTED) {
            u_sess->opt_cxt.query_dop = saved_query_dop;
        }

        /* Assert the query_dop is correctly restored */
        Assert(u_sess->opt_cxt.query_dop == saved_query_dop);

        return result;
    }

    /*
     * Recurse on setOperations tree to generate plans for set ops. The final
     * output plan should have just the column types shown as the output from
     * the top-level node, plus possibly resjunk working columns (we can rely
     * on upper-level nodes to deal with that).
     */
    return recurse_set_operations((Node*)topop,
        root,
        tuple_fraction,
        topop->colTypes,
        topop->colCollations,
        true,
        -1,
        leftmostQuery->targetList,
        sortClauses,
        NULL);
}

/*
 * recurse_set_operations
 *	  Recursively handle one step in a tree of set operations
 *
 * tuple_fraction: fraction of tuples we expect to retrieve from node
 * colTypes: OID list of set-op's result column datatypes
 * colCollations: OID list of set-op's result column collations
 * junkOK: if true, child resjunk columns may be left in the result
 * flag: if >= 0, add a resjunk output column indicating value of flag
 * refnames_tlist: targetlist to take column names from
 *
 * Returns a plan for the subtree, as well as these output parameters:
 * *sortClauses: receives list of SortGroupClauses for result plan, if any
 * *pNumGroups: if not NULL, we estimate the number of distinct groups
 *		in the result, and store it there
 *
 * We don't have to care about typmods here: the only allowed difference
 * between set-op input and output typmods is input is a specific typmod
 * and output is -1, and that does not require a coercion.
 */
static Plan* recurse_set_operations(Node* setOp, PlannerInfo* root, double tuple_fraction, List* colTypes,
    List* colCollations, bool junkOK, int flag, List* refnames_tlist, List** sortClauses, double* pNumGroups)
{
    /* Guard against stack overflow due to overly complex setop nests */
    check_stack_depth();

    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        RangeTblEntry* rte = root->simple_rte_array[rtr->rtindex];
        Query* subquery = rte->subquery;
        RelOptInfo* rel = NULL;
        PlannerInfo* subroot = NULL;
        List* targetlist = NIL;
        Plan* subplan = NULL;
        Plan* plan = NULL;

        bool hack_constants = true;

        AssertEreport(subquery != NULL, MOD_OPT, "subquery should not be NULL in recurse_set_operations");

        /*
         * We need to build a RelOptInfo for each leaf subquery.  This isn't
         * used for anything here, but it carries the subroot data structures
         * forward to setrefs.c processing.
         */
        rel = build_simple_rel(root, rtr->rtindex, RELOPT_BASEREL);
        if (rel == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), (errmsg("Valid rel not found. "))));

        /* plan_params should not be in use in current query level */
        AssertEreport(root->plan_params == NIL,
            MOD_OPT,
            "plan_params should not be in use in current query level in recurse_set_operations");

        /*
         * Generate plan for primitive subquery
         */
        subplan = subquery_planner(root->glob, subquery, root, false, tuple_fraction, &subroot, root->subquery_type, &root->dis_keys);

        /* Save subroot and subplan in RelOptInfo for setrefs.c */
        rel->subplan = subplan;
        rel->subroot = subroot;

        /*
         * It should not be possible for the primitive query to contain any
         * cross-references to other primitive queries in the setop tree.
         */
        if (root->plan_params)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("unexpected outer reference in set operation subquery"))));

        /*
         * Estimate number of groups if caller wants it.  If the subquery used
         * grouping or aggregation, its output is probably mostly unique
         * anyway; otherwise do statistical estimation.
         */
        if (pNumGroups != NULL) {
            if (subquery->groupClause || subquery->distinctClause || subquery->groupingSets || subroot->hasHavingQual ||
                subquery->hasAggs) {
                pNumGroups[0] = PLAN_LOCAL_ROWS(subplan);
                pNumGroups[1] = subplan->plan_rows;
            } else {
                double numdistinct[DISTINCT_GROUPS_LEN] = {1, 1};
                get_num_distinct(subroot,
                    get_tlist_exprs(subquery->targetList, false),
                    PLAN_LOCAL_ROWS(subplan),
                    subplan->plan_rows,
                    ng_get_dest_num_data_nodes(subplan),
                    numdistinct);
                pNumGroups[0] = numdistinct[0];
                pNumGroups[1] = numdistinct[1];
            }
        }

        /*
         * If agg include groupingSets, can not hack const else can lead to result error,
         * because this const can be set to NULL after groupingSet.
         */
        if (subquery->groupingSets) {
            hack_constants = false;
        }

        /*
         * Add a SubqueryScan with the caller-requested targetlist
         */
        targetlist = generate_setop_tlist(
            colTypes, colCollations, flag, rtr->rtindex, hack_constants, subplan->targetlist, refnames_tlist);
#ifdef STREAMPLAN
        if (IS_STREAM_PLAN && is_execute_on_datanodes(subplan) && is_hashed_plan(subplan)) {
            /* Get distribute key index from subplan */
            List* distribute_index = distributeKeyIndex(rel->subroot, subplan->distributed_keys, subplan->targetlist);
            if (distribute_index == NIL) {
                rel->distribute_keys = NIL;
                rel->rangelistOid = InvalidOid;
            } else {
                ListCell* lc = NULL;
                /* Find each distribute key for new subquery */
                foreach (lc, distribute_index) {
                    int resno = lfirst_int(lc);
                    ListCell* lc2 = NULL;
                    Var* var = NULL;

                    /*
                     * Find from subquery targetlist for distribute key. We should traverse
                     * the targetlist and get the real var, because targetlist of subquery can
                     * be a subset of subplan's targetlist, and there can be type cast on base
                     * vars
                     */
                    foreach (lc2, targetlist) {
                        TargetEntry* tle = (TargetEntry*)lfirst(lc2);
                        var = locate_distribute_var(tle->expr);
                        if (var != NULL && var->varattno == resno)
                            break;
                    }
                    if (lc2 != NULL) {
                        rel->distribute_keys = lappend(rel->distribute_keys, var);
                    } else {
                        list_free_ext(rel->distribute_keys);
                        rel->distribute_keys = NIL;
                        /* If this distribute keys can not found, rel's distribute_keys should be null. */
                        break;
                    }
                }
            }
            rel->locator_type = get_locator_type(subplan);
        }
#endif
        plan = (Plan*)make_subqueryscan(targetlist, NIL, rtr->rtindex, rel->subplan);
#ifdef STREAMPLAN
        plan->distributed_keys = rel->distribute_keys;
#endif

        root->param_upper = bms_union(root->param_upper, subroot->param_upper);

        /*
         * We don't bother to determine the subquery's output ordering since
         * it won't be reflected in the set-op result anyhow.
         */
        *sortClauses = NIL;

        return plan;
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;
        Plan* plan = NULL;

        /* UNIONs are much different from INTERSECT/EXCEPT */
        if (op->op == SETOP_UNION)
            plan = generate_union_plan(op, root, tuple_fraction, refnames_tlist, sortClauses, pNumGroups);
        else
            plan = generate_nonunion_plan(op, root, tuple_fraction, refnames_tlist, sortClauses, pNumGroups);

        /*
         * If necessary, add a Result node to project the caller-requested
         * output columns.
         *
         * XXX you don't really want to know about this: setrefs.c will apply
         * fix_upper_expr() to the Result node's tlist. This would fail if the
         * Vars generated by generate_setop_tlist() were not exactly equal()
         * to the corresponding tlist entries of the subplan. However, since
         * the subplan was generated by generate_union_plan() or
         * generate_nonunion_plan(), and hence its tlist was generated by
         * generate_append_tlist(), this will work.  We just tell
         * generate_setop_tlist() to use varno 0.
         */
        if (flag >= 0 || !tlist_same_datatypes(plan->targetlist, colTypes, junkOK) ||
            !tlist_same_collations(plan->targetlist, colCollations, junkOK)) {
            plan = (Plan*)make_result(root,
                generate_setop_tlist(colTypes, colCollations, flag, 0, false, plan->targetlist, refnames_tlist),
                NULL,
                plan);
        }
        return plan;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(setOp))));
        return NULL; /* keep compiler quiet */
    }
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * contain system table entries.
 */
static bool rtable_contains_system_table(List* rtable)
{
    ListCell* item = NULL;

    /* May be complicated. Before giving up, just check for pg_catalog usage */
    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->rtekind == RTE_RELATION) {
            if (is_sys_table(rte->relid))
                return true;
        } else if (rte->rtekind == RTE_SUBQUERY && rtable_contains_system_table(rte->subquery->rtable))
            return true;
    }
    return false;
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * contain only RTE_VALUES entries.
 */
static bool rtable_contains_only_values_rte(List* rtable)
{
    ListCell* item = NULL;

    /* May be complicated. Before giving up, just check for pg_catalog usage */
    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->rtekind == RTE_RELATION) {
            return false;
        } else if (rte->rtekind == RTE_FUNCTION) {
            return false;
        } else if (rte->rtekind == RTE_SUBQUERY && !rtable_contains_only_values_rte(rte->subquery->rtable)) {
            return false;
        } else if (rte->rtekind == RTE_CTE && !rte->self_reference)
            return false;
    }
    return true;
}

/*
 * Generate plan for a recursive UNION node
 */
static Plan* generate_recursion_plan(
    SetOperationStmt* setOp, PlannerInfo* root, double tuple_fraction, List* refnames_tlist, List** sortClauses)
{
    Plan* plan = NULL;
    Plan* lplan = NULL;
    Plan* rplan = NULL;
    List* tlist = NIL;
    List* groupList = NIL;
    long numGroups;
    bool ori_under_recursive_treee = false;

    /* Parser should have rejected other cases */
    if (setOp->op != SETOP_UNION)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("only UNION queries can be recursive"))));
    /* Worktable ID should be assigned */
    AssertEreport(root->wt_param_id >= 0, MOD_OPT, "Worktable ID should be assigned in generate_recursion_plan");

    /*
     * MPP with-recursive support
     *
     * Pre recursive CTE plan generation check
     */
    if (STREAM_RECURSIVECTE_SUPPORTED) {
        /* If recursive CTE does not have */
        if (!setOp->all) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "With-Recursive does not contain \"ALL\" to bind recursive & none-recursive branches");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            mark_stream_unsupport();
        }

        /* If range table has system table or has only values rte, cannot be shippable */
        if (rtable_contains_system_table(root->parse->rtable)) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "With-Recursive contains system table is not shippable");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            mark_stream_unsupport();
        } else if (rtable_contains_only_values_rte(root->parse->rtable)) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "With-Recursive contains only values rte is not shippable");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            mark_stream_unsupport();
        }
    }

    /*
     * Unlike a regular UNION node, process the left and right inputs
     * separately without any intention of combining them into one Append.
     */
    bool left_correlated = false;
    bool right_correlated = false;
    lplan = recurse_set_operations(setOp->larg,
        root,
        tuple_fraction,
        setOp->colTypes,
        setOp->colCollations,
        false,
        -1,
        refnames_tlist,
        sortClauses,
        NULL);
    left_correlated = root->is_correlated;

    /* The right plan will want to look at the left one ... */
    root->non_recursive_plan = lplan;

    /*
     * In order to let underlying plan generation to know we are creating the
     * recursive part,we need mark the current planning context under recursive
     * branch, so that in follow-up steps we can take proper processing tips
     * (e.g. disable/enable sth) more straightforward
     */
    ori_under_recursive_treee = root->is_under_recursive_tree;
    root->is_under_recursive_tree = true;

    rplan = recurse_set_operations(setOp->rarg,
        root,
        tuple_fraction,
        setOp->colTypes,
        setOp->colCollations,
        false,
        -1,
        refnames_tlist,
        sortClauses,
        NULL);
    root->is_under_recursive_tree = ori_under_recursive_treee;
    root->non_recursive_plan = NULL;
    right_correlated = root->is_correlated;

    /*
     * recursive term correlated only is not yet supported for now.
     * non-recursive term correlated only or both sides correlated are supported.
     */
    if (left_correlated == false && right_correlated == true) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "With-Recursive recursive term correlated only is not shippable");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        mark_stream_unsupport();
    }

    /*
     * Generate tlist for RecursiveUnion plan node --- same as in Append cases
     */
    tlist =
        generate_append_tlist(setOp->colTypes, setOp->colCollations, false, list_make2(lplan, rplan), refnames_tlist);

    if (STREAM_RECURSIVECTE_SUPPORTED && is_replicated_plan(lplan) && !is_replicated_plan(rplan)) {
        /* Check plan conflict in un-resolve-able case */
        if (is_replicated_plan(lplan) && !is_replicated_plan(rplan)) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "With-Recursive contains conflict distribution in none-recursive(Replicate) recursive(Hash)");
            securec_check_ss_c(sprintf_rc, "\0", "\0");

            mark_stream_unsupport();
        }
    }

    /*
     * If UNION, identify the grouping operators
     */
    if (setOp->all) {
        groupList = NIL;
        numGroups = 0;
    } else {
        double dNumGroups;
        /* Identify the grouping semantics */
        groupList = generate_setop_grouplist(setOp, tlist);
        /* We only support hashing here */
        if (!grouping_is_hashable(groupList))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("could not implement recursive UNION"),
                        errdetail("All column datatypes must be hashable."))));
        /*
         * For the moment, take the number of distinct groups as equal to the
         * total input size, ie, the worst case.
         */
        dNumGroups = lplan->plan_rows + rplan->plan_rows * 10;
        /* Also convert to long int --- but 'ware overflow! */
        numGroups = (long)Min(dNumGroups, (double)LONG_MAX);
    }

    /*
     * And make the RecursiveUnion plan node.
     */
    plan = (Plan*)make_recursive_union(tlist, lplan, rplan, root->wt_param_id, groupList, numGroups);

    /* Traverse the RecursiveUnion's subplan tree to mark has_stream field */
    if (STREAM_RECURSIVECTE_SUPPORTED) {
        RecursiveUnion* recursive_plan = (RecursiveUnion*)plan;

        /* set distribute exec_nodes for recursive union plan */
        mark_distribute_setop(root, (Node*)plan, true, true);

        /* assign if the recursive cte is correlated */
        recursive_plan->is_correlated = root->is_correlated;

        /*
         * Traverse the RecursiveUnion's subplan tree to mark has_stream field
         * set stream flag for recursive union plan.
         */
        List* initplans = NIL;
        mark_stream_recursiveunion_plan((RecursiveUnion*)plan, plan, false, root->glob->subplans, &initplans);
        list_free_ext(initplans);
    } else {
        plan->exec_nodes = ng_get_single_node_group_exec_node();
    }

    *sortClauses = NIL; /* RecursiveUnion result is always unsorted */

    return plan;
}

/*
 * Generate plan for a UNION or UNION ALL node
 */
static Plan* generate_union_plan(SetOperationStmt* op, PlannerInfo* root, double tuple_fraction, List* refnames_tlist,
    List** sortClauses, double* pNumGroups)
{
    List* planlist = NIL;
    List* tlist = NIL;
    Plan* plan = NULL;

    /*
     * If plain UNION, tell children to fetch all tuples.
     *
     * Note: in UNION ALL, we pass the top-level tuple_fraction unmodified to
     * each arm of the UNION ALL.  One could make a case for reducing the
     * tuple fraction for later arms (discounting by the expected size of the
     * earlier arms' results) but it seems not worth the trouble. The normal
     * case where tuple_fraction isn't already zero is a LIMIT at top level,
     * and passing it down as-is is usually enough to get the desired result
     * of preferring fast-start plans.
     */
    if (!op->all)
        tuple_fraction = 0.0;

    /*
     * If any of my children are identical UNION nodes (same op, all-flag, and
     * colTypes) then they can be merged into this node so that we generate
     * only one Append and unique-ification for the lot.  Recurse to find such
     * nodes and compute their children's plans.
     */
    planlist = list_concat(recurse_union_children(op->larg, root, tuple_fraction, op, refnames_tlist),
        recurse_union_children(op->rarg, root, tuple_fraction, op, refnames_tlist));

    /*
     * Generate tlist for Append plan node.
     *
     * The tlist for an Append plan isn't important as far as the Append is
     * concerned, but we must make it look real anyway for the benefit of the
     * next plan level up.
     */
    tlist = generate_append_tlist(op->colTypes, op->colCollations, false, planlist, refnames_tlist);

    /*
     * Append the child results together.
     */
    plan = (Plan*)make_append(planlist, tlist);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        bool isunionall = (op->all ? true : false);
        mark_distribute_setop(root, (Node*)plan, isunionall, true);
        parallel_setop(root, plan, isunionall);
    }
#endif

    /*
     * For UNION ALL, we just need the Append plan.  For UNION, need to add
     * node(s) to remove duplicates.
     */
    if (op->all)
        *sortClauses = NIL; /* result of UNION ALL is always unsorted */
    else
        plan = make_union_unique(op, plan, root, tuple_fraction, sortClauses);

    /*
     * Estimate number of groups if caller wants it.  For now we just assume
     * the output is unique --- this is certainly true for the UNION case, and
     * we want worst-case estimates anyway.
     */
    if (pNumGroups != NULL) {
        pNumGroups[0] = PLAN_LOCAL_ROWS(plan);
        pNumGroups[1] = plan->plan_rows;
    }

    return plan;
}

/*
 * Generate plan for an INTERSECT, INTERSECT ALL, EXCEPT, or EXCEPT ALL node
 */
static Plan* generate_nonunion_plan(SetOperationStmt* op, PlannerInfo* root, double tuple_fraction,
    List* refnames_tlist, List** sortClauses, double* pNumGroups)
{
    Plan* lplan = NULL;
    Plan* rplan = NULL;
    Plan* plan = NULL;
    List* tlist = NIL;
    List* groupList = NIL;
    List* planlist = NIL;
    List* child_sortclauses = NIL;
    double dLeftGroups[DISTINCT_GROUPS_LEN];
    double dRightGroups[DISTINCT_GROUPS_LEN];
    double dNumGroups[DISTINCT_GROUPS_LEN];
    double dNumOutputRows[DISTINCT_GROUPS_LEN];
    long numGroups[DISTINCT_GROUPS_LEN];
    bool use_hash = false;
    SetOpCmd cmd;
    int firstFlag;

    /* Recurse on children, ensuring their outputs are marked */
    lplan = recurse_set_operations(op->larg,
        root,
        0.0 /* all tuples needed */,
        op->colTypes,
        op->colCollations,
        false,
        0,
        refnames_tlist,
        &child_sortclauses,
        dLeftGroups);
    rplan = recurse_set_operations(op->rarg,
        root,
        0.0 /* all tuples needed */,
        op->colTypes,
        op->colCollations,
        false,
        1,
        refnames_tlist,
        &child_sortclauses,
        dRightGroups);

    /*
     * For EXCEPT, we must put the left input first.  For INTERSECT, either
     * order should give the same results, and we prefer to put the smaller
     * input first in order to minimize the size of the hash table in the
     * hashing case.  "Smaller" means the one with the fewer groups.
     */
    if (op->op == SETOP_EXCEPT || dLeftGroups[1] <= dRightGroups[1]) {
        planlist = list_make2(lplan, rplan);
        firstFlag = 0;
    } else {
        planlist = list_make2(rplan, lplan);
        firstFlag = 1;
    }

    /*
     * Generate tlist for Append plan node.
     *
     * The tlist for an Append plan isn't important as far as the Append is
     * concerned, but we must make it look real anyway for the benefit of the
     * next plan level up.	In fact, it has to be real enough that the flag
     * column is shown as a variable not a constant, else setrefs.c will get
     * confused.
     */
    if (!u_sess->attr.attr_sql.enable_dngather) {
        tlist = generate_append_tlist(op->colTypes, op->colCollations, true, planlist, refnames_tlist);
    } else {
        ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("can't generate plan for INTERSECT/EXCEPT with dn gather on.")));
    }

    /*
     * Append the child results together.
     */
    plan = (Plan*)make_append(planlist, tlist);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        mark_distribute_setop(root, (Node*)plan, false, true);
        parallel_setop(root, plan, false);
    }
#endif

    /* Identify the grouping semantics */
    groupList = generate_setop_grouplist(op, tlist);
    /* punt if nothing to group on (can this happen?) */
    if (groupList == NIL) {
        *sortClauses = NIL;
        return plan;
    }

    /*
     * Estimate number of distinct groups that we'll need hashtable entries
     * for; this is the size of the left-hand input for EXCEPT, or the smaller
     * input for INTERSECT.  Also estimate the number of eventual output rows.
     * In non-ALL cases, we estimate each group produces one output row; in
     * ALL cases use the relevant relation size.  These are worst-case
     * estimates, of course, but we need to be conservative.
     */
    if (op->op == SETOP_EXCEPT) {
        dNumGroups[0] = dLeftGroups[0];
        dNumGroups[1] = dLeftGroups[1];
        dNumOutputRows[0] = op->all ? PLAN_LOCAL_ROWS(lplan) : dNumGroups[0];
        dNumOutputRows[1] = op->all ? lplan->plan_rows : dNumGroups[1];
    } else {
        dNumGroups[0] = Min(dLeftGroups[0], dRightGroups[0]);
        dNumGroups[1] = Min(dLeftGroups[1], dRightGroups[1]);
        dNumOutputRows[0] = op->all ? Min(PLAN_LOCAL_ROWS(lplan), PLAN_LOCAL_ROWS(rplan)) : dNumGroups[0];
        dNumOutputRows[1] = op->all ? Min(lplan->plan_rows, rplan->plan_rows) : dNumGroups[1];
    }

    /* Also convert to long int --- but 'ware overflow! */
    numGroups[0] = (long)Min(dNumGroups[0], (double)LONG_MAX);
    numGroups[1] = (long)Min(dNumGroups[1], (double)LONG_MAX);

    OpMemInfo mem_info;
    errno_t rc = memset_s(&mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    /*
     * Decide whether to hash or sort, and add a sort node if needed.
     */
    use_hash = choose_hashed_setop(root,
        groupList,
        plan,
        dNumGroups[0],
        dNumOutputRows[0],
        tuple_fraction,
        (op->op == SETOP_INTERSECT) ? "INTERSECT" : "EXCEPT",
        &mem_info);
    if (!use_hash)
        plan = (Plan*)make_sort_from_sortclauses(root, groupList, plan);
    /*
     * Finally, add a SetOp plan node to generate the correct output.
     */
    switch (op->op) {
        case SETOP_INTERSECT:
            cmd = op->all ? SETOPCMD_INTERSECT_ALL : SETOPCMD_INTERSECT;
            break;
        case SETOP_EXCEPT:
            cmd = op->all ? SETOPCMD_EXCEPT_ALL : SETOPCMD_EXCEPT;
            break;
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized set op: %d", (int)op->op)));

            cmd = SETOPCMD_INTERSECT; /* keep compiler quiet */
        } break;
    }
    plan = (Plan*)make_setop(cmd,
        use_hash ? SETOP_HASHED : SETOP_SORTED,
        plan,
        groupList,
        list_length(op->colTypes) + 1,
        use_hash ? firstFlag : -1,
        numGroups[0],
        dNumOutputRows[0],
        &mem_info);

    /* Result is sorted only if we're not hashing */
    *sortClauses = use_hash ? NIL : groupList;

    if (pNumGroups != NULL) {
        pNumGroups[0] = dNumGroups[0];
        pNumGroups[1] = dNumGroups[1];
    }

    return plan;
}

/*
 * Pull up children of a UNION node that are identically-propertied UNIONs.
 *
 * NOTE: we can also pull a UNION ALL up into a UNION, since the distinct
 * output rows will be lost anyway.
 *
 * NOTE: currently, we ignore collations while determining if a child has
 * the same properties.  This is semantically sound only so long as all
 * collations have the same notion of equality.  It is valid from an
 * implementation standpoint because we don't care about the ordering of
 * a UNION child's result: UNION ALL results are always unordered, and
 * generate_union_plan will force a fresh sort if the top level is a UNION.
 */
static List* recurse_union_children(
    Node* setOp, PlannerInfo* root, double tuple_fraction, SetOperationStmt* top_union, List* refnames_tlist)
{
    List* child_sortclauses = NIL;

    if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;

        if (op->op == top_union->op && (op->all == top_union->all || op->all) &&
            equal(op->colTypes, top_union->colTypes)) {
            /* Same UNION, so fold children into parent's subplan list */
            return list_concat(recurse_union_children(op->larg, root, tuple_fraction, top_union, refnames_tlist),
                recurse_union_children(op->rarg, root, tuple_fraction, top_union, refnames_tlist));
        }
    }

    /*
     * Not same, so plan this child separately.
     *
     * Note we disallow any resjunk columns in child results.  This is
     * necessary since the Append node that implements the union won't do any
     * projection, and upper levels will get confused if some of our output
     * tuples have junk and some don't.  This case only arises when we have an
     * EXCEPT or INTERSECT as child, else there won't be resjunk anyway.
     */
    return list_make1(recurse_set_operations(setOp,
        root,
        tuple_fraction,
        top_union->colTypes,
        top_union->colCollations,
        false,
        -1,
        refnames_tlist,
        &child_sortclauses,
        NULL));
}

/*
 * Add nodes to the given plan tree to unique-ify the result of a UNION.
 */
static Plan* make_union_unique(
    SetOperationStmt* op, Plan* plan, PlannerInfo* root, double tuple_fraction, List** sortClauses)
{
    List* groupList = NIL;
    double dNumGroups[DISTINCT_GROUPS_LEN];
    long numGroups[DISTINCT_GROUPS_LEN];

    /* Identify the grouping semantics */
    groupList = generate_setop_grouplist(op, plan->targetlist);
    /* punt if nothing to group on (can this happen?) */
    if (groupList == NIL) {
        *sortClauses = NIL;
        return plan;
    }

    /*
     * XXX for the moment, take the number of distinct groups as equal to the
     * total input size, ie, the worst case.  This is too conservative, but we
     * don't want to risk having the hashtable overrun memory; also, it's not
     * clear how to get a decent estimate of the true size.  One should note
     * as well the propensity of novices to write UNION rather than UNION ALL
     * even when they don't expect any duplicates...
     */
    dNumGroups[0] = PLAN_LOCAL_ROWS(plan);
    dNumGroups[1] = plan->plan_rows;

    /* Also convert to long int --- but 'ware overflow! */
    numGroups[0] = (long)Min(dNumGroups[0], (double)LONG_MAX);
    numGroups[1] = (long)Min(dNumGroups[1], (double)LONG_MAX);

    /* Decide whether to hash or sort */
    if (choose_hashed_setop(root, groupList, plan, dNumGroups[0], dNumGroups[0], tuple_fraction, "UNION", NULL)) {
        /* Hashed aggregate plan --- no sort needed */
        plan = (Plan*)make_agg(root,
            plan->targetlist,
            NIL,
            AGG_HASHED,
            NULL,
            list_length(groupList),
            extract_grouping_cols(groupList, plan->targetlist),
            extract_grouping_ops(groupList),
            numGroups[0],
            plan,
            NULL,
            false,
            false);
        /* Hashed aggregation produces randomly-ordered results */
        *sortClauses = NIL;
    } else {
        /* Sort and Unique */
        plan = (Plan*)make_sort_from_sortclauses(root, groupList, plan);
        plan = (Plan*)make_unique(plan, groupList);
        set_plan_rows(plan, dNumGroups[1], plan->lefttree->multiple);
        /* We know the sort order of the result */
        *sortClauses = groupList;
    }

    return plan;
}

/*
 * choose_hashed_setop - should we use hashing for a set operation?
 */
static bool choose_hashed_setop(PlannerInfo* root, List* groupClauses, Plan* input_plan, double dNumGroups,
    double dNumOutputRows, double tuple_fraction, const char* construct, OpMemInfo* mem_info)
{
    int numGroupCols = list_length(groupClauses);
    bool can_sort = false;
    bool can_hash = false;
    Size hashentrysize;
    Path hashed_p;
    Path sorted_p;
    double plan_rows = PLAN_LOCAL_ROWS(input_plan);
    OpMemInfo hash_mem_info, sort_mem_info;
    errno_t rc = 0;

    rc = memset_s(&hashed_p, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&sorted_p, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    if (mem_info != NULL) {
        rc = memset_s(&sort_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
        rc = memset_s(&hash_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    }

    /* Check whether the operators support sorting or hashing */
    can_sort = grouping_is_sortable(groupClauses);
    can_hash = grouping_is_hashable(groupClauses);
    if (can_hash && can_sort) {
        /* we have a meaningful choice to make, continue ... */
    } else if (can_hash) {
        return true;
    } else if (can_sort) {
        return false;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    /* translator: %s is UNION, INTERSECT, or EXCEPT */
                    errmsg("could not implement %s", construct),
                    errdetail("Some of the datatypes only support hashing, while others only support sorting."))));
    }

    /* Prefer sorting when u_sess->attr.attr_sql.enable_hashagg is off */
    if (!u_sess->attr.attr_sql.enable_hashagg) {
        return false;
    }
    /*
     * Don't do it if it doesn't look like the hashtable will fit into
     * work_mem.
     */
    if (root->glob->vectorized) {
        hashentrysize = get_plan_actual_total_width(input_plan, root->glob->vectorized, OP_HASHAGG);
    } else {
        hashentrysize = get_hash_entry_size(input_plan->plan_width);
    }
    /*
     * See if the estimated cost is no more than doing it the other way.
     *
     * We need to consider input_plan + hashagg versus input_plan + sort +
     * group.  Note that the actual result plan might involve a SetOp or
     * Unique node, not Agg or Group, but the cost estimates for Agg and Group
     * should be close enough for our purposes here.
     *
     * These path variables are dummies that just hold cost fields; we don't
     * make actual Paths for these steps.
     */
    /*
     * When hashentrysize * dNumGroups > work_mem * 1024L, we
     * support to write data to a temporary file, so we can still use hashing for a set operation
     */
    Distribution* distribution = ng_get_dest_distribution(input_plan);
    ng_copy_distribution(&hashed_p.distribution, distribution);
    cost_agg(&hashed_p,
        root,
        AGG_HASHED,
        NULL,
        numGroupCols,
        dNumGroups,
        input_plan->startup_cost,
        input_plan->total_cost,
        plan_rows,
        input_plan->plan_width,
        hashentrysize,
        1,
        mem_info != NULL ? &hash_mem_info : NULL);

    /*
     * Now for the sorted case.  Note that the input is *always* unsorted,
     * since it was made by appending unrelated sub-relations together.
     */
    sorted_p.startup_cost = input_plan->startup_cost;
    sorted_p.total_cost = input_plan->total_cost;
    ng_copy_distribution(&sorted_p.distribution, distribution);
    /* XXX cost_sort doesn't actually look at pathkeys, so just pass NIL */
    cost_sort(&sorted_p,
        NIL,
        sorted_p.total_cost,
        plan_rows,
        input_plan->plan_width,
        0.0,
        u_sess->opt_cxt.op_work_mem,
        -1.0,
        root->glob->vectorized,
        SET_DOP(input_plan->dop),
        mem_info != NULL ? &sort_mem_info : NULL);
    cost_group(&sorted_p, root, numGroupCols, dNumGroups, sorted_p.startup_cost, sorted_p.total_cost, plan_rows);

    /*
     * Now make the decision using the top-level tuple fraction.  First we
     * have to convert an absolute count (LIMIT) into fractional form.
     */
    if (tuple_fraction >= 1.0) {
        tuple_fraction /= dNumOutputRows;
    }

    if (compare_fractional_path_costs(&hashed_p, &sorted_p, tuple_fraction) < 0) {
        /* Hashed is cheaper, so use it */
        if (mem_info != NULL) {
            rc = memcpy_s(mem_info, sizeof(OpMemInfo), &hash_mem_info, sizeof(OpMemInfo));
            securec_check(rc, "\0", "\0");
        }
        return true;
    }

    if (mem_info != NULL) {
        rc = memcpy_s(mem_info, sizeof(OpMemInfo), &sort_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    }
    return false;
}

/*
 * Generate targetlist for a set-operation plan node
 *
 * colTypes: OID list of set-op's result column datatypes
 * colCollations: OID list of set-op's result column collations
 * flag: -1 if no flag column needed, 0 or 1 to create a const flag column
 * varno: varno to use in generated Vars
 * hack_constants: true to copy up constants (see comments in code)
 * input_tlist: targetlist of this node's input node
 * refnames_tlist: targetlist to take column names from
 */
static List* generate_setop_tlist(List* colTypes, List* colCollations, int flag, Index varno, bool hack_constants,
    List* input_tlist, List* refnames_tlist)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* ctlc = NULL;
    ListCell* cclc = NULL;
    ListCell* itlc = NULL;
    ListCell* rtlc = NULL;
    TargetEntry* tle = NULL;
    Node* expr = NULL;

    /* there's no forfour() so we must chase one list manually */
    rtlc = list_head(refnames_tlist);
    forthree(ctlc, colTypes, cclc, colCollations, itlc, input_tlist)
    {
        Oid colType = lfirst_oid(ctlc);
        Oid colColl = lfirst_oid(cclc);
        TargetEntry* inputtle = (TargetEntry*)lfirst(itlc);
        TargetEntry* reftle = (TargetEntry*)lfirst(rtlc);

        rtlc = lnext(rtlc);

        AssertEreport(inputtle->resno == resno, MOD_OPT, "inputtle's resno mismatch in generate_setop_tlist");
        AssertEreport(reftle->resno == resno, MOD_OPT, "reftle's resno mismatch in generate_setop_tlist");
        AssertEreport(!inputtle->resjunk, MOD_OPT, "inputtle's resjunk should be false in generate_setop_tlist");
        AssertEreport(!reftle->resjunk, MOD_OPT, "reftle's resjunk should be false in generate_setop_tlist");

        /*
         * Generate columns referencing input columns and having appropriate
         * data types and column names.  Insert datatype coercions where
         * necessary.
         *
         * HACK: constants in the input's targetlist are copied up as-is
         * rather than being referenced as subquery outputs.  This is mainly
         * to ensure that when we try to coerce them to the output column's
         * datatype, the right things happen for UNKNOWN constants.  But do
         * this only at the first level of subquery-scan plans; we don't want
         * phony constants appearing in the output tlists of upper-level
         * nodes!
         */
        if (hack_constants && inputtle->expr && IsA(inputtle->expr, Const))
            expr = (Node*)inputtle->expr;
        else
            expr = (Node*)makeVar(varno,
                inputtle->resno,
                exprType((Node*)inputtle->expr),
                exprTypmod((Node*)inputtle->expr),
                exprCollation((Node*)inputtle->expr),
                0);

        if (exprType(expr) != colType) {
            /*
             * Note: it's not really cool to be applying coerce_to_common_type
             * here; one notable point is that assign_expr_collations never
             * gets run on any generated nodes.  For the moment that's not a
             * problem because we force the correct exposed collation below.
             * It would likely be best to make the parser generate the correct
             * output tlist for every set-op to begin with, though.
             */
            expr = coerce_to_common_type(NULL, /* no UNKNOWNs here */
                expr,
                colType,
                "UNION/INTERSECT/EXCEPT");
        }

        /*
         * Ensure the tlist entry's exposed collation matches the set-op. This
         * is necessary because plan_set_operations() reports the result
         * ordering as a list of SortGroupClauses, which don't carry collation
         * themselves but just refer to tlist entries.	If we don't show the
         * right collation then planner.c might do the wrong thing in
         * higher-level queries.
         *
         * Note we use RelabelType, not CollateExpr, since this expression
         * will reach the executor without any further processing.
         */
        if (exprCollation(expr) != colColl) {
            expr = (Node*)makeRelabelType((Expr*)expr, exprType(expr), exprTypmod(expr), colColl, COERCE_DONTCARE);
        }

        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup(reftle->resname), false);
        tlist = lappend(tlist, tle);
    }

    if (flag >= 0) {
        /* Add a resjunk flag column */
        /* flag value is the given constant */
        expr = (Node*)makeConst(INT4OID, -1, InvalidOid, sizeof(int4), Int32GetDatum(flag), false, true);
        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup("flag"), true);
        tlist = lappend(tlist, tle);
    }

    return tlist;
}

/*
 * Generate targetlist for a set-operation Append node
 *
 * colTypes: OID list of set-op's result column datatypes
 * colCollations: OID list of set-op's result column collations
 * flag: true to create a flag column copied up from subplans
 * input_plans: list of sub-plans of the Append
 * refnames_tlist: targetlist to take column names from
 *
 * The entries in the Append's targetlist should always be simple Vars;
 * we just have to make sure they have the right datatypes/typmods/collations.
 * The Vars are always generated with varno 0.
 */
static List* generate_append_tlist(
    List* colTypes, List* colCollations, bool flag, List* input_plans, List* refnames_tlist)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* curColType = NULL;
    ListCell* curColCollation = NULL;
    ListCell* ref_tl_item = NULL;
    int colindex;
    TargetEntry* tle = NULL;
    Node* expr = NULL;
    ListCell* planl = NULL;
    int32* colTypmods = NULL;

    /*
     * First extract typmods to use.
     *
     * If the inputs all agree on type and typmod of a particular column, use
     * that typmod; else use -1.
     */
    colTypmods = (int32*)palloc(list_length(colTypes) * sizeof(int32));

    foreach (planl, input_plans) {
        Plan* subplan = (Plan*)lfirst(planl);
        ListCell* subtlist = NULL;

        curColType = list_head(colTypes);
        colindex = 0;
        foreach (subtlist, subplan->targetlist) {
            TargetEntry* subtle = (TargetEntry*)lfirst(subtlist);

            if (subtle->resjunk)
                continue;
            AssertEreport(curColType != NULL,
                MOD_OPT,
                "set-op's result column datatype should not be NULL in processing in generate_append_tlist");
            if (exprType((Node*)subtle->expr) == lfirst_oid(curColType)) {
                /* If first subplan, copy the typmod; else compare */
                int32 subtypmod = exprTypmod((Node*)subtle->expr);

                if (planl == list_head(input_plans))
                    colTypmods[colindex] = subtypmod;
                else if (subtypmod != colTypmods[colindex])
                    colTypmods[colindex] = -1;
            } else {
                /* types disagree, so force typmod to -1 */
                colTypmods[colindex] = -1;
            }
            curColType = lnext(curColType);
            colindex++;
        }
        AssertEreport(curColType == NULL,
            MOD_OPT,
            "set-op's result column datatype should be NULL after processing in generate_append_tlist");
    }

    /*
     * Now we can build the tlist for the Append.
     */
    colindex = 0;
    forthree(curColType, colTypes, curColCollation, colCollations, ref_tl_item, refnames_tlist)
    {
        Oid colType = lfirst_oid(curColType);
        int32 colTypmod = colTypmods[colindex++];
        Oid colColl = lfirst_oid(curColCollation);
        TargetEntry* reftle = (TargetEntry*)lfirst(ref_tl_item);

        AssertEreport(reftle->resno == resno,
            MOD_OPT,
            "resno mismatch when building the tlist for the Append in generate_append_tlist");
        AssertEreport(!reftle->resjunk,
            MOD_OPT,
            "resjunk should be fase when building the tlist for the Append in generate_append_tlist");
        expr = (Node*)makeVar(0, resno, colType, colTypmod, colColl, 0);
        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup(reftle->resname), false);
        tlist = lappend(tlist, tle);
    }

    if (flag) {
        /* Add a resjunk flag column */
        /* flag value is shown as copied up from subplan */
        expr = (Node*)makeVar(0, resno, INT4OID, -1, InvalidOid, 0);
        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup("flag"), true);
        tlist = lappend(tlist, tle);
    }

    pfree_ext(colTypmods);

    return tlist;
}

/*
 * generate_setop_grouplist
 *		Build a SortGroupClause list defining the sort/grouping properties
 *		of the setop's output columns.
 *
 * Parse analysis already determined the properties and built a suitable
 * list, except that the entries do not have sortgrouprefs set because
 * the parser output representation doesn't include a tlist for each
 * setop.  So what we need to do here is copy that list and install
 * proper sortgrouprefs into it and into the targetlist.
 */
static List* generate_setop_grouplist(SetOperationStmt* op, List* targetlist)
{
    List* grouplist = (List*)copyObject(op->groupClauses);
    ListCell* lg = NULL;
    ListCell* lt = NULL;
    Index refno = 1;

    lg = list_head(grouplist);
    foreach (lt, targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lt);
        SortGroupClause* sgc = NULL;

        /* tlist shouldn't have any sortgrouprefs yet */
        AssertEreport(tle->ressortgroupref == 0,
            MOD_OPT,
            "tlist shouldn't have any sortgrouprefs yet in generate_setop_grouplist");

        if (tle->resjunk)
            continue; /* ignore resjunk columns */

        /* non-resjunk columns should have grouping clauses */
        AssertEreport(
            lg != NULL, MOD_OPT, "non-resjunk columns should have grouping clauses in generate_setop_grouplist");
        sgc = (SortGroupClause*)lfirst(lg);
        lg = lnext(lg);
        AssertEreport(sgc->tleSortGroupRef == 0,
            MOD_OPT,
            "SortGroupClause's tleSortGroupRef should be 0 in generate_setop_grouplist");

        /* we could use assignSortGroupRef here, but seems a bit silly */
        sgc->tleSortGroupRef = tle->ressortgroupref = refno++;
    }
    AssertEreport(lg == NULL,
        MOD_OPT,
        "non-resjunk columns should have grouping clauses after processing in generate_setop_grouplist");
    return grouplist;
}

/*
 * expand_inherited_tables
 *		Expand each rangetable entry that represents an inheritance set
 *		into an "append relation".	At the conclusion of this process,
 *		the "inh" flag is set in all and only those RTEs that are append
 *		relation parents.
 */
void expand_inherited_tables(PlannerInfo* root)
{
    Index nrtes;
    Index rti;
    ListCell* rl = NULL;

    /*
     * expand_inherited_rtentry may add RTEs to parse->rtable; there is no
     * need to scan them since they can't have inh=true.  So just scan as far
     * as the original end of the rtable list.
     */
    nrtes = list_length(root->parse->rtable);
    rl = list_head(root->parse->rtable);
    for (rti = 1; rti <= nrtes; rti++) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(rl);

        expand_inherited_rtentry(root, rte, rti);
        rl = lnext(rl);
    }
}

/*
 * expand_inherited_rtentry
 *		Check whether a rangetable entry represents an inheritance set.
 *		If so, add entries for all the child tables to the query's
 *		rangetable, and build AppendRelInfo nodes for all the child tables
 *		and add them to root->append_rel_list.	If not, clear the entry's
 *		"inh" flag to prevent later code from looking for AppendRelInfos.
 *
 * Note that the original RTE is considered to represent the whole
 * inheritance set.  The first of the generated RTEs is an RTE for the same
 * table, but with inh = false, to represent the parent table in its role
 * as a simple member of the inheritance set.
 *
 * A childless table is never considered to be an inheritance set; therefore
 * a parent RTE must always have at least two associated AppendRelInfos.
 */
static void expand_inherited_rtentry(PlannerInfo* root, RangeTblEntry* rte, Index rti)
{
    Query* parse = root->parse;
    Oid parentOID;
    Relation parentRel = NULL;
    PlanRowMark* oldrc = NULL;
    Relation oldrelation;
    LOCKMODE lockmode;
    List* inhOIDs = NIL;
    List* appinfos = NIL;
    ListCell* l = NULL;

    /* Does RT entry allow inheritance? */
    if (!rte->inh)
        return;
    /* Ignore any already-expanded UNION ALL nodes */
    if (rte->rtekind != RTE_RELATION) {
        AssertEreport(rte->rtekind == RTE_SUBQUERY,
            MOD_OPT,
            "RangeTblEntry should be kind of SUBQUERY if not RELATION in expand_inherited_rtentry");
        return;
    }
    /* Fast path for common case of childless table */
    parentOID = rte->relid;
    parentRel = RelationIdGetRelation(parentOID);
    if (parentRel == NULL) {
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("No Such Relation")));
    }

    if (!has_subclass(parentOID)) {
        /* Clear flag before returning */
        if (!RelationIsDfsStore(parentRel) || (!u_sess->opt_cxt.is_stream && IS_PGXC_COORDINATOR)) {
            rte->inh = false;
        }
        RelationClose(parentRel);
        return;
    }

    RelationClose(parentRel);

    /*
     * The rewriter should already have obtained an appropriate lock on each
     * relation named in the query.  However, for each child relation we add
     * to the query, we must obtain an appropriate lock, because this will be
     * the first use of those relations in the parse/rewrite/plan pipeline.
     *
     * If the parent relation is the query's result relation, then we need
     * RowExclusiveLock.  Otherwise, if it's accessed FOR UPDATE/SHARE, we
     * need RowShareLock; otherwise AccessShareLock.  We can't just grab
     * AccessShareLock because then the executor would be trying to upgrade
     * the lock, leading to possible deadlocks.  (This code should match the
     * parser and rewriter.)
     */
    oldrc = get_plan_rowmark(root->rowMarks, rti);
    if (rti == (unsigned int)parse->resultRelation)
        lockmode = RowExclusiveLock;
    else if (oldrc && RowMarkRequiresRowShareLock(oldrc->markType))
        lockmode = RowShareLock;
    else
        lockmode = AccessShareLock;
    /* Scan for all members of inheritance set, acquire needed locks */
    inhOIDs = find_all_inheritors(parentOID, lockmode, NULL);
    /*
     * Check that there's at least one descendant, else treat as no-child
     * case.  This could happen despite above has_subclass() check, if table
     * once had a child but no longer does.
     */
    if (list_length(inhOIDs) < 2) { /* 2 is the least one descendant. Clear flag before returning. */
        rte->inh = false;
        return;
    }
    /*
     * If parent relation is selected FOR UPDATE/SHARE, we need to mark its
     * PlanRowMark as isParent = true, and generate a new PlanRowMark for each
     * child.
     */
    if (oldrc != NULL)
        oldrc->isParent = true;

    /*
     * Must open the parent relation to examine its tupdesc.  We need not lock
     * it; we assume the rewriter already did.
     */
    oldrelation = heap_open(parentOID, NoLock);

    /* Scan the inheritance set and expand it */
    appinfos = NIL;
    foreach (l, inhOIDs) {
        Oid childOID = lfirst_oid(l);
        Relation newrelation;
        RangeTblEntry* childrte = NULL;
        Index childRTindex;
        AppendRelInfo* appinfo = NULL;

        /* Open rel if needed; we already have required locks */
        if (childOID != parentOID)
            newrelation = heap_open(childOID, NoLock);
        else
            newrelation = oldrelation;

        /*
         * It is possible that the parent table has children that are temp
         * tables of other backends.  We cannot safely access such tables
         * (because of buffering issues), and the best thing to do seems to be
         * to silently ignore them.
         */
        if (childOID != parentOID && RELATION_IS_OTHER_TEMP(newrelation)) {
            heap_close(newrelation, lockmode);
            continue;
        }

        /*
         * Build an RTE for the child, and attach to query's rangetable list.
         * We copy most fields of the parent's RTE, but replace relation OID,
         * and set inh = false.  Also, set requiredPerms to zero since all
         * required permissions checks are done on the original RTE.
         * Likewise, set the child's securityQuals to empty, because we only
         * want to apply the parent's RLS conditions regardless of what RLS
         * properties individual children may have.  (This is an intentional
         * choice to make inherited RLS work like regular permissions checks.)
         * The parent securityQuals will be propagated to children along with
         * other base restriction clauses, so we don't need to do it here.
         */
        childrte = (RangeTblEntry*)copyObject(rte);
        childrte->relid = childOID;
        childrte->inh = false;
        childrte->requiredPerms = 0;
        childrte->securityQuals = NIL;
        parse->rtable = lappend(parse->rtable, childrte);
        childRTindex = list_length(parse->rtable);

        if (RELATION_IS_PARTITIONED(newrelation)) {
            childrte->ispartrel = true;
        } else {
            childrte->ispartrel = false;
        }

        /*
         * Build an AppendRelInfo for this parent and child.
         */
        appinfo = makeNode(AppendRelInfo);
        appinfo->parent_relid = rti;
        appinfo->child_relid = childRTindex;
        appinfo->parent_reltype = oldrelation->rd_rel->reltype;
        appinfo->child_reltype = newrelation->rd_rel->reltype;
        make_inh_translation_list(oldrelation, newrelation, childRTindex, &appinfo->translated_vars);
        appinfo->parent_reloid = parentOID;
        appinfos = lappend(appinfos, appinfo);

        /*
         * Translate the column permissions bitmaps to the child's attnums (we
         * have to build the translated_vars list before we can do this). But
         * if this is the parent table, leave copyObject's result alone.
         *
         * Note: we need to do this even though the executor won't run any
         * permissions checks on the child RTE.  The modifiedCols bitmap may
         * be examined for trigger-firing purposes.
         */
        if (childOID != parentOID) {
            childrte->selectedCols = translate_col_privs(rte->selectedCols, appinfo->translated_vars);
            childrte->insertedCols = translate_col_privs(rte->insertedCols, appinfo->translated_vars);
            childrte->updatedCols = translate_col_privs(rte->updatedCols, appinfo->translated_vars);
            childrte->extraUpdatedCols = translate_col_privs(rte->extraUpdatedCols, appinfo->translated_vars);
        }

        /*
         * Build a PlanRowMark if parent is marked FOR UPDATE/SHARE.
         */
        if (oldrc != NULL) {
            PlanRowMark* newrc = makeNode(PlanRowMark);

            newrc->rti = childRTindex;
            newrc->prti = rti;
            newrc->rowmarkId = oldrc->rowmarkId;
            newrc->markType = oldrc->markType;
            newrc->noWait = oldrc->noWait;
            newrc->waitSec = oldrc->waitSec;
            newrc->isParent = false;

            root->rowMarks = lappend(root->rowMarks, newrc);
        }

        /* Close child relations, but keep locks */
        if (childOID != parentOID)
            heap_close(newrelation, NoLock);
    }

    heap_close(oldrelation, NoLock);

    /*
     * If all the children were temp tables, pretend it's a non-inheritance
     * situation.  The duplicate RTE we added for the parent table is
     * harmless, so we don't bother to get rid of it.
     */
    if (list_length(appinfos) < 2) { /* 2 is the least one descendant. Clear flag before returning. */
        rte->inh = false;
        list_free_deep(appinfos);
        return;
    }

    /* Otherwise, OK to add to root->append_rel_list */
    root->append_rel_list = list_concat(root->append_rel_list, appinfos);
}

/*
 * make_inh_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  an inheritance child.
 *
 * For paranoia's sake, we match type/collation as well as attribute name.
 */
void make_inh_translation_list(Relation oldrelation, Relation newrelation, Index newvarno, List** translated_vars)
{
    List* vars = NIL;
    TupleDesc old_tupdesc = RelationGetDescr(oldrelation);
    TupleDesc new_tupdesc = RelationGetDescr(newrelation);
    int oldnatts = old_tupdesc->natts;
    int newnatts = new_tupdesc->natts;
    int old_attno;

    for (old_attno = 0; old_attno < oldnatts; old_attno++) {
        Form_pg_attribute att;
        char* attname = NULL;
        Oid atttypid;
        int32 atttypmod;
        Oid attcollation;
        int new_attno;

        att = old_tupdesc->attrs[old_attno];
        if (att->attisdropped) {
            /* Just put NULL into this list entry */
            vars = lappend(vars, NULL);
            continue;
        }
        attname = NameStr(att->attname);
        atttypid = att->atttypid;
        atttypmod = att->atttypmod;
        attcollation = att->attcollation;

        /*
         * When we are generating the "translation list" for the parent table
         * of an inheritance set, no need to search for matches.
         */
        if (oldrelation == newrelation) {
            vars = lappend(vars, makeVar(newvarno, (AttrNumber)(old_attno + 1), atttypid, atttypmod, attcollation, 0));
            continue;
        }

        /*
         * Otherwise we have to search for the matching column by name.
         * There's no guarantee it'll have the same column position, because
         * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
         * However, in simple cases it will be the same column number, so try
         * that before we go groveling through all the columns.
         *
         * Note: the test for (att = ...) != NULL cannot fail, it's just a
         * notational device to include the assignment into the if-clause.
         * Since the idea of inherited table to achieve DFS table, so ther following
         * logic must be covered.
         */
        if (old_attno < newnatts && (att = new_tupdesc->attrs[old_attno]) != NULL &&
            ((att->attisdropped && att->attinhcount != 0) || RelationIsDfsStore(oldrelation)) &&
            strcmp(attname, NameStr(att->attname)) == 0)
            new_attno = old_attno;
        else {
            for (new_attno = 0; new_attno < newnatts; new_attno++) {
                att = new_tupdesc->attrs[new_attno];
                if (!att->attisdropped && att->attinhcount != 0 && strcmp(attname, NameStr(att->attname)) == 0)
                    break;
            }
            if (new_attno >= newnatts)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("could not find inherited attribute \"%s\" of relation \"%s\"",
                            attname,
                            RelationGetRelationName(newrelation)))));
        }

        /* Found it, check type and collation match */
        if (atttypid != att->atttypid || atttypmod != att->atttypmod)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("attribute \"%s\" of relation \"%s\" does not match parent's type",
                        attname,
                        RelationGetRelationName(newrelation)))));
        if (attcollation != att->attcollation)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("attribute \"%s\" of relation \"%s\" does not match parent's collation",
                        attname,
                        RelationGetRelationName(newrelation)))));

        vars = lappend(vars, makeVar(newvarno, (AttrNumber)(new_attno + 1), atttypid, atttypmod, attcollation, 0));
    }

    *translated_vars = vars;
}

/*
 * translate_col_privs
 *	  Translate a bitmapset representing per-column privileges from the
 *	  parent rel's attribute numbering to the child's.
 *
 * The only surprise here is that we don't translate a parent whole-row
 * reference into a child whole-row reference.	That would mean requiring
 * permissions on all child columns, which is overly strict, since the
 * query is really only going to reference the inherited columns.  Instead
 * we set the per-column bits for all inherited columns.
 */
Bitmapset* translate_col_privs(const Bitmapset* parent_privs, List* translated_vars)
{
    Bitmapset* child_privs = NULL;
    bool whole_row = false;
    int attno;
    ListCell* lc = NULL;

    /* System attributes have the same numbers in all tables */
    for (attno = FirstLowInvalidHeapAttributeNumber + 1; attno < 0; attno++) {
        if (bms_is_member(attno - FirstLowInvalidHeapAttributeNumber, parent_privs))
            child_privs = bms_add_member(child_privs, attno - FirstLowInvalidHeapAttributeNumber);
    }

    /* Check if parent has whole-row reference */
    whole_row = bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber, parent_privs);

    /* And now translate the regular user attributes, using the vars list */
    attno = InvalidAttrNumber;
    foreach (lc, translated_vars) {
        Var* var = (Var*)lfirst(lc);

        attno++;
        if (var == NULL) /* ignore dropped columns */
            continue;
        AssertEreport(IsA(var, Var), MOD_OPT, "Node should be Var in translate_col_privs");
        if (whole_row || bms_is_member(attno - FirstLowInvalidHeapAttributeNumber, parent_privs))
            child_privs = bms_add_member(child_privs, var->varattno - FirstLowInvalidHeapAttributeNumber);
    }

    return child_privs;
}

/*
 * adjust_appendrel_attrs
 *	  Copy the specified query or expression and translate Vars referring
 *	  to the parent rel of the specified AppendRelInfo to refer to the
 *	  child rel instead.  We also update rtindexes appearing outside Vars,
 *	  such as resultRelation and jointree relids.
 *
 * Note: this is only applied after conversion of sublinks to subplans,
 * so we don't need to cope with recursion into sub-queries.
 *
 * Note: this is not hugely different from what pullup_replace_vars() does;
 * maybe we should try to fold the two routines together.
 */
Node* adjust_appendrel_attrs(PlannerInfo* root, Node* node, AppendRelInfo* appinfo)
{
    Node* result = NULL;
    adjust_appendrel_attrs_context context;

    context.root = root;
    context.appinfo = appinfo;

    /*
     * Must be prepared to start with a Query or a bare expression tree.
     */
    if (node && IsA(node, Query)) {
        Query* newnode = NULL;

        newnode = query_tree_mutator((Query*)node,
            (Node* (*)(Node*, void*)) adjust_appendrel_attrs_mutator,
            (void*)&context,
            QTW_IGNORE_RC_SUBQUERIES);
        if ((unsigned int)newnode->resultRelation == appinfo->parent_relid) {
            newnode->resultRelation = appinfo->child_relid;
            /* Fix tlist resnos too, if it's inherited UPDATE */
            if (newnode->commandType == CMD_UPDATE)
                newnode->targetList = adjust_inherited_tlist(newnode->targetList, appinfo);
        }
        result = (Node*)newnode;
    } else
        result = adjust_appendrel_attrs_mutator(node, &context);

    return result;
}

static Node* adjust_appendrel_attrs_mutator(Node* node, adjust_appendrel_attrs_context* context)
{
    AppendRelInfo* appinfo = context->appinfo;

    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)copyObject(node);

        if (var->varlevelsup == 0 && var->varno == appinfo->parent_relid) {
            var->varno = appinfo->child_relid;
            var->varnoold = appinfo->child_relid;
            if (var->varattno > 0) {
                Node* newnode = NULL;

                if (var->varattno > list_length(appinfo->translated_vars))
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            (errmsg("attribute %d of relation \"%s\" does not exist",
                                var->varattno,
                                get_rel_name(appinfo->parent_reloid)))));
                newnode = (Node*)copyObject(list_nth(appinfo->translated_vars, var->varattno - 1));
                if (newnode == NULL)
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
                            (errmsg("attribute %d of relation \"%s\" does not exist",
                                var->varattno,
                                get_rel_name(appinfo->parent_reloid)))));
                return newnode;
            } else if (var->varattno == 0) {
                /*
                 * Whole-row Var: if we are dealing with named rowtypes, we
                 * can use a whole-row Var for the child table plus a coercion
                 * step to convert the tuple layout to the parent's rowtype.
                 * Otherwise we have to generate a RowExpr.
                 */
                if (OidIsValid(appinfo->child_reltype)) {
                    AssertEreport(var->vartype == appinfo->parent_reltype,
                        MOD_OPT,
                        "type OID mismatch for Var and AppendRelInfo in adjust_appendrel_attrs_mutator");
                    if (appinfo->parent_reltype != appinfo->child_reltype) {
                        ConvertRowtypeExpr* r = makeNode(ConvertRowtypeExpr);

                        r->arg = (Expr*)var;
                        r->resulttype = appinfo->parent_reltype;
                        r->convertformat = COERCE_IMPLICIT_CAST;
                        r->location = -1;
                        /* Make sure the Var node has the right type ID, too */
                        var->vartype = appinfo->child_reltype;
                        return (Node*)r;
                    }
                } else {
                    /*
                     * Build a RowExpr containing the translated variables.
                     *
                     * In practice var->vartype will always be RECORDOID here,
                     * so we need to come up with some suitable column names.
                     * We use the parent RTE's column names.
                     *
                     * Note: we can't get here for inheritance cases, so there
                     * is no need to worry that translated_vars might contain
                     * some dummy NULLs.
                     */
                    RowExpr* rowexpr = NULL;
                    List* fields = NIL;
                    RangeTblEntry* rte = NULL;

                    rte = rt_fetch(appinfo->parent_relid, context->root->parse->rtable);
                    fields = (List*)copyObject(appinfo->translated_vars);
                    rowexpr = makeNode(RowExpr);
                    rowexpr->args = fields;
                    rowexpr->row_typeid = var->vartype;
                    rowexpr->row_format = COERCE_IMPLICIT_CAST;
                    rowexpr->colnames = (List*)copyObject(rte->eref->colnames);
                    rowexpr->location = -1;

                    return (Node*)rowexpr;
                }
            }
            /* system attributes don't need any other translation */
        }
        return (Node*)var;
    }
    if (IsA(node, CurrentOfExpr)) {
        CurrentOfExpr* cexpr = (CurrentOfExpr*)copyObject(node);

        if (cexpr->cvarno == appinfo->parent_relid)
            cexpr->cvarno = appinfo->child_relid;
        return (Node*)cexpr;
    }
    if (IsA(node, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)copyObject(node);

        if ((Index)rtr->rtindex == appinfo->parent_relid)
            rtr->rtindex = appinfo->child_relid;
        return (Node*)rtr;
    }
    if (IsA(node, JoinExpr)) {
        /* Copy the JoinExpr node with correct mutation of subnodes */
        JoinExpr* j = NULL;

        j = (JoinExpr*)expression_tree_mutator(
            node, (Node* (*)(Node*, void*)) adjust_appendrel_attrs_mutator, (void*)context);
        /* now fix JoinExpr's rtindex (probably never happens) */
        if ((Index)j->rtindex == appinfo->parent_relid)
            j->rtindex = appinfo->child_relid;
        return (Node*)j;
    }
    if (IsA(node, PlaceHolderVar)) {
        /* Copy the PlaceHolderVar node with correct mutation of subnodes */
        PlaceHolderVar* phv = NULL;

        phv = (PlaceHolderVar*)expression_tree_mutator(
            node, (Node* (*)(Node*, void*)) adjust_appendrel_attrs_mutator, (void*)context);
        /* now fix PlaceHolderVar's relid sets */
        if (phv->phlevelsup == 0)
            phv->phrels = adjust_relid_set(phv->phrels, appinfo->parent_relid, appinfo->child_relid);
        return (Node*)phv;
    }
    /* Shouldn't need to handle planner auxiliary nodes here */
    AssertEreport(!IsA(node, SpecialJoinInfo),
        MOD_OPT,
        "Shouldn't need to handle planner auxiliary node SpecialJoinInfo in adjust_appendrel_attrs_mutator");
    Assert(!IsA(node, LateralJoinInfo));
    AssertEreport(!IsA(node, AppendRelInfo),
        MOD_OPT,
        "Shouldn't need to handle planner auxiliary node AppendRelInfo in adjust_appendrel_attrs_mutator");
    AssertEreport(!IsA(node, PlaceHolderInfo),
        MOD_OPT,
        "Shouldn't need to handle planner auxiliary node PlaceHolderInfo in adjust_appendrel_attrs_mutator");
    AssertEreport(!IsA(node, MinMaxAggInfo),
        MOD_OPT,
        "Shouldn't need to handle planner auxiliary node MinMaxAggInfo in adjust_appendrel_attrs_mutator");

    /*
     * We have to process RestrictInfo nodes specially.  (Note: although
     * set_append_rel_pathlist will hide RestrictInfos in the parent's
     * baserestrictinfo list from us, it doesn't hide those in joininfo.)
     */
    if (IsA(node, RestrictInfo)) {
        RestrictInfo* oldinfo = (RestrictInfo*)node;
        RestrictInfo* newinfo = makeNode(RestrictInfo);
        errno_t rc = EOK; /* Initialize rc to keep compiler slient */

        /* Copy all flat-copiable fields */
        rc = memcpy_s(newinfo, sizeof(RestrictInfo), oldinfo, sizeof(RestrictInfo));
        securec_check(rc, "", "");

        /* Recursively fix the clause itself */
        newinfo->clause = (Expr*)adjust_appendrel_attrs_mutator((Node*)oldinfo->clause, context);

        /* and the modified version, if an OR clause */
        newinfo->orclause = (Expr*)adjust_appendrel_attrs_mutator((Node*)oldinfo->orclause, context);

        /* adjust relid sets too */
        newinfo->clause_relids = adjust_relid_set(oldinfo->clause_relids, appinfo->parent_relid, appinfo->child_relid);
        newinfo->required_relids =
            adjust_relid_set(oldinfo->required_relids, appinfo->parent_relid, appinfo->child_relid);
        newinfo->outer_relids = adjust_relid_set(oldinfo->outer_relids, appinfo->parent_relid, appinfo->child_relid);
        newinfo->nullable_relids =
            adjust_relid_set(oldinfo->nullable_relids, appinfo->parent_relid, appinfo->child_relid);
        newinfo->left_relids = adjust_relid_set(oldinfo->left_relids, appinfo->parent_relid, appinfo->child_relid);
        newinfo->right_relids = adjust_relid_set(oldinfo->right_relids, appinfo->parent_relid, appinfo->child_relid);

        /*
         * Reset cached derivative fields, since these might need to have
         * different values when considering the child relation.  Note we
         * don't reset left_ec/right_ec: each child variable is implicitly
         * equivalent to its parent, so still a member of the same EC if any.
         */
        newinfo->eval_cost.startup = -1;
        newinfo->norm_selec = -1;
        newinfo->outer_selec = -1;
        newinfo->left_em = NULL;
        newinfo->right_em = NULL;
        newinfo->scansel_cache = NIL;
        rc = memset_s(&newinfo->left_bucketsize, sizeof(BucketSize), 0, sizeof(BucketSize));
        securec_check(rc, "\0", "\0");
        rc = memset_s(&newinfo->right_bucketsize, sizeof(BucketSize), 0, sizeof(BucketSize));
        securec_check(rc, "\0", "\0");

        return (Node*)newinfo;
    }

    /*
     * NOTE: we do not need to recurse into sublinks, because they should
     * already have been converted to subplans before we see them.
     */
    AssertEreport(!IsA(node, SubLink),
        MOD_OPT,
        "Node should not be Sublink and already have been converted before in adjust_appendrel_attrs_mutator");
    AssertEreport(!IsA(node, Query),
        MOD_OPT,
        "Node should not be Query and already have been converted before in adjust_appendrel_attrs_mutator");

    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) adjust_appendrel_attrs_mutator, (void*)context);
}

/*
 * Substitute newrelid for oldrelid in a Relid set
 */
static Relids adjust_relid_set(Relids relids, Index oldrelid, Index newrelid)
{
    if (bms_is_member(oldrelid, relids)) {
        /* Ensure we have a modifiable copy */
        relids = bms_copy(relids);
        /* Remove old, add new */
        relids = bms_del_member(relids, oldrelid);
        relids = bms_add_member(relids, newrelid);
    }
    return relids;
}

/*
 * Adjust the targetlist entries of an inherited UPDATE operation
 *
 * The expressions have already been fixed, but we have to make sure that
 * the target resnos match the child table (they may not, in the case of
 * a column that was added after-the-fact by ALTER TABLE).	In some cases
 * this can force us to re-order the tlist to preserve resno ordering.
 * (We do all this work in special cases so that preptlist.c is fast for
 * the typical case.)
 *
 * The given tlist has already been through expression_tree_mutator;
 * therefore the TargetEntry nodes are fresh copies that it's okay to
 * scribble on.
 *
 * Note that this is not needed for INSERT because INSERT isn't inheritable.
 */
static List* adjust_inherited_tlist(List* tlist, AppendRelInfo* context)
{
    bool changed_it = false;
    ListCell* tl = NULL;
    List* new_tlist = NIL;
    bool more = false;
    int attrno;

    /* This should only happen for an inheritance case, not UNION ALL */
    AssertEreport(OidIsValid(context->parent_reloid),
        MOD_OPT,
        "OID of parent relation should be valid in adjust_inherited_tlist");

    /* Scan tlist and update resnos to match attnums of child rel */
    foreach (tl, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);
        Var* childvar = NULL;

        if (tle->resjunk)
            continue; /* ignore junk items */

        /* Look up the translation of this column: it must be a Var */
        if (tle->resno <= 0 || tle->resno > list_length(context->translated_vars))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
                    (errmsg("attribute %d of relation \"%s\" does not exist",
                        tle->resno,
                        get_rel_name(context->parent_reloid)))));
        childvar = (Var*)list_nth(context->translated_vars, tle->resno - 1);
        if (childvar == NULL || !IsA(childvar, Var))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
                    (errmsg("attribute %d of relation \"%s\" does not exist",
                        tle->resno,
                        get_rel_name(context->parent_reloid)))));

        if (tle->resno != childvar->varattno) {
            tle->resno = childvar->varattno;
            changed_it = true;
        }
    }

    /*
     * If we changed anything, re-sort the tlist by resno, and make sure
     * resjunk entries have resnos above the last real resno.  The sort
     * algorithm is a bit stupid, but for such a seldom-taken path, small is
     * probably better than fast.
     */
    if (!changed_it)
        return tlist;

    new_tlist = NIL;
    more = true;
    for (attrno = 1; more; attrno++) {
        more = false;
        foreach (tl, tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(tl);

            if (tle->resjunk)
                continue; /* ignore junk items */

            if (tle->resno == attrno)
                new_tlist = lappend(new_tlist, tle);
            else if (tle->resno > attrno)
                more = true;
        }
    }

    foreach (tl, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);

        if (!tle->resjunk)
            continue; /* here, ignore non-junk items */

        tle->resno = attrno;
        new_tlist = lappend(new_tlist, tle);
        attrno++;
    }

    return new_tlist;
}

/*
 * Brief        : Expand the Dfs table using the rte.
 * Input        : root, the PlannerInfo struct.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
void expand_dfs_tables(PlannerInfo* root)
{
    Index rtesNum;
    Index rti;
    ListCell* rl = NULL;

    /*
     * expand_internal_rtentry may add RTEs to parse->rtable;
     */
    rtesNum = list_length(root->parse->rtable);
    rl = list_head(root->parse->rtable);
    for (rti = 1; rti <= rtesNum; rti++) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(rl);

        expand_internal_rtentry(root, rte, rti);
        rl = lnext(rl);
    }
}

/*
 * Brief        : If the table is Dfs table, not only make main
 *                table rte and delta table rte, but also create main table AppendRelInfo
 *                and detal table AppendRelInfo.
 * Input        : root, the PlannerInfo struct.
 *                rte, the range table entry stuct.
 *                rti, the rte index in range table entry list.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
void expand_internal_rtentry(PlannerInfo* root, RangeTblEntry* rte, Index rti)
{
    Query* parse = root->parse;
    Oid parentOid = InvalidOid;
    Relation prarentRel;
    PlanRowMark* oldrc = NULL;
    LOCKMODE lockmode;
    List* interOids = NIL;
    List* appinfos = NIL;
    ListCell* l = NULL;

    rte->mainRelName = NULL;
    rte->mainRelNameSpace = NULL;

    /*
     * Does RT entry allow inheritance?
     */
    if (!rte->inh) {
        return;
    }
    /*
     * Ignore any already-expanded UNION ALL nodes.
     */
    if (rte->rtekind != RTE_RELATION) {
        AssertEreport(rte->rtekind == RTE_SUBQUERY,
            MOD_OPT,
            "RangeTblEntry should be kind of SUBQUERY if not RELATION in expand_internal_rtentry");
        return;
    }

    parentOid = rte->relid;
    prarentRel = RelationIdGetRelation(parentOid);
    if (prarentRel == NULL) {
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("No Such Relation")));
    }

    if (!RelationIsDfsStore(prarentRel)) {
        /*
         * Do not set rte->inh = false here, because the inherits table
         * dose not need deal with.
         */
        RelationClose(prarentRel);
        return;
    }

    /*
     * The rewriter should already have obtained an appropriate lock on each
     * relation named in the query.  However, for each child relation we add
     * to the query, we must obtain an appropriate lock, because this will be
     * the first use of those relations in the parse/rewrite/plan pipeline.
     *
     * If the parent relation is the query's result relation, then we need
     * RowExclusiveLock.  Otherwise, if it's accessed FOR UPDATE/SHARE, we
     * need RowShareLock; otherwise AccessShareLock.  We can't just grab
     * AccessShareLock because then the executor would be trying to upgrade
     * the lock, leading to possible deadlocks.  (This code should match the
     * parser and rewriter.)
     */
    oldrc = get_plan_rowmark(root->rowMarks, rti);
    if (rti == (unsigned int)parse->resultRelation) {
        lockmode = RowExclusiveLock;
    } else if (oldrc && RowMarkRequiresRowShareLock(oldrc->markType)) {
        lockmode = RowShareLock;
    } else {
        lockmode = AccessShareLock;
    }
    /*
     * Scan for all members of internal set, acquire needed locks.
     */
    interOids = find_all_internal_tableOids(parentOid);
    /*
     * Check that there's at least one descendant, else treat as no-child
     * case.  This could happen despite above has_subclass() check, if table
     * once had a child but no longer does.
     */
    if (list_length(interOids) < 2) { /* 2 is the least one descendant. Clear flag before returning. */
        rte->inh = false;
        RelationClose(prarentRel);
        return;
    }

    /*
     * If parent relation is selected FOR UPDATE/SHARE, we need to mark its
     * PlanRowMark as isParent = true, and generate a new PlanRowMark for each
     * child.
     */
    if (oldrc != NULL) {
        oldrc->isParent = true;
    }

    /*
     * Must open the parent relation to examine its tupdesc.  We need not lock
     * it; we assume the rewriter already did.
     */
    /*
     * Scan the internal set and expand it.
     */
    appinfos = NIL;
    foreach (l, interOids) {
        Oid childOid = lfirst_oid(l);
        Relation newRel;
        RangeTblEntry* childrte = NULL;
        Index childRTindex;
        AppendRelInfo* appinfo = NULL;

        if (childOid != parentOid) {
            newRel = heap_open(childOid, NoLock);
        } else {
            newRel = prarentRel;
        }

        /*
         * It is possible that the parent table has children that are temp
         * tables of other backends.  We cannot safely access such tables
         * (because of buffering issues), and the best thing to do seems to be
         * to silently ignore them.
         */
        if (childOid != parentOid && RELATION_IS_OTHER_TEMP(newRel)) {
            heap_close(newRel, lockmode);
            continue;
        }

        /*
         * Build an RTE for the child, and attach to query's rangetable list.
         * We copy most fields of the parent's RTE, but replace relation OID,
         * and set inh = false.  Also, set requiredPerms to zero since all
         * required permissions checks are done on the original RTE.
         */
        childrte = (RangeTblEntry*)copyObject(rte);
        childrte->relid = childOid;
        childrte->inh = false;
        childrte->requiredPerms = 0;
        if (childOid != parentOid) {
            /*
             * Check if table is in UStore format.
             */
            childrte->is_ustore = RelationIsUstoreFormat(prarentRel);

            /*
             * Set delta table orientation.
             */
            childrte->orientation = REL_ROW_ORIENTED;

            /*
             * fill main table name and schema name.
             */
            childrte->mainRelName = pstrdup(RelationGetRelationName(prarentRel));
            Oid nameSpaceOid = RelationGetNamespace(prarentRel);
            childrte->mainRelNameSpace = pstrdup(get_namespace_name(nameSpaceOid));
        }
        parse->rtable = lappend(parse->rtable, childrte);
        childRTindex = list_length(parse->rtable);

        /* For hdfs table, we should push scan hint to hdfs main table */
        adjust_scanhint_relid(parse->hintState, rti, childRTindex);

        if (RELATION_IS_PARTITIONED(newRel)) {
            childrte->ispartrel = true;
        } else {
            childrte->ispartrel = false;
        }

        /*
         * Build an AppendRelInfo for this parent and child.
         */
        appinfo = makeNode(AppendRelInfo);
        appinfo->parent_relid = rti;
        appinfo->child_relid = childRTindex;
        appinfo->parent_reltype = prarentRel->rd_rel->reltype;
        appinfo->child_reltype = newRel->rd_rel->reltype;
        make_inh_translation_list(prarentRel, newRel, childRTindex, &appinfo->translated_vars);
        appinfo->parent_reloid = parentOid;
        appinfos = lappend(appinfos, appinfo);

        /*
         * Translate the column permissions bitmaps to the child's attnums (we
         * have to build the translated_vars list before we can do this). But
         * if this is the parent table, leave copyObject's result alone.
         *
         * Note: we need to do this even though the executor won't run any
         * permissions checks on the child RTE.  The modifiedCols bitmap may
         * be examined for trigger-firing purposes.
         */
        if (childOid != parentOid) {
            childrte->selectedCols = translate_col_privs(rte->selectedCols, appinfo->translated_vars);
            childrte->insertedCols = translate_col_privs(rte->insertedCols, appinfo->translated_vars);
            childrte->updatedCols = translate_col_privs(rte->updatedCols, appinfo->translated_vars);
            childrte->extraUpdatedCols = translate_col_privs(rte->extraUpdatedCols, appinfo->translated_vars);
        }

        /*
         * Build a PlanRowMark if parent is marked FOR UPDATE/SHARE.
         */
        if (oldrc != NULL) {
            PlanRowMark* newrc = makeNode(PlanRowMark);

            newrc->rti = childRTindex;
            newrc->prti = rti;
            newrc->rowmarkId = oldrc->rowmarkId;
            newrc->markType = oldrc->markType;
            newrc->noWait = oldrc->noWait;
            newrc->waitSec = oldrc->waitSec;
            newrc->isParent = false;

            root->rowMarks = lappend(root->rowMarks, newrc);
        }

        /*
         * Close child relations, but keep locks.
         */
        if (childOid != parentOid)
            heap_close(newRel, NoLock);
    }

    RelationClose(prarentRel);

    /*
     * If all the children were temp tables, pretend it's a non-inheritance
     * situation.  The duplicate RTE we added for the parent table is
     * harmless, so we don't bother to get rid of it.
     */
    if (list_length(appinfos) < 2) { /* 2 is the least one descendant. Clear flag before returning. */
        rte->inh = false;
        list_free_deep(appinfos);
        return;
    }

    /*
     * Otherwise, OK to add to root->append_rel_list.
     */
    root->append_rel_list = list_concat(root->append_rel_list, appinfos);
}

/*
 * Brief        : Find all internal table(delata table) for Dfs table.
 * Input        : parentOid, the main table oid.
 * Output       : None.
 * Return Value : Return the internal oid list.
 * Notes        : None.
 */
List* find_all_internal_tableOids(Oid parentOid)
{
    List* oids = NULL;
    Oid deltaTblOid = InvalidOid;
    HeapTuple tuple = NULL;
    Form_pg_class relForm = NULL;

    AssertEreport(OidIsValid(parentOid), MOD_OPT, "parent Oid should be valid in find_all_internal_tableOids");
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(parentOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("Relation with OID %u does not exist.", parentOid))));
    }

    relForm = (Form_pg_class)GETSTRUCT(tuple);
    deltaTblOid = relForm->reldeltarelid;

    oids = list_make1_oid(parentOid);
    oids = lappend_oid(oids, deltaTblOid);

    ReleaseSysCache(tuple);
    return oids;
}

/*
 * Add local redistribute to parallel setop.
 *
 * @param[IN] plan: subplan of setop
 * return
 */
void parallel_setop(PlannerInfo* root, Plan* plan, bool isunionall)
{
    if (isunionall) {
        return;
    }
    /*
     * Check if local redistribute is needed to parallel setOp.
     */
    if (plan->dop > 1) {
        List* subplans = NIL;
        ListCell* lc = NULL;

        if (IsA(plan, Append))
            subplans = ((Append*)plan)->appendplans;
        else if (IsA(plan, MergeAppend))
            subplans = ((MergeAppend*)plan)->mergeplans;

        foreach (lc, subplans) {
            Plan* subplan = (Plan*)lfirst(lc);
            if (is_local_redistribute_needed(subplan)) {
                Plan* newplan = NULL;
                newplan = make_stream_plan(root, subplan, subplan->distributed_keys, 1.0);
                ((Stream*)newplan)->smpDesc.distriType = LOCAL_DISTRIBUTE;
                lfirst(lc) = (void*)newplan;
            }
        }
    }
}

/*
 * @Description: After the sublink is pulled, the can_push flag of the
 *      parent query and subquery changes and needs to be re-marked. 
 *
 * @in parent - Parent query.
 * @in child - Child query.
 * @return : void.
 */
void mark_parent_child_pushdown_flag(Query *parent, Query *child)
{
    /* Set the flag only when cost_param is set to maximized pushdown. */
    if (IS_STREAM_PLAN && ((parent->can_push && !child->can_push) ||
        (!parent->can_push && child->can_push))) {
        if (check_base_rel_in_fromlist(parent, (Node *)parent->jointree)) {
#ifndef ENABLE_MULTIPLE_NODES
            if (u_sess->opt_cxt.is_stream_support) {
                mark_stream_unsupport();
            }
#endif
            set_stream_off();
        } else {
            parent->can_push = false;
            child->can_push = false;
        }
    }
}


bool check_base_rel_in_fromlist(Query *parse, Node *jtnode)
{
    if (jtnode == NULL) {
        return false;
    }

    if (IsA(jtnode, RangeTblRef)) {
        int rtindex = ((RangeTblRef *) jtnode)->rtindex;
        RangeTblEntry *rte = rt_fetch(rtindex, parse->rtable);
        return (rte->rtekind == RTE_RELATION) ? true : false;
    } else if (IsA(jtnode, FromExpr)) {
        ListCell *lc = NULL;
        FromExpr *f = (FromExpr *) jtnode;

        foreach(lc , f->fromlist)
        {
            return check_base_rel_in_fromlist(parse, (Node *) lfirst(lc));
        }
        
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr *j = (JoinExpr *) jtnode;
        bool left_has_base_rel = false;
        bool right_has_base_rel = false;

        left_has_base_rel = check_base_rel_in_fromlist(parse, j->larg);
        right_has_base_rel = check_base_rel_in_fromlist(parse, j->rarg);

        return (left_has_base_rel || right_has_base_rel) ? true : false;
    } else {
        ereport(ERROR,
                (errmodule(MOD_OPT),
                 errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                 errmsg("unrecognized node type: %d", (int) nodeTag(jtnode))));
        
        return false;
    }

    return false;
}


UNIONALL_SHIPPING_TYPE precheck_shipping_union_all(Query *subquery, Node *setOp)
{
    if (setOp == NULL) {
        elog(ERROR, "subquery's setOperations tree should not be NULL in pull_up_simple_union_all");
    }

    if (IsA(setOp, RangeTblRef)) {
        RangeTblEntry * rte = rt_fetch(((RangeTblRef *) setOp)->rtindex, subquery->rtable);
        return (rte->subquery->can_push) ? SHIPPING_ALL : SHIPPING_NONE;
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* setOpStmt = (SetOperationStmt *) setOp;
        UNIONALL_SHIPPING_TYPE larg_shippihg_state = SHIPPING_NONE;
        UNIONALL_SHIPPING_TYPE rarg_shippihg_state = SHIPPING_NONE;

        larg_shippihg_state = precheck_shipping_union_all(subquery, setOpStmt->larg);
        rarg_shippihg_state = precheck_shipping_union_all(subquery, setOpStmt->rarg);

        if (larg_shippihg_state == rarg_shippihg_state) {
            return (larg_shippihg_state == SHIPPING_ALL) ? SHIPPING_ALL : SHIPPING_NONE;
            
        } else {
            return SHIPPING_PARTIAL;
        }
    } else {
        ereport(ERROR,
                (errmodule(MOD_OPT),
                 errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                 errmsg("unrecognized node type: %d", (int) nodeTag(setOp))));
        return SHIPPING_NONE;
    }
}
