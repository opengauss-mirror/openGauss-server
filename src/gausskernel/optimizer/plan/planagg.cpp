/* -------------------------------------------------------------------------
 *
 * planagg.cpp
 *	  Special planning for aggregate queries.
 *
 * This module tries to replace MIN/MAX aggregate functions by subqueries
 * of the form
 *		(SELECT col FROM tab
 *		 WHERE col IS NOT NULL AND existing-quals
 *		 ORDER BY col ASC/DESC
 *		 LIMIT 1)
 * Given a suitable index on tab.col, this can be much faster than the
 * generic scan-all-the-rows aggregation plan.	We can handle multiple
 * MIN/MAX aggregates by generating multiple subqueries, and their
 * orderings can be different.	However, if the query contains any
 * non-optimizable aggregates, there's no point since we'll have to
 * scan all the rows anyway.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/planagg.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/subselect.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "pgxc/pgxc.h"
#include "optimizer/streamplan.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/optimizer/planner.h"
#endif

static bool find_minmax_aggs_walker(Node* node, List** context);
static bool build_minmax_path(PlannerInfo* root, MinMaxAggInfo* mminfo, Oid eqop, Oid sortop, bool nulls_first);
static Plan* make_agg_subplan(PlannerInfo* root, MinMaxAggInfo* mminfo);
static Node* replace_aggs_with_params_mutator(Node* node, PlannerInfo* root);
static Oid fetch_agg_sort_op(Oid aggfnoid);

/* min/max optimization support data type. */
#define MINMAXOPTIMIZATIONOID(var_oid)                                                                      \
    ((var_oid) != CHAROID && (var_oid) != INT2OID && (var_oid) != INT4OID && (var_oid) != INT8OID &&        \
        (var_oid) != OIDOID && (var_oid) != DATEOID && (var_oid) != TIMEOID && (var_oid) != TIMESTAMPOID && \
        (var_oid) != NUMERICOID)
/*
 * preprocess_minmax_aggregates - preprocess MIN/MAX aggregates
 *
 * Check to see whether the query contains MIN/MAX aggregate functions that
 * might be optimizable via indexscans.  If it does, and all the aggregates
 * are potentially optimizable, then set up root->minmax_aggs with a list of
 * these aggregates.
 *
 * Note: we are passed the preprocessed targetlist separately, because it's
 * not necessarily equal to root->parse->targetList.
 */
void preprocess_minmax_aggregates(PlannerInfo* root, List* tlist)
{
    Query* parse = root->parse;
    FromExpr* jtnode = NULL;
    RangeTblRef* rtr = NULL;
    RangeTblEntry* rte = NULL;
    List* aggs_list = NIL;
    ListCell* lc = NULL;

    /* minmax_aggs list should be empty at this point */
    AssertEreport(
        root->minmax_aggs == NIL, MOD_OPT, "The minmax_aggs is not empty when preprocessing MIN/MAX aggregates.");

    /* Nothing to do if query has no aggregates */
    if (!parse->hasAggs)
        return;

    AssertEreport(!parse->setOperations,
        MOD_OPT,
        "setOp is not allowed when preprocessing MIN/MAX aggregates."); /* shouldn't get here if a setop */
    AssertEreport(parse->rowMarks == NIL,
        MOD_OPT,
        "RowMarkClause is not allowd when preprocessing MIN/MAX aggregates."); /* nor if FOR UPDATE */

    /*
     * Reject unoptimizable cases.
     *
     * We don't handle GROUP BY or windowing, because our current
     * implementations of grouping require looking at all the rows anyway, and
     * so there's not much point in optimizing MIN/MAX.  (Note: relaxing this
     * would likely require some restructuring in grouping_planner(), since it
     * performs assorted processing related to these features between calling
     * preprocess_minmax_aggregates and optimize_minmax_aggregates.)
     *
     * For example group by grouping sets(()); parse->groupClause is null and
     * the length of parse->groupingSets is 1. In this case, min/max may optimize.
     */
    if (parse->groupClause || parse->hasWindowFuncs || list_length(parse->groupingSets) > 1)
        return;

    /*
     * We also restrict the query to reference exactly one table, since join
     * conditions can't be handled reasonably.  (We could perhaps handle a
     * query containing cartesian-product joins, but it hardly seems worth the
     * trouble.)  However, the single table could be buried in several levels
     * of FromExpr due to subqueries.  Note the "single" table could be an
     * inheritance parent, too, including the case of a UNION ALL subquery
     * that's been flattened to an appendrel.
     */
    jtnode = parse->jointree;
    while (IsA(jtnode, FromExpr)) {
        if (list_length(jtnode->fromlist) != 1)
            return;
        jtnode = (FromExpr*)linitial(jtnode->fromlist);
    }
    if (!IsA(jtnode, RangeTblRef))
        return;
    rtr = (RangeTblRef*)jtnode;
    rte = planner_rt_fetch(rtr->rtindex, root);
    if (rte->rtekind == RTE_RELATION)
        /* ordinary relation, ok */;
    else if (rte->rtekind == RTE_SUBQUERY && rte->inh)
        /* flattened UNION ALL subquery, ok */;
    else
        return;

    /*
     * Scan the tlist and HAVING qual to find all the aggregates and verify
     * all are MIN/MAX aggregates.	Stop as soon as we find one that isn't.
     */
    aggs_list = NIL;
    if (find_minmax_aggs_walker((Node*)tlist, &aggs_list))
        return;
    if (find_minmax_aggs_walker(parse->havingQual, &aggs_list))
        return;

    /*
     * OK, there is at least the possibility of performing the optimization.
     * Build an access path for each aggregate.  (We must do this now because
     * we need to call query_planner with a pristine copy of the current query
     * tree; it'll be too late when optimize_minmax_aggregates gets called.)
     * If any of the aggregates prove to be non-indexable, give up; there is
     * no point in optimizing just some of them.
     */
    foreach (lc, aggs_list) {
        MinMaxAggInfo* mminfo = (MinMaxAggInfo*)lfirst(lc);
        Oid eqop;
        bool reverse = false;

        /*
         * We'll need the equality operator that goes with the aggregate's
         * ordering operator.
         */
        eqop = get_equality_op_for_ordering_op(mminfo->aggsortop, &reverse);
        if (!OidIsValid(eqop)) /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_INVALID_OPERATION),
                    (errmsg("could not find equality operator for ordering operator %u", mminfo->aggsortop))));

        /*
         * We can use either an ordering that gives NULLS FIRST or one that
         * gives NULLS LAST; furthermore there's unlikely to be much
         * performance difference between them, so it doesn't seem worth
         * costing out both ways if we get a hit on the first one.	NULLS
         * FIRST is more likely to be available if the operator is a
         * reverse-sort operator, so try that first if reverse.
         */
        if (build_minmax_path(root, mminfo, eqop, mminfo->aggsortop, reverse))
            continue;
        if (build_minmax_path(root, mminfo, eqop, mminfo->aggsortop, !reverse))
            continue;

        /* No indexable path for this aggregate, so fail */
        return;
    }

    /*
     * We're done until path generation is complete.  Save info for later.
     * (Setting root->minmax_aggs non-NIL signals we succeeded in making index
     * access paths for all the aggregates.)
     */
    root->minmax_aggs = aggs_list;
}

/*
 * optimize_minmax_aggregates - check for optimizing MIN/MAX via indexes
 *
 * Check to see whether using the aggregate indexscans is cheaper than the
 * generic aggregate method.  If so, generate and return a Plan that does it
 * that way.  Otherwise, return NULL.
 *
 * Note: it seems likely that the generic method will never be cheaper
 * in practice, except maybe for tiny tables where it'd hardly matter.
 * Should we skip even trying to build the standard plan, if
 * preprocess_minmax_aggregates succeeds?
 *
 * We are passed the preprocessed tlist, as well as the estimated costs for
 * doing the aggregates the regular way, and the best path devised for
 * computing the input of a standard Agg node.
 */
Plan* optimize_minmax_aggregates(PlannerInfo* root, List* tlist, const AggClauseCosts* aggcosts, Path* best_path)
{
    Query* parse = root->parse;
    Cost total_cost;
    Path agg_path;
    Plan* plan = NULL;
    Plan* sub_plan = NULL;
    Node* hqual = NULL;
    ListCell* lc = NULL;
    errno_t rc;

    /* Nothing to do if preprocess_minmax_aggs rejected the query */
    if (root->minmax_aggs == NIL)
        return NULL;

    rc = memset_s(&agg_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    /*
     * Now we have enough info to compare costs against the generic aggregate
     * implementation.
     *
     * Note that we don't include evaluation cost of the tlist here; this is
     * OK since it isn't included in best_path's cost either, and should be
     * the same in either case.
     */
    total_cost = 0;
    foreach (lc, root->minmax_aggs) {
        MinMaxAggInfo* mminfo = (MinMaxAggInfo*)lfirst(lc);

        total_cost += mminfo->pathcost;
    }

    Distribution* distribution = ng_get_dest_distribution(best_path);
    ng_copy_distribution(&agg_path.distribution, distribution);
    cost_agg(&agg_path,
        root,
        AGG_PLAIN,
        aggcosts,
        0,
        0,
        best_path->startup_cost,
        best_path->total_cost,
        RELOPTINFO_LOCAL_FIELD(root, best_path->parent, rows));

    if (total_cost > agg_path.total_cost)
        return NULL; /* too expensive */

    /*
     * OK, we are going to generate an optimized plan.
     *
     * First, generate a subplan and output Param node for each agg.
     */
    foreach (lc, root->minmax_aggs) {
        MinMaxAggInfo* mminfo = (MinMaxAggInfo*)lfirst(lc);
        sub_plan = make_agg_subplan(root, mminfo);
    }

    /*
     * Modify the targetlist and HAVING qual to reference subquery outputs
     */
    tlist = (List*)replace_aggs_with_params_mutator((Node*)tlist, root);
    hqual = replace_aggs_with_params_mutator(parse->havingQual, root);

    /*
     * We have to replace Aggrefs with Params in equivalence classes too, else
     * ORDER BY or DISTINCT on an optimized aggregate will fail.  We don't
     * need to process child eclass members though, since they aren't of
     * interest anymore --- and replace_aggs_with_params_mutator isn't able
     * to handle Aggrefs containing translated child Vars, anyway.
     *
     * Note: at some point it might become necessary to mutate other data
     * structures too, such as the query's sortClause or distinctClause. Right
     * now, those won't be examined after this point.
     */
    mutate_eclass_expressions(root, (Node* (*)()) replace_aggs_with_params_mutator, (void*)root, false);

    /*
     * Generate the output plan --- basically just a Result
     */
    plan = (Plan*)make_result(root, tlist, hqual, NULL);

    if (sub_plan != NULL) {
        inherit_plan_locator_info(plan, sub_plan);
    } else {
        plan->exec_nodes = ng_get_default_computing_group_exec_node();
    }

    /* Account for evaluation cost of the tlist (make_result did the rest) */
    add_tlist_costs_to_plan(root, plan, tlist);

    return plan;
}

/*
 * find_minmax_aggs_walker
 *		Recursively scan the Aggref nodes in an expression tree, and check
 *		that each one is a MIN/MAX aggregate.  If so, build a list of the
 *		distinct aggregate calls in the tree.
 *
 * Returns TRUE if a non-MIN/MAX aggregate is found, FALSE otherwise.
 * (This seemingly-backward definition is used because expression_tree_walker
 * aborts the scan on TRUE return, which is what we want.)
 *
 * Found aggregates are added to the list at *context; it's up to the caller
 * to initialize the list to NIL.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans.  There mustn't be outer-aggregate
 * references either.
 */
static bool find_minmax_aggs_walker(Node* node, List** context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Aggref)) {
        Aggref* aggref = (Aggref*)node;
        Oid aggsortop;
        TargetEntry* curTarget = NULL;
        MinMaxAggInfo* mminfo = NULL;
        ListCell* l = NULL;

        AssertEreport(aggref->agglevelsup == 0,
            MOD_OPT,
            "The agg does not belong to current query"
            "when scaning the Aggref nodes in an expression tree recursively to find a MIN/MAX aggregate.");
        if (list_length(aggref->args) != 1 || aggref->aggorder != NIL)
            return true; /* it couldn't be MIN/MAX */
        /* note: we do not care if DISTINCT is mentioned ... */
        curTarget = (TargetEntry*)linitial(aggref->args);

        aggsortop = fetch_agg_sort_op(aggref->aggfnoid);
        if (!OidIsValid(aggsortop))
            return true; /* not a MIN/MAX aggregate */

        if (contain_mutable_functions((Node*)curTarget->expr))
            return true; /* not potentially indexable */

        if (type_is_rowtype(exprType((Node*)curTarget->expr)))
            return true; /* IS NOT NULL would have weird semantics */

        /*
         * Check whether it's already in the list, and add it if not.
         */
        foreach (l, *context) {
            mminfo = (MinMaxAggInfo*)lfirst(l);
            if (mminfo->aggfnoid == aggref->aggfnoid && equal(mminfo->target, curTarget->expr))
                return false;
        }

        mminfo = makeNode(MinMaxAggInfo);
        mminfo->aggfnoid = aggref->aggfnoid;
        mminfo->aggsortop = aggsortop;
        mminfo->aggref = aggref;
        mminfo->target = curTarget->expr;
        mminfo->subroot = NULL; /* don't compute path yet */
        mminfo->path = NULL;
        mminfo->pathcost = 0;
        mminfo->param = NULL;

        *context = lappend(*context, mminfo);

        /*
         * We need not recurse into the argument, since it can't contain any
         * aggregates.
         */
        return false;
    }
    AssertEreport(!IsA(node, SubLink),
        MOD_OPT,
        "invalid node type when scan the Aggref nodes in an expression tree recursively to find a MIN/MAX aggregate.");
    return expression_tree_walker(node, (bool (*)())find_minmax_aggs_walker, (void*)context);
}

static bool HasNOTNULLConstraint(Query* parse, NullTest* ntest)
{
    if (IsA(ntest->arg, Var)) {
        /* Check whether NOT NULL check can be guaranteed by table defination */
        RangeTblEntry* rte = rt_fetch(((Var*)ntest->arg)->varno, parse->rtable);
        Oid reloid = rte->relid;
        AttrNumber attno = ((Var*)ntest->arg)->varoattno;
        if (reloid != InvalidOid && attno != InvalidAttrNumber) {
            HeapTuple atttuple =
                SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(reloid), Int16GetDatum(attno));
            if (!HeapTupleIsValid(atttuple)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for attribute %hd of relation %u",
                        attno, reloid)));
            }
            Form_pg_attribute attStruct = (Form_pg_attribute)GETSTRUCT(atttuple);
            if (attStruct->attnotnull) {
                heap_freetuple_ext(atttuple);
                return true;
            }
            heap_freetuple_ext(atttuple);
        }
    }
    return false;
}

#ifndef ENABLE_MULTIPLE_NODES
static bool is_indexpath_useful(Expr* mminfo_targe, List* indextlist)
{
    Assert(mminfo_targe != NULL);
    if (IsA(mminfo_targe, RelabelType)) {
        mminfo_targe = ((RelabelType*)mminfo_targe)->arg;
    }
    if (indextlist == NULL || !IsA(mminfo_targe, Var)) {
        return false;
    }

    TargetEntry* indexTarget = (TargetEntry*)linitial(indextlist);
    if (indexTarget->expr == NULL || !IsA(indexTarget->expr, Var)) {
        return false;
    }
    Var* indexVar = (Var*)indexTarget->expr;
    Var* mminfoVar = (Var*)mminfo_targe;
    if (mminfoVar->varno == indexVar->varno && mminfoVar->varattno == indexVar->varattno) {
        return true;
    }
    return false;
}
#endif

/*
 * The partitioned data has no sequence. If the pathkey of the subpath is directly inherited,
 * the sort operator needs to be added at the end of the PartIterator plan in make_agg_subplan.
 * For details about the inheritance rules of pathkey in the partitioned table,
 * see function make_partiterator_pathkey.
 */
static void get_pathkeys_for_partiteratorpath(RelOptInfo *final_rel, Expr* mminfo_target)
{
#ifndef ENABLE_MULTIPLE_NODES
    if (!final_rel->isPartitionedTable) {
        return;
    }
    ListCell *l = NULL;
    foreach (l, final_rel->pathlist) {
        Path *path = (Path *)lfirst(l);
        if (!IsA(path, PartIteratorPath)) {
            continue;
        }
        PartIteratorPath *itrpath = (PartIteratorPath *)path;
        if (itrpath->path.pathkeys == NULL &&
            (itrpath->subPath->pathtype == T_IndexOnlyScan || itrpath->subPath->pathtype == T_IndexScan)) {
            IndexPath *indexPath = (IndexPath *)itrpath->subPath;
            // only supprot btree index.
            if (!OID_IS_BTREE(indexPath->indexinfo->relam)) {
                continue;
            }
            if (!is_indexpath_useful(mminfo_target, indexPath->indexinfo->indextlist)) {
                continue;
            }
            IndexesUsableType usable_type = eliminate_partition_index_unusable(indexPath->indexinfo->indexoid,
                                                                               final_rel->pruning_result, NULL, NULL);
            if (usable_type != INDEXES_FULL_USABLE) {
                continue;
            }
            itrpath->path.pathkeys = itrpath->subPath->pathkeys;
            itrpath->needSortNode = true;
        }
    }
#endif
}

/*
 * build_minmax_path
 *		Given a MIN/MAX aggregate, try to build an indexscan Path it can be
 *		optimized with.
 *
 * If successful, stash the best path in *mminfo and return TRUE.
 * Otherwise, return FALSE.
 */
static bool build_minmax_path(PlannerInfo* root, MinMaxAggInfo* mminfo, Oid eqop, Oid sortop, bool nulls_first)
{
    PlannerInfo* subroot = NULL;
    Query* parse = NULL;
    TargetEntry* tle = NULL;
    NullTest* ntest = NULL;
    SortGroupClause* sortcl = NULL;
    Path* cheapest_path = NULL;
    Path* sorted_path = NULL;
    double dNumGroups[2] = {1, 1};
    Cost path_cost;
    double path_fraction;
    errno_t errorno = EOK;
    RelOptInfo* final_rel = NULL;
    standard_qp_extra qp_extra;

    /* ----------
     * Generate modified query of the form
     *		(SELECT col FROM tab
     *		 WHERE col IS NOT NULL AND existing-quals
     *		 ORDER BY col ASC/DESC
     *		 LIMIT 1)
     * ----------
     */
    subroot = (PlannerInfo*)palloc(sizeof(PlannerInfo));
    errorno = memcpy_s(subroot, sizeof(PlannerInfo), root, sizeof(PlannerInfo));
    securec_check_c(errorno, "\0", "\0");
    subroot->parse = parse = (Query*)copyObject(root->parse);
    /* make sure subroot planning won't change root->init_plans contents */
    subroot->init_plans = list_copy(root->init_plans);
    /* There shouldn't be any OJ info to translate, as yet */
    AssertEreport(subroot->join_info_list == NIL,
        MOD_OPT,
        "join info list is not null when building an index path at a given MIN/MAX aggregate.");
    Assert(subroot->lateral_info_list == NIL);
    /* and we haven't created PlaceHolderInfos, either */
    AssertEreport(subroot->placeholder_list == NIL,
        MOD_OPT,
        "place holder list is not null when building an index path at a given MIN/MAX aggregate.");

    /* single tlist entry that is the aggregate target */
    tle = makeTargetEntry((Expr*)copyObject(mminfo->target), (AttrNumber)1, pstrdup("agg_target"), false);
    parse->targetList = list_make1(tle);

    /* No HAVING, no DISTINCT, no aggregates, no grouping sets  anymore */
    parse->havingQual = NULL;
    subroot->hasHavingQual = false;
    parse->distinctClause = NIL;
    parse->hasDistinctOn = false;
    parse->hasAggs = false;
    parse->groupingSets = NULL;

    /* Build "target IS NOT NULL" expression */
    ntest = makeNode(NullTest);
    ntest->nulltesttype = IS_NOT_NULL;
    ntest->arg = (Expr*)copyObject(mminfo->target);
    /* we checked it wasn't a rowtype in find_minmax_aggs_walker */
    ntest->argisrow = false;

    /* User might have had that in WHERE already */
    if (!list_member((List*)parse->jointree->quals, ntest) && !HasNOTNULLConstraint(parse, ntest))
        parse->jointree->quals = (Node*)lcons(ntest, (List*)parse->jointree->quals);

    /* Build suitable ORDER BY clause */
    sortcl = makeNode(SortGroupClause);
    sortcl->tleSortGroupRef = assignSortGroupRef(tle, parse->targetList);
    sortcl->eqop = eqop;
    sortcl->sortop = sortop;
    sortcl->nulls_first = nulls_first;
    sortcl->hashable = false; /* no need to make this accurate */
    parse->sortClause = list_make1(sortcl);

    /* set up expressions for LIMIT 1 */
    parse->limitOffset = NULL;
    parse->limitCount =
        (Node*)makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(1), false, FLOAT8PASSBYVAL);

    /* Initialize the pathkeys */
    standard_qp_init(subroot, &qp_extra, parse->targetList, NULL, NULL);

    /*
     * Generate the best paths for this query, telling query_planner that we
     * have LIMIT 1.
     */

    /* Make tuple_fraction, limit_tuples accessible to lower-level routines */
    subroot->tuple_fraction = 1.0;
    subroot->limit_tuples = 1.0;

    /* 
     * Generate pathlist by query_planner for final_rel and canonicalize 
     * all the pathkeys.
     */
    final_rel = query_planner(subroot, parse->targetList,
                              standard_qp_callback, &qp_extra);

    /* 
     * In the following, generate the best unsorted and presorted paths for 
     * this Query (but note there may not be any presorted path). 
     */
    bool has_groupby = true;

    /* First of all, estimate the number of groups in the query. */
    has_groupby = get_number_of_groups(subroot, 
                                       final_rel, 
                                       dNumGroups);

    /* Then update the tuple_fraction by the number of groups in the query. */
    update_tuple_fraction(subroot, 
                          final_rel, 
                          dNumGroups);

    /* Partition table optimization */
    if (subroot->sort_pathkeys) {
        get_pathkeys_for_partiteratorpath(final_rel, (Expr*)mminfo->target);
    }

    /* 
     * Finally, generate the best unsorted and presorted paths for 
     * this Query. 
     */
    generate_cheapest_and_sorted_path(subroot, 
                                      final_rel,
                                      &cheapest_path, 
                                      &sorted_path, 
                                      dNumGroups, 
                                      has_groupby);


    /*
     * Fail if no presorted path.  However, if query_planner determines that
     * the presorted path is also the cheapest, it will set sorted_path to
     * NULL ... don't be fooled.  (This is kind of a pain here, but it
     * simplifies life for grouping_planner, so leave it be.)
     */
    if (sorted_path == NULL) {
        if (cheapest_path && pathkeys_contained_in(subroot->sort_pathkeys, cheapest_path->pathkeys))
            sorted_path = cheapest_path;
        else
            return false;
    }

    /*
     * Determine cost to get just the first row of the presorted path.
     *
     * Note: cost calculation here should
     * match compare_fractional_path_costs().
     */
    if (RELOPTINFO_LOCAL_FIELD(subroot, sorted_path->parent, rows) > 1.0)
        path_fraction = 1.0 / RELOPTINFO_LOCAL_FIELD(subroot, sorted_path->parent, rows);
    else
        path_fraction = 1.0;

    path_cost = sorted_path->startup_cost + path_fraction * (sorted_path->total_cost - sorted_path->startup_cost);

    /* Save state for further processing */
    mminfo->subroot = subroot;
    mminfo->path = sorted_path;
    mminfo->pathcost = path_cost;

    return true;
}

/*
 * Construct a suitable plan for a converted aggregate query
 */
static Plan* make_agg_subplan(PlannerInfo* root, MinMaxAggInfo* mminfo)
{
    PlannerInfo* subroot = mminfo->subroot;
    Query* subparse = subroot->parse;
    Plan* plan = NULL;

    /*
     * Generate the plan for the subquery. We already have a Path, but we have
     * to convert it to a Plan and attach a LIMIT node above it.
     */
    plan = create_plan(subroot, mminfo->path);

    plan->targetlist = subparse->targetList;

    /* For partition table, we should pass targetlist down to base table scan */
    if (IsA(plan, PartIterator)) {
        plan->lefttree->targetlist = plan->targetlist;
    }
    if (IsA(mminfo->path, PartIteratorPath)) {
        PartIterator *partItr = NULL;
        if (IsA(plan, BaseResult)) {
            partItr = (PartIterator *)((BaseResult*)plan)->plan.lefttree;
        } else if (IsA(plan, PartIterator)) {
            partItr = (PartIterator *)plan;
        } else if (IsPlanForPartitionScan(plan) && ((Scan*)plan)->partition_iterator_elimination) {
            plan = (Plan*)plan; //do nothing
        } else {
            ereport(
                ERROR,
                (errmodule(MOD_OPT), errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                 (errmsg("For this type of plan, the min/max optimization cannot be performed on the partition table."),
                  errdetail("N/A."), errcause("System error."), erraction("Contact engineer to support."))));
        }
        PartIteratorPath* itrpath = (PartIteratorPath*)mminfo->path;
        if (itrpath->needSortNode) {
            Node *limitCount =
                (Node *)makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(1), false, FLOAT8PASSBYVAL);
            Plan *limit_plan = (Plan *)make_limit(subroot, partItr->plan.lefttree, NULL, limitCount, 0, 1);
            partItr->plan.lefttree = limit_plan;
            plan = (Plan *)make_sort_from_pathkeys(subroot, plan, subroot->sort_pathkeys, subroot->limit_tuples);
        }
    }

    plan = (Plan*)make_limit(root, plan, subparse->limitOffset, subparse->limitCount, 0, 1);
#ifdef ENABLE_MULTIPLE_NODES
    /* there is only one result, single node will not choose smp, no need to add agg */
    if (IS_STREAM_PLAN) {
        if (is_execute_on_coordinator(plan) || is_execute_on_allnodes(plan)) {
            // local case or should we assert
        } else {
            bool stream_added = true;
            // all other case, broadcast plan to all nodes,  get all result back and do final agg at the coordinator.
            // The final aggregate can also be done in any single node that's going to consume the results, but do that
            // we need to know the interested partition of final plan which we don't have it yet. One way to do this, is
            // to mark with floating target node (special node group of any_signle_node) and modify it whenever it
            // becomes known. Also add a final pass at the end of optimizer to convert floating partition to known
            // interested partition or coordinator which is always interested because all results should come to
            // coordinator before returning to client.
            if (root->query_level == 1) {
                plan = make_simple_RemoteQuery(plan, root, true);
            } else if (!is_replicated_plan(plan)) {
                Distribution* distribution = ng_get_correlated_subplan_group_distribution();
                plan = make_stream_plan(root, plan, NIL, 1.0, distribution);
            } else
                stream_added = false;

            if (stream_added) {
                Plan* aggplan = NULL;
                AggClauseCosts dummy_aggcosts;
                errno_t errorno = memset_s(&dummy_aggcosts, sizeof(AggClauseCosts), 0, sizeof(AggClauseCosts));
                securec_check(errorno, "\0", "\0");

                // Ideally we should call make_agg, but this case is a bit special in handling the the tlist that's not
                // covered in make_agg shold clean up make agg. a bit.
                TargetEntry* tle =
                    makeTargetEntry((Expr*)copyObject(mminfo->aggref), (AttrNumber)1, pstrdup("agg_target"), false);

                Agg* aggnode = makeNode(Agg);
                aggnode->aggstrategy = AGG_PLAIN;
                aggnode->numCols = 0;
                aggnode->grpColIdx = NULL;
                aggnode->grpOperators = NULL;
                aggnode->numGroups = 1;
                aggplan = &aggnode->plan;

                inherit_plan_locator_info((Plan*)aggnode, plan);

                aggplan->plan_rows = 1;
                aggplan->plan_width = plan->plan_width;
                Path agg_path;
                errno_t rc = EOK;
                rc = memset_s(&agg_path, sizeof(Path), 0, sizeof(Path));
                securec_check(rc, "\0", "\0");
                Distribution* distribution = ng_get_dest_distribution(plan);
                ng_copy_distribution(&agg_path.distribution, distribution);
                cost_agg(&agg_path,
                    root,
                    AGG_PLAIN,
                    &dummy_aggcosts,
                    0,
                    1,
                    plan->startup_cost,
                    plan->total_cost,
                    PLAN_LOCAL_ROWS(plan));
                aggplan->startup_cost = agg_path.startup_cost;
                aggplan->total_cost = agg_path.total_cost;

                aggplan->qual = NULL;
                aggplan->targetlist = list_make1(tle);
                aggplan->lefttree = plan;
                aggplan->righttree = NULL;

                plan = (Plan*)aggplan;
            }
        }
    }

    if (g_instance.attr.attr_common.enable_tsdb) {
        plan = tsdb_modifier(root, plan->targetlist, plan, subroot);
    }
#endif

    /*
     * Convert the plan into an InitPlan, and make a Param for its result.
     */
    mminfo->param = SS_make_initplan_from_plan(
        subroot, plan, exprType((Node*)mminfo->target), -1, exprCollation((Node*)mminfo->target));

    /*
     * Make sure the initplan gets into the outer PlannerInfo, along with any
     * other initplans generated by the sub-planning run.  We had to include
     * the outer PlannerInfo's pre-existing initplans into the inner one's
     * init_plans list earlier, so make sure we don't put back any duplicate
     * entries.
     */
    root->init_plans = list_concat_unique_ptr(root->init_plans, subroot->init_plans);
    return plan;
}

/*
 * Replace original aggregate calls with subplan output Params
 */
static Node* replace_aggs_with_params_mutator(Node* node, PlannerInfo* root)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Aggref)) {
        Aggref* aggref = (Aggref*)node;
        TargetEntry* curTarget = (TargetEntry*)linitial(aggref->args);
        ListCell* lc = NULL;

        foreach (lc, root->minmax_aggs) {
            MinMaxAggInfo* mminfo = (MinMaxAggInfo*)lfirst(lc);

            if (mminfo->aggfnoid == aggref->aggfnoid && equal(mminfo->target, curTarget->expr))
                return (Node*)mminfo->param;
        }
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("failed to re-find MinMaxAggInfo record")));
    }
    AssertEreport(!IsA(node, SubLink),
        MOD_OPT,
        "invalid node type when replaceing original aggregate calls with subplan output Params.");
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) replace_aggs_with_params_mutator, (void*)root);
}

/*
 * Get the OID of the sort operator, if any, associated with an aggregate.
 * Returns InvalidOid if there is no such operator.
 */
static Oid fetch_agg_sort_op(Oid aggfnoid)
{
    HeapTuple aggTuple;
    Form_pg_aggregate aggform;
    Oid aggsortop;

    /* fetch aggregate entry from pg_aggregate */
    aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
    if (!HeapTupleIsValid(aggTuple))
        return InvalidOid;
    aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);
    aggsortop = aggform->aggsortop;
    ReleaseSysCache(aggTuple);

    return aggsortop;
}

/*
 * @Description: Judge this agg if is Min/Max.
 * @in aggref - agg struct
 * @out strategy - return BTGreaterStrategyNumber or BTLessStrategyNumber,
 *      BTGreaterStrategyNumber is mean Max, BTLessStrategyNumber is mean Min.
 * @return - true is mean agg is Min/Max, false not.
 */
bool check_agg_optimizable(Aggref* aggref, int16* strategy)
{
    TargetEntry* curTarget = NULL;
    Oid opfamily = InvalidOid;
    Oid opcintype = InvalidOid;
    Oid aggsortop = InvalidOid;

    /* not a MIN/MAX aggregate. */
    if (list_length(aggref->args) != 1 || aggref->aggorder != NIL) {
        return false;
    }

    curTarget = (TargetEntry*)linitial(aggref->args);
    /* Parameter of agg only can be var type, and can not be system column. */
    if (!IsA(curTarget->expr, Var) || ((Var*)(curTarget->expr))->varattno <= 0) {
        return false;
    }

    Oid var_oid = ((Var*)(curTarget->expr))->vartype;

    if (MINMAXOPTIMIZATIONOID(var_oid)) {
        return false;
    }

    aggsortop = fetch_agg_sort_op(aggref->aggfnoid);
    /* not a MIN/MAX aggregate. */
    if (!OidIsValid(aggsortop)) {
        return false;
    }

    /* see system table pg_amop, out put strategy sign this is min or max. */
    if (!get_ordering_op_properties(aggsortop, &opfamily, &opcintype, strategy)) {
        return false;
    }

    return true;
}

