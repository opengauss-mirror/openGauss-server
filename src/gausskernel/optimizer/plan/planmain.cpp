/* -------------------------------------------------------------------------
 *
 * planmain.cpp
 *	  Routines to plan a single query
 *
 * What's in a name, anyway?  The top-level entry point of the planner/
 * optimizer is over in planner.c, not here as you might think from the
 * file name.  But this is the main code for planning a basic join operation,
 * shorn of features like subselects, inheritance, aggregates, grouping,
 * and so on.  (Those are the things planner.c deals with.)
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/planmain.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"
#include "nodes/print.h"
#include "parser/parse_hint.h"
#include "pgxc/pgxc.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/randomplan.h"
#include "optimizer/tlist.h"
#include "utils/selfuncs.h"

/* Local functions */
static void debug_print_log(PlannerInfo* root, Path* sortedpath, int debug_log_level);

/*
 * query_planner
 *	  Generate a path (that is, a simplified plan) for a basic query,
 *	  which may involve joins but not any fancier features.
 *
 * Since query_planner does not handle the toplevel processing (grouping,
 * sorting, etc) it cannot select the best path by itself.  Instead, it
 * returns the RelOptInfo for the top level of joining, and the caller
 * (grouping_planner) can choose among the surviving paths for the rel.
 *
 * Input parameters:
 * root describes the query to plan
 * tlist is the target list the query should produce
 *		(this is NOT necessarily root->parse->targetList!)
 *
 * Output parameters:
 * final_rel contains the RelOptInfo for the top level of joining
 *
 * Note: the PlannerInfo node also includes a query_pathkeys field, which is
 * both an input and an output of query_planner().	The input value signals
 * query_planner that the indicated sort order is wanted in the final output
 * plan.  But this value has not yet been "canonicalized", since the needed
 * info does not get computed until we scan the qual clauses.  We canonicalize
 * it as soon as that task is done.  (The main reason query_pathkeys is a
 * PlannerInfo field and not a passed parameter is that the low-level routines
 * in indxpath.c need to see it.)
 *
 * Note: the PlannerInfo node includes other pathkeys fields besides
 * query_pathkeys, all of which need to be canonicalized once the info is
 * available.  See canonicalize_all_pathkeys.
 *
 */
RelOptInfo* query_planner(PlannerInfo* root, List* tlist,
             query_pathkeys_callback qp_callback, void *qp_extra)
{
    Query* parse = root->parse;
    List* joinlist = NIL;
    RelOptInfo* final_rel = NULL;
    Index rti;
    double total_pages;

    /*
     * Init planner lists to empty.
     *
     * NOTE: append_rel_list was set up by subquery_planner, so do not touch
     * here; eq_classes and minmax_aggs may contain data already, too.
     */
    root->join_rel_list = NIL;
    root->join_rel_hash = NULL;
    root->join_rel_level = NULL;
    root->join_cur_level = 0;
    root->canon_pathkeys = NIL;
    root->left_join_clauses = NIL;
    root->right_join_clauses = NIL;
    root->full_join_clauses = NIL;
    root->join_info_list = NIL;
    root->lateral_info_list = NIL;
    root->placeholder_list = NIL;
    root->initial_rels = NIL;

    /*
     * Make a flattened version of the rangetable for faster access (this is
     * OK because the rangetable won't change any more), and set up an empty
     * array for indexing base relations.
     */
    setup_simple_rel_arrays(root);

    /*
     * In the trivial case where the jointree is a single RTE_RESULT relation,
     * bypass all the rest of this function and just make a RelOptInfo and its
     * one access path.  This is worth optimizing because it applies for
     * common cases like "SELECT expression" and "INSERT ... VALUES()".
     */
    Assert(parse->jointree->fromlist != NIL);
    if (list_length(parse->jointree->fromlist) == 1) {
        Node       *jtnode = (Node *) linitial(parse->jointree->fromlist);

        if (IsA(jtnode, RangeTblRef)) {
            int          varno = ((RangeTblRef *) jtnode)->rtindex;
            RangeTblEntry *rte = root->simple_rte_array[varno];

            Assert(rte != NULL);
            if (rte->rtekind == RTE_RESULT) {
                /* Make the RelOptInfo for it directly */
                final_rel = build_simple_rel(root, varno, RELOPT_BASEREL);

                /*
                 * The only path for it is a trivial Result path.  We cheat a
                 * bit here by using a GroupResultPath, because that way we
                 * can just jam the quals into it without preprocessing them.
                 * (But, if you hold your head at the right angle, a FROM-less
                 * SELECT is a kind of degenerate-grouping case, so it's not
                 * that much of a cheat.)
                 */
                add_path(root, final_rel, (Path *) create_result_path(root, final_rel,
                         (List *) parse->jointree->quals, NULL, NULL));

                /* Select cheapest path (pretty easy in this case...) */
                set_cheapest(final_rel);

                /*
                 * We still are required to call qp_callback, in case it's
                 * something like "SELECT 2+2 ORDER BY 1".
                 */
                (*qp_callback) (root, qp_extra);

                return final_rel;
            }
        }
    }

    /*
     * Construct RelOptInfo nodes for all base relations in query, and
     * indirectly for all appendrel member relations ("other rels").  This
     * will give us a RelOptInfo for every "simple" (non-join) rel involved in
     * the query.
     *
     * Note: the reason we find the rels by searching the jointree and
     * appendrel list, rather than just scanning the rangetable, is that the
     * rangetable may contain RTEs for rels not actively part of the query,
     * for example views.  We don't want to make RelOptInfos for them.
     */
    add_base_rels_to_query(root, (Node*)parse->jointree);
    check_scan_hint_validity(root);

    /*
     * Examine the targetlist and join tree, adding entries to baserel
     * targetlists for all referenced Vars, and generating PlaceHolderInfo
     * entries for all referenced PlaceHolderVars.	Restrict and join clauses
     * are added to appropriate lists belonging to the mentioned relations. We
     * also build EquivalenceClasses for provably equivalent expressions. The
     * SpecialJoinInfo list is also built to hold information about join order
     * restrictions.  Finally, we form a target joinlist for make_one_rel() to
     * work from.
     */
    build_base_rel_tlists(root, tlist);

    find_placeholders_in_jointree(root);

    find_lateral_references(root);

    joinlist = deconstruct_jointree(root);

    process_security_clause_appendrel(root);

    /*
     * Create the LateralJoinInfo list now that we have finalized
     * PlaceHolderVar eval levels.
     */
    create_lateral_join_info(root);

    /*
     * Reconsider any postponed outer-join quals now that we have built up
     * equivalence classes.  (This could result in further additions or
     * mergings of classes.)
     */
    reconsider_outer_join_clauses(root);

    /*
     * If we formed any equivalence classes, generate additional restriction
     * clauses as appropriate.	(Implied join clauses are formed on-the-fly
     * later.)
     */
    generate_base_implied_equalities(root);

    generate_base_implied_qualities(root);

    /*
     * We have completed merging equivalence sets, so it's now possible to
     * generate pathkeys in canonical form; so compute query_pathkeys and
     * other pathkeys fields in PlannerInfo.
     */
    (*qp_callback) (root, qp_extra);

    /*
     * Examine any "placeholder" expressions generated during subquery pullup.
     * Make sure that the Vars they need are marked as needed at the relevant
     * join level.	This must be done before join removal because it might
     * cause Vars or placeholders to be needed above a join when they weren't
     * so marked before.
     */
    fix_placeholder_input_needed_levels(root);

    /*
     * Remove any useless outer joins.	Ideally this would be done during
     * jointree preprocessing, but the necessary information isn't available
     * until we've built baserel data structures and classified qual clauses.
     */
    joinlist = remove_useless_joins(root, joinlist);

    /*
     * Now distribute "placeholders" to base rels as needed.  This has to be
     * done after join removal because removal could change whether a
     * placeholder is evaluatable at a base rel.
     */
    add_placeholders_to_base_rels(root);

    /*
     * We should now have size estimates for every actual table involved in
     * the query, and we also know which if any have been deleted from the
     * query by join removal; so we can compute total_table_pages.
     *
     * Note that appendrels are not double-counted here, even though we don't
     * bother to distinguish RelOptInfos for appendrel parents, because the
     * parents will still have size zero.
     *
     * XXX if a table is self-joined, we will count it once per appearance,
     * which perhaps is the wrong thing ... but that's not completely clear,
     * and detecting self-joins here is difficult, so ignore it for now.
     */
    total_pages = 0;
    for (rti = 1; rti < (unsigned int)root->simple_rel_array_size; rti++) {
        RelOptInfo* brel = root->simple_rel_array[rti];

        if (brel == NULL)
            continue;

        AssertEreport(brel->relid == rti,
            MOD_OPT,
            "invalid relation oid when generating a path for a basic query."); /* sanity check on array */

        if (brel->reloptkind == RELOPT_BASEREL || brel->reloptkind == RELOPT_OTHER_MEMBER_REL)
            total_pages += (double)brel->pages;
    }
    root->total_table_pages = total_pages;

    /*
     * Ready to do the primary planning.
     */
    final_rel = make_one_rel(root, joinlist);

    if (final_rel == NULL || final_rel->cheapest_total_path == NIL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("failed to construct the join relation")));
    }
    /* cheapest_total_path should not exist para_info */
    ListCell *lc = NULL;
    foreach(lc, final_rel->cheapest_total_path) {
        Path *judge_path = (Path *)lfirst(lc);
        if (judge_path->param_info != NULL && PATH_REQ_OUTER(judge_path) != NULL) {
            ereport(ERROR,
                    (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("cheapest_total_path should not exist para_info")));
        }
    }
    return final_rel;

}

/*
 * get_number_of_groups
 *	  Estimate the number of groups in the query.
 *
 * Since query_planner does not handle the toplevel processing (grouping,
 * sorting, etc) it cannot select the best path by itself.  Instead, it
 * returns the RelOptInfo for the top level of joining, and the caller
 * (grouping_planner) can choose among the surviving paths for the rel.
 *
 * Input parameters:
 * root describes the query to plan,
 * final_rel contains the RelOptInfo for the top level of joining,
 * rollup_groupclauses describes GROUP BY elements (which in itself is semantically 
 * insignificant) after adjusting the ordering to match ORDER BY,
 * rollup_lists describes reorder grouping sets of the query.
 *
 * Output parameters:
 * has_groupby recieves if it has a groupby,
 * *num_groups receives the estimated number of groups, or 1 if query 
 * does not use grouping.
 */
bool get_number_of_groups(PlannerInfo* root, 
                                RelOptInfo* final_rel, 
                                double* num_groups,
                                List* rollup_groupclauses, 
                                List* rollup_lists)
{
    Query* parse = root->parse;

    /* local distinct and global distinct */
    double numdistinct[2] = {1, 1};
    bool has_groupby = true;

    unsigned int num_datanodes = ng_get_dest_num_data_nodes(root, final_rel);

    /*
     * If there's grouping going on, estimate the number of result groups. We
     * couldn't do this any earlier because it depends on relation size
     * estimates that were set up above.
     */
    if (parse->groupClause) {
        List* groupExprs = NIL;
        if (parse->groupingSets) {
            ListCell* lc = NULL;
            ListCell* lc2 = NULL;

            num_groups[0] = num_groups[1] = 0;

            forboth(lc, rollup_groupclauses, lc2, rollup_lists) {
                ListCell* lc3 = NULL;

                groupExprs = get_sortgrouplist_exprs((List*)lfirst(lc), parse->targetList);

                foreach (lc3, (List*)lfirst(lc2)) {
                    List* gset = (List*)lfirst(lc3);

                    get_num_distinct(root,
                        groupExprs,
                        RELOPTINFO_LOCAL_FIELD(root, final_rel, rows),
                        final_rel->rows,
                        num_datanodes,
                        numdistinct,
                        &gset);
                    num_groups[0] += numdistinct[0];
                    num_groups[1] += numdistinct[1];
                }
            }
        } else {
            groupExprs = get_sortgrouplist_exprs(parse->groupClause, parse->targetList);

            get_num_distinct(root,
                groupExprs,
                RELOPTINFO_LOCAL_FIELD(root, final_rel, rows),
                final_rel->rows,
                num_datanodes,
                numdistinct,
                NULL);
            num_groups[0] = numdistinct[0];
            num_groups[1] = numdistinct[1];
        }

    } else if (parse->hasAggs || root->hasHavingQual || parse->groupingSets) {
        /*
         * Ungrouped aggregate will certainly want to read all the tuples,
         * and it will deliver a single result row per grouping set (or 1
         * if no grouping sets were explicitly given, in which case leave
         * dNumGroups as-is).
         * For example, group by grouping sets((), (), ())
         */
        if (parse->groupingSets) {
            num_groups[0] = list_length(parse->groupingSets);
            num_groups[1] = get_global_rows(num_groups[0], 1.0, ng_get_dest_num_data_nodes(root, final_rel));
        }
    } else if (parse->distinctClause) {
        /*
         * Since there was no grouping or aggregation, it's reasonable to
         * assume the UNIQUE filter has effects comparable to GROUP BY. Return
         * the estimated number of output rows for use by caller. (If DISTINCT
         * is used with grouping, we ignore its effects for rowcount
         * estimation purposes; this amounts to assuming the grouped rows are
         * distinct already.)
         */
        List* distinctExprs = NIL;

        distinctExprs = get_sortgrouplist_exprs(parse->distinctClause, parse->targetList);
        get_num_distinct(root,
            distinctExprs,
            RELOPTINFO_LOCAL_FIELD(root, final_rel, rows),
            final_rel->rows,
            num_datanodes,
            numdistinct,
            NULL);

        num_groups[0] = numdistinct[0];
        num_groups[1] = numdistinct[1];

    } else {
        has_groupby = false;
    }
    return has_groupby;
}

/*
 * update_tuple_fraction
 *	  Update the tuple_fraction by the number of groups in the query.
 *
 * Input parameters:
 * root describes the query to plan,
 * final_rel contains the RelOptInfo for the top level of joining,
 * num_groups describes the estimated number of groups, or 1 if query 
 * does not use grouping.
 * 
 * tuple_fraction is interpreted as follows:
 *    0: expect all tuples to be retrieved (normal case)
 *    0 < tuple_fraction < 1: expect the given fraction of tuples available
 *      from the plan to be retrieved
 *    tuple_fraction >= 1: tuple_fraction is the absolute number of tuples
 *      expected to be retrieved (ie, a LIMIT specification)
 * Note that a nonzero tuple_fraction could come from outer context; it is
 * therefore not redundant with limit_tuples.  We use limit_tuples to determine
 * whether a bounded sort can be used at runtime.
 */
void update_tuple_fraction(PlannerInfo* root, 
                                 RelOptInfo* final_rel, 
                                 double* num_groups)
{
    Query* parse = root->parse;
    double tuple_fraction = root->tuple_fraction;
    double limit_tuples = root->limit_tuples;

    /*
     * If there's grouping going on, convert tuple_fraction to fractional 
     * form if it is absolute, and adjust it based on the knowledge that 
     * grouping_planner will be doing grouping or aggregation work with 
     * our result.
     *
     * This introduces some undesirable coupling between this code and
     * grouping_planner, but the alternatives seem even uglier; we couldn't
     * pass back completed paths without making these decisions here.
     */
    if (parse->groupClause) {
        /*
         * In GROUP BY mode, an absolute LIMIT is relative to the number of
         * groups not the number of tuples.  If the caller gave us a fraction,
         * keep it as-is.  (In both cases, we are effectively assuming that
         * all the groups are about the same size.)
         */
        if (tuple_fraction >= 1.0) {
            tuple_fraction /= (double)num_groups[0];
        }

        /*
         * If both GROUP BY and ORDER BY are specified, we will need two
         * levels of sort --- and, therefore, certainly need to read all the
         * tuples --- unless ORDER BY is a subset of GROUP BY.	Likewise if we
         * have both DISTINCT and GROUP BY, or if we have a window
         * specification not compatible with the GROUP BY.
         */
        if (!pathkeys_contained_in(root->sort_pathkeys, root->group_pathkeys) ||
            !pathkeys_contained_in(root->distinct_pathkeys, root->group_pathkeys) ||
            !pathkeys_contained_in(root->window_pathkeys, root->group_pathkeys))
            tuple_fraction = 0.0;

        /* In any case, limit_tuples shouldn't be specified here */
        AssertEreport(limit_tuples < 0,
            MOD_OPT,
            "invalid limit tuples when estimating the number of result groups in grouping process.");
    } else if (parse->hasAggs || root->hasHavingQual || parse->groupingSets) {
        /*
         * Ungrouped aggregate will certainly want to read all the tuples,
         * thus set tuple_fraction to 0.
         */
        tuple_fraction = 0.0;
    } else if (parse->distinctClause) {
        /*
         * Adjust tuple_fraction the same way as for GROUP BY, too.
         */
        if (tuple_fraction >= 1.0) {
            tuple_fraction /= num_groups[0];
        }

        /* limit_tuples shouldn't be specified here */
        AssertEreport(limit_tuples < 0,
            MOD_OPT,
            "invalid limit tuples when estimating the number of output rows using DISTINCT.");
    } else {
        /*
         * Plain non-grouped, non-aggregated query: an absolute tuple fraction
         * can be divided by the number of tuples.
         */
        if (tuple_fraction >= 1.0)
            tuple_fraction /= clamp_row_est(RELOPTINFO_LOCAL_FIELD(root, final_rel, rows));
    }

    root->tuple_fraction = tuple_fraction;
    root->limit_tuples = limit_tuples;
}

/*
 * generate_cheapest_and_sorted_path
 *	  Generate the best unsorted and presorted paths for this Query.
 *
 * Input parameters:
 * root describes the query to plan,
 * final_rel contains the RelOptInfo for the top level of joining,
 * num_groups describes the estimated number of groups, or 1 if query 
 * does not use grouping,
 * has_groupby describes if it has a groupby.
 *
 * Output parameters:
 * *cheapest_path receives the overall-cheapest path for the query
 * *sorted_path receives the cheapest presorted path for the query,
 *				if any (NULL if there is no useful presorted path).
 */
void generate_cheapest_and_sorted_path(PlannerInfo* root, 
                                                 RelOptInfo* final_rel,
                                                 Path** cheapest_path, 
                                                 Path** sorted_path, 
                                                 double* num_groups, 
                                                 bool has_groupby)
{
    Path* cheapestpath = NULL;
    Path* sortedpath = NULL;
    double tuple_fraction = root->tuple_fraction;
    double limit_tuples = root->limit_tuples;

    /*
     * Pick out the cheapest-total path and the cheapest presorted path for
     * the requested pathkeys (if there is one).  We should take the tuple
     * fraction into account when selecting the cheapest presorted path, but
     * not when selecting the cheapest-total path, since if we have to sort
     * then we'll have to fetch all the tuples.  (But there's a special case:
     * if query_pathkeys is NIL, meaning order doesn't matter, then the
     * "cheapest presorted" path will be the cheapest overall for the tuple
     * fraction.)
     *
     * The cheapest-total path is also the one to use if grouping_planner
     * decides to use hashed aggregation, so we return it separately even if
     * this routine thinks the presorted path is the winner.
     */
    cheapestpath = get_cheapest_path(root, final_rel, num_groups, has_groupby);

    /*
     * For these cases, we don't need sorted path:
     * 	(1) random plan; (2) subplan; (3) global path
     */
    if (OPTIMIZE_PLAN != u_sess->attr.attr_sql.plan_mode_seed ||
        (root->parent_root != NULL && root->parent_root->plan_params != NIL) ||
        cheapestpath != linitial(final_rel->cheapest_total_path))
        sortedpath = NULL;
    else
        sortedpath =
            get_cheapest_fractional_path_for_pathkeys(final_rel->pathlist, root->query_pathkeys, NULL, tuple_fraction);

    /* Don't return same path in both guises; just wastes effort */
    if (sortedpath == NULL || sortedpath == cheapestpath || sortedpath->hint_value < cheapestpath->hint_value) {
        sortedpath = NULL;
    }

    /*
     * Forget about the presorted path if it would be cheaper to sort the
     * cheapest-total path.  Here we need consider only the behavior at the
     * tuple fraction point.
     */
    if (sortedpath != NULL) {
        Path sort_path; /* dummy for result of cost_sort */

        if (root->query_pathkeys == NIL || pathkeys_contained_in(root->query_pathkeys, cheapestpath->pathkeys)) {
            /* No sort needed for cheapest path */
            sort_path.startup_cost = cheapestpath->startup_cost;
            sort_path.total_cost = cheapestpath->total_cost;
        } else {
            /* Figure cost for sorting */
            cost_sort(&sort_path,
                root->query_pathkeys,
                cheapestpath->total_cost,
                RELOPTINFO_LOCAL_FIELD(root, final_rel, rows),
                final_rel->width,
                0.0,
                u_sess->opt_cxt.op_work_mem,
                limit_tuples,
                root->glob->vectorized);
        }

        if (compare_fractional_path_costs(sortedpath, &sort_path, tuple_fraction) > 0) {
            /* Presorted path is a loser */
            debug_print_log(root, sortedpath, DEBUG2);
            sortedpath = NULL;
        }
    }

    *cheapest_path = cheapestpath;
    *sorted_path = sortedpath;
}

static void debug_print_log(PlannerInfo* root, Path* sortedpath, int debug_log_level)
{
    if (log_min_messages > debug_log_level)
        return;

    ereport(debug_log_level,
            (errmodule(MOD_OPT),
                (errmsg("Presorted path is not accepted with cost = %lf .. %lf",
                        sortedpath->startup_cost,
                        sortedpath->total_cost))));

    /* print more details */
    debug1_print_new_path(root, sortedpath, false);
}
