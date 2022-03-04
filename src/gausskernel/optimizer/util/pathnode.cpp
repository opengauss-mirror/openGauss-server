/* -------------------------------------------------------------------------
 *
 * pathnode.cpp
 *	  Routines to manipulate pathlists and create path nodes
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/pathnode.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "bulkload/foreignroutine.h"
#include "catalog/pg_statistic.h"
#include "commands/copy.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/dataskew.h"
#include "optimizer/nodegroups.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/pruning.h"
#include "optimizer/randomplan.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"
#ifdef PGXC
#include "commands/tablecmds.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "pgxc/pgxc.h"
#endif /* PGXC */

static bool is_itst_path(PlannerInfo* root, RelOptInfo* rel, Path* path);
static List* translate_sub_tlist(List* tlist, int relid);
static bool check_join_method_alternative(
    List* restrictlist, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype, bool* try_eq_related_indirectly);

#ifdef STREAMPLAN
static void mark_append_path(PlannerInfo* root, RelOptInfo* rel, Path* pathnode, List* subpaths);
static bool is_ec_usable_for_join(
    Relids suitable_relids, EquivalenceClass* suitable_ec, Node* diskey, Expr* join_clause, bool is_left);
static List* get_otherside_key(
    PlannerInfo* root, List* rinfo, List* targetlist, RelOptInfo* otherside_rel, double* skew_multiple);
static void add_hashjoin_broadcast_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors,
    Path* need_stream_path, Path* non_stream_path, List* restrictlist, Relids required_outer, List* hashclauses,
    bool is_replicate, bool stream_outer, Distribution* target_distribution = NULL, ParallelDesc* need_smpDesc = NULL,
    ParallelDesc* non_smpDesc = NULL, int dop = 1);
static void add_nestloop_broadcast_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors,
    Path* need_stream_path, Path* non_stream_path, List* restrict_clauses, List* pathkeys, Relids required_outer,
    List* stream_pathkeys, bool is_replicate, bool stream_outer, Distribution* target_distribution = NULL,
    ParallelDesc* need_smpDesc = NULL, ParallelDesc* non_smpDesc = NULL, int dop = 1);
static void add_mergejoin_broadcast_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, Path* need_stream_path,
    Path* non_stream_path, List* restrict_clauses, List* pathkeys, Relids required_outer, List* mergeclauses,
    List* outersortkeys, List* innersortkeys, List* stream_pathkeys, List* non_stream_pathkeys, bool is_replicate,
    bool stream_outer, Distribution* target_distribution = NULL);
#endif

/*****************************************************************************
 *		MISC. PATH UTILITIES
 *****************************************************************************/
/*
 * compare_path_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for the specified criterion.
 */
int compare_path_costs(Path* path1, Path* path2, CostSelector criterion)
{
    if (criterion == STARTUP_COST) {
        if (path1->startup_cost < path2->startup_cost)
            return -1;
        if (path1->startup_cost > path2->startup_cost)
            return +1;

        /*
         * If paths have the same startup cost (not at all unlikely), order
         * them by total cost.
         */
        if (path1->total_cost < path2->total_cost)
            return -1;
        if (path1->total_cost > path2->total_cost)
            return +1;
    } else {
        if (path1->total_cost < path2->total_cost)
            return -1;
        if (path1->total_cost > path2->total_cost)
            return +1;

        /*
         * If paths have the same total cost, order them by startup cost.
         */
        if (path1->startup_cost < path2->startup_cost)
            return -1;
        if (path1->startup_cost > path2->startup_cost)
            return +1;
    }
    return 0;
}

/*
 * compare_path_fractional_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for fetching the specified fraction
 *	  of the total tuples.
 *
 * If fraction is <= 0 or > 1, we interpret it as 1, ie, we select the
 * path with the cheaper total_cost.
 */
int compare_fractional_path_costs(Path* path1, Path* path2, double fraction)
{
    Cost cost1, cost2;

    if (fraction <= 0.0 || fraction >= 1.0)
        return compare_path_costs(path1, path2, TOTAL_COST);
    cost1 = path1->startup_cost + fraction * (path1->total_cost - path1->startup_cost);
    cost2 = path2->startup_cost + fraction * (path2->total_cost - path2->startup_cost);
    if (cost1 < cost2)
        return -1;
    if (cost1 > cost2)
        return +1;
    return 0;
}

Path* get_real_path(Path* path) 
{
    /*
     * stream_side_path will added Material above stream
     */
    if (path->pathtype == T_Material) {
        path = ((MaterialPath*)path)->subpath;
    } else if (path->pathtype == T_Unique) {
        path = ((UniquePath*)path)->subpath;
    }
    return path;
}

/*
 * Plan with single node distribution and it's one child is stream path.
 */
bool is_dngather_child_shuffle_path(Path *path) 
{
    if (!IsA(path, StreamPath)) {
        return false;
    }    

    StreamPath *stream_path = (StreamPath *)path;
    return ng_is_single_node_group_distribution(&stream_path->consumer_distribution);
}

/*
 * Plan with single node distribution and it's one child is non-stream path.
 */
bool is_dngather_child_local_path(Path *path) 
{
    if (IsA(path, StreamPath)) {
        return false;
    }    

    return ng_is_single_node_group_distribution(&path->distribution);
}

/*
 * Check whether the join path is fitted for dngather. 
 */
bool is_join_path_fit_dngather(Path* path) 
{
    JoinPath *joinPath = (JoinPath*)path;
    Path* innerJoinPath = get_real_path(joinPath->innerjoinpath);
    Path* outerJoinPath = get_real_path(joinPath->outerjoinpath);

    double dnGatherUpperRows = u_sess->attr.attr_sql.dngather_min_rows;
    if (joinPath->path.rows > dnGatherUpperRows
        || innerJoinPath->rows > dnGatherUpperRows
        || outerJoinPath->rows > dnGatherUpperRows) {
        return false;
    }

    return ng_is_single_node_group_distribution(&(path->distribution));
}

/*
 * Get result whether join's children are all stream.
 */
bool is_join_path_all_children_shuffle(Path* path) 
{
    JoinPath *joinPath = (JoinPath *)path;
    Path* innerJoinPath = get_real_path(joinPath->innerjoinpath);
    Path* outerJoinPath = get_real_path(joinPath->outerjoinpath);
    if (IsA(innerJoinPath, StreamPath) && IsA(outerJoinPath, StreamPath)) {
        return true;
    }
    return false;
}

/* 
 * Join's single node distribution comparsion function.
 */
PathCostComparison compare_join_single_node_distribution(Path* path1, Path* path2) 
{
    if (!u_sess->attr.attr_sql.enable_dngather || !u_sess->opt_cxt.is_dngather_support
        || u_sess->attr.attr_sql.dngather_min_rows < 0) {
        return COSTS_DIFFERENT;
    }

    if((path1->pathtype != T_NestLoop && path1->pathtype != T_MergeJoin && path1->pathtype != T_HashJoin)
        || (path2->pathtype != T_NestLoop && path2->pathtype != T_MergeJoin && path2->pathtype != T_HashJoin)) {    
        return COSTS_DIFFERENT;
    } 

    bool is_dn_gather1 = is_join_path_fit_dngather(path1);
    bool is_dn_gather2 = is_join_path_fit_dngather(path2);
    bool is_all_children_shuffle1 = is_join_path_all_children_shuffle(path1);
    bool is_all_children_shuffle2 = is_join_path_all_children_shuffle(path1);

    if ((is_dn_gather1 && is_dn_gather2) || (!is_dn_gather1 && !is_dn_gather2)) {
        // Depends on cost estimates after.
        return COSTS_DIFFERENT;
    } else if (is_dn_gather1 && is_all_children_shuffle2) {
        return COSTS_BETTER1;
    } else if (is_dn_gather2 && is_all_children_shuffle1) {
        return COSTS_BETTER2;	
    }

    // Depends on cost estimates after.
    return COSTS_DIFFERENT;
}

inline bool IsSeqScanPath(const Path* path)
{
    return path->pathtype == T_SeqScan;
}

inline bool IsBtreeIndexPath(const Path* path)
{
    return path->type == T_IndexPath && OID_IS_BTREE(((IndexPath*)path)->indexinfo->relam);
}

inline bool AreTwoBtreeIdxPaths(const Path* path1, const Path* path2)
{
    return IsBtreeIndexPath(path1) && IsBtreeIndexPath(path2);
}

inline bool IsBtreeIdxAndSeqPath(const Path* path1, const Path* path2)
{
    return (IsBtreeIndexPath(path1) && IsSeqScanPath(path2)) || 
        (IsBtreeIndexPath(path2) && IsSeqScanPath(path1));
}

inline bool IsParamPath(const Path* path)
{
    return path->param_info != NULL;
}

inline bool BothParamPathOrBothNot(const Path* path1, const Path* path2)
{
    return (IsParamPath(path1) && IsParamPath(path2)) || 
        (!IsParamPath(path1) && !IsParamPath(path2));
}

inline bool ContainUniqueCols(const IndexPath* path)
{
    return path->rulesforindexgen & BTREE_INDEX_CONTAIN_UNIQUE_COLS;
}

/*
 * The main entry for unique index first rule.
 * In this rule, we check two aspects: 
 * 1. For Btree index pathA and pathB, pathA contains a unique btree columns and the constraint 
 * conditions are equality constraints. We prefer pathA.
 * Notice: Only consider unique index first rule when the two index paths are both parameterized path 
 * or both not.
 * 2. For Btree index pathA and SeqScan pathB, pathA contains a unique btree columns and the constraint 
 * conditions are equality constraints. We prefer pathA.
 * Notice: This rule is vaild when Btree index pathA is unparameterized path.
 */
bool ImplementUniqueIndexRule(const Path* path1, const Path* path2, PathCostComparison &cost_comparison)
{
    /* Check the first aspect */
    if (AreTwoBtreeIdxPaths(path1, path2) && BothParamPathOrBothNot(path1, path2)) {
        bool path1ContainUniqueCols = ContainUniqueCols((IndexPath*)path1);
        bool path2ContainUniqueCols = ContainUniqueCols((IndexPath*)path2);
        /* Compare with 1 to verify whether one path satisfy unique index first rule and another don't. */
        if (path1ContainUniqueCols + path2ContainUniqueCols == 1) {
            const int suppressionParam = g_instance.cost_cxt.disable_cost_enlarge_factor;
            if (path1ContainUniqueCols == true && path1->total_cost < suppressionParam * path2->total_cost) {
                cost_comparison = COSTS_BETTER1;
                return true;
            } else if (path2ContainUniqueCols == true && path2->total_cost < suppressionParam * path1->total_cost) {
                cost_comparison = COSTS_BETTER2;
                return true;
            }
        }
        return false;
    }

    /* Check the second aspect */
    if (IsBtreeIdxAndSeqPath(path1, path2)) {
        const int suppressionParam = g_instance.cost_cxt.disable_cost_enlarge_factor;
        if (IsSeqScanPath(path1) && !IsParamPath(path2) && ContainUniqueCols((IndexPath*)path2) && 
            path2->total_cost < suppressionParam * path1->total_cost) {
            cost_comparison = COSTS_BETTER2;           
            return true;
        } else if (IsSeqScanPath(path2) && !IsParamPath(path1) && ContainUniqueCols((IndexPath*)path1) && 
            path1->total_cost < suppressionParam * path2->total_cost) {
            cost_comparison = COSTS_BETTER1;
            return true;
        }
        return false;
    }

    return false;
}

void DebugPrintUniqueIndexFirstInfo(const Path* path1, const Path* path2, const PathCostComparison &cost_comparison)
{
    char* preferIndexName = NULL;
    char* ruledOutIndexName = NULL;

    if (cost_comparison == COSTS_BETTER1) {
        preferIndexName = get_rel_name(((IndexPath*)path1)->indexinfo->indexoid);
        ruledOutIndexName = IsBtreeIndexPath(path2) ? get_rel_name(((IndexPath*)path2)->indexinfo->indexoid) : NULL;
    } else {
        preferIndexName = get_rel_name(((IndexPath*)path2)->indexinfo->indexoid);
        ruledOutIndexName = IsBtreeIndexPath(path1) ? get_rel_name(((IndexPath*)path1)->indexinfo->indexoid) : NULL;
    }
    
    /* ruledOutIndexName = NULL means the ruled out path is a seqscan path. */
    if (ruledOutIndexName == NULL) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT),
                errmsg("Implement Unique Index rule in selecting path: prefer to use index: %s, rule out seqscan.", 
                    preferIndexName)));
    } else {
        ereport(DEBUG1,
            (errmodule(MOD_OPT),
                errmsg("Implement Unique Index rule in selecting path: prefer to use index: %s, rule out index: %s.", 
                    preferIndexName, ruledOutIndexName)));
    }

    pfree_ext(preferIndexName);
    pfree_ext(ruledOutIndexName);
}

/* Check whether unique index first rule can be used */
bool CheckUniqueIndexFirstRule(const Path* path1, const Path* path2, PathCostComparison &cost_comparison)
{
    if (!ENABLE_SQL_BETA_FEATURE(NO_UNIQUE_INDEX_FIRST) && ImplementUniqueIndexRule(path1, path2, cost_comparison)) {
        if (log_min_messages <= DEBUG1)
            DebugPrintUniqueIndexFirstInfo(path1, path2, cost_comparison);
        return true;
    }
    return false;
}

/*
 * Check Join Exec Type
 *
 * Return true represent execute on CN
 *        false represent execute on DN
 */
bool CheckJoinExecType(PlannerInfo *root, Path *outer, Path *inner)
{
    if (!permit_gather(root)) {
        return false;
    }

    /* Only both sides execute on Cn then return true */
    if (EXEC_CONTAIN_COORDINATOR(inner->exec_type) &&
        EXEC_CONTAIN_COORDINATOR(outer->exec_type)) {
        return true;
    }

    return false;
}

/*
 * Check inner and outer exec type same or not
 *
 * Return true represent two sides exec type same
 *        false represent two sides exec type different
 */
bool IsSameJoinExecType(PlannerInfo* root, Path* outer, Path* inner)
{
    if (!permit_gather(root)) {
        return true;
    }

    /* Both sides execute on CN then return true */
    if (EXEC_CONTAIN_COORDINATOR(inner->exec_type) &&
        EXEC_CONTAIN_COORDINATOR(outer->exec_type)) {
        return true;
    }

    /* Both sides execute on DN then return true */
    if (EXEC_CONTAIN_DATANODE(inner->exec_type) &&
        EXEC_CONTAIN_DATANODE(outer->exec_type)) {
        return true;
    }

    return false;
}

RemoteQueryExecType SetExectypeForJoinPath(Path* inner_path, Path* outer_path)
{
    if (outer_path->exec_type == EXEC_ON_COORDS || inner_path->exec_type == EXEC_ON_COORDS) {
        return EXEC_ON_COORDS;
    } else if (outer_path->exec_type == EXEC_ON_ALL_NODES && inner_path->exec_type == EXEC_ON_ALL_NODES) {
        return EXEC_ON_ALL_NODES;
    } else {
        return EXEC_ON_DATANODES;
    }
}

RemoteQueryExecType SetBasePathExectype(PlannerInfo* root, RelOptInfo* rel)
{
    RangeTblEntry* rte = root->simple_rte_array[rel->relid];
    if (rte->rtekind == RTE_RELATION && is_sys_table(rte->relid)) {
        return EXEC_ON_COORDS;
    } else {
        return EXEC_ON_DATANODES;
    }
}

/*
 * compare_path_costs_fuzzily
 *	  Compare the costs of two paths to see if either can be said to
 *	  dominate the other.
 *
 * We use fuzzy comparisons so that add_path() can avoid keeping both of
 * a pair of paths that really have insignificantly different cost.
 *
 * The fuzz_factor argument must be 1.0 plus delta, where delta is the
 * fraction of the smaller cost that is considered to be a significant
 * difference.	For example, fuzz_factor = 1.01 makes the fuzziness limit
 * be 1% of the smaller cost.
 *
 * The two paths are said to have "equal" costs if both startup and total
 * costs are fuzzily the same.	Path1 is said to be better than path2 if
 * it has fuzzily better startup cost and fuzzily no worse total cost,
 * or if it has fuzzily better total cost and fuzzily no worse startup cost.
 * Path2 is better than path1 if the reverse holds.  Finally, if one path
 * is fuzzily better than the other on startup cost and fuzzily worse on
 * total cost, we just say that their costs are "different", since neither
 * dominates the other across the whole performance spectrum.
 */
PathCostComparison compare_path_costs_fuzzily(Path* path1, Path* path2, double fuzz_factor)
{
    if (path1->hint_value > path2->hint_value)
        return COSTS_BETTER1;
    else if (path1->hint_value < path2->hint_value)
        return COSTS_BETTER2;

    PathCostComparison cost_comparison;

    /*
     * For index paths, we check if rules can be used to filter.
     * Here, we tend to select path containing unique index columns.
     */
    if (CheckUniqueIndexFirstRule(path1, path2, cost_comparison)) {
        return cost_comparison;
    }

    /* dn gather RBO */
    cost_comparison = compare_join_single_node_distribution(path1, path2);
    if (cost_comparison != COSTS_DIFFERENT) {
        return cost_comparison;
    }

    /*
     * Check total cost first since it's more likely to be different; many
     * paths have zero startup cost.
     */
    if (fuzz_factor - SMALL_FUZZY_FACTOR == 0) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                (errmsg("SMALL_FUZZY_FACTOR is used to compare %lf .. %lf v.s. %lf .. % lf",
                    path1->startup_cost,
                    path1->total_cost,
                    path2->startup_cost,
                    path2->total_cost))));
    }
    if (path1->total_cost > path2->total_cost * fuzz_factor) {
        /* path1 fuzzily worse on total cost */
        if (path2->startup_cost > path1->startup_cost * fuzz_factor && path1->param_info == NULL) {
            /* ... but path2 fuzzily worse on startup, so DIFFERENT */
            return COSTS_DIFFERENT;
        }
        /* else path2 dominates */
        return COSTS_BETTER2;
    }
    if (path2->total_cost > path1->total_cost * fuzz_factor) {
        /* path2 fuzzily worse on total cost */
        if (path1->startup_cost > path2->startup_cost * fuzz_factor && path2->param_info == NULL) {
            /* ... but path1 fuzzily worse on startup, so DIFFERENT */
            return COSTS_DIFFERENT;
        }
        /* else path1 dominates */
        return COSTS_BETTER1;
    }
    /* fuzzily the same on total cost */
    if (path1->startup_cost > path2->startup_cost * fuzz_factor && path2->param_info == NULL) {
        /* ... but path1 fuzzily worse on startup, so path2 wins */
        return COSTS_BETTER2;
    }
    if (path2->startup_cost > path1->startup_cost * fuzz_factor && path1->param_info == NULL) {
        /* ... but path2 fuzzily worse on startup, so path1 wins */
        return COSTS_BETTER1;
    }
    /* fuzzily the same on both costs */
    return COSTS_EQUAL;
}

/* judge if a path is distribute key intersted path */
static bool is_itst_path(PlannerInfo* root, RelOptInfo* rel, Path* path)
{
    ListCell* lc = NULL;

    if (path->distribute_keys == NIL)
        return false;

    /* if distribute key is matching key, it's interested path */
    if (equal_distributekey(root, path->distribute_keys, rel->rel_dis_keys.matching_keys))
        return true;

    /* if distribute key if subset of one superset key, it's interested path */
    foreach (lc, rel->rel_dis_keys.superset_keys) {
        List* item_list = (List*)lfirst(lc);
        if (root != NULL && !needs_agg_stream(root, item_list, path->distribute_keys, &path->distribution))
            return true;
        if (root == NULL && !list_difference(path->distribute_keys, item_list))
            return true;
    }

    return false;
}

/*
 * @Description: Compare cheapest_path with path's costs.
 * @in cheapest_path: Current cheapest path.
 * @in path: New path.
 * @return: Return cheaper path.
 */
static Path* obtain_cheaper_path(Path* cheapest_path, Path* path, CostSelector criterion)
{
    int cmp;

    /* We need first compare hint priority.*/
    if (path->hint_value > cheapest_path->hint_value) {
        return path;
    } else if (path->hint_value == cheapest_path->hint_value) {
        /*
         * If we find two paths of identical costs, try to keep the
         * better-sorted one.  The paths might have unrelated sort orderings,
         * in which case we can only guess which might be better to keep, but
         * if one is superior then we definitely should keep that one.
         */
        cmp = compare_path_costs(cheapest_path, path, criterion);
        if (cmp > 0 || (cmp == 0 && compare_pathkeys(cheapest_path->pathkeys, path->pathkeys) == PATHKEYS_BETTER2)) {
            return path;
        }
    }

    return cheapest_path;
}

/*
 * set_cheapest
 *	  Find the minimum-cost paths from among a relation's paths,
 *	  and save them in the rel's cheapest-path fields.
 *
 * Only unparameterized paths are considered candidates for cheapest_startup
 * and cheapest_total.	The cheapest_parameterized_paths list collects paths
 * that are cheapest-total for their parameterization (i.e., there is no
 * cheaper path with the same or weaker parameterization).	This list always
 * includes the unparameterized cheapest-total path, too.
 *
 * This is normally called only after we've finished constructing the path
 * list for the rel node.
 */
void set_cheapest(RelOptInfo* parent_rel, PlannerInfo* root)
{
    Path* cheapest_startup_path = NULL;
    Path* cheapest_total_path = NULL;
    List* cheapest_total_path_list = NIL;
    Path* best_param_path = NULL;
    List* parameterized_paths = NIL;
    ListCell* p = NULL;
    ListCell* l = NULL;
    List* cheapest_path_list = NIL;

    AssertEreport(IsA(parent_rel, RelOptInfo), MOD_OPT_JOIN, "Paramter of set_cheapest() should be RelOptInfo");

    /*
     * When Cn Gather Hint switch on
     * we will add gather path or exec_on cn path(like systable) to cheapest_gather_path
     * so need to check both pathlist and cheapest_gather_path here, they will be added
     * to parent_rel->pathlist later.
     *
     * Don't worry about when Cn Gather switch off here because there never exist gatherpath,
     * so same as origin behavior.
     * */
    if (parent_rel->pathlist == NIL && parent_rel->cheapest_gather_path == NULL)
        elog(ERROR, "could not devise a query plan for the given query");

    cheapest_startup_path = cheapest_total_path = best_param_path = NULL;
    parameterized_paths = NIL;

    foreach (p, parent_rel->pathlist) {
        Path* path = (Path*)lfirst(p);
        restore_hashjoin_cost(path);
    }

    if (OPTIMIZE_PLAN != u_sess->attr.attr_sql.plan_mode_seed) {
        /* find the random of the paths for this rel if guc plan_mode_seed is not 0 */
        cheapest_path_list = get_random_path(parent_rel, &cheapest_startup_path, &cheapest_total_path);
        cheapest_total_path_list = list_make1(cheapest_total_path);
    } else {
        List* itst_cheapest_path = NIL;

        cheapest_startup_path = cheapest_total_path = NULL;

        foreach (p, parent_rel->pathlist) {
            Path* path = (Path*)lfirst(p);
            int cmp;

            if (path->param_info)
            {
                /* Parameterized path, so add it to parameterized_paths */
                parameterized_paths = lappend(parameterized_paths, path);

                /*
                 * If we have an unparameterized cheapest-total, we no longer care
                 * about finding the best parameterized path, so move on.
                 */
                if (cheapest_total_path)
                    continue;

                /*
                 * Otherwise, track the best parameterized path, which is the one
                 * with least total cost among those of the minimum
                 * parameterization.
                 */
                if (best_param_path == NULL)
                    best_param_path = path;
                else
                {
                    switch (bms_subset_compare(PATH_REQ_OUTER(path),
                                               PATH_REQ_OUTER(best_param_path)))
                    {
                        case BMS_EQUAL:
                            /* keep the cheaper one */
                            if (compare_path_costs(path, best_param_path,
                                                   TOTAL_COST) < 0)
                                best_param_path = path;
                            break;
                        case BMS_SUBSET1:
                            /* new path is less-parameterized */
                            best_param_path = path;
                            break;
                        case BMS_SUBSET2:
                            /* old path is less-parameterized, keep it */
                            break;
                        case BMS_DIFFERENT:
                            /*
                             * This means that neither path has the least possible
                             * parameterization for the rel.  We'll sit on the old
                             * path until something better comes along.
                             */
                            break;
                    }
                }
            }else {
                /* Unparameterized path, so consider it for cheapest slots */
                if (cheapest_total_path == NULL) {
                    cheapest_startup_path = cheapest_total_path = path;
                    if (is_itst_path(root, parent_rel, path))
                        itst_cheapest_path = lappend(itst_cheapest_path, path);
                    continue;
                }

                cheapest_startup_path = obtain_cheaper_path(cheapest_startup_path, path, STARTUP_COST);
                cheapest_total_path = obtain_cheaper_path(cheapest_total_path, path, TOTAL_COST);

                /* store cheapest path for different interested distribute key */
                if (is_itst_path(root, parent_rel, path)) {
                    Path* tmp_path = NULL;
                    foreach (l, itst_cheapest_path) {
                        tmp_path = (Path*)lfirst(l);
                        if (equal_distributekey(root, tmp_path->distribute_keys, path->distribute_keys))
                            break;
                    }
                    if (l == NULL)
                        itst_cheapest_path = lappend(itst_cheapest_path, path);
                    else {
                        cmp = compare_path_costs(tmp_path, path, TOTAL_COST);
                        if (cmp > 0 ||
                            (cmp == 0 && compare_pathkeys(tmp_path->pathkeys, path->pathkeys) == PATHKEYS_BETTER2))
                            lfirst(l) = path;
                    }
                }
            }
        }

        /* add best gather path to pathlist and cheapest total path */
        if (parent_rel->cheapest_gather_path != NULL) {
            parent_rel->pathlist = lappend(parent_rel->pathlist, parent_rel->cheapest_gather_path);
            cheapest_total_path_list = lappend(cheapest_total_path_list, parent_rel->cheapest_gather_path);
            cheapest_startup_path = parent_rel->cheapest_gather_path;
        }

        /*
         * If interested path is so costed, that is, larger than cheapest path plus
         * redistribute cost, we should abondon it. Else, store it in global cheapest
         * path list
         */
        if (cheapest_total_path) {
            cheapest_total_path_list = lappend(cheapest_total_path_list, cheapest_total_path);
        }

        foreach (p, itst_cheapest_path) {
            Path* tmp_path = (Path*)lfirst(p);
            Cost redistribute_cost = 0.0;
            if (tmp_path == cheapest_total_path)
                continue;

            unsigned int num_datanodes = ng_get_dest_num_data_nodes(tmp_path);

            compute_stream_cost(STREAM_REDISTRIBUTE,
                tmp_path->locator_type,
                PATH_LOCAL_ROWS(tmp_path),
                tmp_path->rows,
                1.0,
                parent_rel->width,
                false,
                tmp_path->distribute_keys,
                &redistribute_cost,
                &tmp_path->rows,
                num_datanodes,
                num_datanodes);
            /* only keep the path with same hint value */
            if (tmp_path->total_cost < cheapest_total_path->total_cost + redistribute_cost &&
                tmp_path->hint_value == cheapest_total_path->hint_value)
                cheapest_total_path_list = lappend(cheapest_total_path_list, tmp_path);
            if (list_length(cheapest_total_path_list) == MAX_PATH_NUM)
                break;
        }
        list_free_ext(itst_cheapest_path);
    }

    /* Add cheapest unparameterized path, if any, to parameterized_paths */
    if (cheapest_total_path)
        parameterized_paths = lcons(cheapest_total_path, parameterized_paths);

    /*
     * If there is no unparameterized path, use the best parameterized path as
     * cheapest_total_path (but not as cheapest_startup_path).
     */
    if (cheapest_total_path == NULL && best_param_path != NULL)
        cheapest_total_path_list = list_make1(best_param_path);
    Assert(cheapest_total_path_list != NULL);

    parent_rel->cheapest_startup_path = cheapest_startup_path;
    parent_rel->cheapest_total_path = cheapest_total_path_list;
    parent_rel->cheapest_unique_path = NULL; /* computed only if needed */
    parent_rel->cheapest_parameterized_paths = parameterized_paths;

    /* debug info for global path */
    if (log_min_messages <= DEBUG1) {
        StringInfoData ds;
        initStringInfo(&ds);
        appendBitmapsetToString(&ds, parent_rel->relids);
        ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("rel: %s", ds.data)));
        pfree_ext(ds.data);
        if (cheapest_startup_path != NULL)
            ereport(DEBUG1,
                (errmodule(MOD_OPT_JOIN),
                    errmsg("cheapest startup: %lf, %lf, hint_value: %d",
                        cheapest_startup_path->startup_cost,
                        cheapest_startup_path->total_cost,
                        cheapest_startup_path->hint_value)));
        foreach (l, parent_rel->cheapest_total_path) {
            Path* path = (Path*)lfirst(l);
            ereport(DEBUG1,
                (errmodule(MOD_OPT_JOIN),
                    errmsg("cheapest total: %lf, %lf, hint_value: %d",
                        path->startup_cost,
                        path->total_cost,
                        path->hint_value)));
            elog_node_display(DEBUG1, "[OPT_JOIN] distribute key", path->distribute_keys, true);
        }
    }

    if (OPTIMIZE_PLAN != u_sess->attr.attr_sql.plan_mode_seed) {
        ListCell* pnext = NULL;
        for (p = list_head(parent_rel->pathlist); p != NULL; p = pnext) {
            Path* path = (Path*)lfirst(p);
            pnext = lnext(p);

            if (!list_member_ptr(cheapest_path_list, path))
                parent_rel->pathlist = list_delete_ptr(parent_rel->pathlist, path);
        }

        list_free_ext(cheapest_path_list);
    }
}

Path *FindMatchedPath(PlannerInfo* root, RelOptInfo* rel)
{
    ListCell *lc = NULL;
    Path* matched_path = NULL;

    if (rel->rel_dis_keys.matching_keys != NIL) {
        foreach (lc, rel->cheapest_total_path) {
            Path* tmp_path = (Path*)lfirst(lc);
            if (equal_distributekey(root, tmp_path->distribute_keys, rel->rel_dis_keys.matching_keys)) {
                matched_path = tmp_path;
                break;
            }
        }
    }

    return matched_path;
}

/*
 * get_cheapest_path
 * 	choose an optimal path from optimal path, superset key path and match path of target relation
 *
 * Parameters:
 *	@in root: planner info structure for current query level
 *	@in rel: final join rel with all the table referenced
 *	@in agg_groups: estimated local and global aggregation rows
 *	@in has_groupby: true if there's aggregation involved in current query level. It's used to determine if
 *				we should use rel's rows or aggregation rows to calculate redistribute cost
 * Returns: optimal path
 */
Path* get_cheapest_path(PlannerInfo* root, RelOptInfo* rel, const double* agg_groups, bool has_groupby)
{
    Path* matched_path = NULL;
    Path* cheapest_path = (Path*)linitial(rel->cheapest_total_path);
    Path* superset_path = NULL;
    double cheapest_cost = cheapest_path->total_cost;
    double gblrows;
    Cost final_dis_cost = 0.0;
    Cost agg_dis_cost = 0.0;
    Cost path_dis_cost = 0.0;
    ListCell* lc = NULL;
    bool is_cheapest_super_path = false;

    /* just return cheapest gather path skip cost compare */
    if(permit_gather(root) && rel->cheapest_gather_path != NULL) {
        return rel->cheapest_gather_path;
    }

    /* find matched path if any */
    matched_path = FindMatchedPath(root, rel);

    /* find the cheapest path from superset key path, or mark cheapest total path as super key path */
    if (is_itst_path(root, rel, cheapest_path))
        is_cheapest_super_path = true;
    else {
        foreach (lc, rel->cheapest_total_path) {
            Path* tmp_path = (Path*)lfirst(lc);
            /* skip cheapest total path and matching path */
            if (tmp_path == cheapest_path)
                continue;

            /* Get one cost least path.*/
            if (superset_path == NULL) {
                superset_path = tmp_path;
            } else if (tmp_path->hint_value > superset_path->hint_value) {
                superset_path = tmp_path;
            } else if (superset_path->total_cost > tmp_path->total_cost) {
                superset_path = tmp_path;
            }
        }
    }


    /* The plan for dn gather doen't need to consider distribution key.*/
    if (ng_is_single_node_group_distribution(&cheapest_path->distribution)) {    
        return cheapest_path;
    } 

    /* comparison between cheapest path and cheapest superset key path */
    if (superset_path != NULL) {
        /*
         * If redistribution of aggregation is needed, we should roughly judge a minimum redistribute
         * cost of redistribute+agg path or agg+redistribute+agg path
         */
        if (!is_cheapest_super_path) {
            unsigned int path_num_datanodes = ng_get_dest_num_data_nodes(cheapest_path);

            /* redistribution cost of agg+redistribute+agg path */
            compute_stream_cost(STREAM_REDISTRIBUTE,
                cheapest_path->locator_type,
                agg_groups[0],
                cheapest_path->rows,
                1.0,
                rel->width,
                false,
                superset_path->distribute_keys,
                &agg_dis_cost,
                &gblrows,
                path_num_datanodes,
                path_num_datanodes);
            /* redistribution cost of redistribute+agg path */
            compute_stream_cost(STREAM_REDISTRIBUTE,
                cheapest_path->locator_type,
                PATH_LOCAL_ROWS(cheapest_path),
                cheapest_path->rows,
                1.0,
                rel->width,
                false,
                superset_path->distribute_keys,
                &path_dis_cost,
                &gblrows,
                path_num_datanodes,
                path_num_datanodes);
            cheapest_cost = cheapest_path->total_cost +
                            Min(agg_dis_cost * (1 + agg_groups[0] / PATH_LOCAL_ROWS(cheapest_path)), path_dis_cost);
        }
        /* choose super set key path if it dominates to cheapest path after redistribution */
        if (cheapest_cost > superset_path->total_cost) {
            cheapest_cost = superset_path->total_cost;
            cheapest_path = superset_path;
            ereport(DEBUG1,
                (errmodule(MOD_OPT_JOIN),
                    errmsg("super path dominate: %lf, %lf", superset_path->startup_cost, superset_path->total_cost)));
            elog_node_display(DEBUG1, "[OPT_JOIN] distribute key", superset_path->distribute_keys, true);
        }
    }
    /* comparison between superset key path (cheapest path) and matching path */
    if (matched_path != NULL) {
        double rows;
        /* determin data amount to do redistribution */
        if (!has_groupby) {
            rows = rel->rows;
        } else {
            rows = agg_groups[1];
        }
        /* redistribution cost to target relation */
        if (cheapest_path != matched_path) {
            unsigned int path_num_datanodes = ng_get_dest_num_data_nodes(cheapest_path);

            compute_stream_cost(STREAM_REDISTRIBUTE,
                cheapest_path->locator_type,
                rows,
                cheapest_path->rows,
                1.0,
                rel->width,
                false,
                matched_path->distribute_keys,
                &final_dis_cost,
                &gblrows,
                path_num_datanodes,
                path_num_datanodes);
            /* choose matching key path if it dominates to cheapest path after redistribution */
            if (matched_path->total_cost < cheapest_cost + final_dis_cost) {
                cheapest_path = matched_path;
                ereport(DEBUG1,
                    (errmodule(MOD_OPT_JOIN),
                        errmsg(
                            "matched path dominate: %lf, %lf", matched_path->startup_cost, matched_path->total_cost)));
                elog_node_display(DEBUG1, "[OPT_JOIN] distribute key", matched_path->distribute_keys, true);
            }
        } else
            ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("super path is also matched path")));
    }

    return cheapest_path;
}
/*
 * Target	:Print compare results when log_min_messages <= debug1.
 * In		:Compare results including cost,pathkey,BMS and rows.
 * Retrun	:NA
 * Out	:Print in the pg_log.
 */
void debug1_print_compare_result(PathCostComparison costcmp, PathKeysComparison keyscmp, BMS_Comparison outercmp,
    double rowscmp, PlannerInfo* root, Path* path, bool small_fuzzy_factor_is_used)
{
    StringInfoData buf;

    initStringInfo(&buf);
    appendStringInfoString(&buf, "\n{\n");

    /* print path information */
    char* path_string = debug1_print_path(root, path, 1);
    appendStringInfoString(&buf, path_string);
    pfree_ext(path_string);

    /* print comparison results */
    appendStringInfoString(&buf, "\n\tCost =");
    switch (costcmp) {
        case COSTS_BETTER1:
            appendStringInfoString(&buf, " NewBetter\t|");
            break;
        case COSTS_BETTER2:
            appendStringInfoString(&buf, " OldBetter\t|");
            break;
        case COSTS_DIFFERENT:
            appendStringInfoString(&buf, " Different\t|");
            break;
        case COSTS_EQUAL:
            appendStringInfoString(&buf, " Equal    \t|");
            break;
        default:
            appendStringInfoString(&buf, " NULL     \t|");
            break;
    }

    appendStringInfoString(&buf, "\tPathKeys =");
    switch (keyscmp) {
        case PATHKEYS_BETTER1:
            appendStringInfoString(&buf, " NewBetter\t|");
            break;
        case PATHKEYS_BETTER2:
            appendStringInfoString(&buf, " OldBetter\t|");
            break;
        case PATHKEYS_DIFFERENT:
            appendStringInfoString(&buf, " Different\t|");
            break;
        case PATHKEYS_EQUAL:
            appendStringInfoString(&buf, " Equal    \t|");
            break;
        default:
            appendStringInfoString(&buf, " NULL     \t|");
            break;
    }

    appendStringInfoString(&buf, "\t   BMS =");
    switch (outercmp) {
        case BMS_SUBSET1:
            appendStringInfoString(&buf, " NewBetter\t|");
            break;
        case BMS_SUBSET2:
            appendStringInfoString(&buf, " OldBetter\t|");
            break;
        case BMS_DIFFERENT:
            appendStringInfoString(&buf, " Different\t|");
            break;
        case BMS_EQUAL:
            appendStringInfoString(&buf, " Equal    \t|");
            break;
        default:
            appendStringInfoString(&buf, " NULL     \t|");
            break;
    }

    appendStringInfoString(&buf, "\t  Rows =");
    if (rowscmp > 0)
        appendStringInfoString(&buf, " NewLess \n");
    else if (rowscmp < 0)
        appendStringInfoString(&buf, " OldLess \n");
    else
        appendStringInfoString(&buf, " Equal   \n");

    if (small_fuzzy_factor_is_used)
        appendStringInfoString(&buf, "\tSmall fuzzy factor is used!\n");

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("The old path and the comparison results are:%s}", buf.data))));
    pfree_ext(buf.data);

    return;
}

/*
 * Target	:Print detail information of new path when log_min_messages <= debug1.
 * In		:Root path indent
 * Retrun	:NA
 * Out	:Print in the pg_log.
 */
void debug1_print_new_path(PlannerInfo* root, Path* path, bool small_fuzzy_factor_is_used)
{
    StringInfoData buf;
    initStringInfo(&buf);
    char* path_string = debug1_print_path(root, path, 1);
    appendStringInfoString(&buf, path_string);
    pfree_ext(path_string);

    if (small_fuzzy_factor_is_used)
        appendStringInfoString(&buf, "\tSmall fuzzy factor is used!\n");

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("The detail information of the new path:\n{\n%s}", buf.data))));
    pfree_ext(buf.data);

    return;
}

/*
 * @Description: Find stream hint and set hint_value.
 * @in hint_state: Hint state.
 * @in path: New path.
 * @in inner_outer_path: Inner or outer path.
 */
static void set_stream_hint(HintState* hint_state, Path* path, Path* inner_outer_path)
{
    if (hint_state == NULL) {
        return;
    }

    Path* stream_path = inner_outer_path;

    /*
     * Here we need skip Material or Unique, because that can be added above stream in
     * function stream_side_path.
     */
    if (inner_outer_path->pathtype == T_Material) {
        stream_path = ((MaterialPath*)inner_outer_path)->subpath;
    } else if (inner_outer_path->pathtype == T_Unique) {
        stream_path = ((UniquePath*)inner_outer_path)->subpath;
    }

    if (!IsA(stream_path, StreamPath)) {
        return;
    }

    Relids rel_ids = NULL;

    rel_ids = stream_path->parent->relids;

    StreamPath* streamPath = (StreamPath*)stream_path;

    ListCell* lc = NULL;

    foreach (lc, hint_state->stream_hint) {
        StreamHint* stream_hint = (StreamHint*)lfirst(lc);

        if (bms_equal(stream_hint->joinrelids, rel_ids)) {
            if (stream_hint->stream_type == streamPath->type) {
                stream_hint->base.state = HINT_STATE_USED;
                if (stream_hint->negative)
                    path->hint_value--;
                else
                    path->hint_value++;
            }
        }
    }
}

/*
 * @Description: Find scan hint and set hint_value.
 * @in new_path: New path.
 * @in hstate: Hint state.
 */
static void set_scan_hint(Path* new_path, HintState* hstate)
{
    ScanMethodHint* scanHint = NULL;

    switch (new_path->pathtype) {
        case T_SeqScan:
        case T_CStoreScan:
        case T_DfsScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_SubqueryScan:
        case T_ForeignScan: {
            scanHint = find_scan_hint(hstate, new_path->parent->relids, HINT_KEYWORD_TABLESCAN);
            break;
        }
        case T_IndexScan: {
            scanHint = find_scan_hint(hstate, new_path->parent->relids, HINT_KEYWORD_INDEXSCAN);
            break;
        }
        case T_IndexOnlyScan: {
            scanHint = find_scan_hint(hstate, new_path->parent->relids, HINT_KEYWORD_INDEXONLYSCAN);
            break;
        }
        default:
            break;
    }

    if (scanHint != NULL && scanHint->indexlist != NIL) {
        IndexPath* index_path = (IndexPath*)new_path;
        char* index_name = get_rel_name(index_path->indexinfo->indexoid);
        char* hint_index_name = strVal(linitial(scanHint->indexlist));

        if (index_name && strncmp(hint_index_name, index_name, strlen(index_name) + 1) != 0) {
            scanHint = NULL;
        }
    }

    if (scanHint != NULL) {
        scanHint->base.state = HINT_STATE_USED;
        if (scanHint->negative)
            new_path->hint_value--;
        else
            new_path->hint_value++;
    }
}

/*
 * @Description: Set path's hint kewword.
 * @in join_rel: Join relation information.
 * @in new_path: Generate new path.
 * @in hstate: Current query hint state.
 */
static void set_join_hint(RelOptInfo* join_rel, JoinPath* new_join_path, HintState* hstate)
{
    List* hints = NIL;
    Relids joinrelids = join_rel->relids;
    Relids inner_relids = new_join_path->innerjoinpath->parent->relids;

    switch (new_join_path->path.pathtype) {
        case T_NestLoop:
            hints = find_specific_join_hint(hstate, joinrelids, inner_relids, HINT_KEYWORD_NESTLOOP);
            break;
        case T_MergeJoin:
            hints = find_specific_join_hint(hstate, joinrelids, inner_relids, HINT_KEYWORD_MERGEJOIN);
            break;
        case T_HashJoin:
            hints = find_specific_join_hint(hstate, joinrelids, inner_relids, HINT_KEYWORD_HASHJOIN);
            break;
        default:
            break;
    }

    ListCell* lc = NULL;
    foreach (lc, hints) {
        JoinMethodHint* hint = (JoinMethodHint*)lfirst(lc);
        hint->base.state = HINT_STATE_USED;
        if (hint->negative) {
            new_join_path->path.hint_value--;
        } else {
            new_join_path->path.hint_value++;
        }
    }
}

/*
 * @Description: Skip not join path and find hinted path.
 * @in current_path: Curent path.
 * @return: Join path or scan path.
 */
Path* find_hinted_path(Path* current_path)
{
    Path* path = current_path;

    while (path != NULL) {
        if (path->pathtype == T_Material) {
            path = ((MaterialPath*)path)->subpath;
        } else if (path->pathtype == T_Stream) {
            path = ((StreamPath*)path)->subpath;
        } else if (path->pathtype == T_Unique) {
            path = ((UniquePath*)path)->subpath;
        } else {
            break;
        }
    }

    return path;
}

/*
 * @Description: Inherit child path's hint value.
 * @in new_path: New join path.
 * @in outer_path: Outer path.
 * @in inner_path: Inner path.
 */
static void inherit_child_hintvalue(Path* new_path, Path* outer_path, Path* inner_path)
{
    /* We keep hint value only in join path. */
    Path* outer_join_path = find_hinted_path(outer_path);
    Path* inner_join_path = find_hinted_path(inner_path);

    if (outer_join_path != NULL && inner_join_path != NULL) {
        new_path->hint_value += outer_join_path->hint_value + inner_join_path->hint_value;
    }
}

/*
 * @brief set_predpush_same_level_hint
 *  Set predpush same level hint state. If given hint is valid for the new path, increase the hint value.
 */
static void set_predpush_same_level_hint(HintState* hstate, RelOptInfo* rel, Path* path)
{
    /*
     * Guarding conditions.
     */
    Assert(path != NULL);
    if (path->param_info == NULL || rel->reloptkind != RELOPT_BASEREL) {
        return;
    }

    if (hstate == NULL || hstate->predpush_same_level_hint == NIL) {
        return;
    }

    ListCell *lc = NULL;
    foreach (lc, hstate->predpush_same_level_hint) {
        PredpushSameLevelHint *predpushSameLevelHint = (PredpushSameLevelHint*)lfirst(lc);
        if (is_predpush_same_level_matched(predpushSameLevelHint, rel->relids, path->param_info)) {
            predpushSameLevelHint->base.state = HINT_STATE_USED;
            path->hint_value++;
            break;
        }
    }
}

/*
 * @Description: Set hint values to this new path.
 * @in join_rel: Join relition.
 * @in new_path: New path.
 * @in hstate: Hint state.
 */
void set_hint_value(RelOptInfo* join_rel, Path* new_path, HintState* hstate)
{
    if (hstate == NULL) {
        return;
    }

    AssertEreport(new_path->hint_value == 0, MOD_OPT, "");

    set_scan_hint(new_path, hstate);

    /* Deal with join path. */
    if (IsA(new_path, NestPath) || IsA(new_path, MergePath) || IsA(new_path, HashPath)) {
        JoinPath* join_path = (JoinPath*)new_path;

        Path* outer_path = join_path->outerjoinpath;
        Path* inner_path = join_path->innerjoinpath;

        set_join_hint(join_rel, (JoinPath*)new_path, hstate);

        set_stream_hint(hstate, new_path, outer_path);

        set_stream_hint(hstate, new_path, inner_path);

        inherit_child_hintvalue(new_path, outer_path, inner_path);
    }

    /* Use bit-wise and instead, since root is not accessible and permit_predpush is not supported. */
    if ((PRED_PUSH_FORCE & (uint)u_sess->attr.attr_sql.rewrite_rule)) {
        set_predpush_same_level_hint(hstate, join_rel, new_path);
    }
}

static void AddGatherJoinrel(PlannerInfo* root, RelOptInfo* parentRel,
                                Path* oldPath, Path* newPath)
{
    PathCostComparison costcmp = COSTS_DIFFERENT;
    GatherSource gatherHint = get_gather_hint_source(root);

    if (IS_STREAM_TYPE(oldPath, STREAM_GATHER)) {
        pfree_ext(newPath);
        return;
    }

    /*
     * if GATHER_JOIN Hint swicth on
     */
    if (gatherHint == HINT_GATHER_JOIN &&
        IS_STREAM_TYPE(newPath, STREAM_GATHER) &&
        !IS_STREAM_TYPE(oldPath, STREAM_GATHER)) {
        /* free old one */
        pfree_ext(oldPath);
        parentRel->cheapest_gather_path = newPath;
        return;
    }

    /* just compare cost */
    costcmp = compare_path_costs_fuzzily(newPath, oldPath, FUZZY_FACTOR);

    if (costcmp == COSTS_BETTER1) {
        pfree_ext(oldPath);
        parentRel->cheapest_gather_path = newPath;
    } else {
        pfree_ext(newPath);
    }

    return;
}

static void AddGatherBaserel(RelOptInfo* parentRel, Path* oldPath, Path* newPath)
{
    PathCostComparison costcmp = COSTS_DIFFERENT;

    /* just compare cost */
    costcmp = compare_path_costs_fuzzily(newPath, oldPath, FUZZY_FACTOR);

    if (costcmp == COSTS_BETTER1) {
        pfree_ext(oldPath);
        parentRel->cheapest_gather_path = newPath;
    } else {
        pfree_ext(newPath);
    }

    return;
}

/*
 * add gather path to RelOptInfo
 */
static void AddGatherPath(PlannerInfo* root, RelOptInfo* parentRel, Path* newPath)
{
    Path* oldPath = parentRel->cheapest_gather_path;

    if (oldPath == NULL) {
        parentRel->cheapest_gather_path = newPath;
        return;
    }

    /* Add Path to baserel or joinrel */
    if(parentRel->reloptkind == RELOPT_JOINREL) {
        AddGatherJoinrel(root, parentRel, oldPath, newPath);
    } else {
        AddGatherBaserel(parentRel, oldPath, newPath);
    }

    return;
}

static bool AddPathPreCheck(Path* newPath)
{
    /*
     * In Stream mode, it's not supported if there's param push under stream.
     * So we skip this path in advance to avoid other paths are generated.
     */
    if (IS_STREAM_PLAN && (IsA(newPath, NestPath) || IsA(newPath, HashPath) ||
                            IsA(newPath, MergePath))) {
        JoinPath* np = (JoinPath*)newPath;
        bool invalid = false;
        ContainStreamContext context;

        context.outer_relids = np->outerjoinpath->parent->relids;
        context.upper_params = NULL;
        context.only_check_stream = false;
        context.under_materialize_all = false;
        context.has_stream = false;
        context.has_parameterized_path = false;
        context.has_cstore_index_delta = false;

        stream_path_walker(np->innerjoinpath, &context);

        /*
         * In Executor engine, we'll materializeAll to prevent deadlock when
         * either outer or inner has stream, and meanwhile if there's parameterized
         * path, it's forbidden, so we should exclude it to the candidate
         */
        if (context.has_parameterized_path) {
            /* inner has stream */
            if (context.has_stream || context.has_cstore_index_delta)
                invalid = true;
            /* If inner is not material, materializeAll is not used, so skip outer check */
            else if (IsA(np->innerjoinpath, MaterialPath)) {
                context.outer_relids = NULL;
                context.only_check_stream = false;
                context.under_materialize_all = false;
                context.has_stream = false;
                context.has_parameterized_path = false;
                context.has_cstore_index_delta = false;

                stream_path_walker(np->outerjoinpath, &context);
                /* outer has stream */
                if (context.has_stream || context.has_cstore_index_delta)
                    invalid = true;
            }
        }

        if (invalid) {
            pfree_ext(newPath);
            return false;
        }
    }

    return true;
}

/*
 * add_path
 *	  Consider a potential implementation path for the specified parent rel,
 *	  and add it to the rel's pathlist if it is worthy of consideration.
 *	  A path is worthy if it has a better sort order (better pathkeys) or
 *	  cheaper cost (on either dimension), or generates fewer rows, than any
 *	  existing path that has the same or superset parameterization rels.
 *
 *	  We also remove from the rel's pathlist any old paths that are dominated
 *	  by new_path --- that is, new_path is cheaper, at least as well ordered,
 *	  generates no more rows, and requires no outer rels not required by the
 *	  old path.
 *
 *	  In most cases, a path with a superset parameterization will generate
 *	  fewer rows (since it has more join clauses to apply), so that those two
 *	  figures of merit move in opposite directions; this means that a path of
 *	  one parameterization can seldom dominate a path of another.  But such
 *	  cases do arise, so we make the full set of checks anyway.
 *
 *	  There is one policy decision embedded in this function, along with its
 *	  sibling add_path_precheck: we treat all parameterized paths as having
 *	  NIL pathkeys, so that they compete only on cost.	This is to reduce
 *	  the number of parameterized paths that are kept.	See discussion in
 *	  src/backend/optimizer/README.
 *
 *	  The pathlist is kept sorted by total_cost, with cheaper paths
 *	  at the front.  Within this routine, that's simply a speed hack:
 *	  doing it that way makes it more likely that we will reject an inferior
 *	  path after a few comparisons, rather than many comparisons.
 *	  However, add_path_precheck relies on this ordering to exit early
 *	  when possible.
 *
 *	  NOTE: discarded Path objects are immediately pfree'd to reduce planner
 *	  memory consumption.  We dare not try to free the substructure of a Path,
 *	  since much of it may be shared with other Paths or the query tree itself;
 *	  but just recycling discarded Path nodes is a very useful savings in
 *	  a large join tree.  We can recycle the List nodes of pathlist, too.
 *
 *	  BUT: we do not pfree IndexPath objects, since they may be referenced as
 *	  children of BitmapHeapPaths as well as being paths in their own right.
 *
 * 'parent_rel' is the relation entry to which the path corresponds.
 * 'new_path' is a potential path for parent_rel.
 *
 * Returns nothing, but modifies parent_rel->pathlist.
 */
void add_path(PlannerInfo* root, RelOptInfo* parent_rel, Path* new_path)
{
    bool accept_new = true;        /* unless we find a superior old path */
    ListCell* insert_after = NULL; /* where to insert new item */
    List* new_path_pathkeys = NIL;
    ListCell* p1 = NULL;
    ListCell* p1_prev = NULL;
    ListCell* p1_next = NULL;
    bool small_fuzzy_factor_is_used = false;

    /*
     * This is a convenient place to check for query cancel --- no part of the
     * planner goes very long without calling add_path().
     */
    CHECK_FOR_INTERRUPTS();

    if (!AddPathPreCheck(new_path)) {
        return;
    }

    /* Set path's hint_value. */
    if (root != NULL && root->parse->hintState != NULL) {
        set_hint_value(parent_rel, new_path, root->parse->hintState);
    }

    /* we will add cn gather path when cn gather hint switch on */
    if (root != NULL && EXEC_CONTAIN_COORDINATOR(new_path->exec_type) && permit_gather(root)) {
        RangeTblEntry* rte = root->simple_rte_array[parent_rel->relid];
        bool isSysTable = (rte != NULL && rte->rtekind == RTE_RELATION && is_sys_table(rte->relid));

        if (!isSysTable) {
            AddGatherPath(root, parent_rel, new_path);
            return;
        }
    }

    if (OPTIMIZE_PLAN != u_sess->attr.attr_sql.plan_mode_seed) {
        parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
        return;
    }

    /* Pretend parameterized paths have no pathkeys, per comment above */
    new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

    /*
     * Loop to check proposed new path against old paths.  Note it is possible
     * for more than one old path to be tossed out because new_path dominates
     * it.
     *
     * We can't use foreach here because the loop body may delete the current
     * list cell.
     */
    p1_prev = NULL;
    for (p1 = list_head(parent_rel->pathlist); p1 != NULL; p1 = p1_next) {
        Path* old_path = (Path*)lfirst(p1);
        bool remove_old = false; /* unless new proves superior */
        bool eq_diskey = true;
        PathCostComparison costcmp = COSTS_DIFFERENT;
        PathKeysComparison keyscmp = PATHKEYS_DIFFERENT;
        BMS_Comparison outercmp = BMS_DIFFERENT;
        double rowscmp;

        p1_next = lnext(p1);

        /*
         * Do a fuzzy cost comparison with 1% fuzziness limit.	(XXX does this
         * percentage need to be user-configurable?)
         */
        costcmp = compare_path_costs_fuzzily(new_path, old_path, FUZZY_FACTOR);

        /*
         * If the two paths compare differently for startup and total cost,
         * then we want to keep both, and we can skip comparing pathkeys and
         * required_outer rels.  If they compare the same, proceed with the
         * other comparisons.  Row count is checked last.  (We make the tests
         * in this order because the cost comparison is most likely to turn
         * out "different", and the pathkeys comparison next most likely.  As
         * explained above, row count very seldom makes a difference, so even
         * though it's cheap to compare there's not much point in checking it
         * earlier.)
         */
        if (costcmp != COSTS_DIFFERENT) {
            /* Similarly check to see if either dominates on pathkeys */
            List* old_path_pathkeys = NIL;

            old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
            keyscmp = compare_pathkeys(new_path_pathkeys, old_path_pathkeys);
            if (keyscmp != PATHKEYS_DIFFERENT) {
                switch (costcmp) {
                    case COSTS_EQUAL:
                        outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path), PATH_REQ_OUTER(old_path));
                        if (keyscmp == PATHKEYS_BETTER1) {
                            if ((outercmp == BMS_EQUAL || outercmp == BMS_SUBSET1) && new_path->rows <= old_path->rows)
                                remove_old = true; /* new dominates old */
                        } else if (keyscmp == PATHKEYS_BETTER2) {
                            if ((outercmp == BMS_EQUAL || outercmp == BMS_SUBSET2) && new_path->rows >= old_path->rows)
                                accept_new = false; /* old dominates new */
                        } else {
                            if (outercmp == BMS_EQUAL) {
                                /*
                                 * Same pathkeys and outer rels, and fuzzily
                                 * the same cost, so keep just one; to decide
                                 * which, first check rows and then do a fuzzy
                                 * cost comparison with very small fuzz limit.
                                 * (We used to do an exact cost comparison,
                                 * but that results in annoying
                                 * platform-specific plan variations due to
                                 * roundoff in the cost estimates.)  If things
                                 * are still tied, arbitrarily keep only the
                                 * old path.  Notice that we will keep only
                                 * the old path even if the less-fuzzy
                                 * comparison decides the startup and total
                                 * costs compare differently.
                                 */
                                if (new_path->rows < old_path->rows)
                                    remove_old = true; /* new dominates old */
                                else if (new_path->rows > old_path->rows)
                                    accept_new = false; /* old dominates new */
                                else {
                                    small_fuzzy_factor_is_used = true;
                                    if (compare_path_costs_fuzzily(new_path, old_path, SMALL_FUZZY_FACTOR) ==
                                        COSTS_BETTER1)
                                        remove_old = true; /* new dominates old */
                                    else
                                        accept_new = false; /* old equals or
                                                             * dominates new */
                                }
                            } else if (outercmp == BMS_SUBSET1 && new_path->rows <= old_path->rows)
                                remove_old = true; /* new dominates old */
                            else if (outercmp == BMS_SUBSET2 && new_path->rows >= old_path->rows)
                                accept_new = false; /* old dominates new */
                                                    /* else different parameterizations, keep both */
                        }
                        break;
                    case COSTS_BETTER1:
                        if (keyscmp != PATHKEYS_BETTER2) {
                            outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path), PATH_REQ_OUTER(old_path));
                            if ((outercmp == BMS_EQUAL || outercmp == BMS_SUBSET1) && new_path->rows <= old_path->rows)
                                remove_old = true; /* new dominates old */
                        }
                        break;
                    case COSTS_BETTER2:
                        if (keyscmp != PATHKEYS_BETTER1) {
                            outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path), PATH_REQ_OUTER(old_path));
                            if ((outercmp == BMS_EQUAL || outercmp == BMS_SUBSET2) && new_path->rows >= old_path->rows)
                                accept_new = false; /* old dominates new */
                        }
                        break;
                    default:

                        /*
                         * can't get here, but keep this case to keep compiler
                         * quiet
                         */
                        break;
                }
            }
        }

#ifdef STREAMPLAN
        if (IS_STREAM_PLAN) {
	    /* When compare the path with single node distribution with other path with non-single 
	     * node distribution, the former will be kept and the latter will be removed.
	     */
            bool is_new_path_single_node_distribution = ng_is_single_node_group_distribution(&new_path->distribution);
            bool is_old_path_single_node_distribution = ng_is_single_node_group_distribution(&old_path->distribution);
            /*When open dn gather, only the path with single node distribution will be kept for the cost after.*/
            if ((is_new_path_single_node_distribution && !is_old_path_single_node_distribution)
                || (!is_new_path_single_node_distribution && is_old_path_single_node_distribution)) {
               /* When compare the path with single node distribution with other path with non-single 
                * node distribution, the former will be kept and the latter will be removed.
                */
                if (costcmp == COSTS_BETTER1 || costcmp == COSTS_BETTER2) {
                    eq_diskey = true;
                }
            } else if (!is_new_path_single_node_distribution && !is_old_path_single_node_distribution) {
                eq_diskey = equal_distributekey(root, new_path->distribute_keys, old_path->distribute_keys);
            } else {
                // Remove when they are all single node distribution.
                eq_diskey = true;
            }
        }
#endif
            /*
             * Remove current element from pathlist if dominated by new.
             */
#ifdef STREAMPLAN
        if (remove_old && eq_diskey) {
#else
        if (remove_old) {
#endif       
            ereport(DEBUG1,
                (errmodule(MOD_OPT_JOIN),
                    (errmsg("An old path is removed with cost = %lf .. %lf;  rows = %lf",
                        old_path->startup_cost,
                        old_path->total_cost,
                        old_path->rows))));
            rowscmp = old_path->rows - new_path->rows;
            if (log_min_messages <= DEBUG1)
                debug1_print_compare_result(
                    costcmp, keyscmp, outercmp, rowscmp, root, old_path, small_fuzzy_factor_is_used);
            parent_rel->pathlist = list_delete_cell(parent_rel->pathlist, p1, p1_prev);

            /*
             * Delete the data pointed-to by the deleted cell, if possible
             */
            if (!IsA(old_path, IndexPath))
                pfree_ext(old_path);
            /* p1_prev does not advance */
        } else {
            /* new belongs after this old path if it has cost >= old's */
            if (new_path->total_cost >= old_path->total_cost && new_path->hint_value <= old_path->hint_value)
                insert_after = p1;
            /* p1_prev advances */
            p1_prev = p1;
        }

#ifdef STREAMPLAN
        /* we should accept the new if distribute key differs */
        if (!accept_new && !eq_diskey) {
            accept_new = true;
            /* new belongs after this old path if it has cost >= old's */
            if (new_path->total_cost >= old_path->total_cost && new_path->hint_value <= old_path->hint_value)
                insert_after = p1;
            /* p1_prev advances */
            p1_prev = p1;
        }
#endif

        /*
         * If we found an old path that dominates new_path, we can quit
         * scanning the pathlist; we will not add new_path, and we assume
         * new_path cannot dominate any other elements of the pathlist.
         */
        if (!accept_new) {
            ereport(DEBUG1,
                (errmodule(MOD_OPT_JOIN),
                    (errmsg("A new path is not accepted with cost = %lf .. %lf;  rows = %lf",
                        new_path->startup_cost,
                        new_path->total_cost,
                        new_path->rows))));
            rowscmp = old_path->rows - new_path->rows;
            if (log_min_messages <= DEBUG1) {
                debug1_print_new_path(root, new_path, small_fuzzy_factor_is_used);
                debug1_print_compare_result(
                    costcmp, keyscmp, outercmp, rowscmp, root, old_path, small_fuzzy_factor_is_used);
            }
            break;
        }
    }

    if (accept_new) {
        /* Accept the new path: insert it at proper place in pathlist */
        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                (errmsg("A new path is accepted with cost = %lf .. %lf;  rows = %lf",
                    new_path->startup_cost,
                    new_path->total_cost,
                    new_path->rows))));
        if (log_min_messages <= DEBUG1)
            debug1_print_new_path(root, new_path, small_fuzzy_factor_is_used);
        if (insert_after != NULL)
            lappend_cell(parent_rel->pathlist, insert_after, new_path);
        else
            parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
    } else {
        /* Reject and recycle the new path */
        if (!IsA(new_path, IndexPath))
            pfree_ext(new_path);
    }
}

/*
 * add_path_precheck
 *	  Check whether a proposed new path could possibly get accepted.
 *	  We assume we know the path's pathkeys and parameterization accurately,
 *	  and have lower bounds for its costs.
 *
 * Note that we do not know the path's rowcount, since getting an estimate for
 * that is too expensive to do before prechecking.	We assume here that paths
 * of a superset parameterization will generate fewer rows; if that holds,
 * then paths with different parameterizations cannot dominate each other
 * and so we can simply ignore existing paths of another parameterization.
 * (In the infrequent cases where that rule of thumb fails, add_path will
 * get rid of the inferior path.)
 *
 * At the time this is called, we haven't actually built a Path structure,
 * so the required information has to be passed piecemeal.
 */
bool add_path_precheck(
    RelOptInfo* parent_rel, Cost startup_cost, Cost total_cost, List* pathkeys, Relids required_outer)
{
    List* new_path_pathkeys = NIL;
    ListCell* p1 = NULL;

    /* Pretend parameterized paths have no pathkeys, per add_path comment */
    new_path_pathkeys = required_outer ? NIL : pathkeys;

    foreach (p1, parent_rel->pathlist) {
        Path* old_path = (Path*)lfirst(p1);
        PathKeysComparison keyscmp;
        double fuzzy_factor = IS_STREAM_PLAN ? FUZZY_FACTOR : 1.0;

        /*
         * We are looking for an old_path with the same parameterization (and
         * by assumption the same rowcount) that dominates the new path on
         * pathkeys as well as both cost metrics.  If we find one, we can
         * reject the new path.
         *
         * For speed, we make exact rather than fuzzy cost comparisons. If an
         * old path dominates the new path exactly on both costs, it will
         * surely do so fuzzily. However, in stream case, this is just a initial
         * rough estimation, so use fuzzy cost instead.
         */
        if (total_cost >= old_path->total_cost * fuzzy_factor) {
            if (startup_cost >= old_path->startup_cost || required_outer) {
                List* old_path_pathkeys = NIL;

                old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
                keyscmp = compare_pathkeys(new_path_pathkeys, old_path_pathkeys);
                if (keyscmp == PATHKEYS_EQUAL || keyscmp == PATHKEYS_BETTER2) {
                    if (bms_equal(required_outer, PATH_REQ_OUTER(old_path))) {
                        /* Found an old path that dominates the new one */
                        ereport(DEBUG1,
                            (errmodule(MOD_OPT_JOIN),
                                (errmsg("--Precheck drop new path: startup_cost = %lf;  total_cost = %lf",
                                    startup_cost,
                                    total_cost))));
                        return false;
                    }
                }
            }
        } else {
            /*
             * Since the pathlist is sorted by total_cost, we can stop looking
             * once we reach a path with a total_cost larger than the new
             * path's.
             */
            break;
        }
    }

    return true;
}

/*****************************************************************************
 *		PATH NODE CREATION ROUTINES
 *****************************************************************************/
/*
 * create_seqscan_path
 *	  Creates a path corresponding to a sequential scan, returning the
 *	  pathnode.
 */
Path* create_seqscan_path(PlannerInfo* root, RelOptInfo* rel, Relids required_outer, int dop)
{
    Path* pathnode = makeNode(Path);

    pathnode->pathtype = T_SeqScan;
    pathnode->parent = rel;
    pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer);
    pathnode->pathkeys = NIL; /* seqscan has unordered result */
    pathnode->dop = dop;
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->distribute_keys = rel->distribute_keys;
        pathnode->locator_type = rel->locator_type;
        pathnode->rangelistOid = rel->rangelistOid;

        /* add location information for seqscan path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->distribution, distribution);
        if (InvalidOid == pathnode->distribution.group_oid || bms_is_empty(pathnode->distribution.bms_data_nodeids)) {
            elog(DEBUG1, "[create_seqscan_path] bms is empty. tableoid [%u] relkind [%c]", rte->relid, rte->relkind);
        }
    }
#endif

    RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);
    if (NULL == rte->tablesample) {
        cost_seqscan(pathnode, root, rel, pathnode->param_info);
    } else {
        AssertEreport(rte->rtekind == RTE_RELATION, MOD_OPT_JOIN, "Rel should be base relation");
        cost_samplescan(pathnode, root, rel, pathnode->param_info);
    }

    return pathnode;
}

Path* build_seqScanPath_by_indexScanPath(PlannerInfo* root, Path* index_path)
{
    Path* pathnode = makeNode(Path);

    pathnode->pathtype = T_SeqScan;
    pathnode->parent = index_path->parent;
    pathnode->param_info = index_path->param_info;
    pathnode->pathkeys = NIL;
    pathnode->exec_type = index_path->exec_type;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->distribute_keys = index_path->distribute_keys;
        pathnode->locator_type = index_path->locator_type;

        /* add location information for seqscan path by index scan path */
        RelOptInfo* rel = pathnode->parent;
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->distribution, distribution);
    }
#endif

    cost_seqscan(pathnode, root, pathnode->parent, pathnode->param_info);

    return pathnode;
}

/*
 * create_cstorescan_path with dop parm for parallelism
 * Creates a path corresponding to a column store scan, returning the
 * pathnode.
 */
Path* create_cstorescan_path(PlannerInfo* root, RelOptInfo* rel, int dop)
{
    Path* pathnode = makeNode(Path);

    pathnode->parent = rel;
    pathnode->pathkeys = NIL; /* seqscan has unordered result */
    pathnode->dop = dop;
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->distribute_keys = rel->distribute_keys;
        pathnode->locator_type = rel->locator_type;

        /* add location information for cstorescan path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->distribution, distribution);
    }
#endif

    pathnode->pathtype = (REL_COL_ORIENTED == rel->orientation) ? T_CStoreScan : T_DfsScan;

    RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);
    if (NULL == rte->tablesample) {
        if (REL_COL_ORIENTED == rel->orientation) {
            cost_cstorescan(pathnode, root, rel);
        } else {
            /* PAX on hdfs. */
            AssertEreport(REL_PAX_ORIENTED == rel->orientation, MOD_OPT_JOIN, "Rel should be PAX on hdfs");
            cost_dfsscan(pathnode, root, rel);
        }
    } else {
        AssertEreport(rte->rtekind == RTE_RELATION, MOD_OPT_JOIN, "Rel should be base relation");
        cost_samplescan(pathnode, root, rel, pathnode->param_info);
    }

    return pathnode;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * create_tstorescan_path with dop parm for parallelism
 * Creates a path corresponding to a time series store scan, returning the
 * pathnode.
 */
Path* create_tsstorescan_path(PlannerInfo *root, RelOptInfo *rel, int dop)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype =  T_TsStoreScan;
    pathnode->parent = rel;
    pathnode->pathkeys = NIL;    /* seqscan has unordered result */
    pathnode->dop = dop;
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN)
    {
        pathnode->distribute_keys = rel->distribute_keys;
        pathnode->locator_type = rel->locator_type;

        /* add location information for tsstorescan path */
        RangeTblEntry *rte = root->simple_rte_array[rel->relid];
        Distribution *distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->distribution, distribution);
    }
#endif

    RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
    if (NULL == rte->tablesample)
    {
        cost_tsstorescan(pathnode, root, rel);
    }
    else
    {
        AssertEreport(rte->rtekind == RTE_RELATION,
            MOD_OPT_JOIN, "Rel should be base relation");
        cost_samplescan(pathnode, root, rel, pathnode->param_info);
    }

    return pathnode;
}
#endif   /* ENABLE_MULTIPLE_NODES */

/*
 * Check whether the bitmap heap path just use global partition index.
 */
bool CheckBitmapQualIsGlobalIndex(Path* bitmapqual)
{
    bool bitmapqualIsGlobal = true;
    if (IsA(bitmapqual, IndexPath)) {
        IndexPath* ipath = (IndexPath*)bitmapqual;
        bitmapqualIsGlobal = ipath->indexinfo->isGlobal;
    } else if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;
        ListCell* l = NULL;
        bool allIsGlobal = true;

        foreach (l, apath->bitmapquals) {
            if (CheckBitmapQualIsGlobalIndex((Path*)lfirst(l)) != allIsGlobal) {
                bitmapqualIsGlobal = !allIsGlobal;
                break;
            }
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;
        ListCell* l = NULL;
        bool allIsGlobal = true;

        foreach (l, opath->bitmapquals) {
            if (CheckBitmapQualIsGlobalIndex((Path*)lfirst(l)) != allIsGlobal) {
                bitmapqualIsGlobal = !allIsGlobal;
                break;
            }
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", nodeTag(bitmapqual))));
    }

    return bitmapqualIsGlobal;
}

/*
 * Check whether have global partition index or local partition index in bitmap heap path,
 * Contains at least one, return true.
 */
bool CheckBitmapHeapPathContainGlobalOrLocal(Path* bitmapqual)
{
    bool containGlobalOrLocal = false;
    if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, apath->bitmapquals) {
            containGlobalOrLocal = CheckBitmapHeapPathContainGlobalOrLocal((Path*)lfirst(l));
            if (containGlobalOrLocal)
                break;
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;
        ListCell* head = list_head(opath->bitmapquals);
        ListCell* l = NULL;
        bool allIsGlobal = CheckBitmapQualIsGlobalIndex((Path*)lfirst(head));

        foreach (l, opath->bitmapquals) {
            if (l == head) {
                continue;
            }
            if (CheckBitmapQualIsGlobalIndex((Path*)lfirst(l)) != allIsGlobal) {
                containGlobalOrLocal = true;
                break;
            }
        }
    } else if (IsA(bitmapqual, IndexPath)) {
        containGlobalOrLocal = false;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", nodeTag(bitmapqual))));
    }

    return containGlobalOrLocal;
}

/*
 * CheckBitmapHeapPathIsCrossbucket
 * Check if a given bit map qual path is/can Crossbucket.
 * return true if bitmap heap is crossbucket.
 */
bool CheckBitmapHeapPathIsCrossbucket(Path* bitmapqual)
{
    if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, apath->bitmapquals) {
            if(!CheckBitmapHeapPathIsCrossbucket((Path*)lfirst(l))) {
                return false;
            }
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, opath->bitmapquals) {
            if(!CheckBitmapHeapPathIsCrossbucket((Path*)lfirst(l))) {
                return false;
            }
        }
    } else if (IsA(bitmapqual, IndexPath)) {
        return ((IndexPath*)bitmapqual)->indexinfo->crossbucket;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", nodeTag(bitmapqual))));
    }

    return true;
}

/*
 * Support partiton index unusable.
 * Check if the index in bitmap heap path is unusable. Contains at least one, return false.
 * Hypothetical index does not support partition index unusable.
 */
bool check_bitmap_heap_path_index_unusable(Path* bitmapqual, RelOptInfo* baserel)
{
    bool indexUnusable = true;
    if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, apath->bitmapquals) {
            indexUnusable = check_bitmap_heap_path_index_unusable((Path*)lfirst(l), baserel);
            if (!indexUnusable)
                break;
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, opath->bitmapquals) {
            indexUnusable = check_bitmap_heap_path_index_unusable((Path*)lfirst(l), baserel);
            if (!indexUnusable)
                break;
        }
    } else if (IsA(bitmapqual, IndexPath)) {
        IndexPath* ipath = (IndexPath*)bitmapqual;
        Oid index_oid = ipath->indexinfo->indexoid;
        if (u_sess->attr.attr_sql.enable_hypo_index && ipath->indexinfo->hypothetical) {
            return indexUnusable;
        }
        indexUnusable = checkPartitionIndexUnusable(index_oid, baserel->partItrs, baserel->pruning_result);
        if (!indexUnusable) {
            return indexUnusable;
        }
    } else
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", nodeTag(bitmapqual))));

    return indexUnusable;
}

/*
 * Support partition index unusable.
 * support index/index only scan(b-tree index).
 * Here not support bitmap heap index scan.
 */
bool is_partitionIndex_Subpath(Path* subpath)
{
    bool is_index_path = false;

    switch (subpath->pathtype) {
        case T_IndexScan:
        case T_IndexOnlyScan:
            is_index_path = true;
            break;
        default:
            break;
    }

    return is_index_path;
}

/* check whether the path is a pwj path */
bool is_pwj_path(Path* pwjpath)
{
    bool ret = false;
    if (pwjpath == NULL)
        return ret;

    if (pwjpath->pathtype == T_PartIterator) {
        Path* subpath = ((PartIteratorPath*)pwjpath)->subPath;

        if (subpath != NULL) {
            switch (subpath->pathtype) {
                case T_NestLoop:
                case T_MergeJoin:
                case T_HashJoin:
                    ret = true;
                    break;
                default:
                    break;
            }
        }
    }
    return ret;
}

/*
 * create_index_path
 *	  Creates a path node for an index scan.
 *
 * 'index' is a usable index.
 * 'indexclauses' is a list of RestrictInfo nodes representing clauses
 *			to be used as index qual conditions in the scan.
 * 'indexclausecols' is an integer list of index column numbers (zero based)
 *			the indexclauses can be used with.
 * 'indexorderbys' is a list of bare expressions (no RestrictInfos)
 *			to be used as index ordering operators in the scan.
 * 'indexorderbycols' is an integer list of index column numbers (zero based)
 *			the ordering operators can be used with.
 * 'pathkeys' describes the ordering of the path.
 * 'indexscandir' is ForwardScanDirection or BackwardScanDirection
 *			for an ordered index, or NoMovementScanDirection for
 *			an unordered index.
 * 'indexonly' is true if an index-only scan is wanted.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * Returns the new path node.
 */
IndexPath* create_index_path(PlannerInfo* root, IndexOptInfo* index, List* indexclauses, List* indexclausecols,
    List* indexorderbys, List* indexorderbycols, List* pathkeys, ScanDirection indexscandir, bool indexonly,
    Relids required_outer, Bitmapset *upper_params, double loop_count)
{
    IndexPath* pathnode = makeNode(IndexPath);
    RelOptInfo* rel = index->rel;
    List* indexquals = NIL;
    List* indexqualcols = NIL;

    pathnode->is_ustore = rel->is_ustore;

    pathnode->path.pathtype = indexonly ? T_IndexOnlyScan : T_IndexScan;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer, upper_params);
    pathnode->path.pathkeys = pathkeys;

    /* Convert clauses to indexquals the executor can handle */
    expand_indexqual_conditions(index, indexclauses, indexclausecols, &indexquals, &indexqualcols);

    /* Fill in the pathnode */
    pathnode->indexinfo = index;
    pathnode->indexclauses = indexclauses;
    pathnode->indexquals = indexquals;
    pathnode->indexqualcols = indexqualcols;
    pathnode->indexorderbys = indexorderbys;
    pathnode->indexorderbycols = indexorderbycols;
    pathnode->indexscandir = indexscandir;
    pathnode->path.exec_type = SetBasePathExectype(root, rel);
#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;
        pathnode->path.rangelistOid = rel->rangelistOid;

        /* add location information for index scan path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif

    cost_index(pathnode, root, loop_count);

    return pathnode;
}

/*
 * create_bitmap_heap_path
 *	  Creates a path node for a bitmap scan.
 *
 * 'bitmapqual' is a tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * loop_count should match the value used when creating the component
 * IndexPaths.
 */
BitmapHeapPath* create_bitmap_heap_path(PlannerInfo* root, RelOptInfo* rel, Path* bitmapqual,
            Relids required_outer, Bitmapset* required_upper, double loop_count)
{
    BitmapHeapPath* pathnode = makeNode(BitmapHeapPath);

    pathnode->path.pathtype = T_BitmapHeapScan;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer, required_upper);
    pathnode->path.pathkeys = NIL; /* always unordered */
    pathnode->path.exec_type = SetBasePathExectype(root, rel);

    pathnode->bitmapqual = bitmapqual;

    cost_bitmap_heap_scan(&pathnode->path, root, rel, pathnode->path.param_info, bitmapqual, loop_count);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;

        /* add location information for bitmap heap path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif

    return pathnode;
}

/*
 * create_bitmap_and_path
 *	  Creates a path node representing a BitmapAnd.
 */
BitmapAndPath* create_bitmap_and_path(PlannerInfo* root, RelOptInfo* rel, List* bitmapquals)
{
    BitmapAndPath* pathnode = makeNode(BitmapAndPath);

    pathnode->is_ustore = rel->is_ustore;
    pathnode->path.pathtype = T_BitmapAnd;
    pathnode->path.parent = rel;
    pathnode->path.param_info = NULL; /* not used in bitmap trees */
    pathnode->path.pathkeys = NIL;    /* always unordered */
    pathnode->path.exec_type = SetBasePathExectype(root, rel);

    pathnode->bitmapquals = bitmapquals;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;
        pathnode->path.rangelistOid = rel->rangelistOid;

        /* add location information for bitmap and path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif

    /* this sets bitmapselectivity as well as the regular cost fields: */
    cost_bitmap_and_node(pathnode, root);

    return pathnode;
}

/*
 * create_bitmap_or_path
 *	  Creates a path node representing a BitmapOr.
 */
BitmapOrPath* create_bitmap_or_path(PlannerInfo* root, RelOptInfo* rel, List* bitmapquals)
{
    BitmapOrPath* pathnode = makeNode(BitmapOrPath);

    pathnode->is_ustore = rel->is_ustore;
    pathnode->path.pathtype = T_BitmapOr;
    pathnode->path.parent = rel;
    pathnode->path.param_info = NULL; /* not used in bitmap trees */
    pathnode->path.pathkeys = NIL;    /* always unordered */
    pathnode->path.exec_type = SetBasePathExectype(root, rel);

    pathnode->bitmapquals = bitmapquals;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;
        pathnode->path.rangelistOid = rel->rangelistOid;

        /* add location information for bitmap or path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif

    /* this sets bitmapselectivity as well as the regular cost fields: */
    cost_bitmap_or_node(pathnode, root);

    return pathnode;
}

/*
 * create_tidscan_path
 *	  Creates a path corresponding to a scan by TID, returning the pathnode.
 */
TidPath* create_tidscan_path(PlannerInfo* root, RelOptInfo* rel, List* tidquals)
{
    TidPath* pathnode = makeNode(TidPath);

    pathnode->path.pathtype = T_TidScan;
    pathnode->path.parent = rel;
    pathnode->path.param_info = NULL; /* never parameterized at present */
    pathnode->path.pathkeys = NIL;    /* always unordered */
    pathnode->path.exec_type = SetBasePathExectype(root, rel);

    pathnode->tidquals = tidquals;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;
        pathnode->path.rangelistOid = rel->rangelistOid;

        /* add location information for TID scan path */
        RangeTblEntry* rte = root->simple_rte_array[rel->relid];
        Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif

    cost_tidscan(&pathnode->path, root, rel, tidquals);

    return pathnode;
}

/*
 * append_collect_upper_params
 *    Collect the upper params from the sub-path list.
 */
Bitmapset* append_collect_upper_params(List *subpaths)
{
    Bitmapset* upper_params = NULL;
    ListCell *l = NULL;
    foreach (l, subpaths) {
        Path* subpath = (Path*)lfirst(l);
        upper_params = bms_union(upper_params, PATH_REQ_UPPER(subpath));
    }

    return upper_params;
}

/*
 * create_append_path
 *	  Creates a path corresponding to an Append plan, returning the
 *	  pathnode.
 *
 * Note that we must handle subpaths = NIL, representing a dummy access path.
 */
AppendPath* create_append_path(PlannerInfo* root, RelOptInfo* rel, List* subpaths, Relids required_outer)
{
    AppendPath* pathnode = makeNode(AppendPath);
    ListCell* l = NULL;
    double local_rows = 0;
    Bitmapset* upper_params = append_collect_upper_params(subpaths);

    pathnode->path.pathtype = T_Append;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_appendrel_parampathinfo(rel, required_outer, upper_params);
    pathnode->path.pathkeys = NIL; /* result is always considered
                                    * unsorted */
    pathnode->subpaths = subpaths;

    /*
     * We don't bother with inventing a cost_append(), but just do it here.
     *
     * Compute rows and costs as sums of subplan rows and costs.  We charge
     * nothing extra for the Append itself, which perhaps is too optimistic,
     * but since it doesn't do any selection or projection, it is a pretty
     * cheap node.	If you change this, see also make_append().
     */
    set_path_rows(&pathnode->path, 0, rel->multiple);
    pathnode->path.startup_cost = 0;
    pathnode->path.total_cost = 0;

    pathnode->path.exec_type = EXEC_ON_DATANODES;
    foreach (l, subpaths) {
        Path* subpath = (Path*)lfirst(l);
        if (subpath->exec_type == EXEC_ON_COORDS) {
            pathnode->path.exec_type = EXEC_ON_COORDS;
            break;
        }
    }

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        /*
         * This function can alter subpaths's rows, AppendPath's rows rely on it.
         * So this function need be in advance.
         */
        mark_append_path(root, rel, (Path*)pathnode, subpaths);
    } else {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;

        Distribution* distribution = ng_get_default_computing_group_distribution();
        ng_copy_distribution(&pathnode->path.distribution, distribution);

        if (IsLocatorDistributedBySlice(pathnode->path.locator_type)) {
            /* at this stage append will not inherit distribute_keys for list/range. */
            pathnode->path.distribute_keys = NIL;
        }
    }
#endif

    bool all_parallelized = true;

    /*
     * Handle the HDFS scan situation.
     */
    if (2 == list_length(subpaths)) {
        Path* p1 = (Path*)linitial(subpaths);
        Path* p2 = (Path*)lsecond(subpaths);
        if (p1->pathtype == T_DfsScan && p2->pathtype == T_SeqScan) {
            if (p1->dop > 1) {
                all_parallelized = true;
                p2->dop = p1->dop;
            }
        }
    }

    /*
     * Check if all the subpaths already paralleled,
     * then we can parallel the append path.
     * Otherwise, we need to add local gather
     * above the parallelized subpaths.
     */
    foreach (l, subpaths) {
        Path* subpath = (Path*)lfirst(l);
        if (subpath->dop <= 1)
            all_parallelized = false;
    }

    pathnode->path.dop = (subpaths != NULL && all_parallelized) ? u_sess->opt_cxt.query_dop : 1;

    foreach (l, subpaths) {
        Path* subpath = (Path*)lfirst(l);

        local_rows += PATH_LOCAL_ROWS(subpath);
        pathnode->path.rows += subpath->rows;

        /*
         * Add local gather above the parallelized subpath.
         * Do not allow adding stream path where current subpath was parameterized.
         */
        if (subpath->dop > 1 && !all_parallelized) {
            if (subpath->param_info) {
                /* free memory before return NULL */
                pathnode->path.parent = NULL;
                /*
                 * There could be a new papraminfo build in get_appendrel_parampathinfo(),
                 * but it is unneccessary to worry about the memory leak as we will free it
                 * after all by reseting OptimizerContext.
                 */
                pathnode->path.param_info = NULL;
                pathnode->subpaths = NIL;
                pathnode->path.distribute_keys = NIL;
                bms_free_ext(pathnode->path.distribution.bms_data_nodeids);
                pfree_ext(pathnode);
                pathnode = NULL;
                return pathnode;
            }

            if (IsA(subpath, StreamPath)) {
                StreamPath* stream = (StreamPath*)subpath;
                stream->smpDesc->consumerDop = 1;
            } else {
                ParallelDesc* smp_desc = create_smpDesc(1, subpath->dop, LOCAL_ROUNDROBIN);
                subpath = create_stream_path(
                    root, subpath->parent, STREAM_REDISTRIBUTE, NIL, NIL, subpath, 1.0, NULL, smp_desc);
                lfirst(l) = (void*)subpath;
            }
        }

        if (l == list_head(subpaths)) /* first node? */
            pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost += subpath->total_cost;
        pathnode->path.stream_cost += subpath->stream_cost;

        /* All child paths must have same parameterization */
        AssertEreport(bms_equal(PATH_REQ_OUTER(subpath), required_outer),
            MOD_OPT_JOIN,
            "All child paths must have same parameterization");
    }

    /* DFS relation scan */
    if (rel->rtekind == RTE_RELATION && pathnode->path.param_info == NULL) {
        /* Set dfs base rel rows, rel rows can be change when include rows hint. */
        pathnode->path.rows = rel->rows;
    }

    /* Calculate overal multiple for append path */
    if (pathnode->path.rows != 0)
        pathnode->path.multiple = local_rows / pathnode->path.rows * ng_get_dest_num_data_nodes((Path*)pathnode);
    return pathnode;
}

/*
 * create_merge_append_path
 *	  Creates a path corresponding to a MergeAppend plan, returning the
 *	  pathnode.
 */
MergeAppendPath* create_merge_append_path(
    PlannerInfo* root, RelOptInfo* rel, List* subpaths, List* pathkeys, Relids required_outer)
{
    MergeAppendPath* pathnode = makeNode(MergeAppendPath);
    Cost input_startup_cost;
    Cost input_total_cost;
    Cost input_stream_cost;
    ListCell* l = NULL;
    Bitmapset* upper_params = append_collect_upper_params(subpaths);

    pathnode->path.pathtype = T_MergeAppend;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_appendrel_parampathinfo(rel, required_outer, upper_params);
    pathnode->path.pathkeys = pathkeys;
    pathnode->subpaths = subpaths;

    /*
     * Apply query-wide LIMIT if known and path is for sole base relation.
     * (Handling this at this low level is a bit klugy.)
     */
    if (bms_equal(rel->relids, root->all_baserels))
        pathnode->limit_tuples = root->limit_tuples;
    else
        pathnode->limit_tuples = -1.0;

    /*
     * Add up the sizes and costs of the input paths.
     */
    set_path_rows(&pathnode->path, 0, rel->multiple);
    input_startup_cost = 0;
    input_total_cost = 0;
    input_stream_cost = 0;
    pathnode->mem_info = (OpMemInfo*)palloc0(sizeof(OpMemInfo) * list_length(subpaths));
    int i = 0;
    foreach (l, subpaths) {
        Path* subpath = (Path*)lfirst(l);
        /*
         * For correlated subplan, there will be a broadcast added later,
         * so make the righ estimation of rows beforehand
         */
        bool needbroadcast = root->is_correlated && !is_replicated_path(subpath);

        pathnode->path.rows += subpath->rows;

        if (pathkeys_contained_in(pathkeys, subpath->pathkeys) && !needbroadcast) {
            /* Subpath is adequately ordered, we won't need to sort it */
            input_startup_cost += subpath->startup_cost;
            input_total_cost += subpath->total_cost;
            input_stream_cost += subpath->stream_cost;
        } else {
            /* We'll need to insert a Sort node, so include cost for that */
            Path sort_path; /* dummy for result of cost_sort */
            int subpath_width = get_path_actual_total_width(subpath, root->glob->vectorized, OP_SORT);

            cost_sort(&sort_path,
                pathkeys,
                subpath->total_cost,
                needbroadcast ? subpath->parent->tuples : RELOPTINFO_LOCAL_FIELD(root, subpath->parent, tuples),
                subpath_width,
                0.0,
                u_sess->opt_cxt.op_work_mem,
                pathnode->limit_tuples,
                root->glob->vectorized,
                1,
                &pathnode->mem_info[i]);
            input_startup_cost += sort_path.startup_cost;
            input_total_cost += sort_path.total_cost;
            input_stream_cost += sort_path.stream_cost;
        }

        /* All child paths must have same parameterization */
        AssertEreport(bms_equal(PATH_REQ_OUTER(subpath), required_outer),
            MOD_OPT_JOIN,
            "All child paths must have same parameterization");
        i++;
    }

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        mark_append_path(root, rel, (Path*)pathnode, subpaths);
    } else {
        pathnode->path.distribute_keys = rel->distribute_keys;
        pathnode->path.locator_type = rel->locator_type;
        pathnode->path.exec_type = SetBasePathExectype(root, rel);
        Distribution* distribution = ng_get_default_computing_group_distribution();
        ng_copy_distribution(&pathnode->path.distribution, distribution);

        if (IsLocatorDistributedBySlice(pathnode->path.locator_type)) {
            /* at this stage append will not inherit distribute_keys for list/range. */
            pathnode->path.distribute_keys = NIL;
        }
    }
#endif

    /* Now we can compute total costs of the MergeAppend */
    cost_merge_append(&pathnode->path,
        root,
        pathkeys,
        list_length(subpaths),
        input_startup_cost,
        input_total_cost,
        get_local_rows(rel->tuples,
            rel->multiple,
            IsLocatorReplicated(rel->locator_type),
            ng_get_dest_num_data_nodes(&pathnode->path)));
    pathnode->path.stream_cost = input_stream_cost;

    foreach (l, subpaths) {
        Path* subpath = (Path*)lfirst(l);

        if (subpath->dop > 1) {
            /* Do not allow adding stream path where current subpath was parameterized. */
            if (subpath->param_info) {
                /* free memory before return NULL */
                pathnode->path.parent = NULL;
                /*
                 * There could be a new papraminfo build in get_appendrel_parampathinfo(),
                 * but it is unneccessary to worry about the memory leak as we will free it
                 * after all by reseting OptimizerContext.
                 */
                pathnode->path.param_info = NULL;
                pathnode->subpaths = NIL;
                pathnode->path.distribute_keys = NIL;
                bms_free_ext(pathnode->path.distribution.bms_data_nodeids);
                pfree_ext(pathnode);
                pathnode = NULL;
                return pathnode;
            }

            /* Add local gather above the parallelized subpath. */
            if (IsA(subpath, StreamPath)) {
                StreamPath* stream = (StreamPath*)subpath;
                stream->smpDesc->consumerDop = 1;
            } else {
                ParallelDesc* smp_desc = create_smpDesc(1, subpath->dop, LOCAL_ROUNDROBIN);
                subpath = create_stream_path(root,
                    subpath->parent,
                    STREAM_REDISTRIBUTE,
                    subpath->distribute_keys,
                    NIL,
                    subpath,
                    1.0,
                    NULL,
                    smp_desc);
                lfirst(l) = (void*)subpath;
            }
            /* All child paths must have same parameterization */
            AssertEreport(bms_equal(PATH_REQ_OUTER(subpath), required_outer),
                MOD_OPT_JOIN,
                "All child paths must have same parameterization");
        }
    }

    return pathnode;
}

/*
 * create_result_path
 *	  Creates a path representing a Result-and-nothing-else plan.
 *	  This is only used for the case of a query with an empty jointree.
 */
ResultPath* create_result_path(PlannerInfo *root, RelOptInfo *rel, List* quals,
                               Path* subpath, Bitmapset *upper_params)
{
    ResultPath* pathnode = makeNode(ResultPath);

    pathnode->path.pathtype = T_BaseResult;
    pathnode->path.pathkeys = NIL;
    pathnode->subpath = subpath;
    if (subpath != NULL) {
        if (subpath->param_info != NULL) {
            pathnode->path.param_info = subpath->param_info;
            pathnode->path.param_info->ppi_req_upper = upper_params;
        } else {
            pathnode->path.param_info = get_baserel_parampathinfo(root, rel, NULL, upper_params);
        }

        pathnode->path.pathkeys = subpath->pathkeys;
        pathnode->path.parent = subpath->parent;
        pathnode->pathqual = quals;
        set_path_rows(&pathnode->path, clamp_row_est(Max(subpath->rows * DEFAULT_EQ_SEL, 1)));
        pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost = subpath->total_cost;
        pathnode->path.dop = subpath->dop;
        pathnode->path.stream_cost = subpath->stream_cost;
        pathnode->path.exec_type = subpath->exec_type;
#ifdef STREAMPLAN
        /* result path will inherit node group and distribute information from it's child node */
        inherit_path_locator_info((Path*)pathnode, subpath);
#endif
    } else {
        pathnode->path.parent = NULL;
        pathnode->quals = quals;
        /* Hardly worth defining a cost_result() function ... just do it */
        set_path_rows(&pathnode->path, 1, 1);
        pathnode->path.startup_cost = 0;
        pathnode->path.total_cost = u_sess->attr.attr_sql.cpu_tuple_cost;
        pathnode->path.stream_cost = 0;
        pathnode->path.exec_type = EXEC_ON_ALL_NODES;

        Distribution* distribution = ng_get_default_computing_group_distribution();
        ng_set_distribution(&pathnode->path.distribution, distribution);
    }

    /*
     * In theory we should include the qual eval cost as well, but at present
     * that doesn't accomplish much except duplicate work that will be done
     * again in make_result; since this is only used for degenerate cases,
     * nothing interesting will be done with the path cost values...
     */
    return pathnode;
}

/*
 * create_resultscan_path
 *       Creates a path corresponding to a scan of an RTE_RESULT relation,
 *       returning the pathnode.
 */
Path *create_resultscan_path(PlannerInfo *root, RelOptInfo *rel,
    Relids required_outer)
{
    Path       *pathnode = makeNode(Path);

    pathnode->pathtype = T_BaseResult;
    pathnode->parent = rel;
    pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer);
    pathnode->pathkeys = NIL;       /* result is always unordered */

    cost_resultscan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}

/*
 * create_material_path
 *	  Creates a path corresponding to a Material plan, returning the
 *	  pathnode.
 */
MaterialPath* create_material_path(Path* subpath, bool materialize_all)
{
    MaterialPath* pathnode = makeNode(MaterialPath);
    double input_global_rows = subpath->rows;
    RelOptInfo* rel = subpath->parent;

    pathnode->path.pathtype = T_Material;
    pathnode->path.parent = rel;
    pathnode->path.param_info = subpath->param_info;
    pathnode->path.pathkeys = subpath->pathkeys;
    pathnode->path.dop = subpath->dop;
    pathnode->materialize_all = materialize_all;
    pathnode->path.exec_type = subpath->exec_type;

#ifdef STREAMPLAN
    /* material path will inherit node group and distribute information from it's child node */
    inherit_path_locator_info((Path*)pathnode, subpath);
#endif

    pathnode->subpath = subpath;
    set_path_rows(&pathnode->path, input_global_rows, subpath->multiple);

    cost_material(&pathnode->path, subpath->startup_cost, subpath->total_cost, PATH_LOCAL_ROWS(subpath), rel->width);
    pathnode->path.stream_cost = subpath->stream_cost;

    return pathnode;
}

/*
 * create_unique_path
 *	  Creates a path representing elimination of distinct rows from the
 *	  input data.  Distinct-ness is defined according to the needs of the
 *	  semijoin represented by sjinfo.  If it is not possible to identify
 *	  how to make the data unique, NULL is returned.
 *
 * If used at all, this is likely to be called repeatedly on the same rel;
 * and the input subpath should always be the same (the cheapest_total path
 * for the rel).  So we cache the result.
 */
UniquePath* create_unique_path(PlannerInfo* root, RelOptInfo* rel, Path* subpath, SpecialJoinInfo* sjinfo)
{
    UniquePath* pathnode = NULL;
    Path sort_path; /* dummy for result of cost_sort */
    Path agg_path;  /* dummy for result of cost_agg */
    MemoryContext oldcontext;
    List* in_operators = NIL;
    List* uniq_exprs = NIL;
    bool all_btree = false;
    bool all_hash = false;
    int numCols;
    ListCell* lc = NULL;
    double local_rows, num_groups;
    OpMemInfo sort_mem_info, hash_mem_info;
    errno_t rc = 0;

    rc = memset_s(&sort_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&agg_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    /* Caller made a mistake if subpath isn't cheapest_total ... */
    foreach (lc, rel->cheapest_total_path) {
        if (subpath == lfirst(lc))
            break;
    }
    AssertEreport(lc != NULL, MOD_OPT_JOIN, "Subpath should be one of cheapest total path of rel");
    AssertEreport(subpath->parent == rel || subpath->parent->base_rel == rel, MOD_OPT_JOIN, "");
    /* ... or if SpecialJoinInfo is the wrong one */
    AssertEreport(sjinfo->jointype == JOIN_SEMI, MOD_OPT_JOIN, "Join type should be semi join");
    AssertEreport(
        bms_equal(rel->relids, sjinfo->syn_righthand), MOD_OPT_JOIN, "All relids should be within join right hand");

    /* If result already cached, return it */
    if (rel->cheapest_unique_path)
        return (UniquePath*)rel->cheapest_unique_path;

    /* If we previously failed, return NULL quickly */
    if (sjinfo->join_quals == NIL)
        return NULL;

    /*
     * We must ensure path struct and subsidiary data are allocated in main
     * planning context; otherwise GEQO memory management causes trouble.
     */
    oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    /* ----------
     * Look to see whether the semijoin's join quals consist of AND'ed
     * equality operators, with (only) RHS variables on only one side of
     * each one.  If so, we can figure out how to enforce uniqueness for
     * the RHS.
     *
     * Note that the input join_quals list is the list of quals that are
     * *syntactically* associated with the semijoin, which in practice means
     * the synthesized comparison list for an IN or the WHERE of an EXISTS.
     * Particularly in the latter case, it might contain clauses that aren't
     * *semantically* associated with the join, but refer to just one side or
     * the other.  We can ignore such clauses here, as they will just drop
     * down to be processed within one side or the other.  (It is okay to
     * consider only the syntactically-associated clauses here because for a
     * semijoin, no higher-level quals could refer to the RHS, and so there
     * can be no other quals that are semantically associated with this join.
     * We do things this way because it is useful to be able to run this test
     * before we have extracted the list of quals that are actually
     * semantically associated with the particular join.)
     *
     * Note that the in_operators list consists of the joinqual operators
     * themselves (but commuted if needed to put the RHS value on the right).
     * These could be cross-type operators, in which case the operator
     * actually needed for uniqueness is a related single-type operator.
     * We assume here that that operator will be available from the btree
     * or hash opclass when the time comes ... if not, create_unique_plan()
     * will fail.
     * ----------
     */
    in_operators = NIL;
    uniq_exprs = NIL;
    all_btree = true;
    all_hash = u_sess->attr.attr_sql.enable_hashagg; /* don't consider hash if not enabled */
    foreach (lc, sjinfo->join_quals) {
        OpExpr* op = (OpExpr*)lfirst(lc);
        Oid opno;
        Node* left_expr = NULL;
        Node* right_expr = NULL;
        Relids left_varnos;
        Relids right_varnos;
        Relids all_varnos;
        Oid opinputtype;

        /* Is it a binary opclause? */
        if (!IsA(op, OpExpr) || list_length(op->args) != 2) {
            /* No, but does it reference both sides? */
            all_varnos = pull_varnos((Node*)op);
            if (!bms_overlap(all_varnos, sjinfo->syn_righthand) || bms_is_subset(all_varnos, sjinfo->syn_righthand)) {
                /*
                 * Clause refers to only one rel, so ignore it --- unless it
                 * contains volatile functions, in which case we'd better
                 * punt.
                 */
                if (contain_volatile_functions((Node*)op))
                    goto no_unique_path;
                continue;
            }
            /* Non-operator clause referencing both sides, must punt */
            goto no_unique_path;
        }

        /* Extract data from binary opclause */
        opno = op->opno;
        left_expr = (Node*)linitial(op->args);
        right_expr = (Node*)lsecond(op->args);
        left_varnos = pull_varnos(left_expr);
        right_varnos = pull_varnos(right_expr);
        all_varnos = bms_union(left_varnos, right_varnos);
        opinputtype = exprType(left_expr);

        /* Does it reference both sides? */
        if (!bms_overlap(all_varnos, sjinfo->syn_righthand) || bms_is_subset(all_varnos, sjinfo->syn_righthand)) {
            /*
             * Clause refers to only one rel, so ignore it --- unless it
             * contains volatile functions, in which case we'd better punt.
             */
            if (contain_volatile_functions((Node*)op))
                goto no_unique_path;
            continue;
        }

        /* check rel membership of arguments */
        if (!bms_is_empty(right_varnos) && bms_is_subset(right_varnos, sjinfo->syn_righthand) &&
            !bms_overlap(left_varnos, sjinfo->syn_righthand)) {
            /* typical case, right_expr is RHS variable */
        } else if (!bms_is_empty(left_varnos) && bms_is_subset(left_varnos, sjinfo->syn_righthand) &&
                   !bms_overlap(right_varnos, sjinfo->syn_righthand)) {
            /* flipped case, left_expr is RHS variable */
            opno = get_commutator(opno);
            if (!OidIsValid(opno))
                goto no_unique_path;
            right_expr = left_expr;
        } else
            goto no_unique_path;

        /* all operators must be btree equality or hash equality */
        if (all_btree) {
            /* oprcanmerge is considered a hint... */
            if (!op_mergejoinable(opno, opinputtype) || get_mergejoin_opfamilies(opno) == NIL)
                all_btree = false;
        }
        if (all_hash) {
            /* ... but oprcanhash had better be correct */
            if (!op_hashjoinable(opno, opinputtype))
                all_hash = false;
        }
        if (!(all_btree || all_hash))
            goto no_unique_path;

        /* so far so good, keep building lists */
        in_operators = lappend_oid(in_operators, opno);
        uniq_exprs = lappend(uniq_exprs, copyObject(right_expr));
    }

    /* Punt if we didn't find at least one column to unique-ify */
    if (uniq_exprs == NIL)
        goto no_unique_path;

    /*
     * The expressions we'd need to unique-ify mustn't be volatile.
     */
    if (contain_volatile_functions((Node*)uniq_exprs))
        goto no_unique_path;

    /*
     * If we get here, we can unique-ify using at least one of sorting and
     * hashing.  Start building the result Path object.
     */
    pathnode = makeNode(UniquePath);
    pathnode->path.pathtype = T_Unique;
    pathnode->path.parent = rel;
    pathnode->path.param_info = subpath->param_info;
    pathnode->path.dop = subpath->dop;

    /*
     * Assume the output is unsorted, since we don't necessarily have pathkeys
     * to represent it.  (This might get overridden below.)
     */
    pathnode->path.pathkeys = NIL;

    pathnode->subpath = subpath;
    pathnode->in_operators = in_operators;
    pathnode->uniq_exprs = uniq_exprs;
    pathnode->both_method = false;
    pathnode->hold_tlist = false;
    pathnode->path.exec_type = subpath->exec_type;

#ifdef STREAMPLAN
    inherit_path_locator_info((Path*)pathnode, subpath);
#endif

    /*
     * If the input is a relation and it has a unique index that proves the
     * uniq_exprs are unique, then we don't need to do anything.  Note that
     * relation_has_unique_index_for automatically considers restriction
     * clauses for the rel, as well.
     */
    if (rel->rtekind == RTE_RELATION && all_btree &&
        relation_has_unique_index_for(root, rel, NIL, uniq_exprs, in_operators)) {
        pathnode->umethod = UNIQUE_PATH_NOOP;
        set_path_rows(&pathnode->path, rel->rows, subpath->multiple);
        pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost = subpath->total_cost;
        pathnode->path.stream_cost = subpath->stream_cost;
        pathnode->path.pathkeys = subpath->pathkeys;

        rel->cheapest_unique_path = (Path*)pathnode;

        (void)MemoryContextSwitchTo(oldcontext);

        return pathnode;
    }

    /*
     * If the input is a subquery whose output must be unique already, then we
     * don't need to do anything.  The test for uniqueness has to consider
     * exactly which columns we are extracting; for example "SELECT DISTINCT
     * x,y" doesn't guarantee that x alone is distinct. So we cannot check for
     * this optimization unless uniq_exprs consists only of simple Vars
     * referencing subquery outputs.  (Possibly we could do something with
     * expressions in the subquery outputs, too, but for now keep it simple.)
     */
    if (rel->rtekind == RTE_SUBQUERY) {
        RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);

        if (query_supports_distinctness(rte->subquery)) {
            List* sub_tlist_colnos = translate_sub_tlist(uniq_exprs, rel->relid);

            if (sub_tlist_colnos != NIL && query_is_distinct_for(rte->subquery, sub_tlist_colnos, in_operators)) {
                pathnode->umethod = UNIQUE_PATH_NOOP;
                pathnode->path.rows = rel->rows;
                pathnode->path.startup_cost = subpath->startup_cost;
                pathnode->path.total_cost = subpath->total_cost;
                pathnode->path.pathkeys = subpath->pathkeys;

                rel->cheapest_unique_path = (Path*)pathnode;

                MemoryContextSwitchTo(oldcontext);

                return pathnode;
            }
        }
    }

    /* Estimate number of output rows */
    local_rows = RELOPTINFO_LOCAL_FIELD(root, rel, rows);
    num_groups =
        estimate_num_groups(root, uniq_exprs, local_rows, ng_get_dest_num_data_nodes(root, rel), STATS_TYPE_LOCAL);
    pathnode->path.rows = Min(get_global_rows(num_groups, 1.0, ng_get_dest_num_data_nodes((Path*)pathnode)), rel->rows);
    if (pathnode->path.rows != 0)
        pathnode->path.multiple = num_groups / pathnode->path.rows * ng_get_dest_num_data_nodes((Path*)pathnode);
    numCols = list_length(uniq_exprs);

    rc = memset_s(&sort_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&hash_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    if (all_btree) {
        int subpath_width = get_path_actual_total_width(subpath, root->glob->vectorized, OP_SORT);

        /*
         * Estimate cost for sort+unique implementation
         */
        cost_sort(&sort_path,
            NIL,
            subpath->total_cost,
            local_rows,
            subpath_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized,
            1,
            &sort_mem_info);

        /*
         * Charge one cpu_operator_cost per comparison per input tuple. We
         * assume all columns get compared at most of the tuples. (XXX
         * probably this is an overestimate.)  This should agree with
         * make_unique.
         */
        sort_path.total_cost +=
            u_sess->attr.attr_sql.cpu_operator_cost * RELOPTINFO_LOCAL_FIELD(root, rel, rows) * numCols;
    }

    if (all_hash) {
        Size hashentrysize = 0;

        if (root->glob->vectorized)
            hashentrysize = get_path_actual_total_width(subpath, root->glob->vectorized, OP_HASHAGG, 0);
        else
            hashentrysize = get_hash_entry_size(rel->width);

        Distribution* distribution = ng_get_dest_distribution((Path*)pathnode);
        ng_copy_distribution(&agg_path.distribution, distribution);
        cost_agg(&agg_path,
            root,
            AGG_HASHED,
            NULL,
            numCols,
            num_groups,
            subpath->startup_cost,
            subpath->total_cost,
            local_rows,
            rel->width,
            hashentrysize,
            1,
            &hash_mem_info);
    }

    if (all_btree && all_hash) {
        if (agg_path.total_cost < sort_path.total_cost)
            pathnode->umethod = UNIQUE_PATH_HASH;
        else
            pathnode->umethod = UNIQUE_PATH_SORT;

        pathnode->both_method = true;
    } else if (all_btree) {
        pathnode->umethod = UNIQUE_PATH_SORT;
    } else if (all_hash) {
        pathnode->umethod = UNIQUE_PATH_HASH;
    } else {
        goto no_unique_path;
    }

    if (pathnode->umethod == UNIQUE_PATH_HASH) {
        pathnode->path.startup_cost = agg_path.startup_cost;
        pathnode->path.total_cost = agg_path.total_cost;
        rc = memcpy_s(&pathnode->mem_info, sizeof(OpMemInfo), &hash_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    } else {
        pathnode->path.startup_cost = sort_path.startup_cost;
        pathnode->path.total_cost = sort_path.total_cost;
        rc = memcpy_s(&pathnode->mem_info, sizeof(OpMemInfo), &sort_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    }

    pathnode->path.stream_cost = subpath->stream_cost;
    rel->cheapest_unique_path = (Path*)pathnode;

    (void)MemoryContextSwitchTo(oldcontext);

    return pathnode;

no_unique_path: /* failure exit */

    /* Mark the SpecialJoinInfo as not unique-able */
    sjinfo->join_quals = NIL;

    (void)MemoryContextSwitchTo(oldcontext);

    return NULL;
}

/*
 * translate_sub_tlist - get subquery column numbers represented by tlist
 *
 * The given targetlist usually contains only Vars referencing the given relid.
 * Extract their varattnos (ie, the column numbers of the subquery) and return
 * as an integer List.
 *
 * If any of the tlist items is not a simple Var, we cannot determine whether
 * the subquery's uniqueness condition (if any) matches ours, so punt and
 * return NIL.
 */
static List* translate_sub_tlist(List* tlist, int relid)
{
    List* result = NIL;
    ListCell* l = NULL;

    foreach (l, tlist) {
        Var* var = (Var*)lfirst(l);

        if (var == NULL || !IsA(var, Var) || var->varno != (unsigned int)relid)
            return NIL; /* punt */

        result = lappend_int(result, var->varattno);
    }
    return result;
}

/*
 * create_paritial_push_path
 *	  add gahter above subplan if query cannot push
 */
Plan* create_paritial_push_plan(PlannerInfo* root, RelOptInfo* rel)
{
    /* no need to add gather above subplan if parent can push */
    if (root->parse->can_push) {
        return rel->subplan;
    }

    /* need to add gather above subplan if parent can not push */
    if (rel->subplan->exec_type == EXEC_ON_DATANODES) {
        Plan *remote_query = make_simple_RemoteQuery(rel->subplan, root, false);
        if (IsA(rel->subplan, Sort)) {
            SimpleSort *simple_sort = makeNode(SimpleSort);

            simple_sort->numCols = ((Sort *) rel->subplan)->numCols;
            simple_sort->sortColIdx = ((Sort *) rel->subplan)->sortColIdx;
            simple_sort->sortOperators = ((Sort *) rel->subplan)->sortOperators;
            simple_sort->nullsFirst = ((Sort *) rel->subplan)->nullsFirst;
            simple_sort->sortToStore = false;
            simple_sort->sortCollations = ((Sort *) rel->subplan)->collations;
            if (IsA(remote_query, RemoteQuery))
                ((RemoteQuery*)remote_query)->sort = simple_sort;
            else if (IsA(remote_query, Stream)) {
                ((Stream*)remote_query)->sort = simple_sort;
            }
        }
        rel->subplan = remote_query;
        return rel->subplan;
    } else {
        /*
         * If a query contains dummy subquery, for example, select 1, and the query
         * contains non-push-down factors, the execution node of the result plan
         * generated by select1 is changed to CN execution.  In this case,
         * mark_stream_unsupport is executed in the finalize_node_id interface to form
         * a non-stream plan.
         */
        rel->subplan->exec_type = EXEC_ON_COORDS;
        return rel->subplan;
    }

    return rel->subplan;
}

/*
 * create_subqueryscan_path
 *	  Creates a path corresponding to a sequential scan of a subquery,
 *	  returning the pathnode.
 */
Path* create_subqueryscan_path(PlannerInfo* root, RelOptInfo* rel, List* pathkeys, Relids required_outer, List* subplan_params)
{
    SubqueryScanPath* subquery_path = makeNode(SubqueryScanPath);
    Path* pathnode = (Path *)subquery_path;
    Bitmapset *upper_params = NULL;
    Bitmapset *curr_params = rel->subroot->param_upper;

    if (subplan_params == NULL) {
        upper_params = curr_params;
    } else if (curr_params != NULL) {
        int paramid = -1;
        bool find = false;
        while((paramid = bms_next_member(curr_params, paramid)) >= 0) {
            ListCell *ppl = NULL;
            find = false;
            foreach (ppl, subplan_params) {
                PlannerParamItem *pitem = (PlannerParamItem*)lfirst(ppl);
                if (pitem->paramId == paramid)
                {
                    required_outer = bms_union(required_outer, pull_varnos(pitem->item));
                    find = true;
                    break;
                }
            }

            if (!find)
                upper_params = bms_add_member(upper_params, paramid);
        }
    }

    pathnode->pathtype = T_SubqueryScan;
    pathnode->parent = rel;
    pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer, upper_params);
    pathnode->pathkeys = pathkeys;
    pathnode->exec_type = rel->subplan->exec_type;

    cost_subqueryscan(pathnode, root, rel, pathnode->param_info);

    /* reset distribute keys ,set it later. */
    list_free_ext(rel->distribute_keys);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {

        Plan* subplan = create_paritial_push_plan(root, rel);

        if (subplan->dop > 1)
            pathnode->dop = subplan->dop;
        else
            pathnode->dop = 1;

        if (is_execute_on_datanodes(subplan)) {
            if (is_replicated_plan(subplan)) {
                rel->distribute_keys = NULL;
                rel->locator_type = LOCATOR_TYPE_REPLICATED;
            } else if (is_hashed_plan(subplan) || is_rangelist_plan(subplan)) {
                List* distribute_index =
                    distributeKeyIndex(rel->subroot, subplan->distributed_keys, subplan->targetlist);
                if (distribute_index == NIL) {
                    rel->distribute_keys = NIL;
                } else {
                    ListCell* lc = NULL;
                    ListCell* lc2 = NULL;
                    foreach (lc, distribute_index) {
                        int resno = lfirst_int(lc);
                        Var* relvar = NULL;

                        if (rel->base_rel != NULL && !SUBQUERY_IS_PARAM((unsigned int)rel->subroot)) {
                            /*
                             * for cost-base query rewrite dummy subquery rel, subplan targetlist
                             * is in same order as rel targetlist, so find it by sequence
                             */
                            Expr* expr = (Expr*)list_nth(rel->reltargetlist, resno - 1);
                            relvar = locate_distribute_var(expr);
                            AssertEreport(relvar != NULL, MOD_OPT, "");
                            rel->distribute_keys = lappend(rel->distribute_keys, relvar);
                        } else {
                            /*
                             * Find from subquery targetlist for distribute key. We should traverse
                             * the targetlist and get the real var, because targetlist of subquery can
                             * be a subset of subplan's targetlist, and there can be type cast on base
                             * vars
                             */
                            foreach (lc2, rel->reltargetlist) {
                                relvar = locate_distribute_var((Expr*)lfirst(lc2));
                                if (relvar != NULL && relvar->varattno == resno)
                                    break;
                            }
                            /* Find it, then add it to subquery distribute key, or set it to null */
                            if (lc2 != NULL)
                                rel->distribute_keys = lappend(rel->distribute_keys, relvar);
                            else {
                                list_free_ext(rel->distribute_keys);
                                rel->distribute_keys = NIL;
                                break;
                            }
                        }
                    }
                }
                rel->locator_type = get_locator_type(subplan);
            }
        } else {
            rel->distribute_keys = NIL;
            rel->locator_type = LOCATOR_TYPE_REPLICATED;
        }
        pathnode->exec_type = subplan->exec_type;
    }

    pathnode->distribute_keys = rel->distribute_keys;
    pathnode->locator_type = rel->locator_type;

    /* For subquery scan, read it's node group information from sub-plan directly */
    Distribution* distribution = ng_get_dest_distribution(rel->subplan);
    ng_copy_distribution(&pathnode->distribution, distribution);
#endif

    subquery_path->subroot = rel->subroot;
    subquery_path->subplan = rel->subplan;
    subquery_path->subplan_params = subplan_params;
    return pathnode;
}

/*
 * create_subqueryscan_path_reparam
 *	  Creates a path corresponding to a sequential scan of a subquery,
 *	  returning the pathnode, only used in reparameterize_path.
 */
Path* create_subqueryscan_path_reparam(PlannerInfo* root, RelOptInfo* rel, List* pathkeys, Relids required_outer, List* subplan_params)
{
    SubqueryScanPath* subquery_path = makeNode(SubqueryScanPath);
    Path* pathnode = (Path *)subquery_path;
    Bitmapset *upper_params = NULL;
    Bitmapset *curr_params = rel->subroot->param_upper;

    if (subplan_params == NULL) {
        upper_params = curr_params;
    } else if (curr_params != NULL) {
        int paramid = -1;
        bool find = false;
        while((paramid = bms_next_member(curr_params, paramid)) >= 0) {
            ListCell *ppl = NULL;
            find = false;
            foreach (ppl, subplan_params) {
                PlannerParamItem *pitem = (PlannerParamItem*)lfirst(ppl);
                if (pitem->paramId == paramid)
                {
                    required_outer = bms_union(required_outer, pull_varnos(pitem->item));
                    find = true;
                    break;
                }
            }

            if (!find)
                upper_params = bms_add_member(upper_params, paramid);
        }
    }

    pathnode->pathtype = T_SubqueryScan;
    pathnode->parent = rel;
    pathnode->param_info = get_subquery_parampathinfo(root, rel, required_outer, upper_params);
    pathnode->pathkeys = pathkeys;
    pathnode->exec_type = rel->subplan->exec_type;

    cost_subqueryscan(pathnode, root, rel, pathnode->param_info);

    /* reset distribute keys ,set it later. */
    list_free_ext(rel->distribute_keys);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN) {

        Plan* subplan = create_paritial_push_plan(root, rel);

        if (subplan->dop > 1)
            pathnode->dop = subplan->dop;
        else
            pathnode->dop = 1;

        if (is_execute_on_datanodes(subplan)) {
            if (is_replicated_plan(subplan)) {
                rel->distribute_keys = NULL;
                rel->locator_type = LOCATOR_TYPE_REPLICATED;
            } else if (is_hashed_plan(subplan)) {
                List* distribute_index =
                    distributeKeyIndex(rel->subroot, subplan->distributed_keys, subplan->targetlist);
                if (distribute_index == NIL) {
                    rel->distribute_keys = NIL;
                } else {
                    ListCell* lc = NULL;
                    ListCell* lc2 = NULL;
                    foreach (lc, distribute_index) {
                        int resno = lfirst_int(lc);
                        Var* relvar = NULL;

                        if (rel->base_rel != NULL && !SUBQUERY_IS_PARAM(rel->subroot)) {
                            /*
                             * for cost-base query rewrite dummy subquery rel, subplan targetlist
                             * is in same order as rel targetlist, so find it by sequence
                             */
                            Expr* expr = (Expr*)list_nth(rel->reltargetlist, resno - 1);
                            relvar = locate_distribute_var(expr);
                            AssertEreport(relvar != NULL, MOD_OPT, "");
                            rel->distribute_keys = lappend(rel->distribute_keys, relvar);
                        } else {
                            /*
                             * Find from subquery targetlist for distribute key. We should traverse
                             * the targetlist and get the real var, because targetlist of subquery can
                             * be a subset of subplan's targetlist, and there can be type cast on base
                             * vars
                             */
                            foreach (lc2, rel->reltargetlist) {
                                relvar = locate_distribute_var((Expr*)lfirst(lc2));
                                if (relvar != NULL && relvar->varattno == resno)
                                    break;
                            }
                            /* Find it, then add it to subquery distribute key, or set it to null */
                            if (lc2 != NULL)
                                rel->distribute_keys = lappend(rel->distribute_keys, relvar);
                            else {
                                list_free_ext(rel->distribute_keys);
                                rel->distribute_keys = NIL;
                                break;
                            }
                        }
                    }
                }
                rel->locator_type = get_locator_type(subplan);
            }
        } else {
            rel->distribute_keys = NIL;
            rel->locator_type = LOCATOR_TYPE_REPLICATED;
        }
        pathnode->exec_type = subplan->exec_type;
    }

    pathnode->distribute_keys = rel->distribute_keys;
    pathnode->locator_type = rel->locator_type;

    /* For subquery scan, read it's node group information from sub-plan directly */
    Distribution* distribution = ng_get_dest_distribution(rel->subplan);
    ng_copy_distribution(&pathnode->distribution, distribution);
#endif

    subquery_path->subroot = rel->subroot;
    subquery_path->subplan = rel->subplan;
    subquery_path->subplan_params = subplan_params;
    return pathnode;
}

/*
 * create_functionscan_path
 *	  Creates a path corresponding to a sequential scan of a function,
 *	  returning the pathnode.
 */
Path* create_functionscan_path(PlannerInfo* root, RelOptInfo* rel, Relids required_outer)
{
    Path* pathnode = makeNode(Path);

    pathnode->pathtype = T_FunctionScan;
    pathnode->parent = rel;
    pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer);
    pathnode->pathkeys = NIL;    /* for now, assume unordered result */
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    pathnode->distribute_keys = rel->distribute_keys;
    pathnode->locator_type = rel->locator_type;
    pathnode->rangelistOid = rel->rangelistOid;

    /*
     * For function scan path, it's node group will relate to wheather it's in a correlated sub-plan
     * (1) In a correlated sub-plan, it's node group should as same as "correlated sub-plan node group"
     * (2) In a normal sub-plan, it's node group should be in "compute permission node group"
     */
    Distribution* distribution = NULL;
    if (root->is_correlated) {
        distribution = ng_get_correlated_subplan_group_distribution();
    } else {
        /* We need an exec on everywhere group */
        distribution = ng_get_max_computable_group_distribution();
    }
    ng_copy_distribution(&pathnode->distribution, distribution);
#endif

    cost_functionscan(pathnode, root, rel);

    return pathnode;
}

/*
 * create_valuesscan_path
 *	  Creates a path corresponding to a scan of a VALUES list,
 *	  returning the pathnode.
 */
Path* create_valuesscan_path(PlannerInfo* root, RelOptInfo* rel, Relids required_outer)
{
    Path* pathnode = makeNode(Path);

    pathnode->pathtype = T_ValuesScan;
    pathnode->parent = rel;
    pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer); /* never parameterized at present */
    pathnode->pathkeys = NIL;    /* result is always unordered */
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    pathnode->distribute_keys = NIL;
    pathnode->locator_type = LOCATOR_TYPE_REPLICATED;

    /*
     * For values scan path, it's node group will relate to wheather it's in a correlated sub-plan
     * (1) In a correlated sub-plan, it's node group should as same as "correlated sub-plan node group"
     * (2) In a normal sub-plan, it's node group should be in "compute permission node group"
     */
    Distribution* distribution = NULL;
    if (root->is_correlated) {
        distribution = ng_get_correlated_subplan_group_distribution();
    } else {
        /* We need an exec on everywhere group */
        distribution = ng_get_max_computable_group_distribution();
    }
    ng_copy_distribution(&pathnode->distribution, distribution);
#endif

    cost_valuesscan(pathnode, root, rel);

    return pathnode;
}

/*
 * create_ctescan_path
 *	  Creates a path corresponding to a scan of a non-self-reference CTE,
 *	  returning the pathnode.
 */
Path* create_ctescan_path(PlannerInfo* root, RelOptInfo* rel)
{
    Path* pathnode = makeNode(Path);

    pathnode->pathtype = T_CteScan;
    pathnode->parent = rel;
    pathnode->param_info = NULL; /* never parameterized at present */
    pathnode->pathkeys = NIL;    /* XXX for now, result is always unordered */
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    pathnode->distribute_keys = rel->distribute_keys;
    pathnode->locator_type = rel->locator_type;
    pathnode->rangelistOid = rel->rangelistOid;

    /* add location information for cte scan path */
    Distribution* distribution = ng_get_default_computing_group_distribution();
    ng_copy_distribution(&pathnode->distribution, distribution);
#endif

    cost_ctescan(pathnode, root, rel);

    return pathnode;
}

/*
 * create_worktablescan_path
 *	  Creates a path corresponding to a scan of a self-reference CTE,
 *	  returning the pathnode.
 */
Path* create_worktablescan_path(PlannerInfo* root, RelOptInfo* rel)
{
    Path* pathnode = makeNode(Path);

    pathnode->pathtype = T_WorkTableScan;
    pathnode->parent = rel;
    pathnode->param_info = NULL; /* never parameterized at present */
    pathnode->pathkeys = NIL;    /* result is always unordered */
    pathnode->exec_type = SetBasePathExectype(root, rel);

#ifdef STREAMPLAN
    /* build worktable's distribution info */
    if (IS_STREAM_PLAN && u_sess->attr.attr_sql.enable_stream_recursive) {
        Plan* none_recursive_plan = NULL;
        PlannerInfo* cur_root = root;

        /* Iteratively find the corresponding root to fetch the non-recursive plan */
        while (cur_root != NULL) {
            if (cur_root->hasRecursion) {
                none_recursive_plan = cur_root->non_recursive_plan;
                break;
            }

            cur_root = cur_root->parent_root;
        }

        if (none_recursive_plan == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("none_recursive_plan could not be NULL")));
        }
        /*
         * For worktablescan path, we inherit the plan distribution information from the
         * none-recursive term.
         */
        pathnode->distribute_keys = NIL;
        pathnode->locator_type = none_recursive_plan->exec_nodes->baselocatortype;

        /* add location information for cte scan path */
        Distribution* distribution = ng_get_default_computing_group_distribution();
        ng_copy_distribution(&pathnode->distribution, distribution);
    }
#endif

    /* Cost is the same as for a regular CTE scan */
    cost_ctescan(pathnode, root, rel);

    return pathnode;
}

/*
 * create_foreignscan_path
 *	  Creates a path corresponding to a scan of a foreign table,
 *	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any
 * way to calculate them in core.
 */
ForeignPath* create_foreignscan_path(PlannerInfo* root, RelOptInfo* rel, Cost startup_cost, Cost total_cost,
    List* pathkeys, Relids required_outer, List* fdw_private, int dop)
{
    ForeignPath* pathnode = makeNode(ForeignPath);

    pathnode->path.pathtype = T_ForeignScan;
    pathnode->path.parent = rel;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    set_path_rows(&pathnode->path, rel->rows, rel->multiple);
    pathnode->path.startup_cost = startup_cost;
    pathnode->path.total_cost = total_cost;
    pathnode->path.pathkeys = pathkeys;
    pathnode->path.locator_type = rel->locator_type;
    pathnode->path.exec_type = SetBasePathExectype(root, rel);
    pathnode->path.stream_cost = 0;

    pathnode->fdw_private = fdw_private;

    pathnode->path.dop = 1;
    dop = SET_DOP(dop);

    /* Create a parallel foreignscan path. */
    if (root->parse && dop > 1) {
        RangeTblEntry* source = rt_fetch(rel->relid, root->parse->rtable);

        AssertEreport(NULL != source, MOD_OPT_JOIN, "There should be rtable in table list");

        Oid tblId = source->relid;

        ServerTypeOption serverType = getServerType(tblId);

        /*
         * This function is called by each kind of FDW_handler's xxxForeignGetPaths, we should
         * judge which pg_foreign_server used in the query. Now we support SMP for server with
         * different scope.
         * OBS Server: we support OBS roundrobin table SMP feature for command
         *			  CMD_INSERT && CMD_SELECT
         *			  CMD_INSERT: insert into table select * from OBS_TBL;
         *			  CMD_SELECT: select xxx from OBS_TBL, table,xxx where xxx;
         *			  we support two kinds of OBS table, roundrobin and replicate. If we scan
         *			  roundrobin table, the execute plan always looks like
         *			  streaming(Gather) or streaming(redistribute)
         *				   foreign scan: obs table
         *			  It is comfortable to add smp foreign scan for this scenario.
         * HDFS Server: we don't add smp feature for this kind of server. No reason.
         * Others:	  Keep constant with the original logic.
         */
        if (T_OBS_SERVER == serverType) {
            if ((CMD_SELECT == root->parse->commandType || CMD_INSERT == root->parse->commandType) &&
                LOCATOR_TYPE_RROBIN == source->locator_type)
                pathnode->path.dop = u_sess->opt_cxt.query_dop;
        } else if (T_HDFS_SERVER == serverType) {
            if ((CMD_SELECT == root->parse->commandType || CMD_INSERT == root->parse->commandType) &&
                LOCATOR_TYPE_RROBIN == source->locator_type)
                pathnode->path.dop = u_sess->opt_cxt.query_dop;
        } else if (T_PGFDW_SERVER == serverType) {
            if ((CMD_SELECT == root->parse->commandType || CMD_INSERT == root->parse->commandType) &&
                LOCATOR_TYPE_RROBIN == source->locator_type)
                pathnode->path.dop = u_sess->opt_cxt.query_dop;
        } else {
            /*
             * Parallelize foreign scan.
             * When 'INSERT INTO .. SELECT * FROM foreign_table'.
             * The destination table is hashed rather than replicate,
             * and the source table must be gds foreign table.
             */
            if (CMD_INSERT == root->parse->commandType) {
                /* Check if it is obs source, OBS text and csv are not supported. */
                DistImportPlanState* planstate = (DistImportPlanState*)rel->fdw_private;
                const char* first_url = strVal(lfirst(list_head(planstate->source)));

                /*
                 * Only support destination table of hash distribution,
                 * and normal mode of gds import.
                 */
                if (!is_obs_protocol(first_url) && MODE_NORMAL == planstate->mode) {
                    pathnode->path.dop = u_sess->opt_cxt.query_dop;
                }
            }
        }
    }

    /*
     * Add location information for foreign scan path.
     * It should be in installation group.
     */
    Distribution* distribution = ng_get_installation_group_distribution();
    ng_copy_distribution(&pathnode->path.distribution, distribution);

    return pathnode;
}

/*
 * calc_nestloop_required_outer
 *	  Compute the required_outer set for a nestloop join path
 *
 * Note: result must not share storage with either input
 */
Relids calc_nestloop_required_outer(Path* outer_path, Path* inner_path)
{
    Relids outer_paramrels = PATH_REQ_OUTER(outer_path);
    Relids inner_paramrels = PATH_REQ_OUTER(inner_path);
    Relids required_outer;

    /* inner_path can require rels from outer path, but not vice versa */
    AssertEreport(!bms_overlap(outer_paramrels, inner_path->parent->relids),
        MOD_OPT_JOIN,
        "Outer path shouldn't require rels from inner path");
    /* easy case if inner path is not parameterized */
    if (!inner_paramrels)
        return bms_copy(outer_paramrels);
    /* else, form the union ... */
    required_outer = bms_union(outer_paramrels, inner_paramrels);
    /* ... and remove any mention of now-satisfied outer rels */
    required_outer = bms_del_members(required_outer, outer_path->parent->relids);
    /* maintain invariant that required_outer is exactly NULL if empty */
    if (bms_is_empty(required_outer)) {
        bms_free_ext(required_outer);
        required_outer = NULL;
    }
    return required_outer;
}

/*
 * calc_non_nestloop_required_outer
 *	  Compute the required_outer set for a merge or hash join path
 *
 * Note: result must not share storage with either input
 */
Relids calc_non_nestloop_required_outer(Path* outer_path, Path* inner_path)
{
    Relids outer_paramrels = PATH_REQ_OUTER(outer_path);
    Relids inner_paramrels = PATH_REQ_OUTER(inner_path);
    Relids required_outer;

    /* neither path can require rels from the other */
    AssertEreport(!bms_overlap(outer_paramrels, inner_path->parent->relids),
        MOD_OPT_JOIN,
        "Outer path shouldn't require rels from inner path");
    AssertEreport(!bms_overlap(inner_paramrels, outer_path->parent->relids),
        MOD_OPT_JOIN,
        "Inner path shouldn't require rels from outer path");
    /* form the union ... */
    required_outer = bms_union(outer_paramrels, inner_paramrels);
    /* we do not need an explicit test for empty; bms_union gets it right */
    return required_outer;
}

/*
 * Target	: Print relids in pg_log when log_min_messages <= DEBUG3.
 * In		: The first relids and second relids to print, root contains rels' name.
 * Out	: The buf contains the print string.
 * Return	: NA
 */
void debug3_print_two_relids(Relids first_relids, Relids second_relids, PlannerInfo* root, StringInfoData* buf)
{
    initStringInfo(buf);

    if (root != NULL && root->parse != NULL) {
        char* relidStr = debug1_print_relids(first_relids, root->parse->rtable);
        appendStringInfoString(buf, relidStr);
        pfree_ext(relidStr);
        appendStringInfoString(buf, " || ");
        relidStr = debug1_print_relids(second_relids, root->parse->rtable);
        appendStringInfoString(buf, relidStr);
        pfree_ext(relidStr);
    }
    return;
}

/*
 * Target	: Find whether there exists indirect equivalence relationship between inner_relids and outer_relids.
 * In		: The inner relids and outer relids to scan, root contains rels' equivalence classes.
 * Out	: NA. * Return	: Return true if exists  indirect equivalence relationship, otherwise return false.
 * Notes	: If find a indirect path to hashjoin or mergejoin the rels, we can add g_instance.cost_cxt.disable_cost to
 * nestloop path.
 */
bool equivalence_class_overlap(PlannerInfo* root, Relids outer_relids, Relids inner_relids)
{
    if (root->eq_classes == NULL)
        return false;

    StringInfoData buf;
    bool still_has_eq_class_to_match = true;

    /* The expanded equivalence classes based on inner relids */
    Relids expanded_eq_classes_of_inner_relids = bms_copy(inner_relids);

    /* The mark list of eq classes tells which eq class has already been linked to inner rels */
    bool* mark_list_of_linked_eq_class = (bool*)palloc0((root->eq_classes->length) * sizeof(bool));

    /* print Outer relids and Inner relids for debug */
    if (log_min_messages <= DEBUG3 && root->parse) {
        initStringInfo(&buf);
        char* relid_string = debug1_print_relids(outer_relids, root->parse->rtable);
        appendStringInfoString(&buf, relid_string);
        pfree_ext(relid_string);
        ereport(DEBUG3, (errmodule(MOD_OPT_JOIN), (errmsg("[EQ] Outer relids:\n\n%s\n", buf.data), errhidestmt(true))));
        pfree_ext(buf.data);
        initStringInfo(&buf);
        relid_string = debug1_print_relids(inner_relids, root->parse->rtable);
        appendStringInfoString(&buf, relid_string);
        pfree_ext(relid_string);
        ereport(DEBUG3, (errmodule(MOD_OPT_JOIN), (errmsg("[EQ] Inner relids:\n\n%s\n", buf.data), errhidestmt(true))));
        pfree_ext(buf.data);
    }

    /* Scan the eq classes list again and again until no new eq class can be linked to inner rels in one loop */
    while (still_has_eq_class_to_match) {
        int count = 0;
        ListCell* lc = NULL;
        still_has_eq_class_to_match = false;

        foreach (lc, root->eq_classes) {
            EquivalenceClass* eqc = (EquivalenceClass*)lfirst(lc);

            /*
             * If the equivalence relationship can finally reach a const,
             * then it can be replaced by a filter.
             * Else there can be hashjoin or mergejoin path even though not directly.
             */
            if (!eqc->ec_has_const && !mark_list_of_linked_eq_class[count]) {
                ListCell* slc = NULL;
                bool inner_in_eq = false;
                Bitmapset* linked_relids = (Relids)palloc0(sizeof(Bitmapset));
                BMS_Comparison bms_result;

                foreach (slc, eqc->ec_members) {
                    EquivalenceMember* em = (EquivalenceMember*)lfirst(slc);
                    linked_relids = bms_add_members(linked_relids, em->em_relids);
                    bms_result = bms_subset_compare(em->em_relids, expanded_eq_classes_of_inner_relids);
                    if (bms_result == BMS_EQUAL || bms_result == BMS_SUBSET1) {
                        inner_in_eq = true;
                        mark_list_of_linked_eq_class[count] = true;
                        still_has_eq_class_to_match = true;
                    }
                }

                /*
                 * If find equivalence relationship, expand the eq_classes linked to innner_relids
                 * and compare with outer_relids.
                 */
                bms_result = bms_subset_compare(expanded_eq_classes_of_inner_relids, linked_relids);

                if (inner_in_eq && bms_result != BMS_EQUAL && bms_result != BMS_SUBSET2) {
                    if (log_min_messages <= DEBUG3) {
                        debug3_print_two_relids(expanded_eq_classes_of_inner_relids, linked_relids, root, &buf);
                        ereport(DEBUG3,
                            (errmodule(MOD_OPT_JOIN),
                                (errmsg("[EQ] Expand relids eq-linked (%d) to inner relids (%d):\n\n%s\n",
                                     expanded_eq_classes_of_inner_relids->nwords,
                                     linked_relids->nwords,
                                     buf.data),
                                    errhidestmt(true))));
                        pfree_ext(buf.data);
                    }
                    expanded_eq_classes_of_inner_relids =
                        bms_add_members(expanded_eq_classes_of_inner_relids, linked_relids);
                    if (bms_overlap(expanded_eq_classes_of_inner_relids, outer_relids)) {
                        if (log_min_messages <= DEBUG3) {
                            debug3_print_two_relids(outer_relids, expanded_eq_classes_of_inner_relids, root, &buf);
                            ereport(DEBUG3,
                                (errmodule(MOD_OPT_JOIN),
                                    (errmsg("[EQ] Find outer_relids in eq-expanded inner relids:\n\n%s\n", buf.data),
                                        errhidestmt(true))));
                            pfree_ext(buf.data);
                        }
                        pfree_ext(mark_list_of_linked_eq_class);
                        bms_free_ext(linked_relids);
                        bms_free_ext(expanded_eq_classes_of_inner_relids);
                        return true;
                    }
                } else {
                    if (log_min_messages <= DEBUG3) {
                        debug3_print_two_relids(expanded_eq_classes_of_inner_relids, linked_relids, root, &buf);
                        ereport(DEBUG3,
                            (errmodule(MOD_OPT_JOIN),
                                (errmsg("[EQ] Not find any member eq-linked to inner_relids in eq-classes:\n\n%s\n",
                                     buf.data),
                                    errhidestmt(true))));
                        pfree_ext(buf.data);
                    }
                }
                bms_free_ext(linked_relids);
            }
            count++;
        }
    }

    if (log_min_messages <= DEBUG3) {
        debug3_print_two_relids(outer_relids, expanded_eq_classes_of_inner_relids, root, &buf);
        ereport(DEBUG3,
            (errmodule(MOD_OPT_JOIN),
                (errmsg("[EQ] Not find outer_relids in eq-expanded inner relids:\n\n%s\n", buf.data),
                    errhidestmt(true))));
        pfree_ext(buf.data);
    }

    pfree_ext(mark_list_of_linked_eq_class);
    bms_free_ext(expanded_eq_classes_of_inner_relids);
    return false;
}

/*
 * create_nestloop_path
 *	  Creates a pathnode corresponding to a nestloop join between two
 *	  relations.
 *
 * 'joinrel' is the join relation.
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_nestloop
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 *
 * Returns the resulting path node.
 */
NestPath* create_nestloop_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinCostWorkspace* workspace,
    SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path, Path* inner_path,
    List* restrict_clauses, List* pathkeys, Relids required_outer, int dop)
{
    NestPath* pathnode = makeNode(NestPath);
    Relids inner_req_outer = PATH_REQ_OUTER(inner_path);
    bool try_eq_related_indirectly = false;
    bool hasalternative = check_join_method_alternative(
        restrict_clauses, outer_path->parent, inner_path->parent, jointype, &try_eq_related_indirectly);

    if (outer_path->parent != NULL && inner_path->parent != NULL && root != NULL && !hasalternative &&
        try_eq_related_indirectly && !u_sess->attr.attr_sql.enable_nestloop)
        hasalternative = equivalence_class_overlap(root, outer_path->parent->relids, inner_path->parent->relids);
    if (!hasalternative && log_min_messages <= DEBUG3) {
        StringInfoData buf;

        if (outer_path->parent == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("outer_path and parent in outer_path could not be NULL")));
        }
        if (inner_path->parent == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("inner_path and parent in inner_path could not be NULL")));
        }

        debug3_print_two_relids(outer_path->parent->relids, inner_path->parent->relids, root, &buf);
        ereport(
            DEBUG3, (errmodule(MOD_OPT_JOIN), "[OPTHashjoin]Print Outer relids and Inner relids:\n\n%s\n", buf.data));
        pfree_ext(buf.data);

        ListCell* l = NULL;
        foreach (l, restrict_clauses) {
            RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);
            StringInfoData buf2;
            debug3_print_two_relids(restrictinfo->left_relids, restrictinfo->right_relids, root, &buf2);
            ereport(
                DEBUG3, (errmodule(MOD_OPT_JOIN), "[OPTHashjoin]Print clause left and right side:\n\n%s\n", buf2.data));
            pfree_ext(buf2.data);
        }
    }
    /*
     * If the inner path is parameterized by the outer, we must drop any
     * restrict_clauses that are due to be moved into the inner path.  We have
     * to do this now, rather than postpone the work till createplan time,
     * because the restrict_clauses list can affect the size and cost
     * estimates for this path.
     */
    if (outer_path->parent == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("outer_path and parent in outer_path could not be NULL")));
    }

    if (bms_overlap(inner_req_outer, outer_path->parent->relids)) {
        if (inner_path->parent == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("inner_path and parent in inner_path could not be NULL")));
        }

        Relids inner_and_outer = bms_union(inner_path->parent->relids, inner_req_outer);
        List* jclauses = NIL;
        ListCell* lc = NULL;

        foreach (lc, restrict_clauses) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

            if (!join_clause_is_movable_into(rinfo, inner_path->parent->relids, inner_and_outer))
                jclauses = lappend(jclauses, rinfo);
        }
        restrict_clauses = jclauses;
    }

    pathnode->path.pathtype = T_NestLoop;
    pathnode->path.parent = joinrel;
    pathnode->path.param_info =
        get_joinrel_parampathinfo(root, joinrel, outer_path, inner_path, sjinfo, required_outer, &restrict_clauses);
    pathnode->path.pathkeys = pathkeys;
    if (IsA(outer_path, StreamPath) && NIL == outer_path->pathkeys) {
        pathnode->path.pathkeys = NIL;
    }
    pathnode->path.dop = dop;
    pathnode->jointype = jointype;
    pathnode->outerjoinpath = outer_path;
    pathnode->innerjoinpath = inner_path;
    pathnode->joinrestrictinfo = restrict_clauses;

    pathnode->path.exec_type = SetExectypeForJoinPath(inner_path, outer_path);

#ifdef STREAMPLAN
    pathnode->path.locator_type = locator_type_join(outer_path->locator_type, inner_path->locator_type);
    ProcessRangeListJoinType(&pathnode->path, outer_path, inner_path);

    if (IS_STREAM_PLAN) {
        /* add location information for nest loop join path */
        Distribution* distribution = ng_get_join_distribution(outer_path, inner_path);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif

    final_cost_nestloop(root, pathnode, workspace, sjinfo, semifactors, hasalternative, dop);

    return pathnode;
}

/*
 * create_mergejoin_path
 *	  Creates a pathnode corresponding to a mergejoin join between
 *	  two relations
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_mergejoin
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 * 'mergeclauses' are the RestrictInfo nodes to use as merge clauses
 *		(this should be a subset of the restrict_clauses list)
 * 'outersortkeys' are the sort varkeys for the outer relation
 * 'innersortkeys' are the sort varkeys for the inner relation
 */
MergePath* create_mergejoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, Path* outer_path, Path* inner_path, List* restrict_clauses,
    List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys, List* innersortkeys)
{
    MergePath* pathnode = makeNode(MergePath);
    bool try_eq_related_indirectly = false;

    pathnode->jpath.path.pathtype = T_MergeJoin;
    pathnode->jpath.path.parent = joinrel;
    pathnode->jpath.path.param_info =
        get_joinrel_parampathinfo(root, joinrel, outer_path, inner_path, sjinfo, required_outer, &restrict_clauses);
    pathnode->jpath.path.pathkeys = pathkeys;
    pathnode->jpath.jointype = jointype;
    pathnode->jpath.outerjoinpath = outer_path;
    pathnode->jpath.innerjoinpath = inner_path;
    pathnode->jpath.joinrestrictinfo = restrict_clauses;
    pathnode->path_mergeclauses = mergeclauses;
    pathnode->outersortkeys = outersortkeys;
    pathnode->innersortkeys = innersortkeys;

    pathnode->jpath.path.exec_type = SetExectypeForJoinPath(inner_path, outer_path);

    /* pathnode->materialize_inner will be set by final_cost_mergejoin */
#ifdef STREAMPLAN
    pathnode->jpath.path.locator_type = locator_type_join(outer_path->locator_type, inner_path->locator_type);
    ProcessRangeListJoinType(&pathnode->jpath.path, outer_path, inner_path);


    if (IS_STREAM_PLAN) {
        /* add location information for merge join path */
        Distribution* distribution = ng_get_join_distribution(outer_path, inner_path);
        ng_copy_distribution(&pathnode->jpath.path.distribution, distribution);
    }
#endif

    final_cost_mergejoin(root,
        pathnode,
        workspace,
        sjinfo,
        check_join_method_alternative(
            restrict_clauses, outer_path->parent, inner_path->parent, jointype, &try_eq_related_indirectly));

    return pathnode;
}

/*
 * create_hashjoin_path
 *	  Creates a pathnode corresponding to a hash join between two relations.
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_hashjoin
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 * 'outer_path' is the cheapest outer path
 * 'inner_path' is the cheapest inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'required_outer' is the set of required outer rels
 * 'hashclauses' are the RestrictInfo nodes to use as hash clauses
 *		(this should be a subset of the restrict_clauses list)
 */
HashPath* create_hashjoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinCostWorkspace* workspace,
    SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path, Path* inner_path,
    List* restrict_clauses, Relids required_outer, List* hashclauses, int dop)
{
    HashPath* pathnode = makeNode(HashPath);
    bool try_eq_related_indirectly = false;

    pathnode->jpath.path.pathtype = T_HashJoin;
    pathnode->jpath.path.parent = joinrel;
    pathnode->jpath.path.param_info =
        get_joinrel_parampathinfo(root, joinrel, outer_path, inner_path, sjinfo, required_outer, &restrict_clauses);

    /*
     * A hashjoin never has pathkeys, since its output ordering is
     * unpredictable due to possible batching.      XXX If the inner relation is
     * small enough, we could instruct the executor that it must not batch,
     * and then we could assume that the output inherits the outer relation's
     * ordering, which might save a sort step.      However there is considerable
     * downside if our estimate of the inner relation size is badly off. For
     * the moment we don't risk it.  (Note also that if we wanted to take this
     * seriously, joinpath.c would have to consider many more paths for the
     * outer rel than it does now.)
     */
    pathnode->jpath.path.pathkeys = NIL;
    pathnode->jpath.path.dop = dop;
    pathnode->jpath.jointype = jointype;
    pathnode->jpath.outerjoinpath = outer_path;
    pathnode->jpath.innerjoinpath = inner_path;
    pathnode->jpath.joinrestrictinfo = restrict_clauses;
    pathnode->path_hashclauses = hashclauses;

    pathnode->jpath.path.exec_type = SetExectypeForJoinPath(inner_path, outer_path);

#ifdef STREAMPLAN
    pathnode->jpath.path.locator_type = locator_type_join(inner_path->locator_type, outer_path->locator_type);
    ProcessRangeListJoinType(&pathnode->jpath.path, outer_path, inner_path);

    if (IS_STREAM_PLAN) {
        /* add location information for hash join path */
        Distribution* distribution = ng_get_join_distribution(outer_path, inner_path);
        ng_copy_distribution(&pathnode->jpath.path.distribution, distribution);
    }
#endif

    /* final_cost_hashjoin will fill in pathnode->num_batches */
    final_cost_hashjoin(root,
        pathnode,
        workspace,
        sjinfo,
        semifactors,
        check_join_method_alternative(
            restrict_clauses, outer_path->parent, inner_path->parent, jointype, &try_eq_related_indirectly),
        dop);

    return pathnode;
}

/*
 * reparameterize_path
 *		Attempt to modify a Path to have greater parameterization
 *
 * We use this to attempt to bring all child paths of an appendrel to the
 * same parameterization level, ensuring that they all enforce the same set
 * of join quals (and thus that that parameterization can be attributed to
 * an append path built from such paths).  Currently, only a few path types
 * are supported here, though more could be added at need.	We return NULL
 * if we can't reparameterize the given path.
 *
 * Note: we intentionally do not pass created paths to add_path(); it would
 * possibly try to delete them on the grounds of being cost-inferior to the
 * paths they were made from, and we don't want that.  Paths made here are
 * not necessarily of general-purpose usefulness, but they can be useful
 * as members of an append path.
 */
Path* reparameterize_path(PlannerInfo* root, Path* path, Relids required_outer, double loop_count)
{
    RelOptInfo* rel = path->parent;
    Bitmapset *required_upper = PATH_REQ_UPPER(path);

    /* Can only increase, not decrease, path's parameterization */
    if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
        return NULL;
    switch (path->pathtype) {
        case T_SeqScan:
            return create_seqscan_path(root, rel, required_outer);
        case T_IndexScan:
        case T_IndexOnlyScan: {
            IndexPath* ipath = (IndexPath*)path;
            IndexPath* newpath = makeNode(IndexPath);

            /*
             * We can't use create_index_path directly, and would not want
             * to because it would re-compute the indexqual conditions
             * which is wasted effort.	Instead we hack things a bit:
             * flat-copy the path node, revise its param_info, and redo
             * the cost estimate.
             */
            errno_t errorno = EOK;
            errorno = memcpy_s(newpath, sizeof(IndexPath), ipath, sizeof(IndexPath));
            securec_check(errorno, "", "");

            newpath->path.param_info = get_baserel_parampathinfo(root, rel, required_outer, required_upper);
            cost_index(newpath, root, loop_count);
            return (Path*)newpath;
        }
        case T_BitmapHeapScan: {
            BitmapHeapPath* bpath = (BitmapHeapPath*)path;

            return (Path*)create_bitmap_heap_path(root, rel, bpath->bitmapqual,
                            required_outer, required_upper, loop_count);
        }
        case T_SubqueryScan:
            return create_subqueryscan_path_reparam(root, rel, path->pathkeys, required_outer, NULL);
        case T_PartIterator: {
            PartIteratorPath *ppath = (PartIteratorPath *)path;
            PartIteratorPath* newpath = makeNode(PartIteratorPath);

            errno_t errorno = EOK;
            errorno = memcpy_s(newpath, sizeof(PartIteratorPath), ppath, sizeof(PartIteratorPath));
            securec_check(errorno, "", "");

            newpath->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
            return (Path *)newpath;
        }
        case T_BaseResult: {
            ResultPath *rpath = (ResultPath *)path;
            ResultPath *newpath = makeNode(ResultPath);

            errno_t errorno = EOK;
            errorno = memcpy_s(newpath, sizeof(ResultPath), rpath, sizeof(ResultPath));
            securec_check(errorno, "", "");

            newpath->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
            return (Path *)newpath;
        }
        case T_CStoreScan: {
            Path *rpath = (Path *)path;
            Path *newpath = makeNode(Path);

            errno_t errorno = EOK;
            errorno = memcpy_s(newpath, sizeof(Path), rpath, sizeof(Path));
            securec_check(errorno, "", "");

            newpath->param_info = get_baserel_parampathinfo(root, rel, required_outer);
            return newpath;
        }
        case T_Append: {
            AppendPath *apath = (AppendPath *) path;
            List       *childpaths = NIL;
            int         i;
            ListCell   *lc;

            /* Reparameterize the children */
            i = 0;
            foreach(lc, apath->subpaths) {
                Path       *spath = (Path *) lfirst(lc);

                spath = reparameterize_path(root, spath,
                            required_outer,
                            loop_count);
                if (spath == NULL) {
                  return NULL;
                }

                /* We have to re-split the regular and partial paths */
                childpaths = lappend(childpaths, spath);
                i++;
            }

            return (Path *) create_append_path(root, rel, childpaths, required_outer);  
        }

        default:
            break;
    }
    return NULL;
}

/*
 * check_join_method_alternative
 *
 * check if there's any alternatives when we disable one or more methods, and if
 * not, we should add large cost for the sole path, which will influence judgement
 * of other joins
 */
bool check_join_method_alternative(
    List* restrictlist, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype, bool* try_eq_related_indirectly)
{
    bool hasalternative = false;
    ListCell* l = NULL;

    foreach (l, restrictlist) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);

        /* Check if clause is a hashable or mergeable operator clause */
        if (restrictinfo->can_join && clause_sides_match_join(restrictinfo, outerrel, innerrel)) {
            if (u_sess->attr.attr_sql.enable_hashjoin && restrictinfo->hashjoinoperator != InvalidOid)
                hasalternative = true;
            if (u_sess->attr.attr_sql.enable_mergejoin && restrictinfo->mergeopfamilies != NIL)
                hasalternative = true;
            if (u_sess->attr.attr_sql.enable_hashjoin || u_sess->attr.attr_sql.enable_mergejoin)
                *try_eq_related_indirectly = true;
        }
        if (hasalternative)
            break;
    }
    if (u_sess->attr.attr_sql.enable_nestloop && jointype != JOIN_FULL)
        hasalternative = true;

    return hasalternative;
}

#ifdef STREAMPLAN

bool is_replicated_path(Path* path)
{
    return path->locator_type == LOCATOR_TYPE_REPLICATED;
}

/*
 * @Description: Find the distribute location in path's targetlist.
 * @in distribute_keys: distribute key list.
 * @in target_list: target list.
 * @return List: locations in targetlist.
 */
static List* find_distrikey_in_targetlist(List* distribute_keys, List* target_list)
{
    List* dis_location = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    if (NIL == target_list || NIL == distribute_keys)
        return NIL;

    foreach (lc1, distribute_keys) {
        Var* var1 = (Var*)lfirst(lc1);
        if (IsA(var1, Var)) {
            AttrNumber varloc = 1;
            /*
             * Check if the distribute_key in the targetlist,
             * if so, record the location in targetlist.
             */
            foreach (lc2, target_list) {
                Var* var2 = (Var*)lfirst(lc2);
                if (IsA(var2, Var) && var1->varattno == var2->varattno) {
                    dis_location = lappend_int(dis_location, varloc);
                    break;
                }
                varloc++;
            }
        } else {
            list_free_ext(dis_location);
            dis_location = NIL;
            break;
        }
    }

    /*
     * If we can find all distribute keys in targetlists,
     * then we can not use this dis_location as append distribute key.
     */
    if (list_length(dis_location) < list_length(distribute_keys))
        return NIL;

    return dis_location;
}

/*
 * @Description: Mark append node's distribute_keys and locator_type information.
 * @in root: Per-query information for planning/optimization.
 * @in rel: Append relation information.
 * @in pathnode: Append node.
 * @in subpaths: Append sub path.
 */
static void mark_append_path(PlannerInfo* root, RelOptInfo* rel, Path* pathnode, List* subpaths)
{
    Path* subpath = NULL;
    ListCell* cell = NULL;
    Bitmapset* replicatePathSet = NULL;
    int subPathIndex = 0;
    List* dis_varattno = NIL;
    List* dis_varattno2 = NIL;
    Distribution* target_distribution = NULL;
    bool hasRangeListPlan = false;

    foreach (cell, subpaths) {
        subpath = (Path*)lfirst(cell);

        target_distribution = (NULL == target_distribution) ? ng_get_dest_distribution(subpath) : target_distribution;

        if (root != NULL && root->is_correlated && !SUBQUERY_PREDPUSH((unsigned int)root)) {
            Distribution* distribution = ng_get_dest_distribution(subpath);
            Distribution* subplan_dist = ng_get_correlated_subplan_group_distribution();

            if (!is_replicated_path(subpath) || !ng_is_same_group(distribution, subplan_dist)) {
                subpath = create_stream_path(root, rel, STREAM_BROADCAST, NIL, NIL, subpath, 1.0, subplan_dist);
            }

            ContainStreamContext context;
            context.outer_relids = NULL;
            context.only_check_stream = true;
            context.under_materialize_all = false;
            context.has_stream = false;
            context.has_parameterized_path = false;
            context.has_cstore_index_delta = false;
            context.upper_params = NULL;

            stream_path_walker(subpath, &context);
            if (context.has_stream || context.has_cstore_index_delta) {
                Cost rescan_startup_cost, rescan_total_cost;

                subpath = (Path*)create_material_path(subpath, true);
                cost_rescan(
                    root, subpath, &rescan_startup_cost, &rescan_total_cost, &((MaterialPath*)subpath)->mem_info);
                ((MaterialPath*)subpath)->mem_info.regressCost *= DEFAULT_NUM_ROWS;
            }

            if (subpath != lfirst(cell)) {
                lfirst(cell) = subpath;
            }
        }
        /* For append path, we eliminate dummy path */
        if (subpath->parent->subplan != NULL && is_dummy_plan(subpath->parent->subplan)) {
            subPathIndex--;
        } else if (is_replicated_path(subpath)) {
            replicatePathSet = bms_add_member(replicatePathSet, subPathIndex);
        } else if (IsLocatorDistributedBySlice(subpath->locator_type)) {
            hasRangeListPlan = true;
            dis_varattno = NIL;
            dis_varattno2 = NIL;
        } else if (subpath->distribute_keys != NIL) { /* Check if there is common distribute key for all path */
            if (cell == list_head(subpaths)) { /* first subpath */
                dis_varattno = find_distrikey_in_targetlist(subpath->distribute_keys, subpath->parent->reltargetlist);
            } else { /* other subpath */
                dis_varattno2 = find_distrikey_in_targetlist(subpath->distribute_keys, subpath->parent->reltargetlist);

                /* The dis_varattno should be exact the same. */
                if (!equal(dis_varattno, dis_varattno2)) {
                    dis_varattno = NIL;
                    dis_varattno2 = NIL;
                }
            }
        } else {
            dis_varattno = NIL;
            dis_varattno2 = NIL;
        }
        subPathIndex++;
    }

    if (subPathIndex == bms_num_members(replicatePathSet)) {
        pathnode->distribute_keys = NIL;
        pathnode->locator_type = LOCATOR_TYPE_REPLICATED;

        rel->locator_type = pathnode->locator_type;
        rel->distribute_keys = pathnode->distribute_keys;
    } else if (bms_is_empty(replicatePathSet) && dis_varattno != NIL) {
        /* If we have common distribute key, marked it locator type hash */
        ListCell* lc = NULL;

        foreach (cell, dis_varattno) {
            AttrNumber disno = lfirst_int(cell);
            AttrNumber varno = 1;
            foreach (lc, rel->reltargetlist) {
                Var* var = (Var*)lfirst(lc);
                if (IsA(var, Var) && varno == disno) {
                    pathnode->distribute_keys = lappend(pathnode->distribute_keys, var);
                    break;
                }
                varno++;
            }
        }
        pathnode->locator_type = LOCATOR_TYPE_HASH;
    } else {
        pathnode->distribute_keys = rel->distribute_keys;
        pathnode->locator_type = rel->locator_type;
        if (hasRangeListPlan) {
            /* at this stage append will not inherit distribute_keys for list/range. */
            pathnode->distribute_keys = NIL;
        }
    }

    /* add location information for append path */
    if (NIL == subpaths) {
        /* dummy node */
        Distribution* distribution = ng_get_default_computing_group_distribution();
        ng_copy_distribution(&pathnode->distribution, distribution);
    } else {
        ng_copy_distribution(&pathnode->distribution, target_distribution);
    }
}

/*
 * find_ec_memeber_for_var:
 *	find the equivalence node of key in one eqclass
 * Parameters:
 *	@in ec: equivalence class to find the key
 *	@in key: the item whose equivalence node is to be found
 * Return:
 *	true if found, or false
 */
bool find_ec_memeber_for_var(EquivalenceClass* ec, Node* key)
{
    ListCell* lc = NULL;

    if (ec == NULL || key == NULL)
        return false;

    foreach (lc, ec->ec_members) {
        EquivalenceMember* em = (EquivalenceMember*)lfirst(lc);
        Expr* emexpr = NULL;

        emexpr = em->em_expr;

        if (IsA(key, Var)) {
            Var* emvar = locate_distribute_var(emexpr);
            if (emvar != NULL && _equalSimpleVar(emvar, key))
                return true;
        } else {
            if (equal(emexpr, key))
                return true;
        }
    }

    return false;
}

/*
 * is_ec_usable_for_join:
 *	see if the join clause can be found in equivalence class can be used for join
 *  and the diskey and joinkey are compatible types for hashing
 *
 *  the EquivalenceClass only record the equality types implied from join clauses
 *  however, the members in EC might not join directly when have different types of hashing
 *  for example if type:date and type:timestamp are in EC but using different hashing functions
 *  hence the two cols can not be join directly without redistribution
 */
static bool is_ec_usable_for_join(
    Relids suitable_relids, EquivalenceClass* suitable_ec, Node* diskey, Expr* join_clause, bool is_left)
{
    Node* joinkey = NULL;

    if (suitable_relids == NULL)
        return false;

    if (suitable_ec == NULL)
        return false;

    if (!find_ec_memeber_for_var(suitable_ec, diskey))
        return false;

    /*
     * if we found a ec member for the diskey, the join_clause
     * should be an OpExpr. but for backward compatiblity with
     * the old original code logic return true if not an OpExpr.
     * however we are expecting an OpExpr here
     */
    if (!IsA(join_clause, OpExpr))
        return true;

    /*
     * the join clause is an OpExpr if we reach here
     * extract the joinkey from one side of the join_clause
     * and test it against the diskey for hashing compatiblity
     */
    joinkey = join_clause_get_join_key((Node*)join_clause, is_left);

    return is_diskey_and_joinkey_compatible(diskey, joinkey);
}

/*
 * given a join clause as an operator type return the join_key on either side
 */
Node* join_clause_get_join_key(Node* join_clause, bool is_var_on_left)
{
    Node* join_key = NULL;
    OpExpr* join_op = NULL;

    if (!IsA(join_clause, OpExpr))
        return NULL;

    join_op = (OpExpr*)join_clause;

    if (is_var_on_left) {
        join_key = (Node*)linitial(join_op->args);
    } else {
        join_key = (Node*)lsecond(join_op->args);
    }

    return join_key;
}

/*
 * see if distribute key and join key are compatible for hashing
 */
bool is_diskey_and_joinkey_compatible(Node* diskey, Node* joinkey)
{
    Oid joinkey_type = InvalidOid;
    Oid diskey_type = InvalidOid;

    joinkey_type = exprType(joinkey);
    diskey_type = exprType(diskey);

    if (joinkey_type == InvalidOid || diskey_type == InvalidOid)
        return false;

    return is_compatible_type(joinkey_type, diskey_type);
}

/*
 * is_distribute_need_on_joinclauses:
 *	Judge if redistribution is needed when join with joinclauses based on
 *	current distribute key
 * Parameters:
 *	@in root: planner info of current query level
 *	@in cur_distkeys: distribute key of current rel
 *	@in joinclauses: the join clauses used by current rel
 *	@in cur_relids: relids of current rel
 *	@in other_relids: relids of the other rel joined with current rel
 *	@out rrinfo: if redistribution is unnecessary, return restrictinfo on
 *			which we can join directly
 * Return:
 *	true if redistribution is needed, else false
 */
bool is_distribute_need_on_joinclauses(PlannerInfo* root, List* cur_distkeys, List* joinclauses,
    const RelOptInfo* side_rel, const RelOptInfo* other_rel, List** rrinfo)
{
    ListCell* lcell = NULL;
    Node* diskey = NULL;

    Relids side_relids = side_rel->relids;
    Relids other_relids = other_rel->relids;
    bool result = false;

    *rrinfo = NULL;

    if (cur_distkeys == NULL)
        return true;

    ListCell* cell = NULL;
    foreach (cell, cur_distkeys) {
        diskey = (Node*)lfirst(cell);

        foreach (lcell, joinclauses) {
            /*
             * We judge whether need to redistribute on one rel,
             * which is match to be left_relids or right_relids of rinfo.
             */
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lcell);
            Relids suitable_relids = NULL;
            EquivalenceClass* suitable_ec = NULL;
            bool is_left = false;

            if (rinfo->orclause || !rinfo->left_ec || !rinfo->right_ec)
                continue;

            if (!rinfo->left_relids || !rinfo->right_relids)
                continue;

            /*
             * To op expr, we need judge args's hash arithmetic compatibility, if they are not compatible, we need
             * redistribute or broadcast. For example, timestamp and date.
             */
            if (IsA(rinfo->clause, OpExpr) && !is_args_type_compatible((OpExpr*)rinfo->clause)) {
                continue;
            }

            if (bms_is_subset(rinfo->left_relids, side_relids) &&
                (bms_is_subset(rinfo->right_relids, other_relids) || other_relids == NULL)) {
                suitable_relids = rinfo->left_relids;
                suitable_ec = rinfo->left_ec;
                is_left = true;
            } else if (bms_is_subset(rinfo->right_relids, side_relids) &&
                       (bms_is_subset(rinfo->left_relids, other_relids) || other_relids == NULL)) {
                suitable_relids = rinfo->right_relids;
                suitable_ec = rinfo->right_ec;
                is_left = false;
            }

            if (is_ec_usable_for_join(suitable_relids, suitable_ec, diskey, rinfo->clause, is_left)) {
                *rrinfo = lappend(*rrinfo, rinfo);

                break;
            }
        }

        if (lcell == NULL) {
            return true;
        }
    }

    return result;
}

/*
 * locate_distribute_key:
 *		get distribute key of join rel from desired key, outer and inner distribute key
 * Parameters:
 *	@in jointype: join type of outerrel and innerrel
 *	@in outer_distributekey: distribute key of outerrel
 *	@in inner_distributekey: distribute key of innerrel
 *	@in	desired_key: matching or superset key in upper level,
 *		which is desired in join distribute key
 *	@in	exact_match: note whether in exact mode, and in this
 *		mode, we directly return desired_key
 * Return:
 *	final distribute key for join rel
 */
List* locate_distribute_key(
    JoinType jointype, List* outer_distributekey, List* inner_distributekey, List* desired_key, bool exact_match)
{
    List* join_distributekey = NIL;

    AssertEreport(JOIN_UNIQUE_INNER != jointype && JOIN_UNIQUE_OUTER != jointype && JOIN_FULL != jointype,
        MOD_OPT_JOIN,
        "Join type is not expected");

    if (exact_match) {
        AssertEreport(desired_key != NIL, MOD_OPT_JOIN, "Must have a desired key when exact match");
        join_distributekey = desired_key;
    } else if (jointype == JOIN_INNER && desired_key != NIL) {
        ListCell* lc1 = NULL;
        ListCell* lc2 = NULL;
        forboth(lc1, outer_distributekey, lc2, inner_distributekey)
        {
            Node* n1 = (Node*)lfirst(lc1);
            Node* n2 = (Node*)lfirst(lc2);
            Node* desired_node = NULL;

            if (list_member(desired_key, n2))
                desired_node = n2;
            else
                desired_node = n1;
            join_distributekey = lappend(join_distributekey, desired_node);
        }
    } else if (LHS_join(jointype))
        join_distributekey = outer_distributekey;
    else if (RHS_join(jointype))
        join_distributekey = inner_distributekey;

    return join_distributekey;
}

double get_skew_ratio(double distinct_value)
{
    double dn_num;

    if (distinct_value == 0)
        dn_num = u_sess->pgxc_cxt.NumDataNodes;
    else if (distinct_value < (double)u_sess->pgxc_cxt.NumDataNodes / 3)
        dn_num = distinct_value;
    else
        dn_num = distinct_value - (distinct_value - (double)u_sess->pgxc_cxt.NumDataNodes / 3) * 2 / 3;

    if (dn_num < 1)
        dn_num = 1;
    else if (dn_num > u_sess->pgxc_cxt.NumDataNodes)
        dn_num = u_sess->pgxc_cxt.NumDataNodes;

    return (double)u_sess->pgxc_cxt.NumDataNodes / dn_num;
}

/*
 * get_redist_unique
 *     Compute cost and build redistribute + unique path
 */
Path* get_redist_unique(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key, List* pathkeys,
    double skew, Distribution* target_distribution, ParallelDesc* smpDesc, bool cost_only)
{
    AssertEreport(IsA(path, UniquePath), MOD_OPT_JOIN, "Path should be UniquePath");
    UniquePath* origin_path = (UniquePath*)path;

    /* Get sub-path of the UniquePath */
    Path* origin_subpath = NULL;
    if (cost_only) {
        origin_subpath = makeNode(Path);
        origin_subpath->rows = origin_path->subpath->rows;
        origin_subpath->multiple = origin_path->subpath->multiple;
        origin_subpath->startup_cost = origin_path->subpath->startup_cost;
        origin_subpath->total_cost = origin_path->subpath->total_cost;
        origin_subpath->stream_cost = origin_path->subpath->stream_cost;
        Distribution* distribution = ng_get_dest_distribution(origin_path->subpath);
        ng_copy_distribution(&origin_subpath->distribution, distribution);
        origin_subpath->locator_type = origin_path->subpath->locator_type;
    } else {
        origin_subpath = origin_path->subpath;
    }

    /* create a stream on the sub-path */
    Path* stream_path = create_stream_path(root,
        origin_path->subpath->parent,
        stream_type,
        distribute_key,
        pathkeys,
        (Path*)origin_subpath,
        skew,
        target_distribution,
        smpDesc);

    /* create a UniquePath on the stream */
    UniquePath* newpath = makeNode(UniquePath);
    newpath->path.pathtype = T_Unique;
    newpath->path.parent = origin_path->path.parent;
    newpath->path.param_info = origin_path->path.param_info;
    newpath->path.pathkeys = stream_path->pathkeys;
    newpath->path.exec_type = stream_path->exec_type;

#ifdef STREAMPLAN
    inherit_path_locator_info((Path*)newpath, stream_path);
#endif

    /* calculate final agg rows for agg operator */
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(stream_path);
    newpath->path.rows = estimate_num_groups(
        root, origin_path->uniq_exprs, origin_path->path.parent->rows, num_datanodes, STATS_TYPE_GLOBAL);
    newpath->path.rows = clamp_row_est(newpath->path.rows);
    newpath->path.multiple = 1.0;
    double local_rows = PATH_LOCAL_ROWS(&newpath->path);

    /*
     * Assume the output is unsorted, since we don't necessarily have pathkeys
     * to represent it.  (This might get overridden below.)
     */
    newpath->subpath = stream_path;
    newpath->in_operators = origin_path->in_operators;
    newpath->uniq_exprs = origin_path->uniq_exprs;

    Path sort_path;
    Path agg_path;
    int numCols = list_length(origin_path->uniq_exprs);

    OpMemInfo sort_mem_info, hash_mem_info;
    errno_t rc = 0;

    rc = memset_s(&sort_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&agg_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&sort_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&hash_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    /* Estimate cost for hashaggregate */
    if (origin_path->both_method || UNIQUE_PATH_HASH == origin_path->umethod) {
        Size hashentrysize = 0;

        if (root->glob->vectorized) {
            hashentrysize = get_path_actual_total_width(stream_path, root->glob->vectorized, OP_HASHAGG, 0);
        } else {
            hashentrysize = get_hash_entry_size(origin_path->path.parent->width);
        }

        Distribution* distribution = ng_get_dest_distribution((Path*)newpath);
        ng_copy_distribution(&agg_path.distribution, distribution);
        cost_agg(&agg_path,
            root,
            AGG_HASHED,
            NULL,
            numCols,
            local_rows,
            stream_path->startup_cost,
            stream_path->total_cost,
            PATH_LOCAL_ROWS(stream_path),
            origin_path->path.parent->width,
            hashentrysize,
            1,
            &hash_mem_info);
    }

    /* Estimate cost for sort+unique implementation */
    if (origin_path->both_method || UNIQUE_PATH_SORT == origin_path->umethod) {
        int subpath_width = get_path_actual_total_width(stream_path, root->glob->vectorized, OP_SORT);

        cost_sort(&sort_path,
            NIL,
            stream_path->total_cost,
            PATH_LOCAL_ROWS(stream_path),
            subpath_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized,
            1,
            &sort_mem_info);
        /*
         * Charge one cpu_operator_cost per comparison per input tuple. We
         * assume all columns get compared at most of the tuples. (XXX
         * probably this is an overestimate.)  This should agree with
         * make_unique.
         */
        sort_path.total_cost += u_sess->attr.attr_sql.cpu_operator_cost * local_rows * numCols;
    }

    /* Determine how to make the data unique */
    if (origin_path->both_method) {
        if (agg_path.total_cost < sort_path.total_cost) {
            newpath->umethod = UNIQUE_PATH_HASH;
        } else {
            newpath->umethod = UNIQUE_PATH_SORT;
        }
    } else {
        newpath->umethod = origin_path->umethod;
    }

    if (UNIQUE_PATH_HASH == newpath->umethod) {
        if (!cost_only) {
            origin_path->hold_tlist = true;
        }
        newpath->path.startup_cost = agg_path.startup_cost;
        newpath->path.total_cost = agg_path.total_cost;
        rc = memcpy_s(&newpath->mem_info, sizeof(OpMemInfo), &hash_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    } else if (UNIQUE_PATH_SORT == newpath->umethod) {
        newpath->path.startup_cost = sort_path.startup_cost;
        newpath->path.total_cost = sort_path.total_cost;
        rc = memcpy_s(&newpath->mem_info, sizeof(OpMemInfo), &sort_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    } else {
        newpath->path.startup_cost = stream_path->startup_cost;
        newpath->path.total_cost = stream_path->total_cost;
    }

    newpath->path.stream_cost = stream_path->stream_cost;
    return (Path*)newpath;
}

/*
 * get_unique_redist
 *     Compute cost and build unique + redistribute path
 */
Path* get_unique_redist(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key, List* pathkeys,
    double skew, Distribution* target_distribution, ParallelDesc* smpDesc)
{
    AssertEreport(IsA(path, UniquePath), MOD_OPT_JOIN, "Path should be UniquePath");
    UniquePath* origin_path = (UniquePath*)path;

    Path* stream_path = create_stream_path(root,
        origin_path->path.parent,
        stream_type,
        distribute_key,
        pathkeys,
        (Path*)origin_path,
        skew,
        target_distribution,
        smpDesc);

    return stream_path;
}

/*
 * get_unique_redist_unique
 *     Compute cost and build unique + redistribute + unique path
 */
Path* get_unique_redist_unique(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key,
    List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc, bool cost_only)
{
    AssertEreport(IsA(path, UniquePath), MOD_OPT_JOIN, "Path should be UniquePath");
    UniquePath* origin_path = (UniquePath*)path;
    Path* stream_path = NULL;
    UniquePath* newpath = NULL;
    RelOptInfo* rel = origin_path->path.parent;
    int numCols = list_length(origin_path->uniq_exprs);
    Path sort_path;
    Path agg_path;
    double local_rows;
    double local_distinct;
    errno_t rc = 0;

    rc = memset_s(&sort_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&agg_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    /* re-estimate rows of first agg */
    double numdistinct[2] = {1, 1};
    get_num_distinct(root,
        origin_path->uniq_exprs,
        RELOPTINFO_LOCAL_FIELD(root, origin_path->path.parent, rows),
        origin_path->path.parent->rows,
        ng_get_dest_num_data_nodes(root, rel),
        numdistinct,
        NULL);

    local_distinct = estimate_agg_num_distinct(root, origin_path->uniq_exprs, (Path*)origin_path->subpath, numdistinct);

    Size first_hashentrysize = 0;

    if (root->glob->vectorized) {
        first_hashentrysize = get_path_actual_total_width((Path*)origin_path, root->glob->vectorized, OP_HASHAGG, 0);
    } else {
        first_hashentrysize = get_hash_entry_size(rel->width);
    }

    if (UNIQUE_PATH_HASH == origin_path->umethod) {
        OpMemInfo origin_hash_mem_info;
        double multiple = ((Path*)origin_path)->multiple;

        rc = memset_s(&origin_hash_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
        securec_check_c(rc, "\0", "\0");
        cost_agg((Path*)origin_path,
            root,
            AGG_HASHED,
            NULL,
            numCols,
            local_distinct,
            origin_path->subpath->startup_cost,
            origin_path->subpath->total_cost,
            PATH_LOCAL_ROWS((Path*)origin_path->subpath),
            origin_path->subpath->parent->width,
            first_hashentrysize,
            origin_path->path.dop,
            &origin_hash_mem_info);
        /* cost_agg will set multiple to 1.0, so restore it */
        ((Path*)origin_path)->multiple = multiple;
        rc = memcpy_s(&origin_path->mem_info, sizeof(OpMemInfo), &origin_hash_mem_info, sizeof(OpMemInfo));
        securec_check_c(rc, "\0", "\0");
    } else if (UNIQUE_PATH_SORT == origin_path->umethod) {
        OpMemInfo origin_sort_mem_info;

        rc = memset_s(&origin_sort_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
        securec_check_c(rc, "\0", "\0");
        cost_sort((Path*)origin_path,
            NIL,
            origin_path->subpath->total_cost,
            PATH_LOCAL_ROWS((Path*)origin_path->subpath),
            origin_path->subpath->parent->width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized,
            origin_path->path.dop,
            &origin_sort_mem_info);
        rc = memcpy_s(&origin_path->mem_info, sizeof(OpMemInfo), &origin_sort_mem_info, sizeof(OpMemInfo));
        securec_check_c(rc, "\0", "\0");
    }

    /* add the redistribute node */
    stream_path = create_stream_path(root,
        origin_path->path.parent,
        stream_type,
        distribute_key,
        pathkeys,
        (Path*)origin_path,
        skew,
        target_distribution,
        smpDesc);

    newpath = makeNode(UniquePath);
    newpath->path.pathtype = T_Unique;
    newpath->path.parent = origin_path->path.parent;
    newpath->path.param_info = origin_path->path.param_info;
    newpath->path.pathkeys = stream_path->pathkeys;
    newpath->path.exec_type = stream_path->exec_type;

#ifdef STREAMPLAN
    inherit_path_locator_info((Path*)newpath, stream_path);
#endif

    /* calculate final agg rows for agg operator */
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(stream_path);
    newpath->path.rows = estimate_num_groups(
        root, origin_path->uniq_exprs, origin_path->path.parent->rows, num_datanodes, STATS_TYPE_GLOBAL);
    newpath->path.rows = clamp_row_est(newpath->path.rows);
    newpath->path.multiple = 1.0;
    local_rows = PATH_LOCAL_ROWS(&newpath->path);

    /*
     * Assume the output is unsorted, since we don't necessarily have pathkeys
     * to represent it.  (This might get overridden below.)
     */
    newpath->subpath = stream_path;
    newpath->in_operators = origin_path->in_operators;
    newpath->uniq_exprs = origin_path->uniq_exprs;

    OpMemInfo sort_mem_info, hash_mem_info;

    rc = memset_s(&sort_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&hash_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");

    /* Estimate cost for hashaggregate */
    if (origin_path->both_method || UNIQUE_PATH_HASH == origin_path->umethod) {
        Size hashentrysize = 0;

        if (root->glob->vectorized) {
            hashentrysize = get_path_actual_total_width(stream_path, root->glob->vectorized, OP_HASHAGG, 0);
        } else {
            hashentrysize = get_hash_entry_size(rel->width);
        }

        Distribution* distribution = ng_get_dest_distribution((Path*)newpath);
        ng_copy_distribution(&agg_path.distribution, distribution);
        cost_agg(&agg_path,
            root,
            AGG_HASHED,
            NULL,
            numCols,
            local_rows,
            stream_path->startup_cost,
            stream_path->total_cost,
            PATH_LOCAL_ROWS(stream_path),
            rel->width,
            hashentrysize,
            1,
            &hash_mem_info);
    }

    /* Estimate cost for sort+unique implementation */
    if (origin_path->both_method || UNIQUE_PATH_SORT == origin_path->umethod) {
        int subpath_width = get_path_actual_total_width(stream_path, root->glob->vectorized, OP_SORT);

        cost_sort(&sort_path,
            NIL,
            stream_path->total_cost,
            PATH_LOCAL_ROWS(stream_path),
            subpath_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized,
            1,
            &sort_mem_info);
        /*
         * Charge one cpu_operator_cost per comparison per input tuple. We
         * assume all columns get compared at most of the tuples. (XXX
         * probably this is an overestimate.)  This should agree with
         * make_unique.
         */
        sort_path.total_cost += u_sess->attr.attr_sql.cpu_operator_cost * local_rows * numCols;
    }

    /* Determine how to make the data unique */
    if (origin_path->both_method) {
        if (agg_path.total_cost < sort_path.total_cost) {
            newpath->umethod = UNIQUE_PATH_HASH;
        } else {
            newpath->umethod = UNIQUE_PATH_SORT;
        }
    } else {
        newpath->umethod = origin_path->umethod;
    }

    if (UNIQUE_PATH_HASH == newpath->umethod) {
        if (!cost_only) {
            origin_path->hold_tlist = true;
        }
        newpath->path.startup_cost = agg_path.startup_cost;
        newpath->path.total_cost = agg_path.total_cost;
        rc = memcpy_s(&newpath->mem_info, sizeof(OpMemInfo), &hash_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    } else if (UNIQUE_PATH_SORT == newpath->umethod) {
        newpath->path.startup_cost = sort_path.startup_cost;
        newpath->path.total_cost = sort_path.total_cost;
        rc = memcpy_s(&newpath->mem_info, sizeof(OpMemInfo), &sort_mem_info, sizeof(OpMemInfo));
        securec_check(rc, "\0", "\0");
    } else {
        newpath->path.startup_cost = stream_path->startup_cost;
        newpath->path.total_cost = stream_path->total_cost;
    }

    newpath->path.stream_cost = stream_path->stream_cost;
    return (Path*)newpath;
}

/*
 * get_redist_unique_redist_unique
 *     Compute cost and build redistribute + unique + redistribute + unique path
 */
Path* get_redist_unique_redist_unique(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key,
    List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc, bool cost_only)
{
    /*
     * step 1: get less skewed distribute keys
     *     Generate less skew distribute key for potential shuffle
     */
    List* final_list_exprs = NULL;
    if (Abs(path->multiple - 1.0) < 0.001 && NULL != path->distribute_keys) {
        final_list_exprs = path->distribute_keys;
    } else {
        final_list_exprs = path->parent->reltargetlist;
    }
    double multiple_less_skew = 0.0;
    List* distribute_key_less_skew =
        get_distributekey_from_tlist(root, NIL, final_list_exprs, path->rows, &multiple_less_skew);

    /*
     * We can not generate this kind of path when:
     * (1) a less skewed distribute key could not be found
     * (2) less skewed distribute key is not good enough
     * (3) less skewed distribute key is same as original distribute key
     */
    if (NULL == distribute_key_less_skew || multiple_less_skew > skew ||
        equal_distributekey(root, distribute_key, distribute_key_less_skew)) {
        return NULL;
    }

    /* step 2: redistribute to the less skewed distribute keys */
    Path* path_stage_1 = get_redist_unique(root,
        path,
        stream_type,
        distribute_key_less_skew,
        pathkeys,
        multiple_less_skew,
        target_distribution,
        smpDesc,
        cost_only);

    /* step 3: do normal unique */
    Path* path_stage_2 = get_unique_redist_unique(
        root, path_stage_1, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc, cost_only);

    return path_stage_2;
}

/*
 * get_optimal_join_unique_path
 *     get the best join unique path method
 */
SJoinUniqueMethod get_optimal_join_unique_path(PlannerInfo* root, Path* path, StreamType stream_type,
    List* distribute_key, List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc)
{
    AssertEreport(IsA(path, UniquePath), MOD_OPT_JOIN, "Path should be UniquePath");
    UniquePath* origin_path = (UniquePath*)path;
    Path* newpath = NULL;
    SJoinUniqueMethod option = UNIQUE_REDISTRIBUTE_UNIQUE;
    Cost best_cost = 0.0;

    /*
     * analyze stream reason, two possible reasons
     * (1) because of unmatched distribute key or smp
     * (2) because of unmatched node group
     */
    bool stream_reason_distkey_smp = (smpDesc && smpDesc->distriType != PARALLEL_NONE) ||
                                     needs_agg_stream(root, distribute_key, origin_path->path.distribute_keys, &origin_path->path.distribution);
#ifdef ENABLE_MULTIPLE_NODES
    bool stream_reason_nodegroup = !ng_is_same_group(ng_get_dest_distribution(path), target_distribution);
#else
    /* single node's nodegroup is always same */
    bool stream_reason_nodegroup = false;
#endif
    /* Path 1: redistribute + unique */
    newpath =
        get_redist_unique(root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc, true);

    ereport(
        DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] Path method 1: total_cost %lf.", newpath->total_cost)));

    if (best_cost < 0.0001 || newpath->total_cost < best_cost) {
        best_cost = newpath->total_cost;
        option = REDISTRIBUTE_UNIQUE;
    }

    /*
     * Path 2: unique + redistribute
     *     This path only support unmatched node group.
     */
    if (!stream_reason_distkey_smp && stream_reason_nodegroup) {
        newpath =
            get_unique_redist(root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] Path method 2: total_cost %lf.", newpath->total_cost)));

        if (best_cost < 0.0001 || newpath->total_cost < best_cost) {
            best_cost = newpath->total_cost;
            option = UNIQUE_REDISTRIBUTE;
        }
    }

    /*
     * Path 3: unique + redistribute + unique
     *     This path only support unmatched distribute key
     */
    if (stream_reason_distkey_smp && !stream_reason_nodegroup) {
        newpath = get_unique_redist_unique(
            root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc, true);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] Path method 3: total_cost %lf.", newpath->total_cost)));

        if (best_cost < 0.0001 || newpath->total_cost < best_cost) {
            best_cost = newpath->total_cost;
            option = UNIQUE_REDISTRIBUTE_UNIQUE;
        }
    }

    /* Path 4: redistribute + unique + redistribute + unique */
    newpath = get_redist_unique_redist_unique(
        root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc, true);
    if (NULL != newpath) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] Path method 4: total_cost %lf.", newpath->total_cost)));
    }
    if (NULL != newpath && (best_cost < 0.0001 || newpath->total_cost < best_cost) && best_cost < NG_FORBIDDEN_COST) {
        best_cost = newpath->total_cost;
        option = REDISTRIBUTE_UNIQUE_REDISTRIBUTE_UNIQUE;
    }

    return option;
}

/*
 * make_join_unique_path
 *     make a join unique path
 *     (1) get the optimal method for join unique base on cost
 *     (2) build a path base on the optimal method
 */
static Path* make_join_unique_path(PlannerInfo* root, Path* path, StreamType stream_type, List* distribute_key,
    List* pathkeys, double skew, Distribution* target_distribution, ParallelDesc* smpDesc)
{
    /* Get the optimal method to make this join unuque path */
    SJoinUniqueMethod option = get_optimal_join_unique_path(
        root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] Best path method is No. %d.", option + 1)));

    /* Make this join unique path */
    Path* best_path = NULL;
    switch (option) {
        case REDISTRIBUTE_UNIQUE:
            best_path = get_redist_unique(
                root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);
            break;
        case UNIQUE_REDISTRIBUTE:
            best_path = get_unique_redist(
                root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);
            break;
        case UNIQUE_REDISTRIBUTE_UNIQUE:
            best_path = get_unique_redist_unique(
                root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);
            break;
        case REDISTRIBUTE_UNIQUE_REDISTRIBUTE_UNIQUE:
            best_path = get_redist_unique_redist_unique(
                root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);
            break;
    }

    if (best_path != NULL)
        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                errmsg("[Join Unique] Finish building path, final startup cost : %lf, final total cost : %lf.",
                    best_path->startup_cost,
                    best_path->total_cost)));

    return best_path;
}

/*
 * Add a Stream path on a path
 */
Path* stream_side_path(PlannerInfo* root, Path* path, JoinType jointype, bool is_replicate, StreamType stream_type,
    List* distribute_key, List* pathkeys, bool is_inner, double skew, Distribution* target_distribution,
    ParallelDesc* smpDesc)
{

    /* target_distribution not likey to be NULL, but let's play safe */
    if (NULL == target_distribution) {
        target_distribution = NewDistribution();
        Distribution* distribution = ng_get_dest_distribution(path);
        ng_set_distribution(target_distribution, distribution);
    }

    if (is_replicate) {
        if (STREAM_BROADCAST == stream_type) {
            /*
             * If a STREAM_BROADCAST above a replicate table and node group changed,
             *     we still need to shuffle it.
             * Else, return original path directly.
             */
            if (ng_is_shuffle_needed(root, path, target_distribution)) {
                return create_stream_path(
                    root, path->parent, STREAM_BROADCAST, NIL, pathkeys, path, skew, target_distribution, smpDesc);
            } else {
                return path;
            }
        } else if (STREAM_REDISTRIBUTE == stream_type) {
            return create_stream_path(root,
                path->parent,
                STREAM_REDISTRIBUTE,
                distribute_key,
                pathkeys,
                path,
                skew,
                target_distribution,
                smpDesc);
        }
    } else {
        if ((JOIN_UNIQUE_INNER == jointype && is_inner) || (JOIN_UNIQUE_OUTER == jointype && !is_inner)) {
            return make_join_unique_path(
                root, path, stream_type, distribute_key, pathkeys, skew, target_distribution, smpDesc);
        } else {
            return create_stream_path(
                root, path->parent, stream_type, distribute_key, pathkeys, path, skew, target_distribution, smpDesc);
        }
    }

    return NULL;
}

double get_node_mcf(PlannerInfo* root, Node* v, double rows)
{
    VariableStatData vardata;
    /* u_sess->pgxc_cxt.NumDataNodes is used for default value of mcvfreq */
    double mcvfreq = pow(u_sess->pgxc_cxt.NumDataNodes, (double)1 / 3) / u_sess->pgxc_cxt.NumDataNodes;
    float4* numbers = NULL;
    int nnumbers = 0;

    examine_variable(root, v, 0, &vardata);

    /*
     * Look up the frequency of the most common value, if available.
     */
    if (HeapTupleIsValid(vardata.statsTuple)) {
        Form_pg_statistic stats;
        stats = (Form_pg_statistic)GETSTRUCT(vardata.statsTuple);
        mcvfreq = 0.0;

        if (get_attstatsslot(vardata.statsTuple,
                vardata.atttype,
                vardata.atttypmod,
                STATISTIC_KIND_MCV,
                InvalidOid,
                NULL,
                NULL,
                NULL,
                &numbers,
                &nnumbers)) {
            double relrows;

            /*
             * The first MCV stat is for the most common value.
             */
            if (nnumbers > 0)
                mcvfreq = numbers[0];

            /*
             * for total rows large than rel rows in coalesce expr, we think null
             * may be added, so adjust biase value
             */
            relrows = vardata.rel->rows;
            if (relrows < rows) {
                mcvfreq *= clamp_row_est(rows) / clamp_row_est(relrows);
                if (mcvfreq > 1.0)
                    mcvfreq = 1.0;
            }

            free_attstatsslot(vardata.atttype, NULL, 0, numbers, nnumbers);
        }

        mcvfreq = Max(stats->stanullfrac, mcvfreq);
    }

    ReleaseVariableStats(vardata);
    return mcvfreq;
}

/*
 * is_exact_match_keys_full:
 *		Judge if every item of matching keys has a available distribute key.
 *		In matching key mode, we should ganrantee that, or the distribute
 *		key can't match to matching key
 * Parameters:
 *	@in match_keys: matching key record array from upper level
 *	@in	length: the length of record array
 * Return:
 *	true if all the matching key location is occupied
 */
bool is_exact_match_keys_full(Node** match_keys, int length)
{
    int i;
    for (i = 0; i < length; i++) {
        if (match_keys[i] == NULL)
            break;
    }
    return (i == length);
}

/*
 * get_distribute_node:
 *		get single distribute key from one restrictinfo
 * Parameters:
 *	@in root: planner info of current query level
 *	@in rinfo: current restrictinfo, should be equal condition
 *	@in parent_rel: current rel used to find distribute key
 *	@in local_left: whether the current rel is located in left side
 *	@out skew_multiple: the multiple of distribute key founded
 *	@in	desired_keys: upper matching key that distribute key should be found according to
 *	@in exact_match_keys: In matching key mode, we should record the distribute key of
 *		every matching key, this array is used to do the record
 * Return:
 *	distribute key found from the restrictinfo, or NULL
 */
Node* get_distribute_node(PlannerInfo* root, RestrictInfo* rinfo, RelOptInfo* parent_rel, bool local_left,
    double* skew_multiple, List* desired_keys, Node** exact_match_keys)
{
#define MARKED_MATCHED_NODE (Node*)0x1
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ListCell* lc3 = NULL;
    Node* match_var = NULL;
    Node* match_expr = NULL;
    Node* joinkey = NULL;
    /* We should match desired keys if exists */
    bool desired_matched = (desired_keys != NIL) ? false : true;
    /* When exact match, we should delete matched items fro desired keys, so make a copy */
    List* desired_keys_copy = exact_match_keys != NULL ? list_copy(desired_keys) : NIL;

    EquivalenceClass* oeclass = NULL;

    if (local_left) {
        oeclass = rinfo->left_ec;
        joinkey = join_clause_get_join_key((Node*)rinfo->clause, true);
    } else {
        oeclass = rinfo->right_ec;
        joinkey = join_clause_get_join_key((Node*)rinfo->clause, false);
    }

    AssertEreport(rinfo->orclause == NULL && oeclass != NULL,
        MOD_OPT_JOIN,
        "Restrictinfo should be equal join condition without or clause");

    foreach (lc1, oeclass->ec_members) {
        EquivalenceMember* em = (EquivalenceMember*)lfirst(lc1);
        Node* nem = (Node*)em->em_expr;
        Oid datatype = exprType(nem);
        List* vars = NIL;
        Relids relIds;

        if (!OidIsValid(datatype) || !IsTypeDistributable(datatype))
            continue;

        /*
         * check if the choosen diskey (which might come from an eclass)
         * have compatiable type for hashing with the join key.
         */
        if (!is_diskey_and_joinkey_compatible(nem, joinkey)) {
            continue;
        }

        /* For desired key match, we should first set the matched item */
        if (desired_keys != NIL) {
            int i = 0;
            bool matched = false;
            foreach (lc2, desired_keys) {
                /*
                 * If found, first use a note to record it, and
                 * then replace with final decided distribute key
                 */
                if (equal(lfirst(lc2), nem)) {
                    if (exact_match_keys != NULL)
                        exact_match_keys[i] = MARKED_MATCHED_NODE;
                    matched = true;
                }
                i++;
            }
            if (matched) {
                /*
                 * Delete matched item from desired keys, NIL can be passed into
                 * this function so no need to judge if it's exact match case
                 */
                desired_keys_copy = list_delete(desired_keys_copy, nem);
                desired_matched = true;
            }
        }

        relIds = pull_varnos(nem);
        if (bms_is_empty(relIds) || !bms_is_subset(relIds, parent_rel->relids)) {
            bms_free_ext(relIds);
            continue;
        }
        bms_free_ext(relIds);

        if (list_member(parent_rel->reltargetlist, nem)) {
            match_var = nem;
        } else if (match_var == NULL) {
            /*
             * Check if all vars in sub targetlist
             *
             * For coalesce column in target list, it will presented as a Place Holder,
             * so we will leave it as it is without expand it.
             */
            vars = pull_var_clause(nem, PVC_REJECT_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
            foreach (lc2, vars) {
                Node* node = (Node*)lfirst(lc2);
                foreach (lc3, parent_rel->reltargetlist) {
                    Node* te = (Node*)lfirst(lc3);
                    if ((IsA(te, Var) && _equalSimpleVar((Var*)te, node)) || (!IsA(te, Var) && equal(te, node))) {
                        break;
                    }
                }
                if (lc3 == NULL) /* doesn't find the same in sub target list */
                    break;
            }
            list_free_ext(vars);
            if (lc2 != NULL) { /* not all vars in sub targetlist */
                continue;
            } else if (match_expr == NULL) {
                match_expr = nem;
            }
        }

        /* find the matched item, but should have further in desired key match case */
        if (match_var != NULL) {
            /* For non exact match case, break when desired_matched is true */
            if (exact_match_keys == NULL && desired_matched) {
                break;
            }
                
            /* For exact match case, we should traverse until all desired keys match */
            if (exact_match_keys != NULL && desired_keys_copy == NIL) {
                AssertEreport(desired_matched, MOD_OPT_JOIN, "Desired keys should be matched");
                break;
            }
        }
    }

    if (match_var == NULL) {
        match_var = match_expr;
    }

    if (desired_matched && match_var != NULL) {
        if (exact_match_keys != NULL) {
            /* Then replace marked items with real distribute key */
            for (int i = 0; i < list_length(desired_keys); i++) {
                if (exact_match_keys[i] == MARKED_MATCHED_NODE)
                    exact_match_keys[i] = match_var;
            }
            list_free_ext(desired_keys_copy);
        } else {
            List* diskey = list_make1(match_var);
            *skew_multiple = get_multiple_by_distkey(root, diskey, parent_rel->rows);
            list_free_ext(diskey);
        }
        return match_var;
    } else {
        if (exact_match_keys != NULL) {
            /* There's no match, so we should set all the match keys to NULL */
            for (int i = 0; i < list_length(desired_keys); i++) {
                if (exact_match_keys[i] == MARKED_MATCHED_NODE)
                    exact_match_keys[i] = NULL;
            }
            list_free_ext(desired_keys_copy);
        }
        return NULL;
    }
}

/*
 * get_distribute_keys:
 *		Get a final distribute key from the join clauses
 * Parameters:
 *	@in root: planner info of current query level
 *	@in joinclauses: join clauses that two join rel uses
 *	@in outer_path: path of outer join rel
 *	@in inner_path: path of inner join rel
 *	@out skew_outer: skew multiple of outer distribute key
 *	@out skew_inner: skew multiple of inner distribute key
 *	@out distribute_keys_outer: returned outer distribute key
 *	@out distribute_keys_inner: returned inner distribute key
 *	@in desired_keys: desired key that try to meet
 *	@in exact_match: if there's a desired key, whether we should do exact match
 */
void get_distribute_keys(PlannerInfo* root, List* joinclauses, Path* outer_path, Path* inner_path, double* skew_outer,
    double* skew_inner, List** distribute_keys_outer, List** distribute_keys_inner, List* desired_keys,
    bool exact_match)
{
    ListCell* cell = NULL;
    RelOptInfo* outerrel = outer_path->parent;
    RelOptInfo* innerrel = inner_path->parent;
    bool locate_left_inner = true;
    bool locate_left_outer = true;
    double min_skew = -1.0;
    Node* tmp_inner = NULL;
    Node* tmp_outer = NULL;
    Node* better_inner_key = NULL;
    Node* better_outer_key = NULL;
    List* disKeyInner = NULL;
    List* disKeyOuter = NULL;
    double skew_multiple_inner = 0.0;
    double skew_multiple_outer = 0.0;
    /* In exact match case, we should record distribute key location in both side */
    Node** exact_match_keys_outer = exact_match ? (Node**)palloc0(list_length(desired_keys) * sizeof(Node*)) : NULL;
    Node** exact_match_keys_inner = exact_match ? (Node**)palloc0(list_length(desired_keys) * sizeof(Node*)) : NULL;

    foreach (cell, joinclauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(cell);

        locate_left_inner = false;
        locate_left_outer = false;

        /*
         * Unsupported redistribution joinclauss must be filtered,
         * and assign locator info.
         */
        if (rinfo->orclause || !rinfo->left_ec || !rinfo->right_ec)
            continue;

        if (!rinfo->left_relids || !rinfo->right_relids)
            continue;

        /*
         * To op expr, we need judge args's hash arithmetic compatibility, if they are not compatible, we can not
         * choose it as distribute key. For example, timestamp '' and date '', they values is same but hash value is
         * different.
         */
        if (IsA(rinfo->clause, OpExpr)) {
            if (!is_args_type_compatible((OpExpr*)rinfo->clause)) {
                continue;
            }
        }

        /* Find the var in left and right of rinfo comes from which side */
        if (bms_is_subset(rinfo->left_relids, innerrel->relids) &&
            bms_is_subset(rinfo->right_relids, outerrel->relids)) {
            locate_left_inner = true;
            locate_left_outer = false;
        } else if (bms_is_subset(rinfo->right_relids, innerrel->relids) &&
                   bms_is_subset(rinfo->left_relids, outerrel->relids)) {
            locate_left_inner = false;
            locate_left_outer = true;
        }

        /* Get a single distribute item from current join clause */
        tmp_inner = get_distribute_node(
            root, rinfo, innerrel, !locate_left_outer, &skew_multiple_inner, desired_keys, exact_match_keys_inner);
        tmp_outer = get_distribute_node(
            root, rinfo, outerrel, !locate_left_inner, &skew_multiple_outer, desired_keys, exact_match_keys_outer);

        /* Distribute key is found */
        if (tmp_inner != NULL && tmp_outer != NULL) {
            /* For exact match case, we should match all keys and quit */
            if (exact_match) {
                if (is_exact_match_keys_full(exact_match_keys_outer, list_length(desired_keys)) &&
                    is_exact_match_keys_full(exact_match_keys_inner, list_length(desired_keys)))
                    break;
            } else {
                disKeyInner = lappend(disKeyInner, tmp_inner);
                disKeyOuter = lappend(disKeyOuter, tmp_outer);

                /* If overall multiple is less than formal ones, record new */
                if (min_skew == -1.0 || skew_multiple_inner * skew_multiple_outer < min_skew) {
                    min_skew = skew_multiple_inner * skew_multiple_outer;
                    better_inner_key = tmp_inner;
                    better_outer_key = tmp_outer;
                    *skew_inner = skew_multiple_inner;
                    *skew_outer = skew_multiple_outer;
                }
                /* If there's no skew of distribute key, break */
                if (skew_multiple_inner <= 1.0 && skew_multiple_outer <= 1.0)
                    break;
            }
        }
    }

    /* In exact match case, if not exact match, return NIL; or return the exact match distribute key */
    if (exact_match) {
        if (!is_exact_match_keys_full(exact_match_keys_outer, list_length(desired_keys)) ||
            !is_exact_match_keys_full(exact_match_keys_inner, list_length(desired_keys))) {
            pfree_ext(exact_match_keys_outer);
            pfree_ext(exact_match_keys_inner);
            return;
        }
        /* When all the match keys are exact match, we make the list and later calculate the multiple */
        for (int i = 0; i < list_length(desired_keys); i++) {
            disKeyInner = lappend(disKeyInner, exact_match_keys_inner[i]);
            disKeyOuter = lappend(disKeyOuter, exact_match_keys_outer[i]);
        }
        pfree_ext(exact_match_keys_outer);
        pfree_ext(exact_match_keys_inner);
    }

    if (disKeyInner == NIL && disKeyOuter == NIL)
        return;

    List* inList = NULL;
    List* ouList = NULL;

    /* If there's a non-skew distribute column, just return it */
    if (*skew_outer <= 1.0 && *skew_inner <= 1.0 && !exact_match) {
        *distribute_keys_inner = lappend(*distribute_keys_inner, copyObject(better_inner_key));
        *distribute_keys_outer = lappend(*distribute_keys_outer, copyObject(better_outer_key));
    } else if (disKeyInner != NULL && disKeyOuter != NULL) {
        /*
         * If all single column is skewed, we'll try to find multiple column
         * as distribute key. For the simplicity, we only consider prefix of
         * all matching columns until we find non-skew combination
         */
        int len = list_length(disKeyInner);
        int group_num;
        double single_min_skew = min_skew;

        /* Initialize distribute key search by adding the first one */
        inList = lappend(inList, linitial(disKeyInner));
        ouList = lappend(ouList, linitial(disKeyOuter));

        /* Continually adding new columns to calculate multiple */
        for (group_num = 2; group_num <= len; group_num++) {
            inList = lappend(inList, list_nth(disKeyInner, group_num - 1));
            ouList = lappend(ouList, list_nth(disKeyOuter, group_num - 1));

            if (group_num < len && exact_match)
                continue;

            skew_multiple_inner = get_multiple_by_distkey(root, inList, inner_path->rows);
            skew_multiple_outer = get_multiple_by_distkey(root, ouList, outer_path->rows);

            /* Find a less skewed column combination, then record it */
            if (skew_multiple_inner * skew_multiple_outer < min_skew) {
                min_skew = skew_multiple_inner * skew_multiple_outer;
                *skew_inner = skew_multiple_inner;
                *skew_outer = skew_multiple_outer;
            }
            /* Found a non-skewed combination, then break */
            if (skew_multiple_inner <= 1.0 && skew_multiple_outer <= 1.0)
                break;
        }
        /* Choose combined distribuite key. we need choose an min skew */
        if (min_skew < single_min_skew || exact_match) {
            *distribute_keys_inner = (List*)copyObject(inList);
            *distribute_keys_outer = (List*)copyObject(ouList);
        } else if (better_inner_key != NULL) { /* Choose single distribuite key */
            *distribute_keys_inner = lappend(*distribute_keys_inner, copyObject(better_inner_key));
            *distribute_keys_outer = lappend(*distribute_keys_outer, copyObject(better_outer_key));

        } else { /* have not distribuite key */
            *distribute_keys_inner = NIL;
            *distribute_keys_outer = NIL;
        }

        list_free_ext(inList);
        list_free_ext(ouList);
    }

    list_free_ext(disKeyInner);
    list_free_ext(disKeyOuter);

    if (!ng_is_distribute_key_valid(root, *distribute_keys_outer, outer_path->parent->reltargetlist) ||
        !ng_is_distribute_key_valid(root, *distribute_keys_inner, inner_path->parent->reltargetlist)) {
        *distribute_keys_inner = NIL;
        *distribute_keys_outer = NIL;
    }

    return;
}

static List* get_otherside_key(
    PlannerInfo* root, List* rinfo, List* targetlist, RelOptInfo* otherside_rel, double* skew_multiple)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ListCell* lc3 = NULL;
    List* key_list = NULL;
    Node* match_var = NULL;
    ListCell* cell = NULL;

    foreach (cell, rinfo) {
        EquivalenceClass* oeclass = NULL;
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(cell);
        Node* joinkey = NULL;

        match_var = NULL;

        if (bms_is_subset(restrictinfo->left_relids, otherside_rel->relids)) {
            oeclass = restrictinfo->left_ec;
            joinkey = join_clause_get_join_key((Node*)restrictinfo->clause, true);
        } else if (bms_is_subset(restrictinfo->right_relids, otherside_rel->relids)) {
            oeclass = restrictinfo->right_ec;
            joinkey = join_clause_get_join_key((Node*)restrictinfo->clause, false);
        }

        AssertEreport(restrictinfo->orclause == NULL && oeclass != NULL,
            MOD_OPT_JOIN,
            "Restrictinfo should be equal join condition without or clause");

        foreach (lc1, oeclass->ec_members) {
            EquivalenceMember* em = (EquivalenceMember*)lfirst(lc1);
            Node* nem = (Node*)em->em_expr;
            Oid datatype = exprType(nem);
            List* vars = NIL;
            Relids relIds;

            if (!OidIsValid(datatype) || !IsTypeDistributable(datatype))
                continue;

            /*
             * check if the choosen diskey (which might come from an eclass)
             * have compatiable type for hashing with the join key.
             */
            if (!is_diskey_and_joinkey_compatible(nem, joinkey)) {
                continue;
            }

            relIds = pull_varnos(nem);
            if (bms_is_empty(relIds) || !bms_is_subset(relIds, otherside_rel->relids)) {
                bms_free_ext(relIds);
                continue;
            }
            bms_free_ext(relIds);

            /*
             * Check if all vars in sub targetlist
             *
             * For coalesce column in target list, it will presented as a Place Holder,
             * so we will leave it as it is without expand it.
             */
            vars = pull_var_clause(nem, PVC_REJECT_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
            foreach (lc2, vars) {
                Node* node = (Node*)lfirst(lc2);
                foreach (lc3, targetlist) {
                    Node* te = (Node*)lfirst(lc3);
                    if ((IsA(te, Var) && _equalSimpleVar((Var*)te, node)) || (!IsA(te, Var) && equal(te, node))) {
                        break;
                    }
                }
                if (lc3 == NULL) /* doesn't find the same in sub target list */
                    break;
            }
            list_free_ext(vars);
            if (lc2 != NULL) /* not all vars in sub targetlist */
                continue;

            match_var = nem;
            break;
        }

        if (match_var != NULL) {
            key_list = lappend(key_list, copyObject(match_var));
        } else {
            list_free_ext(key_list);
            return NIL;
        }
    }

    *skew_multiple = get_multiple_by_distkey(root, key_list, otherside_rel->rows);

    if (!ng_is_distribute_key_valid(root, key_list, targetlist)) {
        list_free_ext(key_list);
        key_list = NIL;
    }

    return key_list;
}

/* check if subplan is executed on coordinator */
bool is_subplan_exec_on_coordinator(Path* path)
{
    if ((path->parent->subplan && is_execute_on_coordinator(path->parent->subplan)))
        return true;
    else if (path->distribution.bms_data_nodeids == NULL)
        return true;
    else
        return false;
}

/*
 * @Description:
 *    Check if we shoule use smp code to create a join path.
 *
 * @param[IN] inner: inner join path.
 * @param[IN] outer: outer join path.
 * @return bool: true -- we should use smp code.
 */
static bool parallel_enable(Path* inner, Path* outer)
{
    /*
     * If both sides are not parallelized,
     * then there is no need to parallel the join.
     */
    if (inner->dop <= 1 && outer->dop <= 1)
        return false;

    if (u_sess->opt_cxt.query_dop > 1 && IS_STREAM_PLAN)
        return true;
    else
        return false;
}

/*
 * @Description:
 *    Check if a Join path can be parallelized.
 *
 * @param[IN] inner: inner join path.
 * @param[IN] outer: outer join path.
 * @return bool: true -- can be parallelized.
 */
static bool can_parallel(Path* inner, Path* outer)
{
    /* Avoid parameterized path to be parallelized. */
    if (inner->param_info != NULL || outer->param_info != NULL)
        return false;

    return true;
}

/*
 * @Description:
 *    Set distribute key for each join path in the pathlist,
 *    and add it to rel->pathlist.
 *
 * @param[IN] joinpath_list: the join path list wiat to be processed.
 * @param[IN] jointype: join type of outerrel and innerrel.
 * @param[IN] joinrel: the join relatin.
 * @param[IN] joinrel: the planner info.
 * @param[IN] outer_distributekey: distribute key of outerrel.
 * @param[IN] inner_distributekey: distribute key of innerrel.
 * @param[IN] desired_key: matching or superset key in upper level,
 *		which is desired in join distribute key.
 * @param[IN] exact_match: note whether in exact mode, and in this
 *		mode, we directly return desired_key.
 * @return void
 */
static void add_path_list(List* joinpath_list, JoinType jointype, RelOptInfo* joinrel, PlannerInfo* root,
    List* outer_distributekey, List* inner_distributekey, List* desired_key = NIL, bool exact_match = false)
{
    ListCell* lc = NULL;
    JoinPath* joinpath = NULL;
    foreach (lc, joinpath_list) {
        joinpath = (JoinPath*)lfirst(lc);
        if (jointype != JOIN_FULL) {
            joinpath->path.distribute_keys =
                locate_distribute_key(jointype, outer_distributekey, inner_distributekey, desired_key, exact_match);
            if (joinpath->path.distribute_keys)
                add_path(root, joinrel, (Path*)joinpath);
            else
                pfree_ext(joinpath);
        } else
            add_path(root, joinrel, (Path*)joinpath);
    }

    /* Free the list. */
    list_free_ext(joinpath_list);
}

/*
 * @Description:
 *    Set distribute key for join path with replicate subpath.
 *
 * @param[IN] joinrel: RelOptInfo of join.
 * @param[IN] root: PlannerInfo of join.
 * @param[IN] save_jointype: save join type.
 * @param[IN] joinpath: join path to be processed.
 * @param[IN] replicate_outer/replicate_inner: if the inner/outer path is replicate.
 * @param[IN] redistribute_inner/redistribute_outer: if the inner/outer path can redistribute.
 * @return bool: true -- we can generate redistribute join path.
 */
static bool add_replica_join_path(RelOptInfo* joinrel, PlannerInfo* root, JoinType save_jointype, JoinPath* joinpath,
    bool replicate_outer, bool replicate_inner, bool redistribute_inner, bool redistribute_outer)
{
    Path* inner_path = joinpath->innerjoinpath;
    Path* outer_path = joinpath->outerjoinpath;
    bool can_redistribute = true;

    if (replicate_outer && replicate_inner) {
        joinpath->path.distribute_keys = NIL;
    } else {
        if (replicate_outer) {
            joinpath->path.distribute_keys = inner_path->distribute_keys;
        } else {
            joinpath->path.distribute_keys = outer_path->distribute_keys;
        }
    }

    /* Re-set if one side is on CN */
    if (is_subplan_exec_on_coordinator(outer_path) || is_subplan_exec_on_coordinator(inner_path)) {
        joinpath->path.locator_type = LOCATOR_TYPE_REPLICATED;
        joinpath->path.distribute_keys = NIL;
        joinpath->path.exec_type = EXEC_ON_COORDS;
        replicate_outer = true;
        replicate_inner = true;
    }

    /*
     * Followed cases can choose local plan:
     *   1.Outer is replicate and inner is hash: RHS join or probing side execute on CN, and build side need
     * redistribute; 2.Outer is hash and inner is replicate: LHS join or probing side execute on CN, and build side need
     * redistribute;
     */
    if (replicate_outer && !replicate_inner) {
        if ((RHS_join(save_jointype) || (is_subplan_exec_on_coordinator(outer_path))) && redistribute_inner) {
            add_path(root, joinrel, (Path*)joinpath);
            can_redistribute = false;
        }
    } else if (!replicate_outer && replicate_inner) {
        if ((LHS_join(save_jointype) || (is_subplan_exec_on_coordinator(inner_path))) && redistribute_outer) {
            add_path(root, joinrel, (Path*)joinpath);
            can_redistribute = false;
        }
    } else {
        add_path(root, joinrel, (Path*)joinpath);
        can_redistribute = false;
    }

    return can_redistribute;
}

/*
 * @Description:
 *    Create and add a join path with redistribute.
 *    This function is especially designed for smp,
 *    which can be used for hashjoin and nestloop.
 *
 * @param[IN] inner_smpDesc: smp info for inner path.
 * @param[IN] outer_smpDesc: smp info for outer path.
 * Other input param please refer to comment of add_join_parallel_path().
 *
 * @return JoinPath*: the JoinPath created by this func.
 */
static JoinPath* add_join_redistribute_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors,
    Path* inner_path, Path* outer_path, ParallelDesc* inner_smpDesc, ParallelDesc* outer_smpDesc, List* restrictlist,
    List* hashclauses, Relids required_outer, double skew_inner, double skew_outer, List* stream_distribute_key_inner,
    List* stream_distribute_key_outer, bool replicate_inner, bool replicate_outer, NodeTag nodetag,
    Distribution* target_distribution, List* inner_pathkeys = NIL, List* outer_pathkeys = NIL)
{
    Path* stream_path_inner = inner_path;
    Path* stream_path_outer = outer_path;
    JoinPath* joinpath = NULL;

    /* Set distribute key for local distribute node. */
    if (stream_distribute_key_inner == NULL)
        stream_distribute_key_inner = inner_path->distribute_keys;
    if (stream_distribute_key_outer == NULL)
        stream_distribute_key_outer = outer_path->distribute_keys;

    /* Confirm the dop of join. */
    AssertEreport(inner_smpDesc->consumerDop == outer_smpDesc->consumerDop,
        MOD_OPT_JOIN,
        "Dop of outer_path and inner_path should be the same");
    int joinDop = inner_smpDesc->consumerDop;

    /* Add stream path if needed. */
    /* case1: prallel join path. */
    if (joinDop > 1) {
        if (PARALLEL_NONE != inner_smpDesc->distriType)
            stream_path_inner = stream_side_path(root,
                inner_path,
                save_jointype,
                replicate_inner,
                STREAM_REDISTRIBUTE,
                stream_distribute_key_inner,
                inner_pathkeys,
                true,
                skew_inner,
                target_distribution,
                inner_smpDesc);

        if (PARALLEL_NONE != outer_smpDesc->distriType)
            stream_path_outer = stream_side_path(root,
                outer_path,
                save_jointype,
                replicate_outer,
                STREAM_REDISTRIBUTE,
                stream_distribute_key_outer,
                outer_pathkeys,
                false,
                skew_outer,
                target_distribution,
                outer_smpDesc);

    } else {
        /* case2: do not parallel join path. 
		 *Check if we need to add extra stream node. 
		 */
        if (inner_smpDesc->producerDop > 1 || REMOTE_DISTRIBUTE == inner_smpDesc->distriType)
            stream_path_inner = stream_side_path(root,
                inner_path,
                save_jointype,
                replicate_inner,
                STREAM_REDISTRIBUTE,
                stream_distribute_key_inner,
                inner_pathkeys,
                true,
                skew_inner,
                target_distribution,
                inner_smpDesc);

        if (outer_smpDesc->producerDop > 1 || REMOTE_DISTRIBUTE == outer_smpDesc->distriType)
            stream_path_outer = stream_side_path(root,
                outer_path,
                save_jointype,
                replicate_outer,
                STREAM_REDISTRIBUTE,
                stream_distribute_key_outer,
                outer_pathkeys,
                false,
                skew_outer,
                target_distribution,
                outer_smpDesc);
    }

    /* Create join path. */
    if (nodetag == T_HashJoin) {
        initial_cost_hashjoin(
            root, workspace, jointype, hashclauses, stream_path_outer, stream_path_inner, sjinfo, semifactors, joinDop);

        joinpath = (JoinPath*)create_hashjoin_path(root,
            joinrel,
            jointype,
            workspace,
            sjinfo,
            semifactors,
            stream_path_outer,
            stream_path_inner,
            restrictlist,
            required_outer,
            hashclauses,
            joinDop);
    } else {
        initial_cost_nestloop(
            root, workspace, jointype, stream_path_outer, stream_path_inner, sjinfo, semifactors, joinDop);

        /*
         * When nestloop, hashclauses refer to pathkeys.
         * If the outer path's pathkeys == NIL, then we
         * need to set pathkeys to NIL;
         */
        if (stream_path_outer->pathkeys == NIL)
            hashclauses = NIL;

        joinpath = (JoinPath*)create_nestloop_path(root,
            joinrel,
            jointype,
            workspace,
            sjinfo,
            semifactors,
            stream_path_outer,
            stream_path_inner,
            restrictlist,
            hashclauses,
            required_outer,
            joinDop);
    }

    return joinpath;
}

/*
 * @Description:
 *    Add the logic to handle parallel broadcast situation.
 *
 * @param[IN] need_smpDesc: the smp info in broadcast side.
 * @param[IN] non_smpDesc: the smp info in no-broadcast side.
 * @param[IN] dop: degree of join parallel.
 * Other input param please refer to comment of add_join_parallel_path().
 *
 * @return JoinPath*: the JoinPath created by this func.
 */
static void add_hashjoin_broadcast_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors,
    Path* need_stream_path, Path* non_stream_path, List* restrictlist, Relids required_outer, List* hashclauses,
    bool is_replicate, bool stream_outer, Distribution* target_distribution, ParallelDesc* need_smpDesc,
    ParallelDesc* non_smpDesc, int dop)
{
    Path* streamed_path = NULL;
    Path* other_side = NULL;
    JoinPath* joinpath = NULL;
    Path* new_outer_path = NULL;
    Path* new_inner_path = NULL;

    /* target_distribution not likey to be NULL, but let's play safe */
    if (NULL == target_distribution) {
        target_distribution = ng_get_default_computing_group_distribution();
    }

    /* If parallel, add parallel info to the path. */
    streamed_path = stream_side_path(root,
        need_stream_path,
        save_jointype,
        is_replicate,
        STREAM_BROADCAST,
        NIL,
        NIL,
        !stream_outer,
        1.0,
        target_distribution,
        need_smpDesc);

    /* non-broadcast side also needs shuffle if node group is un-matched */
    non_stream_path = ng_stream_non_broadcast_side_for_join(
        root, non_stream_path, save_jointype, NIL, is_replicate, stream_outer, target_distribution);
    if (NULL == non_stream_path) {
        /* non-broadcast side can not shuffle */
        return;
    }

    if (NULL == non_smpDesc) {
        other_side = non_stream_path;
    } else {
        if (PARALLEL_NONE != non_smpDesc->distriType) {
            other_side = stream_side_path(root,
                non_stream_path,
                save_jointype,
                is_replicate,
                STREAM_REDISTRIBUTE,
                NIL,
                NIL,
                stream_outer,
                1.0,
                target_distribution,
                non_smpDesc);
        } else
            other_side = non_stream_path;
    }

    /* Confirm the inner path and outer path. */
    new_outer_path = stream_outer ? streamed_path : other_side;
    new_inner_path = stream_outer ? other_side : streamed_path;

    initial_cost_hashjoin(
        root, workspace, jointype, hashclauses, new_outer_path, new_inner_path, sjinfo, semifactors, dop);

    joinpath = (JoinPath*)create_hashjoin_path(root,
        joinrel,
        jointype,
        workspace,
        sjinfo,
        semifactors,
        new_outer_path,
        new_inner_path,
        restrictlist,
        required_outer,
        hashclauses,
        dop);

    joinpath->path.distribute_keys = non_stream_path->distribute_keys;
    add_path(root, joinrel, (Path*)joinpath);
}

/*
 * @Description:
 *    Add parallel info for join with replicate table.
 *
 * @param[IN] is_replicate: is the path a replicate one.
 * @param[IN] path: the path needed to be processed.
 * @param[IN] smpDesc: smp info for this path.
 * @param[IN] stream: streamType -- broadcast/redistribute/none
 *
 * @return void
 */
static void set_replicate_parallel_info(bool is_replicate, Path* path, ParallelDesc* smpDesc, StreamType stream)
{
    if (is_replicate) {
        /*
         * Generally we do not parallel replicate path,
         * unless we want to make the join path parallel.
         */
        if (STREAM_REDISTRIBUTE == stream) {
            smpDesc->distriType = REMOTE_SPLIT_DISTRIBUTE;
        } else {
            smpDesc->distriType = LOCAL_BROADCAST;
        }
    }
}

/*
 * @Description:
 *    Create a parallel and unparallel join path when enable smp.
 *    And add them to the path list.
 *    This function is designed for hashjoin and nestloop.
 *
 * @param[IN] inner_stream: the inner side stream type.
 * @param[IN] outer_stream: the outer side stream type.
 * @param[IN] root: the plannerInfo for this join.
 * @param[IN] joinrel: the join relation
 * @param[IN] sjinfo: extra info about the join for selectivity estimation
 * @param[IN] semifactors: contains valid data if jointype is SEMI or ANTI.
 * @param[IN] inner_path: the inner subpath for join.
 * @param[IN] outer_path: the outer subpath for join.
 * @param[IN] skew_inner: data skew for inner path.
 * @param[IN] skew_outer: data skew for outer path.
 * @param[IN] jointype: join type.
 * @param[IN] save_jointype: save join type.
 * @param[IN] required_outer: the set of required outer rels.
 * @param[IN] workspace: workspace to record join cost.
 * @param[IN] replicate_inner: is inner path replicate or not.
 * @param[IN] replicate_outer: is outer path replicate or not.
 * @param[IN] stream_distribute_key_inner: distributekey for inner stream.
 * @param[IN] stream_distribute_key_outer: distributekey for outer stream.
 * @param[IN] hashclauses:  the RestrictInfo nodes to use as hash clauses.
 * @param[IN] restrictlist: the RestrictInfo nodes to apply at the join.
 * @param[IN] nodetag: Nestloop or HashJoin.
 * @param[IN] inner_pathkeys: inner path sort key.
 *
 * @param[OUT] outer_pathkeys: outer path sort key.
 *
 * @return void
 */
static List* add_join_parallel_path(StreamType inner_stream, StreamType outer_stream, PlannerInfo* root,
    RelOptInfo* joinrel, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* inner_path, Path* outer_path,
    double skew_inner, double skew_outer, JoinType jointype, JoinType save_jointype, Relids required_outer,
    JoinCostWorkspace* workspace, bool replicate_inner, bool replicate_outer, List* stream_distribute_key_inner,
    List* stream_distribute_key_outer, List* hashclauses, List* restrictlist, NodeTag nodetag,
    Distribution* target_distribution, List* inner_pathkeys = NIL, List* outer_pathkeys = NIL)
{
    /*
     * When the user turn on SMP, we need to add parallel path
     * to the alternative path list.
     * That means we need to handle two main situations:
     *     1. Create a parallel join path whether the subpath
     *        is parallel or not.
     *     2. Create a unparallel join path especially when
     *        the subpath is parallel. If the subpath is
     *        unparalleled, then we just treat it as serial
     *        path, otherwise we need to do additional handling.
     */
    AssertEreport(
        nodetag == T_HashJoin || nodetag == T_NestLoop, MOD_OPT_JOIN, "Join method should be hashjoin or nestloop");
    List* joinpath_list = NIL;
    JoinPath* joinpath = NULL;

    /* 1. Create parallel join path. */
    /* create two smp desc. */
    ParallelDesc* inner_smpDesc = create_smpDesc(u_sess->opt_cxt.query_dop, inner_path->dop, PARALLEL_NONE);
    ParallelDesc* outer_smpDesc = create_smpDesc(u_sess->opt_cxt.query_dop, outer_path->dop, PARALLEL_NONE);

    bool inner_can_local_distribute =
        check_dsitribute_key_in_targetlist(root, inner_path->distribute_keys, inner_path->parent->reltargetlist);
    bool outer_can_local_distribute =
        check_dsitribute_key_in_targetlist(root, outer_path->distribute_keys, outer_path->parent->reltargetlist);

    /*
     * If we already have redistribute or local redistribute
     * in the subquery path, then there is no need to add
     * new local redistribute for parallelism.
     */
    bool inner_need_local_distribute = true;
    bool outer_need_local_distribute = true;

    Path* in_tmp = inner_path;
    Path* out_tmp = outer_path;

    /* This kind of unique path is dummy path, skip it. */
    if (T_Unique == inner_path->pathtype && UNIQUE_PATH_NOOP == ((UniquePath*)inner_path)->umethod)
        in_tmp = ((UniquePath*)inner_path)->subpath;
    if (T_Unique == outer_path->pathtype && UNIQUE_PATH_NOOP == ((UniquePath*)outer_path)->umethod)
        out_tmp = ((UniquePath*)outer_path)->subpath;

    /*
     * Check the subqueryscan path to avoid additional redistribution
     * incase that subplan has already local distributed.
     */
    if (in_tmp->pathtype == T_SubqueryScan) {
        Plan* subplan = in_tmp->parent->subplan;
        inner_need_local_distribute = is_local_redistribute_needed(subplan);
    }

    if (out_tmp->pathtype == T_SubqueryScan) {
        Plan* subplan = out_tmp->parent->subplan;
        outer_need_local_distribute = is_local_redistribute_needed(subplan);
    }

    /*
     * Create parallel join path.
     */
    /* can set pathkey to NIL. */
    if (can_parallel(inner_path, outer_path)) {
        /* Scenario 1: local join. */
        if (STREAM_NONE == inner_stream && STREAM_NONE == outer_stream) {
            if (replicate_inner || replicate_outer) {
                /* Set parallel info. */
                set_replicate_parallel_info(replicate_inner, inner_path, inner_smpDesc, inner_stream);
                set_replicate_parallel_info(replicate_outer, outer_path, outer_smpDesc, outer_stream);

                joinpath = (JoinPath*)add_join_redistribute_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    inner_smpDesc,
                    outer_smpDesc,
                    restrictlist,
                    hashclauses,
                    required_outer,
                    skew_inner,
                    skew_outer,
                    NIL,
                    NIL,
                    replicate_inner,
                    replicate_outer,
                    nodetag,
                    target_distribution,
                    NIL,
                    NIL);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            } else {
                /* There is 3 possible parallel path. */
                /* case 1:local broadcast inner */
                if (outer_path->pathtype != T_Unique && inner_path->pathtype != T_Unique &&
                    can_broadcast_inner(jointype, save_jointype, replicate_outer, NIL, NIL)) {
                    inner_smpDesc->distriType = LOCAL_BROADCAST;
                    if (outer_smpDesc->producerDop <= 1)
                        outer_smpDesc->distriType = LOCAL_ROUNDROBIN;

                    joinpath = (JoinPath*)add_join_redistribute_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        inner_smpDesc,
                        outer_smpDesc,
                        restrictlist,
                        hashclauses,
                        required_outer,
                        skew_inner,
                        skew_outer,
                        stream_distribute_key_inner,
                        stream_distribute_key_outer,
                        replicate_inner,
                        replicate_outer,
                        nodetag,
                        target_distribution,
                        NIL,
                        NIL);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                }

                /* case 2:local broadcast outer */
                if (outer_path->pathtype != T_Unique && inner_path->pathtype != T_Unique &&
                    can_broadcast_outer(jointype, save_jointype, replicate_inner, NIL, NIL)) {
                    ParallelDesc* inner_smpDesc1 =
                        create_smpDesc(u_sess->opt_cxt.query_dop, inner_path->dop, PARALLEL_NONE);
                    ParallelDesc* outer_smpDesc1 =
                        create_smpDesc(u_sess->opt_cxt.query_dop, outer_path->dop, PARALLEL_NONE);

                    outer_smpDesc1->distriType = LOCAL_BROADCAST;
                    if (inner_smpDesc1->producerDop <= 1)
                        inner_smpDesc1->distriType = LOCAL_ROUNDROBIN;

                    joinpath = (JoinPath*)add_join_redistribute_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        inner_smpDesc1,
                        outer_smpDesc1,
                        restrictlist,
                        hashclauses,
                        required_outer,
                        skew_inner,
                        skew_outer,
                        stream_distribute_key_inner,
                        stream_distribute_key_outer,
                        replicate_inner,
                        replicate_outer,
                        nodetag,
                        target_distribution,
                        NIL,
                        NIL);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                }

                /* Check if we can use local redistribute. */
                if (inner_can_local_distribute && outer_can_local_distribute) {
                    /* case 3:local distribute inner and outer */
                    ParallelDesc* inner_smpDesc2 =
                        create_smpDesc(u_sess->opt_cxt.query_dop, inner_path->dop, PARALLEL_NONE);
                    ParallelDesc* outer_smpDesc2 =
                        create_smpDesc(u_sess->opt_cxt.query_dop, outer_path->dop, PARALLEL_NONE);

                    if (inner_need_local_distribute)
                        inner_smpDesc2->distriType = LOCAL_DISTRIBUTE;
                    if (outer_need_local_distribute)
                        outer_smpDesc2->distriType = LOCAL_DISTRIBUTE;

                    joinpath = (JoinPath*)add_join_redistribute_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        inner_smpDesc2,
                        outer_smpDesc2,
                        restrictlist,
                        hashclauses,
                        required_outer,
                        skew_inner,
                        skew_outer,
                        stream_distribute_key_inner,
                        stream_distribute_key_outer,
                        replicate_inner,
                        replicate_outer,
                        nodetag,
                        target_distribution,
                        NIL,
                        NIL);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                }
            }
        } else if (STREAM_REDISTRIBUTE == inner_stream || STREAM_REDISTRIBUTE == outer_stream) {
            /* Scenario 2: join with redistribute. */
            if (STREAM_REDISTRIBUTE == inner_stream && STREAM_REDISTRIBUTE == outer_stream) {
                inner_smpDesc->distriType = REMOTE_SPLIT_DISTRIBUTE;
                outer_smpDesc->distriType = REMOTE_SPLIT_DISTRIBUTE;
            } else if (STREAM_REDISTRIBUTE == inner_stream && STREAM_REDISTRIBUTE != outer_stream) {
                inner_smpDesc->distriType = REMOTE_SPLIT_DISTRIBUTE;
                if (outer_need_local_distribute)
                    outer_smpDesc->distriType = LOCAL_DISTRIBUTE;
            } else if (STREAM_REDISTRIBUTE != inner_stream && STREAM_REDISTRIBUTE == outer_stream) {
                if (inner_need_local_distribute)
                    inner_smpDesc->distriType = LOCAL_DISTRIBUTE;
                outer_smpDesc->distriType = REMOTE_SPLIT_DISTRIBUTE;
            }

            if ((LOCAL_DISTRIBUTE != inner_smpDesc->distriType || inner_can_local_distribute) &&
                (LOCAL_DISTRIBUTE != outer_smpDesc->distriType || outer_can_local_distribute)) {
                /* Set parallel info for replicate table. */
                if (replicate_inner)
                    set_replicate_parallel_info(replicate_inner, inner_path, inner_smpDesc, inner_stream);
                if (replicate_outer)
                    set_replicate_parallel_info(replicate_outer, outer_path, outer_smpDesc, outer_stream);

                joinpath = (JoinPath*)add_join_redistribute_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    inner_smpDesc,
                    outer_smpDesc,
                    restrictlist,
                    hashclauses,
                    required_outer,
                    skew_inner,
                    skew_outer,
                    stream_distribute_key_inner,
                    stream_distribute_key_outer,
                    replicate_inner,
                    replicate_outer,
                    nodetag,
                    target_distribution,
                    NIL,
                    NIL);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            }
        } else {
            /* Scenario 3: join with broadcast. */
            /* Do not support replicate table with broadcast. */
            if (!replicate_inner && !replicate_outer) {
                if (STREAM_BROADCAST == inner_stream) {
                    inner_smpDesc->distriType = REMOTE_SPLIT_BROADCAST;
                    if (outer_smpDesc->producerDop <= 1)
                        outer_smpDesc->distriType = LOCAL_ROUNDROBIN;

                    if (nodetag == T_HashJoin)
                        add_hashjoin_broadcast_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            inner_path,
                            outer_path,
                            restrictlist,
                            required_outer,
                            hashclauses,
                            replicate_inner,
                            false,
                            target_distribution,
                            inner_smpDesc,
                            outer_smpDesc,
                            u_sess->opt_cxt.query_dop);
                    else
                        add_nestloop_broadcast_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            inner_path,
                            outer_path,
                            restrictlist,
                            hashclauses,
                            required_outer,
                            NIL,
                            replicate_inner,
                            false,
                            target_distribution,
                            inner_smpDesc,
                            outer_smpDesc,
                            u_sess->opt_cxt.query_dop);
                } else if (STREAM_BROADCAST == outer_stream) {
                    outer_smpDesc->distriType = REMOTE_SPLIT_BROADCAST;
                    if (inner_smpDesc->producerDop <= 1)
                        inner_smpDesc->distriType = LOCAL_ROUNDROBIN;

                    if (nodetag == T_HashJoin)
                        add_hashjoin_broadcast_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            outer_path,
                            inner_path,
                            restrictlist,
                            required_outer,
                            hashclauses,
                            replicate_outer,
                            true,
                            target_distribution,
                            outer_smpDesc,
                            inner_smpDesc,
                            u_sess->opt_cxt.query_dop);
                    else
                        add_nestloop_broadcast_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            outer_path,
                            inner_path,
                            restrictlist,
                            hashclauses,
                            required_outer,
                            NIL,
                            replicate_outer,
                            true,
                            target_distribution,
                            outer_smpDesc,
                            inner_smpDesc,
                            u_sess->opt_cxt.query_dop);
                }
            }
        }
    }

    /* 2. Add a unparallel join path. */
    /* Create two smp desc. */
    ParallelDesc* inner_unpara_smpDesc = create_smpDesc(1, inner_path->dop, PARALLEL_NONE);
    ParallelDesc* outer_unpara_smpDesc = create_smpDesc(1, outer_path->dop, PARALLEL_NONE);

    if (STREAM_BROADCAST != inner_stream && STREAM_BROADCAST != outer_stream) {
        if (STREAM_NONE == inner_stream && STREAM_NONE == outer_stream) {
            inner_unpara_smpDesc->distriType = LOCAL_ROUNDROBIN;
            outer_unpara_smpDesc->distriType = LOCAL_ROUNDROBIN;
        } else if (STREAM_REDISTRIBUTE == inner_stream && STREAM_REDISTRIBUTE == outer_stream) {
            inner_unpara_smpDesc->distriType = REMOTE_DISTRIBUTE;
            outer_unpara_smpDesc->distriType = REMOTE_DISTRIBUTE;
        } else if (STREAM_REDISTRIBUTE == inner_stream && STREAM_REDISTRIBUTE != outer_stream) {
            inner_unpara_smpDesc->distriType = REMOTE_DISTRIBUTE;
            outer_unpara_smpDesc->distriType = LOCAL_ROUNDROBIN;
        } else if (STREAM_REDISTRIBUTE != inner_stream && STREAM_REDISTRIBUTE == outer_stream) {
            inner_unpara_smpDesc->distriType = LOCAL_ROUNDROBIN;
            outer_unpara_smpDesc->distriType = REMOTE_DISTRIBUTE;
        }
        joinpath = add_join_redistribute_path(root,
            joinrel,
            jointype,
            save_jointype,
            workspace,
            sjinfo,
            semifactors,
            inner_path,
            outer_path,
            inner_unpara_smpDesc,
            outer_unpara_smpDesc,
            restrictlist,
            hashclauses,
            required_outer,
            skew_inner,
            skew_outer,
            stream_distribute_key_inner,
            stream_distribute_key_outer,
            replicate_inner,
            replicate_outer,
            nodetag,
            target_distribution,
            NIL,
            NIL);
        joinpath_list = lappend(joinpath_list, (void*)joinpath);
    } else {
        if (STREAM_BROADCAST == inner_stream) {
            inner_unpara_smpDesc->distriType = REMOTE_BROADCAST;
            if (outer_unpara_smpDesc->producerDop > 1) {
                outer_unpara_smpDesc->distriType = LOCAL_ROUNDROBIN;
                /* Do not add local gather path above replicate table. */
                if (is_replicated_path(outer_path))
                    return joinpath_list;
            }
            if (nodetag == T_HashJoin)
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_inner,
                    false,
                    target_distribution,
                    inner_unpara_smpDesc,
                    outer_unpara_smpDesc);
            else
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrictlist,
                    hashclauses,
                    required_outer,
                    NIL,
                    replicate_inner,
                    false,
                    target_distribution,
                    inner_unpara_smpDesc,
                    outer_unpara_smpDesc,
                    1);
        } else if (STREAM_BROADCAST == outer_stream) {
            if (inner_unpara_smpDesc->producerDop > 1) {
                inner_unpara_smpDesc->distriType = LOCAL_ROUNDROBIN;
                if (is_replicated_path(inner_path))
                    return joinpath_list;
            }
            outer_unpara_smpDesc->distriType = REMOTE_BROADCAST;

            if (nodetag == T_HashJoin)
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_outer,
                    true,
                    target_distribution,
                    outer_unpara_smpDesc,
                    inner_unpara_smpDesc);
            else
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrictlist,
                    hashclauses,
                    required_outer,
                    NIL,
                    replicate_outer,
                    true,
                    target_distribution,
                    outer_unpara_smpDesc,
                    inner_unpara_smpDesc,
                    1);
        }
    }

    return joinpath_list;
}

/*
 * @Description:
 *    Add hashjoin path in stream mode.
 *
 * The input param please refer to comment of add_join_parallel_path().
 *
 * @return JoinPath*: the JoinPath created by this func.
 */
void add_hashjoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path,
    Path* inner_path, List* restrictlist, Relids required_outer, List* hashclauses, Distribution* target_distribution)
{
    bool redistribute_inner = true;
    bool redistribute_outer = true;
    bool replicate_inner = false;
    bool replicate_outer = false;
    List* distribute_keys_inner = NIL;
    List* distribute_keys_outer = NIL;
    List* joinclauses = NIL;
    RelOptInfo* outerrel = NULL;
    RelOptInfo* innerrel = NULL;
    JoinPath* joinpath = NULL;
    List* joinpath_list = NIL;
    ListCell* lc = NULL;

    List* rrinfo_inner = NULL;
    List* rrinfo_outer = NULL;
    List* stream_distribute_key = NIL;

    NodeTag nodetag = T_HashJoin;

    outerrel = outer_path->parent;
    innerrel = inner_path->parent;
    joinclauses = hashclauses;

    distribute_keys_inner = inner_path->distribute_keys;
    distribute_keys_outer = outer_path->distribute_keys;

    if (is_replicated_path(outer_path))
        replicate_outer = true;

    if (is_replicated_path(inner_path))
        replicate_inner = true;

    /* joinclauses of hashjoin should be Non-null. */
    AssertEreport(joinclauses != NIL, MOD_OPT_JOIN, "Joinclauses of hashjoin should be Non-null");

    /*
     * Wheather inner or outer need to be redistributed base on their distribute key and join clauses
     *     TRUE means need to be redistributed,
     *     FALSE means do not need to be redistributed
     */
    redistribute_inner = is_distribute_need_on_joinclauses(
        root, inner_path->distribute_keys, joinclauses, innerrel, outerrel, &rrinfo_inner);
    redistribute_outer = is_distribute_need_on_joinclauses(
        root, outer_path->distribute_keys, joinclauses, outerrel, innerrel, &rrinfo_outer);

    /*
     * Check node group distribution
     *     If path's distribution is different from target_distribution (computing node group), shuffle is needed
     */
    redistribute_inner = redistribute_inner || ng_is_shuffle_needed(root, inner_path, target_distribution);
    redistribute_outer = redistribute_outer || ng_is_shuffle_needed(root, outer_path, target_distribution);

    /*
     * If either side is replicated, join locally.
     */
    if (replicate_outer || replicate_inner) {
        /*
         * Check if we need do further redistribution even with two replicate table
         *     and shuffle them to same computing node group.
         */
        Path* outer_path_t = outer_path;
        Path* inner_path_t = inner_path;
        ng_stream_side_paths_for_replicate(
            root, &outer_path_t, &inner_path_t, save_jointype, false, target_distribution);

        if (NULL != outer_path_t && NULL != inner_path_t) {
            /*
             * Do not parallel join when both sides are replicate table.
             */
            if (!parallel_enable(inner_path_t, outer_path_t)) {
                if (outer_path != outer_path_t || inner_path != inner_path_t) {
                    initial_cost_hashjoin(
                        root, workspace, jointype, hashclauses, outer_path_t, inner_path_t, sjinfo, semifactors, 1);
                }

                joinpath = (JoinPath*)create_hashjoin_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path_t,
                    inner_path_t,
                    restrictlist,
                    required_outer,
                    hashclauses);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            } else {
                joinpath_list = add_join_parallel_path(STREAM_NONE,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path_t,
                    outer_path_t,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
            }

            bool can_redistribute = true;
            foreach (lc, joinpath_list) {
                joinpath = (JoinPath*)lfirst(lc);
                can_redistribute = can_redistribute && add_replica_join_path(joinrel,
                                                           root,
                                                           save_jointype,
                                                           joinpath,
                                                           replicate_outer,
                                                           replicate_inner,
                                                           redistribute_inner,
                                                           redistribute_outer);
            }

            /* Can not create redistribute path anymore. */
            if (!can_redistribute)
                return;
        }
    }

    /*
     * Four scenarios
     */
    Path* stream_path_inner = NULL;
    Path* stream_path_outer = NULL;
    if (redistribute_inner && !redistribute_outer) {
        /*
         * Three paths, redistribute inner or broadcast outer or broadcast inner(if redistribute inner is unavailable)
         */
        double skew_stream = 0.0;
        joinpath_list = NIL;

        /* For redistribute, the distribute key should be in the targetlist of joinrel */
        stream_distribute_key =
            get_otherside_key(root, rrinfo_outer, innerrel->reltargetlist, inner_path->parent, &skew_stream);
        if (stream_distribute_key != NIL) {
            if (!parallel_enable(inner_path, outer_path)) {
                stream_path_inner = stream_side_path(root,
                    inner_path,
                    save_jointype,
                    replicate_inner,
                    STREAM_REDISTRIBUTE,
                    stream_distribute_key,
                    NIL,
                    true,
                    skew_stream,
                    target_distribution);
                initial_cost_hashjoin(
                    root, workspace, jointype, hashclauses, outer_path, stream_path_inner, sjinfo, semifactors, 1);

                joinpath = (JoinPath*)create_hashjoin_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    (Path*)stream_path_inner,
                    restrictlist,
                    required_outer,
                    hashclauses);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            } else {
                joinpath_list = add_join_parallel_path(STREAM_REDISTRIBUTE,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    skew_stream,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    stream_distribute_key,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
            }

            add_path_list(joinpath_list, jointype, joinrel, root, distribute_keys_outer, stream_distribute_key);
        }

        if (stream_distribute_key == NIL &&
            can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_inner,
                    false,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_BROADCAST,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
        }

        if (can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_outer,
                    true,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_NONE,
                    STREAM_BROADCAST,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
        }
    } else if (!redistribute_inner && redistribute_outer) {
        /*
         * Three paths, broadcast inner or redistribute outer or broadcast outer(if redistribute outer is unavailable)
         */
        {
            double skew_stream = 0.0;
            joinpath_list = NIL;

            stream_distribute_key =
                get_otherside_key(root, rrinfo_inner, outerrel->reltargetlist, outer_path->parent, &skew_stream);
            if (stream_distribute_key != NIL) {
                if (!parallel_enable(inner_path, outer_path)) {
                    stream_path_outer = stream_side_path(root,
                        outer_path,
                        save_jointype,
                        replicate_outer,
                        STREAM_REDISTRIBUTE,
                        stream_distribute_key,
                        NIL,
                        false,
                        skew_stream,
                        target_distribution);

                    initial_cost_hashjoin(
                        root, workspace, jointype, hashclauses, stream_path_outer, inner_path, sjinfo, semifactors, 1);

                    joinpath = (JoinPath*)create_hashjoin_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        (Path*)stream_path_outer,
                        inner_path,
                        restrictlist,
                        required_outer,
                        hashclauses);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                } else {
                    joinpath_list = add_join_parallel_path(STREAM_NONE,
                        STREAM_REDISTRIBUTE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        1.0,
                        skew_stream,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        NIL,
                        stream_distribute_key,
                        hashclauses,
                        restrictlist,
                        nodetag,
                        target_distribution);
                }

                add_path_list(joinpath_list, jointype, joinrel, root, stream_distribute_key, distribute_keys_inner);
            }
        }

        if (can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_inner,
                    false,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_BROADCAST,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
        }

        if (stream_distribute_key == NIL &&
            can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_outer,
                    true,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_NONE,
                    STREAM_BROADCAST,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
        }

    } else if (redistribute_inner && redistribute_outer) {
        int i = joinrel->rel_dis_keys.matching_keys != NIL ? -1 : 0; /* loop start */
        int key_num = list_length(joinrel->rel_dis_keys.superset_keys);
        List* old_distribute_keys = NIL;
        bool choose_optimal = false;

        /*
         * Three paths, broadcast inner or broadcast outer or redistribute inner and outer
         */
        /*
         * For redistribute path, we check all the matching key and superset keys
         * to be distribute keys if possible. We check with the following sequence:
         * (1) matching key; (2) superset key; (3) optimal key. We use variable i
         * to track all process, with (1) i = -1; (2) i = 0 to key_num -1;
         * (3) i = key_num. During whole process, we skip if distribute key is already
         * used before. Also, if (3) is found in (1) and (2), we just skip (3).
         */
        for (; i <= key_num; i++) {
            List* redistribute_keys_inner = NIL;
            List* redistribute_keys_outer = NIL;
            double skew_outer = 0.0;
            double skew_inner = 0.0;
            List* desired_keys = NIL;
            joinpath_list = NIL;

            if (i == -1)
                desired_keys = joinrel->rel_dis_keys.matching_keys;
            else if (i < key_num)
                desired_keys = (List*)list_nth(joinrel->rel_dis_keys.superset_keys, i);

            if (i == key_num && choose_optimal)
                continue;

            /* Determine which clause both sides redistribute on */
            get_distribute_keys(root,
                joinclauses,
                outer_path,
                inner_path,
                &skew_outer,
                &skew_inner,
                &redistribute_keys_outer,
                &redistribute_keys_inner,
                desired_keys,
                (i == -1));

            if (redistribute_keys_inner != NIL && redistribute_keys_outer != NIL) {
                if (skew_outer <= 1.0 && skew_inner <= 1.0)
                    choose_optimal = true;

                if (list_member(old_distribute_keys, redistribute_keys_outer))
                    continue;
                else
                    old_distribute_keys = lappend(old_distribute_keys, redistribute_keys_outer);

                if (!parallel_enable(inner_path, outer_path)) {
                    stream_path_inner = stream_side_path(root,
                        inner_path,
                        save_jointype,
                        replicate_inner,
                        STREAM_REDISTRIBUTE,
                        redistribute_keys_inner,
                        NIL,
                        true,
                        skew_inner,
                        target_distribution);

                    stream_path_outer = stream_side_path(root,
                        outer_path,
                        save_jointype,
                        replicate_outer,
                        STREAM_REDISTRIBUTE,
                        redistribute_keys_outer,
                        NIL,
                        false,
                        skew_outer,
                        target_distribution);

                    initial_cost_hashjoin(root,
                        workspace,
                        jointype,
                        hashclauses,
                        stream_path_outer,
                        stream_path_inner,
                        sjinfo,
                        semifactors,
                        1);

                    joinpath = (JoinPath*)create_hashjoin_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        (Path*)stream_path_outer,
                        (Path*)stream_path_inner,
                        restrictlist,
                        required_outer,
                        hashclauses);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                } else {
                    joinpath_list = add_join_parallel_path(STREAM_REDISTRIBUTE,
                        STREAM_REDISTRIBUTE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        skew_inner,
                        skew_outer,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        redistribute_keys_inner,
                        redistribute_keys_outer,
                        hashclauses,
                        restrictlist,
                        nodetag,
                        target_distribution);
                }
                add_path_list(joinpath_list,
                    jointype,
                    joinrel,
                    root,
                    redistribute_keys_outer,
                    redistribute_keys_inner,
                    desired_keys,
                    (i == -1));
            }
        }
        list_free_ext(old_distribute_keys);

        if (can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_inner,
                    false,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_BROADCAST,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
        }
        if (can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_hashjoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrictlist,
                    required_outer,
                    hashclauses,
                    replicate_outer,
                    true,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_NONE,
                    STREAM_BROADCAST,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
        }
    } else if (!redistribute_inner && !redistribute_outer) {
        /*
         * if redistribute on different join key, still need to redistribute either one.
         */
        if (rrinfo_inner != NULL && rrinfo_outer != NULL && !equal(rrinfo_inner, rrinfo_outer)) {
            /*
             * The distribute_keys_inner should be identical to innerpath->distribute_keys here.
             * The distribute_keys_outer should be identical to outerpath->distribute_keys here.
             */
            {
                double skew_stream = 0.0;
                joinpath_list = NIL;

                /* For redistribute, the distribute key should be in the targetlist of joinrel */
                stream_distribute_key =
                    get_otherside_key(root, rrinfo_outer, innerrel->reltargetlist, inner_path->parent, &skew_stream);
                if (stream_distribute_key != NIL) {
                    if (!parallel_enable(inner_path, outer_path)) {
                        stream_path_inner = stream_side_path(root,
                            inner_path,
                            save_jointype,
                            replicate_inner,
                            STREAM_REDISTRIBUTE,
                            stream_distribute_key,
                            NIL,
                            true,
                            skew_stream,
                            target_distribution);

                        initial_cost_hashjoin(root,
                            workspace,
                            jointype,
                            hashclauses,
                            outer_path,
                            stream_path_inner,
                            sjinfo,
                            semifactors,
                            1);

                        joinpath = (JoinPath*)create_hashjoin_path(root,
                            joinrel,
                            jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            outer_path,
                            (Path*)stream_path_inner,
                            restrictlist,
                            required_outer,
                            hashclauses);
                        joinpath_list = lappend(joinpath_list, (void*)joinpath);
                    } else {
                        joinpath_list = add_join_parallel_path(STREAM_REDISTRIBUTE,
                            STREAM_NONE,
                            root,
                            joinrel,
                            sjinfo,
                            semifactors,
                            inner_path,
                            outer_path,
                            skew_stream,
                            1.0,
                            jointype,
                            save_jointype,
                            required_outer,
                            workspace,
                            replicate_inner,
                            replicate_outer,
                            stream_distribute_key,
                            NIL,
                            hashclauses,
                            restrictlist,
                            nodetag,
                            target_distribution);
                    }

                    add_path_list(joinpath_list, jointype, joinrel, root, distribute_keys_outer, stream_distribute_key);
                }
            }

            {
                double skew_stream = 0.0;
                joinpath_list = NIL;

                /* For redistribute, the distribute key should be in the targetlist of joinrel */
                stream_distribute_key =
                    get_otherside_key(root, rrinfo_inner, outerrel->reltargetlist, outer_path->parent, &skew_stream);
                if (stream_distribute_key != NIL) {
                    if (!parallel_enable(inner_path, outer_path)) {
                        stream_path_outer = stream_side_path(root,
                            outer_path,
                            save_jointype,
                            replicate_outer,
                            STREAM_REDISTRIBUTE,
                            stream_distribute_key,
                            NIL,
                            false,
                            skew_stream,
                            target_distribution);

                        initial_cost_hashjoin(root,
                            workspace,
                            jointype,
                            hashclauses,
                            stream_path_outer,
                            inner_path,
                            sjinfo,
                            semifactors,
                            1);

                        joinpath = (JoinPath*)create_hashjoin_path(root,
                            joinrel,
                            jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            (Path*)stream_path_outer,
                            inner_path,
                            restrictlist,
                            required_outer,
                            hashclauses);
                        joinpath_list = lappend(joinpath_list, (void*)joinpath);
                    } else {
                        joinpath_list = add_join_parallel_path(STREAM_NONE,
                            STREAM_REDISTRIBUTE,
                            root,
                            joinrel,
                            sjinfo,
                            semifactors,
                            inner_path,
                            outer_path,
                            1.0,
                            skew_stream,
                            jointype,
                            save_jointype,
                            required_outer,
                            workspace,
                            replicate_inner,
                            replicate_outer,
                            NIL,
                            stream_distribute_key,
                            hashclauses,
                            restrictlist,
                            nodetag,
                            target_distribution);
                    }

                    add_path_list(joinpath_list, jointype, joinrel, root, stream_distribute_key, distribute_keys_inner);
                }
            }

            if (stream_distribute_key == NIL &&
                can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
                if (!parallel_enable(inner_path, outer_path))
                    add_hashjoin_broadcast_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        restrictlist,
                        required_outer,
                        hashclauses,
                        replicate_inner,
                        false,
                        target_distribution);
                else
                    add_join_parallel_path(STREAM_BROADCAST,
                        STREAM_NONE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        1.0,
                        1.0,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        NIL,
                        NIL,
                        hashclauses,
                        restrictlist,
                        nodetag,
                        target_distribution);
            }

            if (stream_distribute_key == NIL &&
                can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
                if (!parallel_enable(inner_path, outer_path))
                    add_hashjoin_broadcast_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        outer_path,
                        inner_path,
                        restrictlist,
                        required_outer,
                        hashclauses,
                        replicate_outer,
                        true,
                        target_distribution);
                else
                    add_join_parallel_path(STREAM_NONE,
                        STREAM_BROADCAST,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        1.0,
                        1.0,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        NIL,
                        NIL,
                        hashclauses,
                        restrictlist,
                        nodetag,
                        target_distribution);
            }
        } else {
            /* Join local */
            joinpath_list = NIL;
            if (!parallel_enable(inner_path, outer_path)) {
                joinpath = (JoinPath*)create_hashjoin_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrictlist,
                    required_outer,
                    hashclauses);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            } else {
                joinpath_list = add_join_parallel_path(STREAM_NONE,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    hashclauses,
                    restrictlist,
                    nodetag,
                    target_distribution);
            }
            add_path_list(joinpath_list, jointype, joinrel, root, distribute_keys_outer, distribute_keys_inner);
        }
    }

    list_free_ext(rrinfo_inner);
    list_free_ext(rrinfo_outer);
}

/*
 * @Description:
 *    Add nestloop join path with broadcast.
 *
 * @param[IN] need_smpDesc: the smp info in broadcast side.
 * @param[IN] non_smpDesc: the smp info in no-broadcast side.
 * @param[IN] dop: degree of join parallel.
 * Other input param please refer to comment of add_join_parallel_path().
 *
 * @return JoinPath*: the JoinPath created by this func.
 */
static void add_nestloop_broadcast_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors,
    Path* need_stream_path, Path* non_stream_path, List* restrict_clauses, List* pathkeys, Relids required_outer,
    List* stream_pathkeys, bool is_replicate, bool stream_outer, Distribution* target_distribution,
    ParallelDesc* need_smpDesc, ParallelDesc* non_smpDesc, int dop)
{
    Path* streamed_path = NULL;
    Path* other_side = NULL;
    JoinPath* joinpath = NULL;
    Path* new_outer_path = NULL;
    Path* new_inner_path = NULL;

    /* target_distribution would be NULL in SMP path, set it to default group of current mode */
    if (NULL == target_distribution) {
        target_distribution = ng_get_default_computing_group_distribution();
    }

    /* If parallel, add parallel info to the path. */
    streamed_path = stream_side_path(root,
        need_stream_path,
        save_jointype,
        is_replicate,
        STREAM_BROADCAST,
        NIL,
        stream_pathkeys,
        !stream_outer,
        1.0,
        target_distribution,
        need_smpDesc);

    /* non-broadcast side also needs shuffle if node group is un-matched */
    non_stream_path = ng_stream_non_broadcast_side_for_join(
        root, non_stream_path, save_jointype, NIL, is_replicate, stream_outer, target_distribution);
    if (NULL == non_stream_path) {
        /* non-broadcast side can not shuffle */
        return;
    }

    if (NULL != non_smpDesc && PARALLEL_NONE != non_smpDesc->distriType) {
        other_side = stream_side_path(root,
            non_stream_path,
            save_jointype,
            is_replicate,
            STREAM_REDISTRIBUTE,
            NIL,
            NIL,
            stream_outer,
            1.0,
            target_distribution,
            non_smpDesc);
    } else {
        other_side = non_stream_path;
    }

    new_outer_path = stream_outer ? streamed_path : other_side;
    new_inner_path = stream_outer ? other_side : streamed_path;

    initial_cost_nestloop(root, workspace, jointype, new_outer_path, new_inner_path, sjinfo, semifactors, dop);

    joinpath = (JoinPath*)create_nestloop_path(root,
        joinrel,
        jointype,
        workspace,
        sjinfo,
        semifactors,
        new_outer_path,
        new_inner_path,
        restrict_clauses,
        pathkeys,
        required_outer,
        dop);

    joinpath->path.distribute_keys = non_stream_path->distribute_keys;
    add_path(root, joinrel, (Path*)joinpath);
}

/*
 * @Description:
 *    Add nestloop join path in stream mode.
 *
 * The input param please refer to comment of add_join_parallel_path().
 *
 * @return JoinPath*: the JoinPath created by this func.
 */
void add_nestloop_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path,
    Path* inner_path, List* restrict_clauses, List* pathkeys, Relids required_outer, Distribution* target_distribution)
{
    /* Full-outer join doesn't support nestloop yet */
    AssertEreport(jointype != JOIN_FULL, MOD_OPT_JOIN, "Join type shouldn't be full join for nestloop");

    bool redistribute_inner = false;
    bool redistribute_outer = false;
    bool replicate_inner = false;
    bool replicate_outer = false;
    List* distribute_keys_inner = NIL;
    List* distribute_keys_outer = NIL;
    List* joinclauses = NIL;
    RelOptInfo* outerrel = NULL;
    RelOptInfo* innerrel = NULL;
    JoinPath* joinpath = NULL;
    List* rrinfo_inner = NIL;
    List* rrinfo_outer = NIL;
    List* stream_distribute_key = NIL;
    List* joinpath_list = NIL;
    ListCell* lc = NULL;
    NodeTag nodetag = T_NestLoop;

    outerrel = outer_path->parent;
    innerrel = inner_path->parent;
    joinclauses = restrict_clauses;

    distribute_keys_inner = inner_path->distribute_keys;
    distribute_keys_outer = outer_path->distribute_keys;

    if (is_replicated_path(outer_path))
        replicate_outer = true;

    if (is_replicated_path(inner_path))
        replicate_inner = true;

    if (joinclauses == NIL) {
        /* clauseless join, should make sure we are dealing with distributed table */
        redistribute_inner = true;
        redistribute_outer = true;
    } else {
        redistribute_inner = is_distribute_need_on_joinclauses(
            root, inner_path->distribute_keys, joinclauses, innerrel, outerrel, &rrinfo_inner);
        redistribute_outer = is_distribute_need_on_joinclauses(
            root, outer_path->distribute_keys, joinclauses, outerrel, innerrel, &rrinfo_outer);
    }

    /*
     * Check node group distribution
     *     If path's distribution is different from target_distribution (computing node group), shuffle is needed
     */
    redistribute_inner = redistribute_inner || ng_is_shuffle_needed(root, inner_path, target_distribution);
    redistribute_outer = redistribute_outer || ng_is_shuffle_needed(root, outer_path, target_distribution);

    /*
     * If either side is replicated, join locally.
     */
    if (replicate_outer || replicate_inner) {
        /*
         * Check if we need do further redistribution even with two replicate table
         *     and shuffle them to same computing node group.
         */
        Path* outer_path_t = outer_path;
        Path* inner_path_t = inner_path;
        ng_stream_side_paths_for_replicate(
            root, &outer_path_t, &inner_path_t, save_jointype, false, target_distribution);

        if (NULL != outer_path_t && NULL != inner_path_t) {
            if (!parallel_enable(inner_path_t, outer_path_t)) {
                if (outer_path != outer_path_t || inner_path != inner_path_t) {
                    initial_cost_nestloop(
                        root, workspace, jointype, outer_path_t, inner_path_t, sjinfo, semifactors, 1);
                }

                joinpath = (JoinPath*)create_nestloop_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path_t,
                    inner_path_t,
                    restrict_clauses,
                    pathkeys,
                    required_outer);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            } else {
                joinpath_list = add_join_parallel_path(STREAM_NONE,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path_t,
                    outer_path_t,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    NIL,
                    NIL);
            }

            bool can_redist = true;
            foreach (lc, joinpath_list) {
                joinpath = (JoinPath*)lfirst(lc);
                can_redist = can_redist && add_replica_join_path(joinrel,
                                               root,
                                               save_jointype,
                                               joinpath,
                                               replicate_outer,
                                               replicate_inner,
                                               redistribute_inner,
                                               redistribute_outer);
            }

            /* Redistribute join path is invalid. */
            list_free_ext(joinpath_list);
            if (!can_redist)
                return;
        }
    }

    /*
     * Four scenarios
     */
    Path* stream_path_inner = NULL;
    Path* stream_path_outer = NULL;
    List* inner_pathkeys = NIL;
    List* outer_pathkeys = NIL;

    if (pathkeys != NULL) {
        inner_pathkeys = inner_path->pathkeys;
        outer_pathkeys = outer_path->pathkeys;
    }

    if (redistribute_inner && !redistribute_outer) {
        /*
         * Three paths, redistribute inner or broadcast outer or broadcast inner(if redistribute inner is unavailable)
         */
        {
            double skew_stream = 0.0;
            joinpath_list = NIL;

            /* For redistribute, the distribute key should be in the targetlist of joinrel */
            stream_distribute_key =
                get_otherside_key(root, rrinfo_outer, innerrel->reltargetlist, inner_path->parent, &skew_stream);
            if (stream_distribute_key != NIL) {
                if (!parallel_enable(inner_path, outer_path)) {
                    stream_path_inner = stream_side_path(root,
                        inner_path,
                        save_jointype,
                        replicate_inner,
                        STREAM_REDISTRIBUTE,
                        stream_distribute_key,
                        inner_pathkeys,
                        true,
                        skew_stream,
                        target_distribution);

                    initial_cost_nestloop(
                        root, workspace, jointype, outer_path, stream_path_inner, sjinfo, semifactors, 1);

                    joinpath = (JoinPath*)create_nestloop_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        outer_path,
                        (Path*)stream_path_inner,
                        restrict_clauses,
                        pathkeys,
                        required_outer);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                } else {
                    joinpath_list = add_join_parallel_path(STREAM_REDISTRIBUTE,
                        STREAM_NONE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        skew_stream,
                        1.0,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        stream_distribute_key,
                        NIL,
                        pathkeys,
                        restrict_clauses,
                        nodetag,
                        target_distribution,
                        inner_pathkeys,
                        outer_pathkeys);
                }

                add_path_list(joinpath_list, jointype, joinrel, root, distribute_keys_outer, stream_distribute_key);
            }
        }

        if (stream_distribute_key == NIL &&
            can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    inner_pathkeys,
                    replicate_inner,
                    false,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_BROADCAST,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
        }

        if (can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    outer_pathkeys,
                    replicate_outer,
                    true,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_NONE,
                    STREAM_BROADCAST,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
        }
    } else if (!redistribute_inner && redistribute_outer) {
        /*
         * Three paths, broadcast inner or redistribute outer or broadcast outer(if redistribute outer is unavailable)
         */
        {
            double skew_stream = 0.0;
            joinpath_list = NIL;

            stream_distribute_key =
                get_otherside_key(root, rrinfo_inner, outerrel->reltargetlist, outer_path->parent, &skew_stream);

            if (stream_distribute_key != NIL) {
                if (!parallel_enable(inner_path, outer_path)) {
                    stream_path_outer = stream_side_path(root,
                        outer_path,
                        save_jointype,
                        replicate_outer,
                        STREAM_REDISTRIBUTE,
                        stream_distribute_key,
                        outer_pathkeys,
                        false,
                        skew_stream,
                        target_distribution);

                    initial_cost_nestloop(
                        root, workspace, jointype, stream_path_outer, inner_path, sjinfo, semifactors, 1);

                    joinpath = (JoinPath*)create_nestloop_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        (Path*)stream_path_outer,
                        inner_path,
                        restrict_clauses,
                        pathkeys,
                        required_outer);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                } else {
                    joinpath_list = add_join_parallel_path(STREAM_NONE,
                        STREAM_REDISTRIBUTE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        1.0,
                        skew_stream,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        NIL,
                        stream_distribute_key,
                        pathkeys,
                        restrict_clauses,
                        nodetag,
                        target_distribution,
                        inner_pathkeys,
                        outer_pathkeys);
                }

                add_path_list(joinpath_list, jointype, joinrel, root, stream_distribute_key, distribute_keys_inner);
            }
        }

        if (can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    inner_pathkeys,
                    replicate_inner,
                    false,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_BROADCAST,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
        }

        if (stream_distribute_key == NIL &&
            can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    outer_pathkeys,
                    replicate_outer,
                    true,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_NONE,
                    STREAM_BROADCAST,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
        }

    } else if (redistribute_inner && redistribute_outer) {
        int i = joinrel->rel_dis_keys.matching_keys != NIL ? -1 : 0; /* loop start */
        int key_num = list_length(joinrel->rel_dis_keys.superset_keys);
        List* old_distribute_keys = NIL;
        bool choose_optimal = false;

        /*
         * Three paths, broadcast inner or broadcast outer or redistribute inner and outer
         */
        /*
         * For redistribute path, we check all the matching key and superset keys
         * to be distribute keys if possible. We check with the following sequence:
         * (1) matching key; (2) superset key; (3) optimal key. We use variable i
         * to track all process, with (1) i = -1; (2) i = 0 to key_num -1;
         * (3) i = key_num. During whole process, we skip if distribute key is already
         * used before. Also, if (3) is found in (1) and (2), we just skip (3).
         */
        for (; i <= key_num; i++) {
            List* redistribute_keys_inner = NIL;
            List* redistribute_keys_outer = NIL;
            double skew_outer = 0.0;
            double skew_inner = 0.0;
            List* desired_keys = NIL;
            joinpath_list = NIL;

            if (i == -1)
                desired_keys = joinrel->rel_dis_keys.matching_keys;
            else if (i < key_num)
                desired_keys = (List*)list_nth(joinrel->rel_dis_keys.superset_keys, i);

            if (i == key_num && choose_optimal)
                continue;

            /* Determine which clause both sides redistribute on */
            get_distribute_keys(root,
                joinclauses,
                outer_path,
                inner_path,
                &skew_outer,
                &skew_inner,
                &redistribute_keys_outer,
                &redistribute_keys_inner,
                desired_keys,
                (i == -1));

            if (redistribute_keys_inner != NIL && redistribute_keys_outer != NIL) {
                if (skew_outer <= 1.0 && skew_inner <= 1.0)
                    choose_optimal = true;

                if (list_member(old_distribute_keys, redistribute_keys_outer))
                    continue;
                else
                    old_distribute_keys = lappend(old_distribute_keys, redistribute_keys_outer);

                if (!parallel_enable(inner_path, outer_path)) {
                    stream_path_inner = stream_side_path(root,
                        inner_path,
                        save_jointype,
                        replicate_inner,
                        STREAM_REDISTRIBUTE,
                        redistribute_keys_inner,
                        inner_pathkeys,
                        true,
                        skew_inner,
                        target_distribution);

                    stream_path_outer = stream_side_path(root,
                        outer_path,
                        save_jointype,
                        replicate_outer,
                        STREAM_REDISTRIBUTE,
                        redistribute_keys_outer,
                        outer_pathkeys,
                        false,
                        skew_outer,
                        target_distribution);

                    initial_cost_nestloop(
                        root, workspace, jointype, stream_path_outer, stream_path_inner, sjinfo, semifactors, 1);

                    joinpath = (JoinPath*)create_nestloop_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        (Path*)stream_path_outer,
                        (Path*)stream_path_inner,
                        restrict_clauses,
                        pathkeys,
                        required_outer);
                    joinpath_list = lappend(joinpath_list, (void*)joinpath);
                } else {
                    joinpath_list = add_join_parallel_path(STREAM_REDISTRIBUTE,
                        STREAM_REDISTRIBUTE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        skew_inner,
                        skew_outer,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        redistribute_keys_inner,
                        redistribute_keys_outer,
                        pathkeys,
                        restrict_clauses,
                        nodetag,
                        target_distribution,
                        inner_pathkeys,
                        outer_pathkeys);
                }

                add_path_list(joinpath_list,
                    jointype,
                    joinrel,
                    root,
                    redistribute_keys_outer,
                    redistribute_keys_inner,
                    desired_keys,
                    (i == -1));
            }
        }
        list_free_ext(old_distribute_keys);

        if (can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    inner_pathkeys,
                    replicate_inner,
                    false,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_BROADCAST,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
        }
        if (can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            if (!parallel_enable(inner_path, outer_path))
                add_nestloop_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    outer_pathkeys,
                    replicate_outer,
                    true,
                    target_distribution);
            else
                add_join_parallel_path(STREAM_NONE,
                    STREAM_BROADCAST,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
        }
    } else if (!redistribute_inner && !redistribute_outer) {
        /*
         * if redistribute on different join key, still need to redistribute either one.
         */
        if (rrinfo_inner != NULL && rrinfo_outer != NULL && !equal(rrinfo_inner, rrinfo_outer)) {
            /*
             * The distribute_keys_inner should be identical to innerpath->distribute_keys here.
             * The distribute_keys_outer should be identical to outerpath->distribute_keys here.
             */
            {
                double skew_stream = 0.0;
                joinpath_list = NIL;

                /* For redistribute, the distribute key should be in the targetlist of joinrel */
                stream_distribute_key =
                    get_otherside_key(root, rrinfo_outer, innerrel->reltargetlist, inner_path->parent, &skew_stream);
                if (stream_distribute_key != NIL) {
                    if (!parallel_enable(inner_path, outer_path)) {
                        stream_path_inner = stream_side_path(root,
                            inner_path,
                            save_jointype,
                            replicate_inner,
                            STREAM_REDISTRIBUTE,
                            stream_distribute_key,
                            inner_pathkeys,
                            true,
                            skew_stream,
                            target_distribution);

                        initial_cost_nestloop(
                            root, workspace, jointype, outer_path, stream_path_inner, sjinfo, semifactors, 1);

                        joinpath = (JoinPath*)create_nestloop_path(root,
                            joinrel,
                            jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            outer_path,
                            (Path*)stream_path_inner,
                            restrict_clauses,
                            pathkeys,
                            required_outer);
                        joinpath_list = lappend(joinpath_list, (void*)joinpath);
                    } else {
                        joinpath_list = add_join_parallel_path(STREAM_REDISTRIBUTE,
                            STREAM_NONE,
                            root,
                            joinrel,
                            sjinfo,
                            semifactors,
                            inner_path,
                            outer_path,
                            skew_stream,
                            1.0,
                            jointype,
                            save_jointype,
                            required_outer,
                            workspace,
                            replicate_inner,
                            replicate_outer,
                            stream_distribute_key,
                            NIL,
                            pathkeys,
                            restrict_clauses,
                            nodetag,
                            target_distribution,
                            inner_pathkeys,
                            outer_pathkeys);
                    }

                    add_path_list(joinpath_list, jointype, joinrel, root, distribute_keys_outer, stream_distribute_key);
                }
            }

            {
                double skew_stream = 0.0;
                joinpath_list = NIL;

                /* For redistribute, the distribute key should be in the targetlist of joinrel */
                stream_distribute_key =
                    get_otherside_key(root, rrinfo_inner, outerrel->reltargetlist, outer_path->parent, &skew_stream);
                if (stream_distribute_key != NIL) {
                    if (!parallel_enable(inner_path, outer_path)) {
                        stream_path_outer = stream_side_path(root,
                            outer_path,
                            save_jointype,
                            replicate_outer,
                            STREAM_REDISTRIBUTE,
                            stream_distribute_key,
                            outer_pathkeys,
                            false,
                            skew_stream,
                            target_distribution);

                        initial_cost_nestloop(
                            root, workspace, jointype, stream_path_outer, inner_path, sjinfo, semifactors, 1);

                        joinpath = (JoinPath*)create_nestloop_path(root,
                            joinrel,
                            jointype,
                            workspace,
                            sjinfo,
                            semifactors,
                            (Path*)stream_path_outer,
                            inner_path,
                            restrict_clauses,
                            pathkeys,
                            required_outer);
                        joinpath_list = lappend(joinpath_list, (void*)joinpath);
                    } else {
                        joinpath_list = add_join_parallel_path(STREAM_NONE,
                            STREAM_REDISTRIBUTE,
                            root,
                            joinrel,
                            sjinfo,
                            semifactors,
                            inner_path,
                            outer_path,
                            1.0,
                            skew_stream,
                            jointype,
                            save_jointype,
                            required_outer,
                            workspace,
                            replicate_inner,
                            replicate_outer,
                            NIL,
                            stream_distribute_key,
                            pathkeys,
                            restrict_clauses,
                            nodetag,
                            target_distribution,
                            inner_pathkeys,
                            outer_pathkeys);
                    }

                    add_path_list(joinpath_list, jointype, joinrel, root, stream_distribute_key, distribute_keys_inner);
                }
            }

            if (stream_distribute_key == NIL &&
                can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
                if (!parallel_enable(inner_path, outer_path))
                    add_nestloop_broadcast_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        restrict_clauses,
                        pathkeys,
                        required_outer,
                        inner_pathkeys,
                        replicate_inner,
                        false,
                        target_distribution);
                else
                    add_join_parallel_path(STREAM_BROADCAST,
                        STREAM_NONE,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        1.0,
                        1.0,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        NIL,
                        NIL,
                        pathkeys,
                        restrict_clauses,
                        nodetag,
                        target_distribution,
                        inner_pathkeys,
                        outer_pathkeys);
            }

            if (stream_distribute_key == NIL &&
                can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
                if (!parallel_enable(inner_path, outer_path))
                    add_nestloop_broadcast_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        workspace,
                        sjinfo,
                        semifactors,
                        outer_path,
                        inner_path,
                        restrict_clauses,
                        pathkeys,
                        required_outer,
                        outer_pathkeys,
                        replicate_outer,
                        true,
                        target_distribution);
                else
                    add_join_parallel_path(STREAM_NONE,
                        STREAM_BROADCAST,
                        root,
                        joinrel,
                        sjinfo,
                        semifactors,
                        inner_path,
                        outer_path,
                        1.0,
                        1.0,
                        jointype,
                        save_jointype,
                        required_outer,
                        workspace,
                        replicate_inner,
                        replicate_outer,
                        NIL,
                        NIL,
                        pathkeys,
                        restrict_clauses,
                        nodetag,
                        target_distribution,
                        inner_pathkeys,
                        outer_pathkeys);
            }
        } else {
            joinpath_list = NIL;
            if (!parallel_enable(inner_path, outer_path)) {
                joinpath = (JoinPath*)create_nestloop_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    semifactors,
                    outer_path,
                    inner_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer);
                joinpath_list = lappend(joinpath_list, (void*)joinpath);
            } else {
                joinpath_list = add_join_parallel_path(STREAM_NONE,
                    STREAM_NONE,
                    root,
                    joinrel,
                    sjinfo,
                    semifactors,
                    inner_path,
                    outer_path,
                    1.0,
                    1.0,
                    jointype,
                    save_jointype,
                    required_outer,
                    workspace,
                    replicate_inner,
                    replicate_outer,
                    NIL,
                    NIL,
                    pathkeys,
                    restrict_clauses,
                    nodetag,
                    target_distribution,
                    inner_pathkeys,
                    outer_pathkeys);
            }
            add_path_list(joinpath_list, jointype, joinrel, root, distribute_keys_outer, distribute_keys_inner);
        }
    }

    list_free_ext(rrinfo_inner);
    list_free_ext(rrinfo_outer);
}

static void add_mergejoin_broadcast_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinType save_jointype, JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, Path* need_stream_path,
    Path* non_stream_path, List* restrict_clauses, List* pathkeys, Relids required_outer, List* mergeclauses,
    List* outersortkeys, List* innersortkeys, List* stream_pathkeys, List* non_stream_pathkeys, bool is_replicate,
    bool stream_outer, Distribution* target_distribution)
{
    Path* streamed_path = NULL;
    JoinPath* joinpath = NULL;
    Path* new_outer_path = NULL;
    Path* new_inner_path = NULL;

    /* target_distribution would be NULL in SMP path, set it to default group of current mode */
    if (NULL == target_distribution) {
        target_distribution = ng_get_default_computing_group_distribution();
    }

    streamed_path = stream_side_path(root,
        need_stream_path,
        save_jointype,
        is_replicate,
        STREAM_BROADCAST,
        NIL,
        stream_pathkeys,
        !stream_outer,
        1.0,
        target_distribution);

    /* non-broadcast side also needs shuffle if node group is un-matched */
    non_stream_path = ng_stream_non_broadcast_side_for_join(
        root, non_stream_path, save_jointype, non_stream_pathkeys, is_replicate, stream_outer, target_distribution);
    if (NULL == non_stream_path) {
        /* non-broadcast side can not shuffle */
        return;
    }

    new_outer_path = stream_outer ? streamed_path : non_stream_path;
    new_inner_path = stream_outer ? non_stream_path : streamed_path;

    initial_cost_mergejoin(
        root, workspace, jointype, mergeclauses, new_outer_path, new_inner_path, outersortkeys, innersortkeys, sjinfo);

    joinpath = (JoinPath*)create_mergejoin_path(root,
        joinrel,
        jointype,
        workspace,
        sjinfo,
        new_outer_path,
        new_inner_path,
        restrict_clauses,
        pathkeys,
        required_outer,
        mergeclauses,
        outersortkeys,
        innersortkeys);

    joinpath->path.distribute_keys = non_stream_path->distribute_keys;
    add_path(root, joinrel, (Path*)joinpath);
}

void add_mergejoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinCostWorkspace* workspace, SpecialJoinInfo* sjinfo, Path* outer_path, Path* inner_path, List* restrict_clauses,
    List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys, List* innersortkeys,
    Distribution* target_distribution)
{
    bool redistribute_inner = false;
    bool redistribute_outer = false;
    bool replicate_inner = false;
    bool replicate_outer = false;
    List* distribute_keys_inner = NIL;
    List* distribute_keys_outer = NIL;
    List* joinclauses = NIL;
    RelOptInfo* outerrel = NULL;
    RelOptInfo* innerrel = NULL;
    JoinPath* joinpath = NULL;
    List* rrinfo_inner = NULL; 
    List* rrinfo_outer = NULL;
    List* stream_distribute_key = NIL;

    /* Only create unparallel path for mergejoin. */
    if (inner_path->dop > 1 || outer_path->dop > 1)
        return;

    outerrel = outer_path->parent;
    innerrel = inner_path->parent;
    joinclauses = restrict_clauses;

    distribute_keys_inner = inner_path->distribute_keys;
    distribute_keys_outer = outer_path->distribute_keys;

    if (is_replicated_path(outer_path))
        replicate_outer = true;

    if (is_replicated_path(inner_path))
        replicate_inner = true;

    if (!replicate_inner || !replicate_outer) {
        /* joinclauses of hashjoin should be Non-null. */
        AssertEreport(joinclauses != NIL, MOD_OPT_JOIN, "Joinclauses of mergejoin should be Non-null");

        redistribute_inner = is_distribute_need_on_joinclauses(
            root, inner_path->distribute_keys, joinclauses, innerrel, outerrel, &rrinfo_inner);
        redistribute_outer = is_distribute_need_on_joinclauses(
            root, outer_path->distribute_keys, joinclauses, outerrel, innerrel, &rrinfo_outer);
    }

    /*
     * Check node group distribution
     *     If path's distribution is different from target_distribution (computing node group), shuffle is needed
     */
    redistribute_inner = redistribute_inner || ng_is_shuffle_needed(root, inner_path, target_distribution);
    redistribute_outer = redistribute_outer || ng_is_shuffle_needed(root, outer_path, target_distribution);

    /*
     * If either side is replicated, join locally.
     */
    if (replicate_outer || replicate_inner) {
        /*
         * Check if we need do further redistribution even with two replicate table
         *     and shuffle them to same computing node group.
         */
        Path* outer_path_t = outer_path;
        Path* inner_path_t = inner_path;
        ng_stream_side_paths_for_replicate(
            root, &outer_path_t, &inner_path_t, save_jointype, true, target_distribution);

        if (NULL != outer_path_t && NULL != inner_path_t) {
            if (outer_path != outer_path_t || inner_path != inner_path_t) {
                initial_cost_mergejoin(root,
                    workspace,
                    jointype,
                    mergeclauses,
                    outer_path_t,
                    inner_path_t,
                    outersortkeys,
                    innersortkeys,
                    sjinfo);
            }

            joinpath = (JoinPath*)create_mergejoin_path(root,
                joinrel,
                jointype,
                workspace,
                sjinfo,
                outer_path_t,
                inner_path_t,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys);
            bool can_redistribute = true;
            can_redistribute = add_replica_join_path(joinrel,
                root,
                save_jointype,
                joinpath,
                replicate_outer,
                replicate_inner,
                redistribute_inner,
                redistribute_outer);
            if (!can_redistribute)
                return;
        }
    }

    List* inner_pathkeys = inner_path->pathkeys;
    List* outer_pathkeys = outer_path->pathkeys;

    /*
     * Four scenarios
     */
    Path* stream_path_inner = NULL;
    Path* stream_path_outer = NULL;
    if (redistribute_inner && !redistribute_outer) {
        /*
         * Three paths, redistribute inner or broadcast outer or broadcast inner(if redistribute inner is unavailable)
         */
        {
            double skew_stream = 0.0;

            /* For redistribute, the distribute key should be in the targetlist of joinrel */
            stream_distribute_key =
                get_otherside_key(root, rrinfo_outer, innerrel->reltargetlist, inner_path->parent, &skew_stream);
            if (stream_distribute_key != NIL) {
                stream_path_inner = stream_side_path(root,
                    inner_path,
                    save_jointype,
                    replicate_inner,
                    STREAM_REDISTRIBUTE,
                    stream_distribute_key,
                    inner_pathkeys,
                    true,
                    skew_stream,
                    target_distribution);

                initial_cost_mergejoin(root,
                    workspace,
                    jointype,
                    mergeclauses,
                    outer_path,
                    stream_path_inner,
                    outersortkeys,
                    innersortkeys,
                    sjinfo);

                joinpath = (JoinPath*)create_mergejoin_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    outer_path,
                    (Path*)stream_path_inner,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    mergeclauses,
                    outersortkeys,
                    innersortkeys);

                if (jointype != JOIN_FULL) {
                    joinpath->path.distribute_keys =
                        locate_distribute_key(jointype, distribute_keys_outer, stream_distribute_key);
                    if (joinpath->path.distribute_keys)
                        add_path(root, joinrel, (Path*)joinpath);
                } else
                    add_path(root, joinrel, (Path*)joinpath);
            }
        }

        if (stream_distribute_key == NIL &&
            can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            add_mergejoin_broadcast_path(root,
                joinrel,
                jointype,
                save_jointype,
                workspace,
                sjinfo,
                inner_path,
                outer_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                inner_pathkeys,
                outer_pathkeys,
                replicate_inner,
                false,
                target_distribution);
        }

        if (can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            add_mergejoin_broadcast_path(root,
                joinrel,
                jointype,
                save_jointype,
                workspace,
                sjinfo,
                outer_path,
                inner_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                outer_pathkeys,
                inner_pathkeys,
                replicate_outer,
                true,
                target_distribution);
        }
    } else if (!redistribute_inner && redistribute_outer) {
        /*
         * Three paths, broadcast inner or redistribute outer or broadcast outer(if redistribute outer is unavailable)
         */
        {
            double skew_stream = 0.0;

            stream_distribute_key =
                get_otherside_key(root, rrinfo_inner, outerrel->reltargetlist, outer_path->parent, &skew_stream);
            if (stream_distribute_key != NIL) {
                stream_path_outer = stream_side_path(root,
                    outer_path,
                    save_jointype,
                    replicate_outer,
                    STREAM_REDISTRIBUTE,
                    stream_distribute_key,
                    outer_pathkeys,
                    false,
                    skew_stream,
                    target_distribution);

                initial_cost_mergejoin(root,
                    workspace,
                    jointype,
                    mergeclauses,
                    stream_path_outer,
                    inner_path,
                    outersortkeys,
                    innersortkeys,
                    sjinfo);

                joinpath = (JoinPath*)create_mergejoin_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    (Path*)stream_path_outer,
                    inner_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    mergeclauses,
                    outersortkeys,
                    innersortkeys);

                if (jointype != JOIN_FULL) {
                    joinpath->path.distribute_keys =
                        locate_distribute_key(jointype, stream_distribute_key, distribute_keys_inner);
                    if (joinpath->path.distribute_keys)
                        add_path(root, joinrel, (Path*)joinpath);
                } else
                    add_path(root, joinrel, (Path*)joinpath);
            }
        }

        if (can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            add_mergejoin_broadcast_path(root,
                joinrel,
                jointype,
                save_jointype,
                workspace,
                sjinfo,
                inner_path,
                outer_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                inner_pathkeys,
                outer_pathkeys,
                replicate_inner,
                false,
                target_distribution);
        }

        if (stream_distribute_key == NIL &&
            can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            add_mergejoin_broadcast_path(root,
                joinrel,
                jointype,
                save_jointype,
                workspace,
                sjinfo,
                outer_path,
                inner_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                outer_pathkeys,
                inner_pathkeys,
                replicate_outer,
                true,
                target_distribution);
        }

    } else if (redistribute_inner && redistribute_outer) {
        int i = joinrel->rel_dis_keys.matching_keys != NIL ? -1 : 0; /* loop start */
        int key_num = list_length(joinrel->rel_dis_keys.superset_keys);
        List* old_distribute_keys = NIL;
        bool choose_optimal = false;

        /*
         * Three paths, broadcast inner or broadcast outer or redistribute inner and outer
         */
        /*
         * For redistribute path, we check all the matching key and superset keys
         * to be distribute keys if possible. We check with the following sequence:
         * (1) matching key; (2) superset key; (3) optimal key. We use variable i
         * to track all process, with (1) i = -1; (2) i = 0 to key_num -1;
         * (3) i = key_num. During whole process, we skip if distribute key is already
         * used before. Also, if (3) is found in (1) and (2), we just skip (3).
         */
        for (; i <= key_num; i++) {
            List *redistribute_keys_inner = NIL, *redistribute_keys_outer = NIL;
            double skew_outer = 0.0, skew_inner = 0.0;
            List* desired_keys = NIL;

            if (i == -1)
                desired_keys = joinrel->rel_dis_keys.matching_keys;
            else if (i < key_num)
                desired_keys = (List*)list_nth(joinrel->rel_dis_keys.superset_keys, i);

            if (i == key_num && choose_optimal)
                continue;

            /* Determine which clause both sides redistribute on	*/
            get_distribute_keys(root,
                joinclauses,
                outer_path,
                inner_path,
                &skew_outer,
                &skew_inner,
                &redistribute_keys_outer,
                &redistribute_keys_inner,
                desired_keys,
                (i == -1));

            if (redistribute_keys_inner != NIL && redistribute_keys_outer != NIL) {
                if (skew_outer <= 1.0 && skew_inner <= 1.0)
                    choose_optimal = true;

                if (list_member(old_distribute_keys, redistribute_keys_outer))
                    continue;
                else
                    old_distribute_keys = lappend(old_distribute_keys, redistribute_keys_outer);

                stream_path_inner = stream_side_path(root,
                    inner_path,
                    save_jointype,
                    replicate_inner,
                    STREAM_REDISTRIBUTE,
                    redistribute_keys_inner,
                    inner_pathkeys,
                    true,
                    skew_inner,
                    target_distribution);

                stream_path_outer = stream_side_path(root,
                    outer_path,
                    save_jointype,
                    replicate_outer,
                    STREAM_REDISTRIBUTE,
                    redistribute_keys_outer,
                    outer_pathkeys,
                    false,
                    skew_outer,
                    target_distribution);

                initial_cost_mergejoin(root,
                    workspace,
                    jointype,
                    mergeclauses,
                    stream_path_outer,
                    stream_path_inner,
                    outersortkeys,
                    innersortkeys,
                    sjinfo);

                joinpath = (JoinPath*)create_mergejoin_path(root,
                    joinrel,
                    jointype,
                    workspace,
                    sjinfo,
                    (Path*)stream_path_outer,
                    (Path*)stream_path_inner,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    mergeclauses,
                    outersortkeys,
                    innersortkeys);
                if (jointype != JOIN_FULL) {
                    joinpath->path.distribute_keys = locate_distribute_key(
                        jointype, redistribute_keys_outer, redistribute_keys_inner, desired_keys, (i == -1));
                    if (joinpath->path.distribute_keys)
                        add_path(root, joinrel, (Path*)joinpath);
                } else
                    add_path(root, joinrel, (Path*)joinpath);
            }
        }
        list_free_ext(old_distribute_keys);

        if (can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
            add_mergejoin_broadcast_path(root,
                joinrel,
                jointype,
                save_jointype,
                workspace,
                sjinfo,
                inner_path,
                outer_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                inner_pathkeys,
                outer_pathkeys,
                replicate_inner,
                false,
                target_distribution);
        }
        if (can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
            add_mergejoin_broadcast_path(root,
                joinrel,
                jointype,
                save_jointype,
                workspace,
                sjinfo,
                outer_path,
                inner_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                outer_pathkeys,
                inner_pathkeys,
                replicate_outer,
                true,
                target_distribution);
        }
    } else if (!redistribute_inner && !redistribute_outer) {
        /*
         * if redistribute on different join key, still need to redistribute either one.
         */
        if (rrinfo_inner != NULL && rrinfo_outer != NULL && !equal(rrinfo_inner, rrinfo_outer)) {
            /*
             * The distribute_keys_inner should be identical to innerpath->distribute_keys here.
             * The distribute_keys_outer should be identical to outerpath->distribute_keys here.
             */
            {
                double skew_stream = 0.0;

                /* For redistribute, the distribute key should be in the targetlist of joinrel */
                stream_distribute_key =
                    get_otherside_key(root, rrinfo_outer, innerrel->reltargetlist, inner_path->parent, &skew_stream);
                if (stream_distribute_key != NIL) {
                    stream_path_inner = stream_side_path(root,
                        inner_path,
                        save_jointype,
                        replicate_inner,
                        STREAM_REDISTRIBUTE,
                        stream_distribute_key,
                        inner_pathkeys,
                        true,
                        skew_stream,
                        target_distribution);

                    initial_cost_mergejoin(root,
                        workspace,
                        jointype,
                        mergeclauses,
                        outer_path,
                        stream_path_inner,
                        outersortkeys,
                        innersortkeys,
                        sjinfo);

                    joinpath = (JoinPath*)create_mergejoin_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        outer_path,
                        (Path*)stream_path_inner,
                        restrict_clauses,
                        pathkeys,
                        required_outer,
                        mergeclauses,
                        outersortkeys,
                        innersortkeys);

                    if (jointype != JOIN_FULL) {
                        joinpath->path.distribute_keys =
                            locate_distribute_key(jointype, distribute_keys_outer, stream_distribute_key);
                        if (joinpath->path.distribute_keys)
                            add_path(root, joinrel, (Path*)joinpath);
                    } else
                        add_path(root, joinrel, (Path*)joinpath);
                }
            }

            {
                double skew_stream = 0.0;

                /* For redistribute, the distribute key should be in the targetlist of joinrel */
                stream_distribute_key =
                    get_otherside_key(root, rrinfo_inner, outerrel->reltargetlist, outer_path->parent, &skew_stream);
                if (stream_distribute_key != NIL) {
                    stream_path_outer = stream_side_path(root,
                        outer_path,
                        save_jointype,
                        replicate_outer,
                        STREAM_REDISTRIBUTE,
                        stream_distribute_key,
                        outer_pathkeys,
                        false,
                        skew_stream,
                        target_distribution);

                    initial_cost_mergejoin(root,
                        workspace,
                        jointype,
                        mergeclauses,
                        stream_path_outer,
                        inner_path,
                        outersortkeys,
                        innersortkeys,
                        sjinfo);

                    joinpath = (JoinPath*)create_mergejoin_path(root,
                        joinrel,
                        jointype,
                        workspace,
                        sjinfo,
                        (Path*)stream_path_outer,
                        inner_path,
                        restrict_clauses,
                        pathkeys,
                        required_outer,
                        mergeclauses,
                        outersortkeys,
                        innersortkeys);
                    ;

                    if (jointype != JOIN_FULL) {
                        joinpath->path.distribute_keys =
                            locate_distribute_key(jointype, stream_distribute_key, distribute_keys_inner);
                        if (joinpath->path.distribute_keys)
                            add_path(root, joinrel, (Path*)joinpath);
                    } else
                        add_path(root, joinrel, (Path*)joinpath);
                }
            }

            if (stream_distribute_key == NIL &&
                can_broadcast_inner(jointype, save_jointype, replicate_outer, distribute_keys_outer, outer_path)) {
                add_mergejoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    inner_path,
                    outer_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    mergeclauses,
                    outersortkeys,
                    innersortkeys,
                    inner_pathkeys,
                    outer_pathkeys,
                    replicate_inner,
                    false,
                    target_distribution);
            }

            if (stream_distribute_key == NIL &&
                can_broadcast_outer(jointype, save_jointype, replicate_inner, distribute_keys_inner, inner_path)) {
                add_mergejoin_broadcast_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    workspace,
                    sjinfo,
                    outer_path,
                    inner_path,
                    restrict_clauses,
                    pathkeys,
                    required_outer,
                    mergeclauses,
                    outersortkeys,
                    innersortkeys,
                    outer_pathkeys,
                    inner_pathkeys,
                    replicate_outer,
                    true,
                    target_distribution);
            }
        } else {
            joinpath = (JoinPath*)create_mergejoin_path(root,
                joinrel,
                jointype,
                workspace,
                sjinfo,
                outer_path,
                inner_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys);
            if (jointype != JOIN_FULL) {
                joinpath->path.distribute_keys =
                    locate_distribute_key(jointype, distribute_keys_outer, distribute_keys_inner);
                add_path(root, joinrel, (Path*)joinpath);
            } else
                add_path(root, joinrel, (Path*)joinpath);
        }
    }

    list_free_ext(rrinfo_inner);
    list_free_ext(rrinfo_outer);
}

/* needs_agg_stream
 *	judge if redistribution is needed for specific distribute key
 *
 * Parameters:
 *	@in root: Planner info structure of current query level
 *	@in tlist: targetlist with group by expr in it, others are agg exprs
 *	@in distribute_targetlist: distribute key of current plan
 *
 * Returns: true if we need redistribution, else false
 */
bool needs_agg_stream(PlannerInfo* root, List* tlist, List* distribute_targetlist, Distribution* distribution)
{
    ListCell* lc_agg = NULL;
    ListCell* lc_key = NULL;

    if (distribute_targetlist == NULL) {
        return true;
    }

    if (distribution != NULL && ng_is_single_node_group_distribution(distribution)) {
        return false;
    }

    /* Check the distribute key first */
    foreach (lc_key, distribute_targetlist) {
        Node* v = (Node*)lfirst(lc_key);

        foreach (lc_agg, tlist) {
            Node* te = (Node*)lfirst(lc_agg);
            Node* expr = NULL;

            if (IsA(te, TargetEntry))
                expr = (Node*)((TargetEntry*)te)->expr;
            else
                expr = te;

            if (judge_node_compatible(root, v, expr))
                break;
        }

        /* doesn't find any equal expr for current distribute key expr, so need redistribute */
        if (NULL == lc_agg) {
            return true;
        }
    }

    /* find equal expr for every distribute key expr */
    return false;
}

/*
 * equal_distributekey:
 *	Judge if two distribute keys are semantically equal
 * Parameters:
 *	@in root: planner info of current query level
 *	@distribute_key1: compared distribute key 1
 *	@distribute_key2: compared distribute key 2
 * Return:
 *	true if two distribute keys are semantically equal, else false
 */
bool equal_distributekey(PlannerInfo* root, List* distribute_key1, List* distribute_key2)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    if (list_length(distribute_key1) != list_length(distribute_key2))
        return false;

    forboth(lc1, distribute_key1, lc2, distribute_key2)
    {
        Node* key1 = (Node*)lfirst(lc1);
        Node* key2 = (Node*)lfirst(lc2);

        /* check if key1 and key2 from same eq members */
        if (!judge_node_compatible(root, key1, key2))
            return false;
    }

    return true;
}

/*
 * judge_node_compatible
 *	Judge if two nodes are from the same equivalence class and
 *	hash type compatible
 * Parameters:
 *	@in root: planner info of current query level
 *	@in n1: compared node 1
 *	@in n2: compared node 2
 * Return:
 *	true if two nodes are from same equivalence class, else false
 */
bool judge_node_compatible(PlannerInfo* root, Node* n1, Node* n2)
{
    ListCell* lc = NULL;

    if (equal(n1, n2))
        return true;
    if (!is_compatible_type(exprType(n1), exprType(n2)))
        return false;
    if (root == NULL)
        return false;

    foreach (lc, root->eq_classes) {
        EquivalenceClass* ec = (EquivalenceClass*)lfirst(lc);
        bool found1 = find_ec_memeber_for_var(ec, n1);
        bool found2 = find_ec_memeber_for_var(ec, n2);
        if (found1 && found2)
            break;
        else if (found1 || found2)
            return false;
    }
    if (lc == NULL)
        return false;

    return true;
}

#endif
