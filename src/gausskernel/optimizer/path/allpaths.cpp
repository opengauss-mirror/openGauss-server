/* -------------------------------------------------------------------------
 *
 * allpaths.cpp
 *	  Routines to find possible search paths for processing a query
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/allpaths.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "catalog/pg_namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "foreign/fdwapi.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/dataskew.h"
#include "optimizer/geqo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/pruning.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/streamplan.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/tlist.h"
#include "optimizer/bucketpruning.h"
#include "parser/parse_relation.h"
#include "parser/parse_clause.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "storage/buf/bufmgr.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "pgxc/pgxc.h"

/* Hook for plugins to get control in set_rel_pathlist() */
THR_LOCAL set_rel_pathlist_hook_type set_rel_pathlist_hook = NULL;

/* Hook for plugins to get control in add_paths_to_joinrel() */
THR_LOCAL set_join_pathlist_hook_type set_join_pathlist_hook = NULL;

const int min_parallel_table_scan_size = 1073741824 / BLCKSZ;    /* 1GB */
const int max_parallel_maintenance_workers = 32;

static bool check_func_walker(Node* node, bool* found);
static bool check_func(Node* node);
static void set_base_rel_sizes(PlannerInfo* root);
static void set_base_rel_pathlists(PlannerInfo* root);
static void set_correlated_rel_pathlist(PlannerInfo* root, RelOptInfo* rel);
static void set_rel_pathlist(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte);
static void set_plain_rel_size(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_tablesample_rel_size(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_plain_rel_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_foreign_size(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_foreign_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_append_rel_size(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte);
static void set_append_rel_pathlist(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte);
static void generate_mergeappend_paths(
    PlannerInfo* root, RelOptInfo* rel, List* live_childrels, List* all_child_pathkeys);
static Path * get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer);
static Path * get_cheapest_parameterized_child_path_for_upper(PlannerInfo *root, RelOptInfo *rel,
                                      Bitmapset* required_upper);
static List* accumulate_append_subpath(List* subpaths, Path* path);
static void set_dummy_rel_pathlist(RelOptInfo* rel);
static void set_subquery_pathlist(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte);
static void set_subquery_path(PlannerInfo *root, RelOptInfo *rel,
                              Index rti, Query *subquery, int options);
static void set_function_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_values_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_cte_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void set_result_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static void set_worktable_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static RelOptInfo* make_rel_from_joinlist(PlannerInfo* root, List* joinlist);
static bool subquery_is_pushdown_safe(Query* subquery, Query* topquery, bool* unsafeColumns);
static bool recurse_pushdown_safe(Node* setOp, Query* topquery, bool* unsafeColumns);
static void check_output_expressions(Query* subquery, bool* unsafeColumns);
static void compare_tlist_datatypes(List* tlist, List* colTypes, bool* unsafeColumns);
static bool qual_is_pushdown_safe(PlannerInfo* info, Query* subquery, Index rti, Node* qual, const bool* unsafeColumns, bool predpush = false);
static void subquery_push_qual(Query* subquery, RangeTblEntry* rte, Index rti, Node* qual, int levelsup = 0);
static void recurse_push_qual(Node* setOp, Query* topquery, RangeTblEntry* rte, Index rti, Node* qual, int levelsup);

static void try_add_partiterator(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static void add_upperop_for_vecscan_expr(PlannerInfo* root, RelOptInfo* rel, List* quals);
static void passdown_itst_keys_to_rel(
    PlannerInfo* root, RelOptInfo* brel, RangeTblEntry* rte, bool inherit_matching_key);
static void updateRelOptInfoMinSecurity(RelOptInfo* rel);

static void find_index_path(RelOptInfo* rel);
static void bitmap_path_walker(Path* path);

static bool check_func_walker(Node* node, bool* found)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, FuncExpr)) {
        *found = true;
        return true;
    }

    return expression_tree_walker(node, (bool (*)())check_func_walker, found);
}

/*
 * @Description: Check this node if vartype > FirstNormalObjectId.
 * @in qual: Checked node.
 */
static bool check_func(Node* node)
{
    bool found = false;

    (void)check_func_walker(node, &found);

    return found;
}

/*
 * qual_pushdown_in_partialpush:
 *      check when qual can push when in partialpush scenario
 * Paramters:
 *  @in query: the father query in partialpush scenario
 *  @in subquery: the subquery in partialpush scenario
 *  @in qualclause: the qual clause in father query need be checked
 */
static bool qual_pushdown_in_partialpush(Query *query, Query *subquery, Node *qualclause)
{
    if (!query->can_push && subquery->can_push) {
        shipping_context context;
        stream_walker_context_init(&context);

        (void) stream_walker((Node*)qualclause, (void*)(&context));
        return context.current_shippable;
    }
    return true;
}

/*
 * updateRelOptInfoMinSecurity:
 *		update baserestrict_min_security for RelOptInfo rel
 * Paramters:
 *	@in rel: Per-relation information for planning/optimization
 *	@out void
 */
static inline void updateRelOptInfoMinSecurity(RelOptInfo* rel)
{
    Assert(rel != NULL);
    rel->baserestrict_min_security = UINT_MAX;
    ListCell* lc = NULL;
    RestrictInfo* info = NULL;
    foreach (lc, rel->baserestrictinfo) {
        info = (RestrictInfo*)lfirst(lc);
        if (info->security_level < rel->baserestrict_min_security) {
            rel->baserestrict_min_security = info->security_level;
        }
    }
}

/*
 * make_one_rel
 *	  Finds all possible access paths for executing a query, returning a
 *	  single rel that represents the join of all base rels in the query.
 */
RelOptInfo* make_one_rel(PlannerInfo* root, List* joinlist)
{
    RelOptInfo* rel = NULL;
    int rti;
    /* check if the parse only has one rel */
    Relids rels = pull_varnos((Node*)root->dis_keys.matching_keys);
    Index rtindex = 0;
    int num_subq = 0;
    bool has_nonjoin_op = root->parse->groupClause || root->parse->sortClause || root->parse->distinctClause;

    if (bms_membership(rels) == BMS_SINGLETON)
        rtindex = bms_first_member(rels);
    bms_free_ext(rels);

    /*
     * Construct the all_baserels Relids set.
     */
    root->all_baserels = NULL;
    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo* brel = root->simple_rel_array[rti];

        /* there may be empty slots corresponding to non-baserel RTEs */
        if (brel == NULL)
            continue;

        AssertEreport(brel->relid == (uint)rti, MOD_OPT, ""); /* sanity check on array */

        if (IS_STREAM_PLAN)
            passdown_itst_keys_to_rel(root, brel, root->simple_rte_array[rti], ((Index)rti == rtindex));

        /* ignore RTEs that are "other rels" */
        if (brel->reloptkind != RELOPT_BASEREL)
            continue;

        root->all_baserels = bms_add_member(root->all_baserels, brel->relid);

        /* Record how many subqueries in current query level */
        if (root->glob->minopmem > 0 && root->simple_rte_array[rti]->rtekind == RTE_SUBQUERY)
            num_subq++;
#ifdef ENABLE_MULTIPLE_NODES
        bool is_dn_gather = u_sess->attr.attr_sql.enable_dngather && ng_get_different_nodegroup_count() == 2;
        /* Add subplan clause to rel's subplan restriction list */
        if (IS_STREAM_PLAN && root->is_correlated &&
            (GetLocatorType(root->simple_rte_array[rti]->relid) != LOCATOR_TYPE_REPLICATED ||
                (ng_is_multiple_nodegroup_scenario() && !is_dn_gather))) {
            ListCell* lc = NULL;
            List *restrictinfo = brel->baserestrictinfo;
            foreach (lc, brel->baserestrictinfo) {
                RestrictInfo* info = (RestrictInfo*)lfirst(lc);
                /* if clause has subplan expr, keep it in subplan restrictinfo list */
                if (check_param_clause((Node*)info->clause)) {
                    brel->subplanrestrictinfo = lappend(brel->subplanrestrictinfo, info);
                }
            }
            /* remove subplan restrictinfo from orig list */
            if (brel->subplanrestrictinfo != NIL) {
                brel->baserestrictinfo = list_difference(brel->baserestrictinfo,
                                                    brel->subplanrestrictinfo);
                updateRelOptInfoMinSecurity(brel);
                list_free_ext(restrictinfo);
            }
        }
#endif
    }

    /*
     * Generate access paths for the base rels.
     */
    int work_mem_orig = u_sess->opt_cxt.op_work_mem;
    int esti_op_mem_orig = root->glob->estiopmem;

    /* For subquery scan plan, we should adjust available work_mem */
    if (root->glob->minopmem > 0) {
        if (has_nonjoin_op && num_subq == 1) {
            num_subq = 2;
        }
        root->glob->estiopmem = root->glob->estiopmem / (num_subq > 1 ? num_subq : 1);
        root->glob->estiopmem = Max(root->glob->estiopmem, root->glob->minopmem);
        u_sess->opt_cxt.op_work_mem = Min(root->glob->estiopmem, OPT_MAX_OP_MEM);
        Assert(u_sess->opt_cxt.op_work_mem > 0);
    }
    set_base_rel_sizes(root);

    /* For correlated subquery scan plan, we should adjust available work_mem */
    u_sess->opt_cxt.op_work_mem = work_mem_orig;
    root->glob->estiopmem = esti_op_mem_orig;
    if (root->is_correlated && root->glob->minopmem > 0) {
        int num_rel = bms_num_members(root->all_baserels);
        if (num_rel <= 1) {
            if (has_nonjoin_op) {
                num_rel = 2;
            } else {
                num_rel = 1;
            }
        }
        root->glob->estiopmem =
            (int)Max((double)root->glob->minopmem, (double)root->glob->estiopmem / ceil(LOG2(num_rel + 1)));
        u_sess->opt_cxt.op_work_mem = Min(root->glob->estiopmem, OPT_MAX_OP_MEM);
        Assert(u_sess->opt_cxt.op_work_mem > 0);
    }
    set_base_rel_pathlists(root);

    u_sess->opt_cxt.op_work_mem = work_mem_orig;
    root->glob->estiopmem = esti_op_mem_orig;

    /*
     * Generate access paths for the entire join tree.
     */
    rel = make_rel_from_joinlist(root, joinlist);

    /*
     * The result should join all and only the query's base rels.
     */
    AssertEreport(bms_equal(rel->relids, root->all_baserels), MOD_OPT, "");

    return rel;
}

/*
 * set_base_rel_sizes
 *	  Set the size estimates (rows and widths) for each base-relation entry.
 *
 * We do this in a separate pass over the base rels so that rowcount
 * estimates are available for parameterized path generation.
 */
static void set_base_rel_sizes(PlannerInfo* root)
{
    int rti;

    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo* rel = root->simple_rel_array[rti];

        /* there may be empty slots corresponding to non-baserel RTEs */
        if (rel == NULL)
            continue;

        AssertEreport(rel->relid == (uint)rti, MOD_OPT, ""); /* sanity check on array */

        /* ignore RTEs that are "other rels" */
        if (rel->reloptkind != RELOPT_BASEREL)
            continue;

        set_rel_size(root, rel, rti, root->simple_rte_array[rti]);

        /* Try inlist2join optimization */
        inlist2join_qrw_optimization(root, rti);
    }
}

/*
 * set_correlated_rel_pathlist
 *	  If current rel is correated with a recursive CTE, we need add broadcast operator
 *    to move all recursive relevant rels to one datanode, and recursive execution is
 *    processed there
 */
static void set_correlated_rel_pathlist(PlannerInfo* root, RelOptInfo* rel)
{
    /* do nothing in case of none-stream plan for RecursiveUnion */
    if (!STREAM_RECURSIVECTE_SUPPORTED) {
        return;
    }

    Path* pathnode = NULL;
    StreamPath* stream_path = NULL;
    Distribution* target_distribution = NULL;
    ListCell* lc = NULL;
    ListCell* lc1 = NULL;

    /* Loop each available pathnode to add stream(BroadCast) on top of it */
    foreach (lc, rel->pathlist) {
        pathnode = (Path*)lfirst(lc);

        Assert(pathnode->dop <= 1);

        /* no need to add stream(BroadCast) for replication table */
        if (IsLocatorReplicated(pathnode->locator_type) || pathnode->param_info != NULL)
            continue;

        target_distribution = ng_get_correlated_subplan_group_distribution();
        stream_path =
            (StreamPath*)create_stream_path(root, rel, STREAM_BROADCAST, NIL, NIL, pathnode, 1.0, target_distribution);

        /*
         * Change the value of path in the pathlist file to avoid inconsistency
         * between the values of chepeast_startup_path and cheepest_total_path.
         */
        lfirst(lc) = stream_path;

        /* Reset cheapeast total path */
        foreach (lc1, rel->cheapest_total_path) {
            if (pathnode == lfirst(lc1)) {
                lfirst(lc1) = stream_path;
                break;
            }
        }

        /* Reset cheapeast startup path for current rel */
        if (pathnode == rel->cheapest_startup_path) {
            rel->cheapest_startup_path = (Path*)stream_path;
        }
    }

    rel->cheapest_unique_path = NULL;
    rel->cheapest_parameterized_paths = NIL;

    return;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * Check we could reduce broadcast above scan of predpush subquery or not.
 */
static bool reduce_predpush_broadcast(PlannerInfo* root, Path *path)
{
    bool reduce = false;
    ItstDisKey dis_keys = root->dis_keys;

    if (list_length(path->distribute_keys) != 1) {
        return false;
    }

    if (!IsA((Node*)linitial(path->distribute_keys), Var)) {
        return false;
    }

    Var *dist_key = (Var *)linitial(path->distribute_keys);

    if (dis_keys.superset_keys != NULL && list_length(dis_keys.superset_keys) == 1) {
        List *matching = (List *)linitial(dis_keys.superset_keys);

        if (list_length(matching) == 1) {
            Var *var = (Var *)linitial(matching);
            if (equal(var, dist_key)) {
                reduce = true;
            }
        }
    }

    return reduce;
}

static Path *make_predpush_subpath(PlannerInfo* root, RelOptInfo* rel, Path *path)
{
    List* quals = NULL;
    Path *subpath = NULL;
    ListCell* lc = NULL;
    Bitmapset* upper_params = NULL;

    foreach (lc, rel->subplanrestrictinfo) {
        RestrictInfo *res_info = (RestrictInfo*)lfirst(lc);
        quals = lappend(quals, res_info->clause);
    }

    /* get the upper param IDs */
    if (SUBQUERY_PREDPUSH(root)) {
        upper_params = collect_param_clause((Node*)quals);
    }

    subpath = (Path*)create_result_path(root, rel, quals, path, upper_params);

    return subpath;
}

#endif

/*
 * set_base_rel_pathlists
 *	  Finds all paths available for scanning each base-relation entry.
 *	  Sequential scan and any available indices are considered.
 *	  Each useful path is attached to its relation's 'pathlist' field.
 */
static void set_base_rel_pathlists(PlannerInfo* root)
{
    int rti;

    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo* rel = root->simple_rel_array[rti];
        RangeTblEntry* rte = root->simple_rte_array[rti];

        /* there may be empty slots corresponding to non-baserel RTEs */
        if (rel == NULL)
            continue;

        AssertEreport(rel->relid == (uint)rti, MOD_OPT, ""); /* sanity check on array */

        /* ignore RTEs that are "other rels" */
        if (rel->reloptkind != RELOPT_BASEREL)
            continue;

        set_rel_pathlist(root, rel, rti, root->simple_rte_array[rti]);

        /*
         * Set outer rel replicated in case of Recursive CTE is corerlated with an
         * outer rel.
         */
        if (STREAM_RECURSIVECTE_SUPPORTED && rte->correlated_with_recursive_cte && root->is_under_recursive_cte) {
            root->has_recursive_correlated_rte = true;
            set_correlated_rel_pathlist(root, rel);
        }
#ifdef ENABLE_MULTIPLE_NODES
        /* For subplan, we should make the base table replicated, and add filter to the temporary result */
        if (IS_STREAM_PLAN && root->is_correlated &&
            (GetLocatorType(rte->relid) != LOCATOR_TYPE_REPLICATED || ng_is_multiple_nodegroup_scenario())) {
            ListCell* lc = NULL;
            ListCell* lc2 = NULL;
            bool is_cheapest_startup = false;
            Path* subpath = NULL;
            List* incorrect_param_paths = NULL;

            foreach (lc, rel->pathlist) {
                Path* path = (Path*)lfirst(lc);
                if (path->param_info != NULL) {
                    /*
                     * If there are param-clauses on the parameterized path, it
                     * need to add the broadcast stream on it, we can not process
                     * this case so far.
                     */
                    if (SUBQUERY_PREDPUSH(root) &&
                        !PATH_REQ_UPPER(path) &&
                        rel->subplanrestrictinfo != NULL)
                    {
                        /* Stream operator maybe add on it */
                        incorrect_param_paths = lappend(incorrect_param_paths, path);
                    }

                    continue;
                }

                /* Save the path if it is cheapest startup path. */
                if (path == rel->cheapest_startup_path)
                    is_cheapest_startup = true;

                /* Save the path if it is cheapest total path. */
                foreach (lc2, rel->cheapest_total_path) {
                    if (path == lfirst(lc2))
                        break;
                }

                /* Here we want to reduce broadcast in predpush */
                if (SUBQUERY_PREDPUSH(root) &&
                    rel->subplanrestrictinfo != NIL &&
                    reduce_predpush_broadcast(root, path)) {
                    subpath = make_predpush_subpath(root, rel, path);

                    /* Do not free old path, maybe it's used in other places. */
                    lfirst(lc) = subpath;
                    if (lc2 != NULL) {
                        lfirst(lc2) = subpath;
                    }

                    /* Set cheapest startup path as result + predpush-index path */
                    if (is_cheapest_startup) {
                        rel->cheapest_startup_path = subpath;
                    }

                    continue;
                }

                Distribution* distribution = ng_get_dest_distribution(path);
                Distribution* target_distribution = ng_get_correlated_subplan_group_distribution();

                /*
                 * add broadcast node for subplan since it can be execute on each dn node
                 *     we need to broadcast data to a "correlated subplan group",
                 *     where we could definitely get a replicated data from subplan's user
                 * we need to add broadcast in two cases:
                 *     (1) not a replicated path
                 *     (2) path not located in the "correlated subplan group"
                 *     (3) it the path is not parameterized by upper parameters
                 * Notice : When the rel->subplan is single baseresult plan node or execute on cn,
                 * we don't need do this.
                 */
                 if ((!IsLocatorReplicated(path->locator_type) ||
                       !ng_is_same_group(distribution, target_distribution))  &&
                     (!rel->subplan || 
                       !(is_single_baseresult_plan(rel->subplan) || 
                       is_execute_on_coordinator(rel->subplan))) &&
                     !PATH_REQ_UPPER(path)) {
                    Cost rescan_startup_cost;
                    Cost rescan_total_cost;

                    if (check_param_expr((Node*)(rel->reltargetlist))) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unsupported param cross stream")));
                    }

                    subpath = create_stream_path(root, rel, STREAM_BROADCAST, NIL, NIL, path, 1.0, target_distribution);
                    subpath = (Path*)create_material_path(subpath, true);
                    /* Record materialize mem info */
                    cost_rescan(
                        root, subpath, &rescan_startup_cost, &rescan_total_cost, &((MaterialPath*)subpath)->mem_info);
                    ((MaterialPath*)subpath)->mem_info.regressCost *= DEFAULT_NUM_ROWS;
                } else {
                    ContainStreamContext context;
                    bool need_material = false;

                    /*
                     * try to add material path if the path contains stream to
                     * avoid rescan stream operator. Cstore indexscan with delta
                     * doest not support rescan either.
                     */
                    context.outer_relids = NULL;
                    context.upper_params = NULL;
                    context.only_check_stream = true;
                    context.under_materialize_all = false;
                    context.has_stream = false;
                    context.has_parameterized_path = false;
                    context.has_cstore_index_delta = false;

                    stream_path_walker(path, &context);
                    need_material = context.has_stream || context.has_cstore_index_delta;

                    if (false == need_material && RTE_CTE == rte->rtekind /* just for rte */
                        && false == rte->self_reference && false == rte->correlated_with_recursive_cte &&
                        check_param_clause((Node*)rel->subplanrestrictinfo)) {
                        /*
                         * try to add material path to avoid rescan CTE node if
                         * 1. RTE contains correlated restrictinfo
                         * 2. subquery under CTE is not correlated
                         */
                        need_material = true;
                    }

                    subpath = path;
                    if (need_material) {
                        Cost rescan_startup_cost;
                        Cost rescan_total_cost;

                        subpath = (Path*)create_material_path(path, true);
                        /* Record materialize mem info */
                        cost_rescan(root,
                            subpath,
                            &rescan_startup_cost,
                            &rescan_total_cost,
                            &((MaterialPath*)subpath)->mem_info);
                        ((MaterialPath*)subpath)->mem_info.regressCost *= DEFAULT_NUM_ROWS;
                    }
                }

                /* if there's subplan filter, add it above broadcast node */
                if (rel->subplanrestrictinfo != NIL) {
                    List* quals = NULL;
                    ListCell* lc_res = NULL;
                    Bitmapset* upper_params = NULL;
                    foreach (lc_res, rel->subplanrestrictinfo) {
                        RestrictInfo *res_info = (RestrictInfo*)lfirst(lc_res);
                        quals = lappend(quals, res_info->clause);
                    }

                    /* get the upper param IDs */
                    if (SUBQUERY_PREDPUSH(root)) {
                        upper_params = collect_param_clause((Node*)quals);
                    }

                    subpath = (Path*)create_result_path(root, rel, quals, subpath, upper_params);
                }

                if (subpath != path) {
                    if (lc2 != NULL)
                        lfirst(lc2) = subpath;

                    /* Set cheapest startup path as boardcast+material+path */
                    if (is_cheapest_startup)
                        rel->cheapest_startup_path = subpath;
                    lfirst(lc) = subpath;
                }

                is_cheapest_startup = false;
            }

            if (incorrect_param_paths != NULL) {
                rel->pathlist = list_difference_ptr(rel->pathlist, incorrect_param_paths);
            }

            rel->cheapest_unique_path = NULL;
            rel->cheapest_parameterized_paths = NIL;
        }
#endif
    }
}

/*
 * set_rel_size
 *	  Set size estimates for a base relation
 */
void set_rel_size(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte)
{
    if (rel->reloptkind == RELOPT_BASEREL && relation_excluded_by_constraints(root, rel, rte)) {
        /*
         * We proved we don't need to scan the rel via constraint exclusion,
         * so set up a single dummy path for it.  Here we only check this for
         * regular baserels; if it's an otherrel, CE was already checked
         * in set_append_rel_size().
         *
         * In this case, we go ahead and set up the relation's path right away
         * instead of leaving it for set_rel_pathlist to do.  This is because
         * we don't have a convention for marking a rel as dummy except by
         * assigning a dummy path to it.
         */
        set_dummy_rel_pathlist(rel);
    } else if (rte->inh) {
        /* It's an "append relation", process accordingly */
        set_append_rel_size(root, rel, rti, rte);
    } else {
        switch (rel->rtekind) {
            case RTE_RELATION:
                if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
                    /* Foreign table */
                    set_foreign_size(root, rel, rte);
                } else {
                    /* Plain relation */
                    set_plain_rel_size(root, rel, rte);
                }
                break;
            case RTE_SUBQUERY:

                /*
                 * Subqueries don't support parameterized paths, so just go
                 * ahead and build their paths immediately.
                 */
                set_subquery_pathlist(root, rel, rti, rte);
                break;
            case RTE_FUNCTION:
                set_function_size_estimates(root, rel);
                break;
            case RTE_VALUES:
                set_values_size_estimates(root, rel);
                break;
            case RTE_CTE:

                /*
                 * CTEs don't support parameterized paths, so just go ahead
                 * and build their paths immediately.
                 */
                if (rte->self_reference)
                    set_worktable_pathlist(root, rel, rte);
                else
                    set_cte_pathlist(root, rel, rte);
                break;
            case RTE_RESULT:
                /* Might as well just build the path immediately */
                set_result_pathlist(root, rel, rte);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        (errmsg("unexpected rtekind: %d when set relation size", (int)rel->rtekind))));
                break;
        }
    }

    /* We already apply row hint of subquery before create path of subquery */
    if (rel->rtekind != RTE_SUBQUERY)
        adjust_rows_according_to_hint(root->parse->hintState, rel);

    /*
     * We insist that all non-dummy rels have a nonzero rowcount estimate.
     */
    if (rel->base_rel != NULL) {
        Assert(rel->base_rel->rows > 0 || IS_DUMMY_REL(rel->base_rel));
        if (unlikely(!(rel->base_rel->rows > 0 || IS_DUMMY_REL(rel->base_rel))))
            ereport(ERROR,
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmodule(MOD_OPT),
                    errmsg("failed on assertion in %s line %d : %s",
                        __FILE__,
                        __LINE__,
                        "the inlist2join's base relation is not dummy and the relation's rows is not greater than "
                        "0.")));
    } else {
        Assert(rel->rows > 0 || IS_DUMMY_REL(rel));
        if (unlikely(!(rel->rows > 0 || IS_DUMMY_REL(rel))))
            ereport(ERROR,
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmodule(MOD_OPT),
                    errmsg("failed on assertion in %s line %d : %s",
                        __FILE__,
                        __LINE__,
                        "the relation is not dummy and the relation's row is not greater than 0.")));
    }
}

/*
 * Create baserel path for RTE_RELATION kind
 */
static void SetRelationPath(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    /*
     * If the rel can apply inlist2join and the guc qrw_inlist2join_optmode=rule_base
     * So we can just convert inlist to join, there is no need to genenate other paths
     */
    if (rel->alternatives && u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_CBO) {
        set_cheapest(rel);
        return;
    }
    if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
        /* Foreign table */
        set_foreign_pathlist(root, rel, rte);
    } else {
        /* Plain relation */
        set_plain_rel_pathlist(root, rel, rte);
    }

    return;
}

/*
 * set_rel_pathlist
 *	  Build access paths for a base relation
 */
static void set_rel_pathlist(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte)
{
    if (IS_DUMMY_REL(rel)) {
        /* We already proved the relation empty, so nothing more to do */
    } else if (rte->inh) {
        /* It's an "append relation", process accordingly */
        set_append_rel_pathlist(root, rel, rti, rte);
    } else {
        switch (rel->rtekind) {
            case RTE_RELATION:
                SetRelationPath(root, rel, rte);
                break;
            case RTE_SUBQUERY:
                /* Subquery --- fully handled during set_rel_size */
                if (rel->alternatives != NIL) {
                    if (u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_CBO) {
                        Path* pathnode = ((Path*)lsecond(rel->pathlist));
                        rel->pathlist = NIL;
                        /* no need to do add_path, because do it in set_subquery_pathlist */
                        rel->pathlist = lappend(rel->pathlist, pathnode);
                    }
                    set_cheapest(rel);
                }
                break;
            case RTE_FUNCTION:
                /* RangeFunction */
                set_function_pathlist(root, rel, rte);
                break;
            case RTE_VALUES:
                /* Values list */
                set_values_pathlist(root, rel, rte);
                break;
            case RTE_CTE:
                /* CTE reference --- fully handled during set_rel_size */
                break;
            case RTE_RESULT:
                /* simple Result --- fully handled during set_rel_size */
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        (errmsg("unexpected rtekind when set relation path list: %d", (int)rel->rtekind))));
                break;
        }
    }
    /*
     * Allow a plugin to editorialize on the set of Paths for this base
     * relation. It could add new paths (such as CustomPaths) by calling
     * add_path(), or delete or modify paths added by the core code.
     */
    if (set_rel_pathlist_hook) {
        (*set_rel_pathlist_hook) (root, rel, rti, rte);
    }

    /* add baserel cn gather path when gather hint switch on */
    if (IS_STREAM_PLAN && permit_gather(root, HINT_GATHER_REL)) {
        CreateGatherPaths(root, rel, false);
    }

    debug1_print_rel(root, rel);

#ifdef OPTIMIZER_DEBUG
    debug_print_rel(root, rel);
#endif
}

static void SetPlainReSizeWithPruningRatio(RelOptInfo *rel, double pruningRatio)
{
    ListCell *cell = NULL;
    IndexOptInfo *index = NULL;

    Assert(rel->isPartitionedTable);
    rel->pages = clamp_row_est(rel->pages * pruningRatio);

    foreach (cell, rel->indexlist){
        index = (IndexOptInfo *) lfirst(cell);
        index->pages = clamp_row_est(index->pages * pruningRatio);
    }
}

/*
 * This function applies only to single partition key of range partitioned tables in PBE mode.
 */
static bool IsPbeSinglePartition(Relation rel, RelOptInfo* relInfo)
{
    if (relInfo->pruning_result->paramArg == NULL) {
        return false;
    }
    if (RelationIsSubPartitioned(rel)) {
        return false;
    }
    if (rel->partMap->type != PART_TYPE_RANGE) {
        return false;
    }
    RangePartitionMap* partMap = (RangePartitionMap*)rel->partMap;
    int partKeyNum = partMap->partitionKey->dim1;
    if (partKeyNum > 1) {
        return false;
    }
    if (relInfo->pruning_result->isPbeSinlePartition) {
        return true;
    }
    return false;
}

/*
 * set_plain_rel_size
 *	  Set size estimates for a plain relation (no subquery, no inheritance)
 */
static void set_plain_rel_size(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    set_rel_bucketinfo(root, rel, rte);
    if (rte->ispartrel) {
        Relation relation = heap_open(rte->relid, NoLock);
        double pruningRatio = 1.0;

        /* get pruning result */
        if (rte->isContainPartition) {
            rel->pruning_result = singlePartitionPruningForRestrictInfo(rte->partitionOid, relation);
        } else if (rte->isContainSubPartition) {
            rel->pruning_result =
                SingleSubPartitionPruningForRestrictInfo(rte->subpartitionOid, relation, rte->partitionOid);
        } else {
            rel->pruning_result = partitionPruningForRestrictInfo(root, rte, relation, rel->baserestrictinfo);
        }

        Assert(rel->pruning_result);

        if (IsPbeSinglePartition(relation, rel)) {
            rel->partItrs = 1;
        } else {
            /* set flag for dealing with partintioned table */
            rel->partItrs = bms_num_members(rel->pruning_result->bm_rangeSelectedPartitions) +
            bms_num_members(rel->pruning_result->intervalSelectedPartitions);
        }


        if (relation->partMap != NULL && PartitionMapIsRange(relation->partMap)) {
            RangePartitionMap *partMmap = (RangePartitionMap *)relation->partMap;
            pruningRatio = (double)rel->partItrs / partMmap->rangeElementsNum;
        }

        heap_close(relation, NoLock);

        /*
         * refresh pages/tuples since executor will skip some page
         * scan with pruning info
         */
        SetPlainReSizeWithPruningRatio(rel, pruningRatio);
    }
    /*
     * Test any partial indexes of rel for applicability.  We must do this
     * first since partial unique indexes can affect size estimates.
     */
    check_partial_indexes(root, rel);

    if (rte->tablesample == NULL) {
        /* Mark rel with estimated output rows, width, etc */
        set_baserel_size_estimates(root, rel);

        /*
         * Check to see if we can extract any restriction conditions from join
         * quals that are OR-of-AND structures.  If so, add them to the rel's
         * restriction list, and redo the above steps.
         */
        if (create_or_index_quals(root, rel)) {
            check_partial_indexes(root, rel);
            set_baserel_size_estimates(root, rel);
        }
    } else {
        /* Sampled relation */
        set_tablesample_rel_size(root, rel, rte);
    }
}

/*
 * Description: Set size estimates for a sampled relation.
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level.
 *	@in rel: Per-relation information for planning/optimization.
 *	@in rte: range table
 *
 * Return: void
 */
static void set_tablesample_rel_size(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    TableSampleClause* tsc = rte->tablesample;

    /*
     * Call the sampling method's estimation function to estimate the number
     * of pages it will read and the number of tuples it will return.  (Note:
     * we assume the function returns sane values.)
     */
    if (tsc->sampleType == SYSTEM_SAMPLE) {
        system_samplescangetsamplesize(root, rel, tsc->args);
    } else if (tsc->sampleType == BERNOULLI_SAMPLE) {
        bernoulli_samplescangetsamplesize(root, rel, tsc->args);
    } else {
        AssertEreport(tsc->sampleType == HYBRID_SAMPLE, MOD_OPT, "");
        hybrid_samplescangetsamplesize(root, rel, tsc->args);
    }

    /* Mark rel with estimated output rows, width, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * find_index_path
 * bitmap_path_walker
 *
 *     search corresponding index path for index col in g_index_vars from
 *     all optional paths. If index path found, set indexpath of var to
 *     true in g_index_vars.
 */
static void find_index_path(RelOptInfo* rel)
{
    ListCell* lc = NULL;
    foreach (lc, rel->pathlist) {
        Path* path = (Path*)lfirst(lc);
        if (path == NULL)
            continue;

        bitmap_path_walker(path);
    }
}

static void bitmap_path_walker(Path* path)
{
    ListCell* lc = NULL;

    if (IsA(path, BitmapHeapPath)) {
        BitmapHeapPath* bhpath = (BitmapHeapPath*)path;
        bitmap_path_walker(bhpath->bitmapqual);
    } else if (IsA(path, BitmapAndPath) || IsA(path, BitmapOrPath)) {
        BitmapAndPath* bapath = (BitmapAndPath*)path;
        foreach (lc, bapath->bitmapquals) {
            Path* sub_path = (Path*)lfirst(lc);
            bitmap_path_walker(sub_path);
        }
    } else if (IsA(path, IndexPath)) {
        IndexPath* indexpath = (IndexPath*)path;
        foreach (lc, g_index_vars) {
            IndexVar* var = (IndexVar*)lfirst(lc);
            if (list_member_oid(var->indexoids, indexpath->indexinfo->indexoid))
                var->indexpath = true;
        }
    } else
        return;
}

/* check for redistribute func pg_get_redis_rel_start_ctid and pg_get_redis_rel_end_ctid, 
 *  * they can only execute on cscan's filter, return true if has them in this node */
static bool is_cscan_filter_func_for_redis(Node* node)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, FuncExpr)) {
        FuncExpr* func_expr = (FuncExpr*)node;
        char* funcname = get_func_name(func_expr->funcid);
        if (funcname == NULL)
            return false;
        bool is_func_get_start_ctid = pg_strcasecmp(funcname, "pg_get_redis_rel_start_ctid") == 0;
        bool is_func_get_end_ctid = pg_strcasecmp(funcname, "pg_get_redis_rel_end_ctid") == 0;
        return is_func_get_start_ctid || is_func_get_end_ctid;
    }
    return expression_tree_walker(node, (bool (*)())is_cscan_filter_func_for_redis, (void*)NULL);
}

/*
 * set_plain_rel_pathlist
 *	  Build access paths for a plain relation (no subquery, no inheritance)
 */
static void set_plain_rel_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    List* baserestrictinfo = NIL;
    List* quals = NIL;
    bool has_vecengine_unsupport_expr = false;
    ListCell* lc = NULL;

    Relids      required_outer;
    /*
     * We don't support pushing join clauses into the quals of a seqscan, but
     * it could still have required parameterization due to LATERAL refs in
     * its tlist.  (That can only happen if the seqscan is on a relation
     * pulled up out of a UNION ALL appendrel.)
     */
    required_outer = rel->lateral_relids;


#ifdef PGXC
    bool isrp = create_plainrel_rqpath(root, rel, rte);
    /*
     * We do not parallel the scan of replicate table
     * since they are always small table. And we do not
     * support normal row table unless it is partitioned.
     * The partition table can be parallelized when partItrs > u_sess->opt_cxt.query_dop.
     */
    bool can_parallel = IS_STREAM_PLAN && (u_sess->opt_cxt.query_dop > 1) && (!rel->is_ustore) &&
                        (rel->locator_type != LOCATOR_TYPE_REPLICATED) && (rte->tablesample == NULL);
    if (!isrp) {
#endif
        switch (rel->orientation) {
            /*
             * We do not parallel the scan of replicate table
             * since they are always small table. And we do not
             * support normal row table unless it is partitioned.
             * The partition table can be parallelized when partItrs > u_sess->opt_cxt.query_dop.
             */
            case REL_COL_ORIENTED:
            case REL_PAX_ORIENTED:
            case REL_TIMESERIES_ORIENTED: {
                /*
                 * Check there is vector engine unsupport expression in rel->baserestrictinfo
                 * If find, pull up them to resultpath->pathqual
                 * Else remain it on rel->baserestrictinfo
                 */
                if (rel->baserestrictinfo != NIL) {
                    baserestrictinfo = rel->baserestrictinfo;
                    rel->baserestrictinfo = NULL;
                    foreach (lc, baserestrictinfo) {
                        RestrictInfo* restrict = (RestrictInfo*)lfirst(lc);
                        /* redistribute func pg_get_redis_rel_end_ctid and pg_get_redis_rel_end_ctid only execute
                         * correctly on cscan's filter, so we put it's filter on baserestrictinfo for cscan. */
                        bool is_redis_func = is_cscan_filter_func_for_redis((Node*)restrict->clause);
                        if (!is_redis_func && vector_engine_unsupport_expression_walker((Node*)restrict->clause)) {
                            /*
                             * If we find any vector engine unsupport expression
                             */
                            has_vecengine_unsupport_expr = true;
                            quals = lappend(quals, restrict->clause);
                        } else
                            rel->baserestrictinfo = lappend(rel->baserestrictinfo, restrict);
                    }
                }
                if (vector_engine_unsupport_expression_walker((Node*)rel->reltargetlist))
                    has_vecengine_unsupport_expr = true;

                if (rel->orientation == REL_TIMESERIES_ORIENTED) {
#ifdef ENABLE_MULTIPLE_NODES
                    add_path(root, rel, create_tsstorescan_path(root, rel));
                    if (can_parallel)
                        add_path(root, rel, create_tsstorescan_path(root, rel, u_sess->opt_cxt.query_dop));
#endif   /* ENABLE_MULTIPLE_NODES */
                } else {
                        add_path(root, rel, create_cstorescan_path(root, rel));
                        if (can_parallel)
                            add_path(root, rel, create_cstorescan_path(root, rel, u_sess->opt_cxt.query_dop));
                }
                break;
            }
            case REL_ROW_ORIENTED: {
                add_path(root, rel, create_seqscan_path(root, rel, required_outer));
                if (can_parallel)
                    add_path(root, rel, create_seqscan_path(root, rel, required_outer, u_sess->opt_cxt.query_dop));
                break;
            }
            default: {
                ereport(ERROR,
                    (errmodule(MOD_OPT), errcode(ERRCODE_CASE_NOT_FOUND), errmsg("All orientations are not covered.")));
                break;
            }
        }

        /* Tablesample don't support indexscan and tidscan. */
        if (rte->tablesample == NULL) {
            /* Consider index scans */
            create_index_paths(root, rel);

            /*
             * Consider TID scans
             * Since former TidScan is not fit for column store table, just create
             * TidScan path for row store table
             */
            if (rel->orientation == REL_ROW_ORIENTED)
                create_tidscan_paths(root, rel);
        }
#ifdef PGXC
    } else {
        Oid relId = rte->relid;

        Relation relation = RelationIdGetRelation(relId);
        if (!RelationIsValid(relation)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("could not open relation with OID %u", relId)));
        }
        if (RelationIsDfsStore(relation)) {
            rte->inh = true;
        }

        RelationClose(relation);
    }
#endif

    /* Now find the cheapest of the paths for this rel */
    set_cheapest(rel);

    /* Consider partition's path for partitioned table */
#ifdef PGXC
    if (!isrp)
#endif
        try_add_partiterator(root, rel, rte);

    if (has_vecengine_unsupport_expr) {
        /*
         * Cstorescan path has unsupport expression in vector engine,
         * Add result operator to handle it
         */
        add_upperop_for_vecscan_expr(root, rel, quals);
    }

    if ((rel->orientation == REL_ROW_ORIENTED || rel->orientation == REL_COL_ORIENTED) &&
        enable_check_implicit_cast()) {
        find_index_path(rel);
    }
}

/*
 * Description:add result operator over scan operator. And add
 * vector type scan's qual with unsupport expression in vector engine
 * to result operator
 *
 * Parameters:
 *	@in root: plannerinfo struct for current query level.
 *	@in rel: Per-relation information for planning/optimization.
 *	@in quals: filter condition
 *
 * Return: void
 */
static void add_upperop_for_vecscan_expr(PlannerInfo* root, RelOptInfo* rel, List* quals)
{
    ListCell* pathCell = NULL;
    foreach (pathCell, rel->pathlist) {
        Path* path = (Path*)lfirst(pathCell);
        Path* resPath = NULL;
        ListCell* ctPathCell = NULL;
        ListCell* cpPathCell = NULL;
        resPath = (Path*)create_result_path(root, rel, quals, path);
        ((ResultPath*)resPath)->ispulledupqual = true;

        /* replace entry in pathlist */
        lfirst(pathCell) = resPath;

        if (path == rel->cheapest_startup_path) {
            /* replace cheapest_startup_path */
            rel->cheapest_startup_path = resPath;
        }

        if (path == rel->cheapest_unique_path) {
            /* replace cheapest_unique_path */
            rel->cheapest_unique_path = resPath;
        }

        /* replace entry in cheapest_total_path */
        foreach (ctPathCell, rel->cheapest_total_path) {
            if (lfirst(ctPathCell) == path) {
                lfirst(ctPathCell) = resPath;
                break;
            }
        }

        /* replace entry in cheapest_parameterized_paths */
        foreach (cpPathCell, rel->cheapest_parameterized_paths) {
            /* we add cheapest total into cheapest_parameterized_paths in set_cheapest */
            if (lfirst(cpPathCell) == path) {
                lfirst(cpPathCell) = resPath;
                break;
            }
        }
    }
}

/*
 * set_foreign_size
 *		Set size estimates for a foreign table RTE
 */
static void set_foreign_size(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    /* Mark rel with estimated output rows, width, etc */
    set_foreign_size_estimates(root, rel);

    AssertEreport(rel->fdwroutine, MOD_OPT, "");

    /* Let FDW adjust the size estimates, if it can */
    rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

    /* ... but do not let it set the rows estimate to zero */
    rel->rows = clamp_row_est(rel->rows);
}

/*
 * set_foreign_pathlist
 *		Build access paths for a foreign table RTE
 */
static void set_foreign_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    bool isrp = false;

    /*
     * If the query dose not support stream and the distribution type of foreign table
     * is replication or roundrobin, the query can send remotequery to datanode.
     */
    if (!IS_STREAM) {
        isrp = create_plainrel_rqpath(root, rel, rte);
    }

#ifdef ENABLE_MOT
    if (!isrp && rel->fdwroutine->GetFdwType && rel->fdwroutine->GetFdwType() == MOT_ORC) {
        /* Consider index scans */
        create_index_paths(root, rel);
        if (rel->orientation == REL_ROW_ORIENTED) {
            create_tidscan_paths(root, rel);
        }
    }
#endif

    if (!isrp) {
        /* Call the FDW's GetForeignPaths function to generate path(s) */
        rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
    }

    /* Select cheapest path */
#ifdef ENABLE_MOT
    if (!(rel->fdwroutine->GetFdwType && rel->fdwroutine->GetFdwType() == MOT_ORC) || isrp) {
#endif
        set_cheapest(rel);
#ifdef ENABLE_MOT
    }
#endif
}

/*
 * set_append_rel_size
 *	  Set size estimates for an "append relation"
 *
 * The passed-in rel and RTE represent the entire append relation.	The
 * relation's contents are computed by appending together the output of
 * the individual member relations.  Note that in the inheritance case,
 * the first member relation is actually the same table as is mentioned in
 * the parent RTE ... but it has a different RTE and RelOptInfo.  This is
 * a good thing because their outputs are not the same size.
 */
static void set_append_rel_size(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte)
{
    Index parentRTindex = rti;
    bool has_live_children = false;
    double parent_rows = 0;
    double parent_global_rows = 0;
    double parent_tuples = 0;
    double parent_global_tuples = 0;
    double parent_size = 0;
    double* parent_attrsizes = NULL;
    int nattrs;
    ListCell* l = NULL;

    /*
     * Initialize to compute size estimates for whole append relation.
     *
     * We handle width estimates by weighting the widths of different child
     * rels proportionally to their number of rows.  This is sensible because
     * the use of width estimates is mainly to compute the total relation
     * "footprint" if we have to sort or hash it.  To do this, we sum the
     * total equivalent size (in "double" arithmetic) and then divide by the
     * total rowcount estimate.  This is done separately for the total rel
     * width and each attribute.
     *
     * Note: if you consider changing this logic, beware that child rels could
     * have zero rows and/or width, if they were excluded by constraints.
     */
    nattrs = rel->max_attr - rel->min_attr + 1;
    parent_attrsizes = (double*)palloc0(nattrs * sizeof(double));

    foreach (l, root->append_rel_list) {
        AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(l);
        int childRTindex;
        RangeTblEntry* childRTE = NULL;
        RelOptInfo* childrel = NULL;
        List* childquals = NIL;
        Node* childqual = NULL;
        ListCell* parentvars = NULL;
        ListCell* childvars = NULL;

        /* append_rel_list contains all append rels; ignore others */
        if (appinfo->parent_relid != parentRTindex)
            continue;

        childRTindex = appinfo->child_relid;
        childRTE = root->simple_rte_array[childRTindex];

        /*
         * The child rel's RelOptInfo was already created during
         * add_base_rels_to_query.
         */
        childrel = find_base_rel(root, childRTindex);
        AssertEreport(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL, MOD_OPT, "");

        /*
         * We have to copy the parent's targetlist and quals to the child,
         * with appropriate substitution of variables.	However, only the
         * baserestrictinfo quals are needed before we can check for
         * constraint exclusion; so do that first and then check to see if we
         * can disregard this child.
         *
         * As of 8.4, the child rel's targetlist might contain non-Var
         * expressions, which means that substitution into the quals could
         * produce opportunities for const-simplification, and perhaps even
         * pseudoconstant quals.  To deal with this, we strip the RestrictInfo
         * nodes, do the substitution, do const-simplification, and then
         * reconstitute the RestrictInfo layer.
         */
        childquals = get_all_actual_clauses(rel->baserestrictinfo);
        childquals = (List*)adjust_appendrel_attrs(root, (Node*)childquals, appinfo);
        childqual = eval_const_expressions(root, (Node*)make_ands_explicit(childquals));
        if (childqual && IsA(childqual, Const) &&
            (((Const*)childqual)->constisnull || !DatumGetBool(((Const*)childqual)->constvalue))) {
            /*
             * Restriction reduces to constant FALSE or constant NULL after
             * substitution, so this child need not be scanned.
             */
            set_dummy_rel_pathlist(childrel);
            continue;
        }
        childquals = make_ands_implicit((Expr*)childqual);
        childquals = make_restrictinfos_from_actual_clauses(root, childquals);
        childrel->baserestrictinfo = lappend3(childrel->baserestrictinfo, childquals);
        childrel->subplanrestrictinfo = (List*)adjust_appendrel_attrs(root, (Node*)rel->subplanrestrictinfo, appinfo);

        if (relation_excluded_by_constraints(root, childrel, childRTE)) {
            /*
             * This child need not be scanned, so we can omit it from the
             * appendrel.
             */
            set_dummy_rel_pathlist(childrel);
            continue;
        }

        /*
         * CE failed, so finish copying/modifying targetlist and join quals.
         *
         * Note: the resulting childrel->reltargetlist may contain arbitrary
         * expressions, which normally would not occur in a reltargetlist.
         * That is okay because nothing outside of this routine will look at
         * the child rel's reltargetlist.  We do have to cope with the case
         * while constructing attr_widths estimates below, though.
         */
        childrel->joininfo = (List*)adjust_appendrel_attrs(root, (Node*)rel->joininfo, appinfo);
        childrel->reltargetlist = (List*)adjust_appendrel_attrs(root, (Node*)rel->reltargetlist, appinfo);

        /*
         * When childrel's targetlist got changed, we should adjust itersting
         * keys accrodingly
         */
        if (childRTE->subquery != NULL) {
            Assert(list_length(childrel->reltargetlist) == list_length(rel->reltargetlist));
            childrel->rel_dis_keys.matching_keys =
                (List*)replace_node_clause((Node*)childrel->rel_dis_keys.matching_keys,
                    (Node*)rel->reltargetlist,
                    (Node*)childrel->reltargetlist,
                    RNC_COPY_NON_LEAF_NODES);
            childrel->rel_dis_keys.superset_keys =
                (List*)replace_node_clause((Node*)childrel->rel_dis_keys.superset_keys,
                    (Node*)rel->reltargetlist,
                    (Node*)childrel->reltargetlist,
                    RNC_COPY_NON_LEAF_NODES);
        }

        /*
         * We have to make child entries in the EquivalenceClass data
         * structures as well.	This is needed either if the parent
         * participates in some eclass joins (because we will want to consider
         * inner-indexscan joins on the individual children) or if the parent
         * has useful pathkeys (because we should try to build MergeAppend
         * paths that produce those sort orderings).
         */
        if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
            add_child_rel_equivalences(root, appinfo, rel, childrel);
        childrel->has_eclass_joins = rel->has_eclass_joins;

        /*
         * Note: we could compute appropriate attr_needed data for the child's
         * variables, by transforming the parent's attr_needed through the
         * translated_vars mapping.  However, currently there's no need
         * because attr_needed is only examined for base relations not
         * otherrels.  So we just leave the child's attr_needed empty.
         */
        /*
         * Compute the child's size.
         */
        set_rel_size(root, childrel, childRTindex, childRTE);

        /*
         * It is possible that constraint exclusion detected a contradiction
         * within a child subquery, even though we didn't prove one above. If
         * so, we can skip this child.
         */
        if (IS_DUMMY_REL(childrel))
            continue;

        has_live_children = true;

        /*
         * Accumulate size information from each live child.
         */
        Assert(childrel->rows > 0);

        parent_rows += RELOPTINFO_LOCAL_FIELD(root, childrel, rows);
        parent_global_rows += childrel->rows;
        parent_tuples += RELOPTINFO_LOCAL_FIELD(root, childrel, tuples);
        parent_global_tuples += childrel->tuples;
        parent_size += childrel->width * RELOPTINFO_LOCAL_FIELD(root, childrel, rows);

        /*
         * Accumulate per-column estimates too.  We need not do anything
         * for PlaceHolderVars in the parent list.	If child expression
         * isn't a Var, or we didn't record a width estimate for it, we
         * have to fall back on a datatype-based estimate.
         *
         * By construction, child's reltargetlist is 1-to-1 with parent's.
         */
        forboth(parentvars, rel->reltargetlist, childvars, childrel->reltargetlist)
        {
            Var* parentvar = (Var*)lfirst(parentvars);
            Node* childvar = (Node*)lfirst(childvars);

            if (IsA(parentvar, Var)) {
                int pndx = parentvar->varattno - rel->min_attr;
                int32 child_width = 0;

                if (IsA(childvar, Var) && ((Var *) childvar)->varno == childrel->relid) {
                    int cndx = ((Var*)childvar)->varattno - childrel->min_attr;

                    child_width = childrel->attr_widths[cndx];
                }
                if (child_width <= 0)
                    child_width = get_typavgwidth(exprType(childvar), exprTypmod(childvar));
                AssertEreport(child_width > 0, MOD_OPT, "");
                parent_attrsizes[pndx] += child_width * RELOPTINFO_LOCAL_FIELD(root, childrel, rows);
            }
        }
    }

    if (has_live_children) {
        /*
         * Save the finished size estimates.
         */
        int i;

        parent_rows = clamp_row_est(parent_rows);
        parent_global_rows = clamp_row_est(parent_global_rows);
        parent_tuples = clamp_row_est(parent_tuples);
        parent_global_tuples = clamp_row_est(parent_global_tuples);

        rel->rows = parent_global_rows;
        rel->width = (int)rint(parent_size / parent_rows);
        for (i = 0; i < nattrs; i++)
            rel->attr_widths[i] = (int32)rint(parent_attrsizes[i] / parent_rows);

        rel->tuples = parent_global_tuples;
        rel->multiple = parent_tuples / parent_global_tuples * ng_get_dest_num_data_nodes(root, rel);
    } else {
        /*
         * All children were excluded by constraints, so mark the whole
         * appendrel dummy.  We must do this in this phase so that the rel's
         * dummy-ness is visible when we generate paths for other rels.
         */
        set_dummy_rel_pathlist(rel);
    }

    pfree_ext(parent_attrsizes);
}

/*
 * @Description: Judge if path can be parameterized, only true if no materialize paths
 * added due to streamed subplan or cstore index scan with delta.
 * For correlated subpath, if path is hashed or containing stream, return false.
 * @in root: Per-query information for planning/optimization.
 * @in cheapest_total: Need judge path.
 * @return: Return true if this path is non-replicated or contain stream else return false.
 */
static bool judge_path_parameterizable(PlannerInfo* root, Path* path)
{
    if (root != NULL && root->is_correlated) {
        Distribution* distribution = ng_get_dest_distribution(path);
        Distribution* target_distribution = ng_get_correlated_subplan_group_distribution();

        /*
         * conditions with stream of potentially stream will be added will be figured out,
         * and return false directly.
         */
        if (!is_replicated_path(path) || !ng_is_same_group(distribution, target_distribution)) {
            return false;
        } else {
            /* only check if there's stream node */
            ContainStreamContext context;
            context.outer_relids = NULL;
            context.upper_params = NULL;
            context.only_check_stream = true;
            context.under_materialize_all = false;
            context.has_stream = false;
            context.has_parameterized_path = false;
            context.has_cstore_index_delta = false;

            stream_path_walker(path, &context);
            if (context.has_stream || context.has_cstore_index_delta) {
                return false;
            }
        }
    }

    return true;
}

/*
 * Build Append paths for each parameterization seen among the child rels.
 * (This may look pretty expensive, but in most cases of practical
 * interest, the child rels will expose mostly the same parameterizations,
 * so that not that many cases actually get considered here.)
 *
 * The Append node itself cannot enforce quals, so all qual checking must
 * be done in the child paths.  This means that to have a parameterized
 * Append path, we must have the exact same parameterization for each
 * child path; otherwise some children might be failing to check the
 * moved-down quals.  To make them match up, we can try to increase the
 * parameterization of lesser-parameterized paths.
 */
static void build_append_rel_path(PlannerInfo* root, RelOptInfo* rel,
                                    List* all_child_outers, List* all_child_uppers,
                                    List* live_childrels)
{
    ListCell *l = NULL;
    List *subpaths = NIL;
    bool subpaths_valid = true;
    Path* append_path = NULL;

    foreach (l, all_child_outers) {
        Relids required_outer = (Relids)lfirst(l);
        ListCell* lcr = NULL;

        /* Select the child paths for an Append with this parameterization */
        subpaths = NIL;
        subpaths_valid = true;
        foreach (lcr, live_childrels) {
            RelOptInfo* childrel = (RelOptInfo*)lfirst(lcr);
            Path* cheapest_total = NULL;

            cheapest_total = get_cheapest_parameterized_child_path(root, childrel, required_outer);

            if (cheapest_total == NULL) {
                subpaths_valid = false;
                break;
            }

            /*
             * Judge this path, if it is correlation subquery and it is non-replicated or include stream,
             * we will not generate plan with param_info. Because this upper levels add materialization or broadcast
             * node without prarm_info.
             */
            if (!judge_path_parameterizable(root, cheapest_total)) {
                subpaths_valid = false;
                break;
            }

            /* Children must have exactly the desired parameterization */
            if (!bms_equal(PATH_REQ_OUTER(cheapest_total), required_outer)) {
                cheapest_total = reparameterize_path(root, cheapest_total, required_outer, 1.0);
                if (cheapest_total == NULL) {
                    subpaths_valid = false;
                    break;
                }
            }

            subpaths = accumulate_append_subpath(subpaths, cheapest_total);
        }

        if (subpaths_valid) {
            append_path = (Path*)create_append_path(root, rel, subpaths, required_outer);
            if (append_path != NULL) {
                add_path(root, rel, append_path);
            }
        }
    }

    foreach (l, all_child_uppers) {
        Bitmapset* required_upper = (Bitmapset*) lfirst(l);
        ListCell *lcr = NULL;
        Relids collect_outers = NULL;

        /* Select the child paths for an Append with this parameterization */
        subpaths = NIL;
        subpaths_valid = true;
        foreach (lcr, live_childrels) {
            RelOptInfo* childrel = (RelOptInfo*)lfirst(lcr);
            Path* cheapest_total = NULL;

            cheapest_total = get_cheapest_parameterized_child_path_for_upper(root, childrel, required_upper);
            if (cheapest_total == NULL) {
                subpaths_valid = false;
                break;
            }

            collect_outers = bms_union(collect_outers, PATH_REQ_OUTER(cheapest_total));
            subpaths = accumulate_append_subpath(subpaths, cheapest_total);
        }

        if (subpaths_valid && collect_outers == NULL) {
            append_path = (Path*)create_append_path(root, rel, subpaths, NULL);
            if (append_path != NULL) {
                add_path(root, rel, append_path);
            }
        }

        if (subpaths_valid && collect_outers != NULL) {
            List *subpaths_reparam = NULL;
            ListCell *lp = NULL;

            foreach (lp, subpaths) {
                Path *cheapest_param = (Path*)lfirst(lp);

                /* Children must have exactly the desired parameterization */
                if (!bms_equal(PATH_REQ_OUTER(cheapest_param), collect_outers)) {
                    cheapest_param = reparameterize_path(root, cheapest_param, collect_outers, 1.0);
                    if (cheapest_param == NULL) {
                        subpaths_valid = false;
                        break;
                    }
                }

                subpaths_reparam = accumulate_append_subpath(subpaths_reparam, cheapest_param);
            }

            if (subpaths_valid) {
                subpaths = subpaths_reparam;
            }
        }

        if (subpaths_valid) {
            append_path = (Path*)create_append_path(root, rel, subpaths, collect_outers);
            if (append_path != NULL) {
                add_path(root, rel, append_path);
            }
        }
    }

    return;
}

/*
 * set_append_rel_pathlist
 *	  Build access paths for an "append relation"
 */
static void set_append_rel_pathlist(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry* rte)
{
    Index parentRTindex = rti;
    List* live_childrels = NIL;
    List* subpaths = NIL;
    bool subpaths_valid = true;
    List* all_child_pathkeys = NIL;
    List* all_child_outers = NIL;
    List* all_child_uppers = NIL;
    ListCell* l = NULL;
    Path* append_path = NULL;

    /*
     * Generate access paths for each member relation, and remember the
     * cheapest path for each one.	Also, identify all pathkeys (orderings)
     * and parameterizations (required_outer sets) available for the member
     * relations.
     */
    foreach (l, root->append_rel_list) {
        AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(l);
        int childRTindex;
        RangeTblEntry* childRTE = NULL;
        RelOptInfo* childrel = NULL;
        ListCell* lcp = NULL;

        /* append_rel_list contains all append rels; ignore others */
        if (appinfo->parent_relid != parentRTindex)
            continue;

        /* Re-locate the child RTE and RelOptInfo */
        childRTindex = appinfo->child_relid;
        childRTE = root->simple_rte_array[childRTindex];
        childrel = root->simple_rel_array[childRTindex];

        /*
         * Compute the child's access paths.
         */
        set_rel_pathlist(root, childrel, childRTindex, childRTE);

        /*
         * If child is dummy, ignore it.
         */
        if (IS_DUMMY_REL(childrel))
            continue;

        /* Remember which childrels are live, for logic below */
        live_childrels = lappend(live_childrels, childrel);

        /*
         * Child is live, so add its cheapest access path to the Append path
         * we are constructing for the parent.
         */
        ListCell *lc = NULL;
        if (childrel->cheapest_total_path) {
            foreach(lc, childrel->cheapest_total_path) {
                Path *l_path = (Path *)lfirst(lc);
                if (l_path->param_info != NULL) {
                    subpaths_valid = false;
                } else {
                    subpaths = accumulate_append_subpath(subpaths, l_path);
                }
            }
        } else {
            subpaths_valid = false;
        }

        /*
         * Collect lists of all the available path orderings and
         * parameterizations for all the children.	We use these as a
         * heuristic to indicate which sort orderings and parameterizations we
         * should build Append and MergeAppend paths for.
         */
        foreach (lcp, childrel->pathlist) {
            Path* childpath = (Path*)lfirst(lcp);
            List* childkeys = childpath->pathkeys;
            Relids childouter = PATH_REQ_OUTER(childpath);
            Bitmapset *childupper = PATH_REQ_UPPER(childpath);

            /* Unsorted paths don't contribute to pathkey list */
            if (childkeys != NIL) {
                ListCell* lpk = NULL;
                bool found = false;

                /* Have we already seen this ordering? */
                foreach (lpk, all_child_pathkeys) {
                    List* existing_pathkeys = (List*)lfirst(lpk);

                    if (compare_pathkeys(existing_pathkeys, childkeys) == PATHKEYS_EQUAL) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    /* No, so add it to all_child_pathkeys */
                    all_child_pathkeys = lappend(all_child_pathkeys, childkeys);
                }
            }

            /* Unparameterized paths don't contribute to param-set list */
            if (childouter) {
                ListCell* lco = NULL;
                bool found = false;

                /* Have we already seen this param set? */
                foreach (lco, all_child_outers) {
                    Relids existing_outers = (Relids)lfirst(lco);
                    if (bms_equal(existing_outers, childouter)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    /* No, so add it to all_child_outers */
                    all_child_outers = lappend(all_child_outers, childouter);
                }
            }

            /* Unparameterized paths don't contribute to param-set list */
            if (childupper != NULL) {
                ListCell* lco = NULL;
                bool found = false;

                /* Have we already seen this param set? */
                foreach (lco, all_child_uppers) {
                    Relids existing_upper = (Relids)lfirst(lco);

                    if (bms_equal(existing_upper, childupper)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    /* No, so add it to all_child_outers */
                    all_child_uppers = lappend(all_child_uppers, childupper);
                }
            }
        }
    }

    /*
     * Next, build an unordered, unparameterized Append path for the rel.
     * (Note: this is correct even if we have zero or one live subpath due to
     * constraint exclusion.)
     */
    if (subpaths_valid) {
        append_path = (Path*)create_append_path(root, rel, subpaths, NULL);
        add_path(root, rel, append_path);
    }

    /*
     * Build unparameterized MergeAppend paths based on the collected list of
     * child pathkeys.
     */
    if (subpaths_valid) {
        generate_mergeappend_paths(root, rel, live_childrels, all_child_pathkeys);
    }

    /* Build Append paths for each parameterization seen among the child rels. */
    build_append_rel_path(root, rel, all_child_outers, all_child_uppers, live_childrels);

    /* Select cheapest paths */
    set_cheapest(rel);
}

/*
 * generate_mergeappend_paths
 *		Generate MergeAppend paths for an append relation
 *
 * Generate a path for each ordering (pathkey list) appearing in
 * all_child_pathkeys.
 *
 * We consider both cheapest-startup and cheapest-total cases, ie, for each
 * interesting ordering, collect all the cheapest startup subpaths and all the
 * cheapest total paths, and build a MergeAppend path for each case.
 *
 * We don't currently generate any parameterized MergeAppend paths.  While
 * it would not take much more code here to do so, it's very unclear that it
 * is worth the planning cycles to investigate such paths: there's little
 * use for an ordered path on the inside of a nestloop.  In fact, it's likely
 * that the current coding of add_path would reject such paths out of hand,
 * because add_path gives no credit for sort ordering of parameterized paths,
 * and a parameterized MergeAppend is going to be more expensive than the
 * corresponding parameterized Append path.  If we ever try harder to support
 * parameterized mergejoin plans, it might be worth adding support for
 * parameterized MergeAppends to feed such joins.  (See notes in
 * optimizer/README for why that might not ever happen, though.)
 */
static void generate_mergeappend_paths(PlannerInfo* root, RelOptInfo* rel, 
    List* live_childrels, List* all_child_pathkeys)
{
    ListCell* lcp = NULL;

    foreach (lcp, all_child_pathkeys) {
        List* pathkeys = (List*)lfirst(lcp);
        List* startup_subpaths = NIL;
        List* total_subpaths = NIL;
        bool startup_neq_total = false;
        ListCell* lcr = NULL;

        /* Select the child paths for this ordering... */
        foreach (lcr, live_childrels) {
            RelOptInfo* childrel = (RelOptInfo*)lfirst(lcr);
            Path *cheapest_startup, *cheapest_total;

            /* Locate the right paths, if they are available. */
            cheapest_startup = get_cheapest_path_for_pathkeys(childrel->pathlist, pathkeys, NULL, STARTUP_COST);
            cheapest_total = get_cheapest_path_for_pathkeys(childrel->pathlist, pathkeys, NULL, TOTAL_COST);

            /*
             * If we can't find any paths with the right order just use the
             * cheapest-total path; we'll have to sort it later.
             */
            if (cheapest_startup == NULL || cheapest_total == NULL) {
                cheapest_startup = cheapest_total = (Path*)linitial(childrel->cheapest_total_path);
                AssertEreport(cheapest_total->param_info == NULL, MOD_OPT, "");
            }

            /*
             * Notice whether we actually have different paths for the
             * "cheapest" and "total" cases; frequently there will be no point
             * in two create_merge_append_path() calls.
             */
            if (cheapest_startup != cheapest_total)
                startup_neq_total = true;

            startup_subpaths = accumulate_append_subpath(startup_subpaths, cheapest_startup);
            total_subpaths = accumulate_append_subpath(total_subpaths, cheapest_total);
        }

        /* ... and build the MergeAppend paths */
        Path* merge_append_path = (Path*)create_merge_append_path(root, rel, startup_subpaths, pathkeys, NULL);
        if (merge_append_path != NULL) {
            add_path(root, rel, merge_append_path);
        }

        if (startup_neq_total) {
            merge_append_path = (Path*)create_merge_append_path(root, rel, total_subpaths, pathkeys, NULL);
            if (merge_append_path != NULL) {
                add_path(root, rel, merge_append_path);
            }
        }
    }
}

/*
 * get_cheapest_parameterized_child_path_for_upper
 *      Get cheapest path for this relation that has exactly the requested
 *      parameterization.
 *
 * Returns NULL if unable to create such a path.
 */
static Path *
get_cheapest_parameterized_child_path_for_upper(PlannerInfo *root, RelOptInfo *rel,
                                      Bitmapset* required_upper)
{
    ListCell   *lc = NULL;
    Path *cheapest_upper_path = NULL;
    Path *cheapest_outer_path = NULL;
    Path *cheapest_normal_path = NULL;

    foreach (lc, rel->pathlist) {
        Path *path = (Path*)lfirst(lc);

        if (path->param_info == NULL) {
            if (cheapest_normal_path == NULL ||
                compare_path_costs(cheapest_normal_path, path, TOTAL_COST) <= 0) {
                cheapest_normal_path = path;
                continue;
            }
        }

        if (PATH_REQ_OUTER(path) == NULL &&
            bms_equal(required_upper, PATH_REQ_UPPER(path))) {
            if (cheapest_upper_path == NULL ||
                compare_path_costs(cheapest_upper_path, path, TOTAL_COST) <= 0) {
                cheapest_upper_path = path;
                continue;
            }
        }

        if (PATH_REQ_OUTER(path) != NULL) {
            if (cheapest_outer_path == NULL ||
                compare_path_costs(cheapest_upper_path, path, TOTAL_COST) <= 0) {
                cheapest_upper_path = path;
                continue;
            }
        }
    }

    if (cheapest_upper_path != NULL)
        return cheapest_upper_path;

    if (cheapest_normal_path != NULL)
        return cheapest_normal_path;

    return cheapest_outer_path;
}


/*
 * get_cheapest_parameterized_child_path
 *      Get cheapest path for this relation that has exactly the requested
 *      parameterization.
 *
 * Returns NULL if unable to create such a path.
 */
static Path *
get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel,
                                      Relids required_outer)
{
    Path       *cheapest = NULL;
    ListCell   *lc = NULL;

    /*
     * Look up the cheapest existing path with no more than the needed
     * parameterization.  If it has exactly the needed parameterization, we're
     * done.
     */
    cheapest = get_cheapest_path_for_pathkeys(rel->pathlist,
                                              NIL,
                                              required_outer,
                                              TOTAL_COST);

    if (cheapest != NULL &&
        bms_equal(PATH_REQ_OUTER(cheapest), required_outer))
        return cheapest;

    /*
     * Otherwise, we can "reparameterize" an existing path to match the given
     * parameterization, which effectively means pushing down additional
     * joinquals to be checked within the path's scan.  However, some existing
     * paths might check the available joinquals already while others don't;
     * therefore, it's not clear which existing path will be cheapest after
     * reparameterization.  We have to go through them all and find out.
     */
    cheapest = NULL;
    foreach(lc, rel->pathlist)
    {
        Path       *path = (Path *) lfirst(lc);

        /* Can't use it if it needs more than requested parameterization */
        if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
            continue;

        /*
         * Reparameterization can only increase the path's cost, so if it's
         * already more expensive than the current cheapest, forget it.
         */
        if (cheapest != NULL &&
            compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
            continue;

        /* Reparameterize if needed, then recheck cost */
        if (!bms_equal(PATH_REQ_OUTER(path), required_outer))
        {
            path = reparameterize_path(root, path, required_outer, 1.0);
            if (path == NULL)
                continue;       /* failed to reparameterize this one */
            Assert(bms_equal(PATH_REQ_OUTER(path), required_outer));

            if (cheapest != NULL &&
                compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
                continue;
        }

        /* We have a new best path */
        cheapest = path;
    }

    /* Return the best path, or NULL if we found no suitable candidate */
    return cheapest;
}




/*
 * accumulate_append_subpath
 *		Add a subpath to the list being built for an Append or MergeAppend
 *
 * It's possible that the child is itself an Append path, in which case
 * we can "cut out the middleman" and just add its child paths to our
 * own list.  (We don't try to do this earlier because we need to
 * apply both levels of transformation to the quals.)
 */
static List* accumulate_append_subpath(List* subpaths, Path* path)
{
    if (IsA(path, AppendPath)) {
        AppendPath* apath = (AppendPath*)path;

        /* list_copy is important here to avoid sharing list substructure */
        return list_concat(subpaths, list_copy(apath->subpaths));
    } else
        return lappend(subpaths, path);
}

/*
 * set_dummy_rel_pathlist
 *	  Build a dummy path for a relation that's been excluded by constraints
 *
 * Rather than inventing a special "dummy" path type, we represent this as an
 * AppendPath with no members (see also IS_DUMMY_PATH/IS_DUMMY_REL macros).
 */
static void set_dummy_rel_pathlist(RelOptInfo* rel)
{
    /*
     * Set dummy tuples as 1 because it may be avoid some error about divided by 0
     * during estimate selectivity of joinrel.
     */
    rel->tuples = 1;
    /* Set dummy size estimates --- we leave attr_widths[] as zeroes */
    rel->rows = 0;
    rel->width = 0;

    /* Discard any pre-existing paths; no further need for them */
    rel->pathlist = NIL;
    Path* append_path = (Path*)create_append_path(NULL, rel, NIL, NULL);
    if (append_path != NULL) {
        add_path(NULL, rel, append_path);
    }

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
}

/* quick-and-dirty test to see if any joining is needed */
static bool has_multiple_baserels(PlannerInfo* root)
{
    int num_base_rels = 0;
    int rti;

    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo* brel = root->simple_rel_array[rti];

        if (brel == NULL)
            continue;

        /* ignore RTEs that are "other rels" */
        if (brel->reloptkind == RELOPT_BASEREL)
            if (++num_base_rels > 1)
                return true;
    }
    return false;
}

static bool can_push_qual_into_subquery(PlannerInfo* root,
                                                RestrictInfo* rinfo,
                                                RangeTblEntry* rte,
                                                Index rti,
                                                Node* clause,
                                                const bool* unsafeColumns)
{
    Query* subquery = rte->subquery;

    if (rinfo->pseudoconstant) {
        return false;
    }

    if (rte->security_barrier &&  contain_leaky_functions(clause)) {
        return false;
    }

    if (!qual_is_pushdown_safe(root, subquery, rti, clause, unsafeColumns)) {
        return false;
    }

    if (!qual_pushdown_in_partialpush(root->parse, subquery, clause)){
        return false;
    }

    return true;
}

/*
 * fix the varno/varlevelsup when push the predicate into the subquery
 */
typedef struct trans_lateral_vars_t
{
    List *target_list;
    Query *subquery;
    Index rti;
    int levelsup;
}trans_lateral_vars_t;

static Node* trans_lateral_vars_mutator(Node *node, trans_lateral_vars_t *lateral_vars)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
    {
        Var *var = (Var *)node;
        List *target_list = lateral_vars->target_list;
        
        /* Inner Var, need to convert to subquery form */
        if (var->varno == lateral_vars->rti)
        {
            Index varattno = var->varattno;
            TargetEntry *tle = get_tle_by_resno(target_list, varattno);

            return (Node *)copyObject(tle->expr);
        }
        else
        {
            /* Outer Var, need to convert to Param*/
            Var *new_var = (Var*)copyObject(var);
            new_var->varlevelsup += lateral_vars->levelsup;

            return (Node *)new_var;
        }
    }

    return expression_tree_mutator(node,
                (Node* (*)(Node*, void*))trans_lateral_vars_mutator, lateral_vars);
}

static Node* trans_lateral_vars(Query *subquery, Index rti, Node *qual, int levelsup)
{
    trans_lateral_vars_t lateral_vars;
    lateral_vars.subquery = subquery;
    lateral_vars.target_list = subquery->targetList;
    lateral_vars.rti = rti;
    lateral_vars.levelsup = levelsup;

    return query_or_expression_tree_mutator((Node *)qual,
                (Node* (*)(Node*, void*))trans_lateral_vars_mutator, (void *)&lateral_vars, 0);
}

/*
 * collect the lateral vars
 */
typedef struct collect_lateral_vars_t
{
    PlannerInfo *root;
    RelOptInfo *rel;
}collect_lateral_vars_t;

static bool collect_lateral_vars_walker(Node *node, void *context)
{
    collect_lateral_vars_t *lateral_context = (collect_lateral_vars_t *)context;
    RelOptInfo *rel = lateral_context->rel;
    PlannerInfo *root = lateral_context->root;

    if (node == NULL) {
        return false;
    }

    if (IsA(node, Var))
    {
        Var *var = (Var *)node;

        /* Outer Var, need to convert to subquery form */
        if (var->varno != rel->relid)
        {
            rel->lateral_relids = bms_add_member(rel->lateral_relids, var->varno);
            rel->lateral_vars = lappend(rel->lateral_vars, var);
            add_lateral_info(root, rel->relid, bms_make_singleton(var->varno));
        }

        return false;
    }
    else if (IsA(node, RestrictInfo))
    {
        RestrictInfo *restrict_info = (RestrictInfo *)node;
        node = (Node *)restrict_info->clause;
    }

    return expression_tree_walker(node, (bool (*)())collect_lateral_vars_walker, context);
}

static bool
collect_lateral_vars(PlannerInfo *root, Node *restrict, RelOptInfo *rel)
{
    collect_lateral_vars_t lateral_context;
    lateral_context.root = root;
    lateral_context.rel = rel;

    return expression_tree_walker(restrict,
                (bool (*)())collect_lateral_vars_walker, (void *)&lateral_context);
}

/*
 * get the hints for predicate pushdown
 */
static Relids collect_child_relids(PlannerInfo* root, Index rti)
{
    RangeTblEntry *rte = root->simple_rte_array[rti];
    if (!rte->inh)
        return NULL;

    ListCell *lc = NULL;
    AppendRelInfo *appinfo = NULL;
    Relids result = NULL;
    foreach(lc, root->append_rel_list) {
        appinfo = (AppendRelInfo *) lfirst(lc);

        /* find rti */
        if (appinfo->parent_relid == rti) {
            result = bms_add_member(result, appinfo->child_relid);
        }
    }

    return result;
}

static Relids predpush_candidates_append(PlannerInfo *root, Relids parents)
{
    int parent_id = -1;
    Relids results = NULL;
    while ((parent_id = bms_next_member(parents, parent_id)) >= 0) {
        results = bms_union(results, collect_child_relids(root, parent_id));
    }

    return results;
}

static Relids predpush_candidates(PlannerInfo *root, int dest_id)
{
    HintState *hstate = root->parse->hintState;
    if (hstate == NULL)
        return NULL;

    if (hstate->predpush_hint == NULL)
        return NULL;

    ListCell *lc = NULL;
    Relids result = NULL;
    foreach (lc, hstate->predpush_hint) {
        PredpushHint *predpushHint = (PredpushHint*)lfirst(lc);
        if (predpushHint->dest_id != 0) {
            RelOptInfo *rel = root->simple_rel_array[dest_id];
            int parent_rti = 0;
            if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL) {
                ListCell *lca = NULL;
                AppendRelInfo *appinfo = NULL;
                foreach(lca, root->append_rel_list) {
                    appinfo = (AppendRelInfo *) lfirst(lca);

                    /* find rti */
                    if (appinfo->child_relid == (Index)dest_id) {
                        parent_rti = appinfo->parent_relid;
                        break;
                    }
                }
            }

            if (predpushHint->dest_id == dest_id ||
                predpushHint->dest_id == parent_rti) {
                result = bms_union(predpushHint->candidates,
                                   predpush_candidates_append(root, predpushHint->candidates));
                return result;
            }
        } else {
            result = bms_union(result, predpushHint->candidates);
        }
    }

    if (result != NULL)
        result = bms_union(result, predpush_candidates_append(root, result));

    return result;
}

static List *extract_predpush_equivclause(PlannerInfo* root, Relids candidates,
                                        RelOptInfo* rel, Index rti)
{
    List *candidate_restricts = NIL;

    /* ec */
    for (int i = 1; i < root->simple_rel_array_size; i++)
    {
        RelOptInfo *other = root->simple_rel_array[i];

        if (other == NULL || other->reloptkind != RELOPT_BASEREL) {
            continue;
        }

        if (candidates != NULL && !bms_is_member(i, candidates)) {
            continue;
        }

        if (!ENABLE_PRED_PUSH_FORCE(root) && (i > (int)rti)) {
            continue;
        }

        /* pointer compare */
        if (other == rel) {
            continue;
        }

        List * restrict_list = NIL;
        Bitmapset *join_relids = NULL;
        if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL) {
            ListCell *lc = NULL;
            int parent_rti = 0;
            AppendRelInfo *appinfo = NULL;
            foreach(lc, root->append_rel_list) {
                appinfo = (AppendRelInfo *) lfirst(lc);

                /* find rti */
                if (appinfo->child_relid == rti) {
                    parent_rti = appinfo->parent_relid;
                    break;
                }
            }
            RelOptInfo *parent_rel = find_base_rel(root, parent_rti);
            join_relids = bms_union(other->relids, parent_rel->relids);

            restrict_list = generate_join_implied_equalities(root, join_relids,
                    other->relids,
                    parent_rel);

            restrict_list = (List*)adjust_appendrel_attrs(root, (Node*)restrict_list, appinfo);
        } else {
            join_relids = bms_union(other->relids, rel->relids);
            restrict_list = generate_join_implied_equalities(root,
                    join_relids,
                    other->relids,
                    rel);
        }

        if (restrict_list == NIL)
            continue;

        candidate_restricts = list_concat(candidate_restricts, restrict_list);
    }

    return candidate_restricts;
}

/*
 * pred pushdown
 */
static bool predpush_subquery(PlannerInfo* root, RelOptInfo* rel, Index rti,
                                RangeTblEntry *rte, Query *subquery,
                                bool* unsafeColumns)
{
    bool predpush = false;
    List *candidate_restricts = NIL;

    /* process hints */
    Relids candidates = predpush_candidates(root, rti);

    if (ENABLE_PRED_PUSH_FORCE(root) && candidates == NULL) {
        return false;
    }

    candidate_restricts = extract_predpush_equivclause(root, candidates, rel, rti);

    /* extarct on join info */
    ListCell *jr = NULL;
    List *joininfo = NULL;
    foreach (jr, rel->joininfo) {
        int other_relid = 0;
        RestrictInfo *ri = (RestrictInfo *)lfirst(jr);
        if (!ri->can_join) {
            joininfo = lappend(joininfo, ri);
            continue;
        }
        if (!isEqualExpr((Node *)(ri->clause))) {
            joininfo = lappend(joininfo, ri);
            continue;
        }

        if (bms_equal(ri->left_relids, rel->relids)) {
            if (bms_num_members(ri->right_relids) != 1)
            {
                joininfo = lappend(joininfo, ri);
                continue;
            }

            other_relid = bms_singleton_member(ri->right_relids);
        }

        if (bms_equal(ri->right_relids, rel->relids)) {
            if (bms_num_members(ri->left_relids) != 1)
            {
                joininfo = lappend(joininfo, ri);
                continue;
            }

            other_relid = bms_singleton_member(ri->left_relids);
        }

        if (candidates != NULL && !bms_is_member(other_relid, candidates))
        {
            joininfo = lappend(joininfo, ri);
            continue;
        }

        Relids current_and_outer = bms_copy(rel->relids);
        current_and_outer = bms_add_member(current_and_outer, other_relid);
        if (!join_clause_is_movable_into(ri, rel->relids, current_and_outer))
        {
            joininfo = lappend(joininfo, ri);
            continue;
        }

        /*
         * To avoid loss of clauses, we check it in advance, there are redundant
         * check behind, but it is ok.
         */
        if (!can_push_qual_into_subquery(root, ri, rte, rti, (Node *)ri->clause, unsafeColumns) ||
            check_func((Node *)ri->clause)) {
            joininfo = lappend(joininfo, ri);
            continue;
        }

        candidate_restricts = lappend(candidate_restricts, ri);
    }

    rel->joininfo = joininfo;

    /* try to pushdown the predicates */
    ListCell *l = NULL;
    List *cannot_pushdown = NULL;
    Relids required_relids = NULL;
    RestrictInfo *rinfo = NULL;
    foreach (l, candidate_restricts) {
        rinfo = (RestrictInfo*)lfirst(l);
        Node* clause = (Node*)rinfo->clause;

        if (can_push_qual_into_subquery(root, rinfo, rte, rti, clause, unsafeColumns) &&
            !check_func(clause)) {
            /* Mark subquery could predpush */
            predpush = true;

            /* find the lateral vars */
            collect_lateral_vars(root, clause, rel);

            /* Push it down */
            subquery_push_qual(subquery, rte, rti, clause, 1);
            required_relids = bms_union(required_relids, rinfo->required_relids);
        } else {
            cannot_pushdown = lappend(cannot_pushdown, rinfo);
        }
    }

    /* avoid loss of clauses */
    if (cannot_pushdown != NULL && required_relids != NULL) {
        l = NULL;
        rinfo = NULL;
        foreach (l, cannot_pushdown) {
            rinfo = (RestrictInfo*)lfirst(l);
            if (bms_is_subset(rinfo->required_relids, required_relids)) {
                rel->joininfo = lappend(rel->joininfo, rinfo);
            }
        }
    }
    return predpush;
}

static bool judge_predpush_subquery(PlannerInfo* root, bool safe_pushdown, RelOptInfo* rel,
                                        Index rti, RangeTblEntry *rte)
{
    if (!safe_pushdown) {
        return false;
    }

    if (!(rel->reloptkind == RELOPT_BASEREL || rel->reloptkind == RELOPT_OTHER_MEMBER_REL)) {
        return false;
    }

    if (root->hasRecursion || root->is_under_recursive_cte) {
        return false;
    }

    if (!ENABLE_PRED_PUSH_ALL(root)) {
        return false;
    }

    if (!(rte == root->simple_rte_array[rti])) {
        return false;
    }

    if (root->join_null_info != NIL) {
        return false;
    }

    if (!check_stream_support()) {
        return false;
    }

    if (SUBQUERY_IS_SUBLINK(root) && rel->subplanrestrictinfo != NULL) {
        return false;
    }

    return true;
}

/*
 * set_subquery_pathlist
 *		Build the (single) access path for a subquery RTE
 *
 * There's no need for a separate set_subquery_size phase, since we don't
 * support parameterized paths for subqueries.
 */
static void set_subquery_pathlist(PlannerInfo* root, RelOptInfo* rel, Index rti, RangeTblEntry *rte)
{
    Query* subquery = rte->subquery;

    bool* unsafeColumns = NULL;
    bool safe_pushdown = false;
    bool safe_predpush = false;

    /*
     * Here we have finished prep push down in place then we should copy it for next subquery_planner.
     */
    subquery = (Query*)copyObject(subquery);

    /*
     * We need a workspace for keeping track of unsafe-to-reference columns.
     * unsafeColumns[i] is set TRUE if we've found that output column i of the
     * subquery is unsafe to use in a pushed-down qual.
     */
    unsafeColumns = (bool*)palloc0((list_length(subquery->targetList) + 1) * sizeof(bool));

    if (rel->baserestrictinfo != NIL || ENABLE_PRED_PUSH_ALL(root))
        safe_pushdown = subquery_is_pushdown_safe(subquery, subquery, unsafeColumns);

    /*
     * Create the param-path for subquery
     * if want predpush
     * 1. reloptkind only be RELOPT_BASEREL and RELOPT_OTHER_MEMBER_REL
     * 2. do not support rewrite rte on upper level
     */
    if (judge_predpush_subquery(root, safe_pushdown, rel, rti, rte))
    {
        safe_predpush = predpush_subquery(root, rel, rti, rte, subquery, unsafeColumns);
    }

    /*
     * If there are any restriction clauses that have been attached to the
     * subquery relation, consider pushing them down to become WHERE or HAVING
     * quals of the subquery itself.  This transformation is useful because it
     * may allow us to generate a better plan for the subquery than evaluating
     * all the subquery output rows and then filtering them.
     *
     * There are several cases where we cannot push down clauses. Restrictions
     * involving the subquery are checked by subquery_is_pushdown_safe().
     * Restrictions on individual clauses are checked by
     * qual_is_pushdown_safe().  Also, we don't want to push down
     * pseudoconstant clauses; better to have the gating node above the
     * subquery.
     *
     * Also, if the sub-query has "security_barrier" flag, it means the
     * sub-query originated from a view that must enforce row-level security.
     * We must not push down quals in order to avoid information leaks, either
     * via side-effects or error output.
     *
     * Non-pushed-down clauses will get evaluated as qpquals of the
     * SubqueryScan node.
     *
     * XXX Are there any cases where we want to make a policy decision not to
     * push down a pushable qual, because it'd result in a worse plan?
     */
    List* upperrestrictlist = NIL;
    ListCell* l = NULL;
    if (safe_pushdown && rel->baserestrictinfo != NIL)
    {
        foreach (l, rel->baserestrictinfo) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);
            Node* clause = (Node*)rinfo->clause;

            if (can_push_qual_into_subquery(root, rinfo, rte, rti, clause, unsafeColumns)) {
                /* Push it down */
                subquery_push_qual(subquery, rte, rti, clause);
            } else {
                /* Keep it in the upper query */
                upperrestrictlist = lappend(upperrestrictlist, rinfo);
            }
        }
        rel->baserestrictinfo = upperrestrictlist;
    }

    if (ENABLE_PRED_PUSH_FORCE(root) &&
        !SUBQUERY_IS_SUBLINK(root) &&
        safe_pushdown && rel->subplanrestrictinfo != NIL)
    {
        /* OK to consider pushing down individual quals */
        upperrestrictlist = NIL;
        l = NULL;

        foreach (l, rel->subplanrestrictinfo) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);
            Node* clause = (Node*)rinfo->clause;

            if (can_push_qual_into_subquery(root, rinfo, rte, rti, clause, unsafeColumns)) {
                /* Push it down */
                subquery_push_qual(subquery, rte, rti, clause);
                safe_predpush = true;
            } else {
                /* Keep it in the upper query */
                upperrestrictlist = lappend(upperrestrictlist, rinfo);
            }
        }
        rel->subplanrestrictinfo = upperrestrictlist;
    }

    int IS_SUBLINK = (root->subquery_type & SUBQUERY_SUBLINK);
    if (!ENABLE_PRED_PUSH_ALL(root) || !safe_predpush) {
        set_subquery_path(root, rel, rti, subquery, (SUBQUERY_NORMAL | IS_SUBLINK));
    } else if (ENABLE_PRED_PUSH_FORCE(root)) {
        set_subquery_path(root, rel, rti, subquery, (SUBQUERY_PARAM | IS_SUBLINK));
    } else if (ENABLE_PRED_PUSH_NORMAL(root)) {
        set_subquery_path(root, rel, rti, subquery, (SUBQUERY_RESULT | IS_SUBLINK));
    } else {
        Query* param_subquery = NULL;
        RelOptInfo* new_rel = NULL;
        param_subquery = (Query *) copyObject(subquery);
        new_rel = build_alternative_rel(rel, RTE_SUBQUERY);

        set_subquery_path(root, rel, rti, subquery, (SUBQUERY_RESULT | IS_SUBLINK));

        Assert(param_subquery != NULL);
        Assert(new_rel != NULL);
        MemoryContext oldcxt = CurrentMemoryContext;
        bool old_stream_support = u_sess->opt_cxt.is_stream_support;
        PG_TRY();
        {
            rel->alternatives = lappend(rel->alternatives, new_rel);
            set_subquery_path(root, new_rel, rti, param_subquery, (SUBQUERY_PARAM | IS_SUBLINK));
            rel->pathlist = list_concat(rel->pathlist, new_rel->pathlist);
        }
        PG_CATCH();
        {
            rel->alternatives = list_delete_ptr(rel->alternatives, new_rel);
            MemoryContextSwitchTo(oldcxt);
            FlushErrorState();
            root->plan_params = NULL;
            u_sess->opt_cxt.is_stream_support = old_stream_support;
        }
        PG_END_TRY();
    }

    pfree_ext(unsafeColumns);
}

/*
 * set_subquery_path
 *
 * NOTE that the subquery may not equal to the rte->subquery
 */
static void
set_subquery_path(PlannerInfo *root, RelOptInfo *rel,
                  Index rti, Query *subquery, int options)
{
    Query* parse = root->parse;
    double tuple_fraction;
    PlannerInfo* subroot = NULL;
    List* pathkeys = NIL;

    /*
     * We can safely pass the outer tuple_fraction down to the subquery if the
     * outer level has no joining, aggregation, or sorting to do. Otherwise
     * we'd better tell the subquery to plan for full retrieval. (XXX This
     * could probably be made more intelligent ...)
     */
    if (parse->hasAggs || parse->groupClause || parse->groupingSets || parse->havingQual || parse->distinctClause ||
        parse->sortClause || has_multiple_baserels(root))
        tuple_fraction = 0.0; /* default case */
    else
        tuple_fraction = root->tuple_fraction;

    /* plan_params should not be in use in current query level */
    AssertEreport(root->plan_params == NIL, MOD_OPT, "");

    /* Generate the plan for the subquery */
    rel->subplan = subquery_planner(
        root->glob, subquery, root, false, tuple_fraction, &subroot, options, &rel->rel_dis_keys, rel->baserestrictinfo);
    rel->subroot = subroot;
    /* Isolate the params needed by this specific subplan */
    rel->subplan_params = root->plan_params;
    root->plan_params = NIL;

    /*
     * It's possible that constraint exclusion proved the subquery empty. If
     * so, it's convenient to turn it back into a dummy path so that we will
     * recognize appropriate optimizations at this level.
     */
    if (is_dummy_plan(rel->subplan)) {
        set_dummy_rel_pathlist(rel);
        return;
    }

    /* Mark rel with estimated output rows, width, etc */
    if (rel->base_rel == NULL) {
        set_subquery_size_estimates(root, rel);
    }

    adjust_rows_according_to_hint(root->parse->hintState, rel);

    /* Convert subquery pathkeys to outer representation */
    pathkeys = convert_subquery_pathkeys(root, rel, subroot->query_pathkeys);

    /* Generate appropriate path */
    add_path(root, rel, create_subqueryscan_path(root, rel, pathkeys, rel->lateral_relids,rel->subplan_params));

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
    return;
}

/*
 * set_function_pathlist
 *		Build the (single) access path for a function RTE
 */
static void set_function_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Relids required_outer;

    /*
     * If it's a LATERAL function, it might contain some Vars of the current
     * query level, requiring it to be treated as parameterized.
     */
    required_outer = rel->lateral_relids;

    /* Generate appropriate path */
    add_path(root, rel, create_functionscan_path(root, rel, required_outer));

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
}

/*
 * set_values_pathlist
 *		Build the (single) access path for a VALUES RTE
 */
static void set_values_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Relids required_outer = NULL;

    required_outer = rel->lateral_relids;

    /* Generate appropriate path */
    add_path(root, rel, create_valuesscan_path(root, rel, required_outer));

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
}

/*
 * set_cte_pathlist
 *		Build the (single) access path for a non-self-reference CTE RTE
 *
 * There's no need for a separate set_cte_size phase, since we don't
 * support parameterized paths for CTEs.
 */
static void set_cte_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Plan* cteplan = NULL;
    PlannerInfo* cteroot = NULL;
    Index levelsup;
    int ndx;
    ListCell* lc = NULL;
    int plan_id;

    /*
     * Find the referenced CTE, and locate the plan previously made for it.
     */
    levelsup = rte->ctelevelsup;
    cteroot = root;
    while (levelsup-- > 0) {
        cteroot = cteroot->parent_root;
        if (cteroot == NULL) { /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("bad levelsup for CTE \"%s\" when set cte pathlist", rte->ctename)));
        }
    }

    /*
     * Note: cte_plan_ids can be shorter than cteList, if we are still working
     * on planning the CTEs (ie, this is a side-reference from another CTE).
     * So we mustn't use forboth here.
     */
    ndx = 0;
    foreach (lc, cteroot->parse->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

        if (strcmp(cte->ctename, rte->ctename) == 0)
            break;
        ndx++;
    }

    if (lc == NULL) {  /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("could not find CTE \"%s\" when set cte pathlist", rte->ctename)));
    }
    if (ndx >= list_length(cteroot->cte_plan_ids)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("could not find plan for CTE \"%s\" when set cte pathlist", rte->ctename)));
    }
    plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
    AssertEreport(plan_id > 0, MOD_OPT, "");
    cteplan = (Plan*)list_nth(root->glob->subplans, plan_id - 1);

    /* Mark rel with estimated output rows, width, etc */
    set_cte_size_estimates(root, rel, cteplan);

    /* Inherit the locale info */
    if (STREAM_RECURSIVECTE_SUPPORTED && is_hashed_plan(cteplan) && IsA(cteplan, RecursiveUnion)) {
        List* distribute_index = distributeKeyIndex(cteroot, cteplan->distributed_keys, cteplan->targetlist);

        if (NIL != distribute_index) {
            ListCell* cell1 = NULL;
            ListCell* cell2 = NULL;

            foreach (cell1, distribute_index) {
                bool found = false;
                int resno = lfirst_int(cell1);

                foreach (cell2, rel->reltargetlist) {
                    Var* relvar = locate_distribute_var((Expr*)lfirst(cell2));

                    if (relvar != NULL && relvar->varattno == resno) {
                        found = true;
                        rel->distribute_keys = lappend(rel->distribute_keys, relvar);
                        break;
                    }
                }

                if (found == false) {
                    list_free_ext(rel->distribute_keys);
                    break;
                }
            }
        }
    } else {
        rel->distribute_keys = cteplan->distributed_keys;
    }

    if (cteplan->exec_nodes != NULL) {
        rel->locator_type = cteplan->exec_nodes->baselocatortype;
    }

    /* Generate appropriate path */
    add_path(root, rel, create_ctescan_path(root, rel));

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
}

/*
 * set_result_pathlist
 *             Build the (single) access path for an RTE_RESULT RTE
 *
 * There's no need for a separate set_result_size phase, since we
 * don't support join-qual-parameterized paths for these RTEs.
 */
static void set_result_pathlist(PlannerInfo *root, RelOptInfo *rel,
    RangeTblEntry *rte)
{
    Relids          required_outer;

    /* Mark rel with estimated output rows, width, etc */
    set_result_size_estimates(root, rel);

    /*
     * We don't support pushing join clauses into the quals of a Result scan,
     * but it could still have required parameterization due to LATERAL refs
     * in its tlist.
     */
    required_outer = rel->lateral_relids;

    /* Generate appropriate path */
    add_path(root, rel, create_resultscan_path(root, rel, required_outer));

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
}

/*
 * set_worktable_pathlist
 *		Build the (single) access path for a self-reference CTE RTE
 *
 * There's no need for a separate set_worktable_size phase, since we don't
 * support parameterized paths for CTEs.
 */
static void set_worktable_pathlist(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Plan* cteplan = NULL;
    PlannerInfo* cteroot = NULL;
    Index levelsup;

    /*
     * We need to find the non-recursive term's plan, which is in the plan
     * level that's processing the recursive UNION, which is one level *below*
     * where the CTE comes from.
     */
    levelsup = rte->ctelevelsup;
    if (levelsup == 0) { /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("bad levelsup for CTE \"%s\" when set worktable pathlist", rte->ctename)));
    }
    levelsup--;
    cteroot = root;
    while (levelsup-- > 0) {
        cteroot = cteroot->parent_root;
        if (cteroot == NULL) { /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("bad levelsup for CTE \"%s\" when set worktable pathlist", rte->ctename)));
        }
    }
    cteplan = cteroot->non_recursive_plan;
    if (cteplan == NULL) /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("could not find plan for CTE \"%s\" when set worktable pathlist", rte->ctename))));

    /* Mark rel with estimated output rows, width, etc */
    set_cte_size_estimates(root, rel, cteplan);

    /* Generate appropriate path */
    add_path(root, rel, create_worktablescan_path(root, rel));

    /* Select cheapest path (pretty easy in this case...) */
    set_cheapest(rel);
}

/*
 * make_rel_from_joinlist
 *	  Build access paths using a "joinlist" to guide the join path search.
 *
 * See comments for deconstruct_jointree() for definition of the joinlist
 * data structure.
 */
static RelOptInfo* make_rel_from_joinlist(PlannerInfo* root, List* joinlist)
{
    int levels_needed;
    List* initial_rels = NIL;
    ListCell* jl = NULL;

    /*
     * Count the number of child joinlist nodes.  This is the depth of the
     * dynamic-programming algorithm we must employ to consider all ways of
     * joining the child nodes.
     */
    levels_needed = list_length(joinlist);
    if (levels_needed <= 0)
        return NULL; /* nothing to do? */

    /*
     * Construct a list of rels corresponding to the child joinlist nodes.
     * This may contain both base rels and rels constructed according to
     * sub-joinlists.
     */
    initial_rels = NIL;
    foreach (jl, joinlist) {
        Node* jlnode = (Node*)lfirst(jl);
        RelOptInfo* thisrel = NULL;

        if (IsA(jlnode, RangeTblRef)) {
            int varno = ((RangeTblRef*)jlnode)->rtindex;

            thisrel = find_base_rel(root, varno);
        } else if (IsA(jlnode, List)) {
            /* Recurse to handle subproblem */
            thisrel = make_rel_from_joinlist(root, (List*)jlnode);
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    (errmsg("unrecognized joinlist node type when build access paths by joinlist: %d",
                        (int)nodeTag(jlnode)))));
            thisrel = NULL; /* keep compiler quiet */
        }

        initial_rels = lappend(initial_rels, thisrel);
    }

    if (levels_needed == 1) {
        /*
         * Single joinlist node, so we're done.
         */
        return (RelOptInfo*)linitial(initial_rels);
    } else {
        /*
         * Consider the different orders in which we could join the rels,
         * using a plugin, GEQO, or the regular join search code.
         *
         * We put the initial_rels list into a PlannerInfo field because
         * has_legal_joinclause() needs to look at it (ugly :-().
         */
        root->initial_rels = initial_rels;

        if (u_sess->attr.attr_sql.enable_geqo && levels_needed >= u_sess->attr.attr_sql.geqo_threshold)
            return geqo(root, levels_needed, initial_rels);
        else
            return standard_join_search(root, levels_needed, initial_rels);
    }
}

/*
 * standard_join_search
 *	  Find possible joinpaths for a query by successively finding ways
 *	  to join component relations into join relations.
 *
 * 'levels_needed' is the number of iterations needed, ie, the number of
 *		independent jointree items in the query.  This is > 1.
 *
 * 'initial_rels' is a list of RelOptInfo nodes for each independent
 *		jointree item.	These are the components to be joined together.
 *		Note that levels_needed == list_length(initial_rels).
 *
 * Returns the final level of join relations, i.e., the relation that is
 * the result of joining all the original relations together.
 * At least one implementation path must be provided for this relation and
 * all required sub-relations.
 *
 * To support loadable plugins that modify planner behavior by changing the
 * join searching algorithm, we provide a hook variable that lets a plugin
 * replace or supplement this function.  Any such hook must return the same
 * final join relation as the standard code would, but it might have a
 * different set of implementation paths attached, and only the sub-joinrels
 * needed for these paths need have been instantiated.
 *
 * Note to plugin authors: the functions invoked during standard_join_search()
 * modify root->join_rel_list and root->join_rel_hash.	If you want to do more
 * than one join-order search, you'll probably need to save and restore the
 * original states of those data structures.  See geqo_eval() for an example.
 */
RelOptInfo* standard_join_search(PlannerInfo* root, int levels_needed, List* initial_rels)
{
    int lev;
    RelOptInfo* rel = NULL;

    /*
     * This function cannot be invoked recursively within any one planning
     * problem, so join_rel_level[] can't be in use already.
     */
    AssertEreport(root->join_rel_level == NULL, MOD_OPT, "");

    /*
     * We employ a simple "dynamic programming" algorithm: we first find all
     * ways to build joins of two jointree items, then all ways to build joins
     * of three items (from two-item joins and single items), then four-item
     * joins, and so on until we have considered all ways to join all the
     * items into one rel.
     *
     * root->join_rel_level[j] is a list of all the j-item rels.  Initially we
     * set root->join_rel_level[1] to represent all the single-jointree-item
     * relations.
     */
    root->join_rel_level = (List**)palloc0((levels_needed + 1) * sizeof(List*));

    root->join_rel_level[1] = initial_rels;

    for (lev = 2; lev <= levels_needed; lev++) {
        ListCell* lc = NULL;

        /*
         * Determine all possible pairs of relations to be joined at this
         * level, and build paths for making each one from every available
         * pair of lower-level relations.
         */
        join_search_one_level(root, lev);

        /*
         * Do cleanup work on each just-processed rel.
         */
        foreach (lc, root->join_rel_level[lev]) {
            rel = (RelOptInfo*)lfirst(lc);

            /* add joinrel cn gather path when gather hint switch on */
            if (IS_STREAM_PLAN && permit_gather(root, HINT_GATHER_JOIN)) {
                CreateGatherPaths(root, rel, true);
            }

            /* Find and save the cheapest paths for this rel */
            set_cheapest(rel, root);

            debug1_print_rel(root, rel);

#ifdef OPTIMIZER_DEBUG
            debug_print_rel(root, rel);
#endif
        }
    }

    /*
     * We should have a single rel at the final level.
     */
    if (root->join_rel_level[levels_needed] == NIL) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "failed to build joins way in standard join search");
        securec_check_ss_c(sprintf_rc, "\0", "\0");

        mark_stream_unsupport();
    }

    AssertEreport(list_length(root->join_rel_level[levels_needed]) == 1, MOD_OPT, "");

    rel = (RelOptInfo*)linitial(root->join_rel_level[levels_needed]);

    root->join_rel_level = NULL;

    return rel;
}

/*****************************************************************************
 *			PUSHING QUALS DOWN INTO SUBQUERIES
 *****************************************************************************/

static bool window_function_is_pushdown_safe(Query* subquery, Query* topquery, bool* unsafeColumns)
{
    if (subquery->groupClause != NIL) {
        return false;
    }

    /* only partiton by overlaps is allowed to pushdown */
    ListCell *lc = NULL;
    ListCell *lc2 = NULL;
    Bitmapset *sgrefs = NULL;

    foreach(lc, subquery->windowClause) {
        Bitmapset *localrefs = NULL;
        WindowClause *wc = (WindowClause *) lfirst(lc);
    
        foreach(lc2, wc->partitionClause) {
            SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc2);
            localrefs = bms_add_member(localrefs, (int)(sortcl->tleSortGroupRef));
        }
    
        if (lc == list_head(subquery->windowClause)) {
            sgrefs = localrefs;
        } else {
            sgrefs = bms_intersect(sgrefs, localrefs);
            bms_free(localrefs);
        }
    
        if (bms_is_empty(sgrefs)) {
            return false;
        }
    }

    foreach(lc, subquery->targetList) {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);
    
        if (tle->resjunk) {
            continue;
        }
    
        if (!bms_is_member((int)(tle->ressortgroupref), sgrefs)) {
            unsafeColumns[tle->resno] = true;
        }
    }

    return true;
}

/*
 * subquery_is_pushdown_safe - is a subquery safe for pushing down quals?
 *
 * subquery is the particular component query being checked.  topquery
 * is the top component of a set-operations tree (the same Query if no
 * set-op is involved).
 *
 * Conditions checked here:
 *
 * 1. If the subquery has a LIMIT clause, we must not push down any quals,
 * since that could change the set of rows returned.
 *
 * 2. If the subquery contains any window functions, we can't push quals
 * into it, because that could change the results.
 *
 * 3. If the subquery contains EXCEPT or EXCEPT ALL set ops we cannot push
 * quals into it, because that could change the results.
 *
 * In addition, we make several checks on the subquery's output columns
 * to see if it is safe to reference them in pushed-down quals.  If output
 * column k is found to be unsafe to reference, we set unsafeColumns[k] to
 * TRUE, but we don't reject the subquery overall since column k might
 * not be referenced by some/all quals.  The unsafeColumns[] array will be
 * consulted later by qual_is_pushdown_safe(). It's better to do it this
 * way than to make the checks directly in qual_is_pushdown_safe(), because
 * when the subquery involves set operations we have to check the output
 * expressions in each arm of the set op.
 */
static bool subquery_is_pushdown_safe(Query* subquery, Query* topquery, bool* unsafeColumns)
{
    SetOperationStmt* topop = NULL;

    /* Check point 1 */
    if (subquery->limitOffset != NULL || subquery->limitCount != NULL) {
        return false;
    }

    /* Check point 2 */
    if (subquery->hasWindowFuncs &&
        !window_function_is_pushdown_safe(subquery, topquery, unsafeColumns)) {
        return false;
    }

    /*
     * If we're at a leaf query, check for unsafe expressions in its target
     * list, and mark any unsafe ones in unsafeColumns[].  (Non-leaf nodes in
     * setop trees have only simple Vars in their tlists, so no need to check
     * them.)
     */
    if (subquery->setOperations == NULL) {
        check_output_expressions(subquery, unsafeColumns);
    }

    /* Are we at top level, or looking at a setop component? */
    if (subquery == topquery) {
        /* Top level, so check any component queries */
        if (subquery->setOperations != NULL)
            if (!recurse_pushdown_safe(subquery->setOperations, topquery, unsafeColumns))
                return false;
    } else {
        /* Setop component must not have more components (too weird) */
        if (subquery->setOperations != NULL) {
            return false;
        }
        /* Check whether setop component output types match top level */
        topop = (SetOperationStmt*)topquery->setOperations;
        AssertEreport(topop && IsA(topop, SetOperationStmt), MOD_OPT, "");
        compare_tlist_datatypes(subquery->targetList, topop->colTypes, unsafeColumns);
    }
    return true;
}

/*
 * Helper routine to recurse through setOperations tree
 */
static bool recurse_pushdown_safe(Node* setOp, Query* topquery, bool* unsafeColumns)
{
    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        RangeTblEntry* rte = rt_fetch(rtr->rtindex, topquery->rtable);
        Query* subquery = rte->subquery;

        AssertEreport(subquery != NULL, MOD_OPT, "");
        return subquery_is_pushdown_safe(subquery, topquery, unsafeColumns);
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;

        /* Else recurse */
        if (!recurse_pushdown_safe(op->larg, topquery, unsafeColumns))
            return false;
        if (!recurse_pushdown_safe(op->rarg, topquery, unsafeColumns))
            return false;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type when pushdown recurse through setOperations tree: %d",
                    (int)nodeTag(setOp))));
    }
    return true;
}

/*
 * check_output_expressions - check subquery's output expressions for safety
 *
 * There are several cases in which it's unsafe to push down an upper-level
 * qual if it references a particular output column of a subquery. We check
 * each output column of the subquery and set unsafeColumns[k] to TRUE if
 * that column is unsafe for a pushed-down qual to reference.  The conditions
 * checked here are:
 *
 * 1. We must not push down any quals that refer to subselect outputs that
 * return sets, else we'd introduce functions-returning-sets into the
 * subquery's WHERE/HAVING quals.
 *
 * 2. We must not push down any quals that refer to subselect outputs that
 * contain volatile functions, for fear of introducing strange results due
 * to multiple evaluation of a volatile function.
 *
 * 3. If the subquery uses DISTINCT ON, we must not push down any quals that
 * refer to non-DISTINCT output columns, because that could change the set
 * of rows returned.  (This condition is vacuous for DISTINCT, because then
 * there are no non-DISTINCT output columns, so we needn't check.  But note
 * we are assuming that the qual can't distinguish values that the DISTINCT
 * operator sees as equal. This is a bit shaky but we have no way to test
 * for the case, and it's unlikely enough that we shouldn't refuse the
 * optimization just because it could theoretically happen.)
 */
static void check_output_expressions(Query* subquery, bool* unsafeColumns)
{
    ListCell* lc = NULL;

    foreach (lc, subquery->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);

        if (tle->resjunk)
            continue; /* ignore resjunk columns */

        /* We need not check further if output col is already known unsafe */
        if (unsafeColumns[tle->resno])
            continue;

        /* Functions returning sets are unsafe (point 1) */
        if (expression_returns_set((Node*)tle->expr)) {
            unsafeColumns[tle->resno] = true;
            continue;
        }

        /* Volatile functions are unsafe (point 2) */
        if (contain_volatile_functions((Node*)tle->expr)) {
            unsafeColumns[tle->resno] = true;
            continue;
        }

        /* If subquery uses DISTINCT ON, check point 3 */
        if (subquery->hasDistinctOn && !targetIsInSortList(tle, InvalidOid, subquery->distinctClause)) {
            /* non-DISTINCT column, so mark it unsafe */
            unsafeColumns[tle->resno] = true;
            continue;
        }
    }
}

/*
 * For subqueries using UNION/UNION ALL/INTERSECT/INTERSECT ALL, we can
 * push quals into each component query, but the quals can only reference
 * subquery columns that suffer no type coercions in the set operation.
 * Otherwise there are possible semantic gotchas.  So, we check the
 * component queries to see if any of them have output types different from
 * the top-level setop outputs.  unsafeColumns[k] is set true if column k
 * has different type in any component.
 *
 * We don't have to care about typmods here: the only allowed difference
 * between set-op input and output typmods is input is a specific typmod
 * and output is -1, and that does not require a coercion.
 *
 * tlist is a subquery tlist.
 * colTypes is an OID list of the top-level setop's output column types.
 * unsafeColumns[] is the result array.
 */
static void compare_tlist_datatypes(List* tlist, List* colTypes, bool* unsafeColumns)
{
    ListCell* l = NULL;
    ListCell* colType = list_head(colTypes);

    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->resjunk)
            continue; /* ignore resjunk columns */
        if (colType == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_CASE_NOT_FOUND),
                    (errmsg("wrong number of tlist entries when compare a subquery targetlist datatypes"))));
        if (exprType((Node*)tle->expr) != lfirst_oid(colType))
            unsafeColumns[tle->resno] = true;
        colType = lnext(colType);
    }
    if (colType != NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("wrong number of tlist entries when compare a subquery targetlist datatypes"))));
}

/*
 * qual_is_pushdown_safe - is a particular qual safe to push down?
 *
 * qual is a restriction clause applying to the given subquery (whose RTE
 * has index rti in the parent query).
 *
 * Conditions checked here:
 *
 * 1. The qual must not contain any subselects (mainly because I'm not sure
 * it will work correctly: sublinks will already have been transformed into
 * subplans in the qual, but not in the subquery).
 *
 * 2. The qual must not refer to the whole-row output of the subquery
 * (since there is no easy way to name that within the subquery itself).
 *
 * 3. The qual must not refer to any subquery output columns that were
 * found to be unsafe to reference by subquery_is_pushdown_safe().
 */
static bool qual_is_pushdown_to_EXCEPT(PlannerInfo* root, Query* subquery,
            Index rti, Node* qual)
{
    Assert(subquery != NULL);

    /* check for EXCEPT operators*/
    if (subquery->setOperations == NULL) {
        return true;
    }

    SetOperationStmt* op = (SetOperationStmt*)subquery->setOperations;

    /* EXCEPT is no good */
    if (op->op != SETOP_EXCEPT)
        return true;

    if (!IsA(qual, OpExpr)) {
        return false;
    }

    List* args = ((OpExpr*)qual)->args;
    Node* left_arg = (Node *) linitial(args);
    Node* right_arg = (Node *) lsecond(args);

    Relids left_relids = pull_varnos(left_arg, 0, false);
    Relids right_relids = pull_varnos(right_arg, 0, false);

    if (bms_is_member(rti, left_relids) && bms_is_member(rti, right_relids)) {
        if (!is_var_node(left_arg) || !is_var_node(right_arg)) {
            return false;
        }
    } else if (bms_is_member(rti, left_relids)) {
        if (!is_var_node(left_arg)) {
            return false;
        }
    } else if (bms_is_member(rti, right_relids)) {
        if (!is_var_node(right_arg)) {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

static bool qual_is_pushdown_safe(PlannerInfo* root, Query* subquery, Index rti,
                            Node* qual, const bool* unsafeColumns, bool predpush)
{
    bool safe = true;
    List* vars = NIL;
    ListCell* vl = NULL;

    /* Refuse subselects (point 1) */
    if (contain_subplans(qual)) {
        return false;
    }
#ifndef ENABLE_MULTIPLE_NODES
    if(contain_volatile_functions(qual)) {
        return false;
    }
#endif
    /*
     * It would be unsafe to push down window function calls, but at least for
     * the moment we could never see any in a qual anyhow.	(The same applies
     * to aggregates, which we check for in pull_var_clause below.)
     */
    AssertEreport(!contain_window_function(qual), MOD_OPT, "");

    if (!qual_is_pushdown_to_EXCEPT(root, subquery, rti, qual))
        return false;

    /*
     * Examine all Vars used in clause; since it's a restriction clause, all
     * such Vars must refer to subselect output columns.
     */
    vars = pull_var_clause(qual, PVC_REJECT_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
    foreach (vl, vars) {
        Var* var = (Var*)lfirst(vl);

        /*
         * XXX Punt if we find any PlaceHolderVars in the restriction clause.
         * It's not clear whether a PHV could safely be pushed down, and even
         * less clear whether such a situation could arise in any cases of
         * practical interest anyway.  So for the moment, just refuse to push
         * down.
         */
        if (!IsA(var, Var)) {
            safe = false;
            break;
        }

        /* check cte in predpush. */
        if (predpush) {
            RelOptInfo *rel = root->simple_rel_array[var->varno];

            if (rel->rtekind == RTE_CTE) {
                safe = false;
                break;
            }
        }

        if (var->varno != rti)
            continue;

        AssertEreport(var->varattno >= 0, MOD_OPT, "");

        /* Check */
        if (var->varattno == 0) {
            safe = false;
            break;
        }

        /* Check point 3 */
        if (unsafeColumns[var->varattno]) {
            safe = false;
            break;
        }
    }

    list_free_ext(vars);

    return safe;
}

/*
 * subquery_push_qual - push down a qual that we have determined is safe
er*/
static void subquery_push_qual(Query* subquery, RangeTblEntry* rte, Index rti, Node* qual, int levelsup)
{
    if (subquery->setOperations != NULL) {
        /* if levelsup is 0 keep old behaviors, otherwise predpush keeps going. */
        if (levelsup != 0) {
            levelsup += 1;
        }
        /* Recurse to push it separately to each component query */
        recurse_push_qual(subquery->setOperations, subquery, rte, rti, qual, levelsup);
    } else {
        /*
         * We need to replace Vars in the qual (which must refer to outputs of
         * the subquery) with copies of the subquery's targetlist expressions.
         * Note that at this point, any uplevel Vars in the qual should have
         * been replaced with Params, so they need no work.
         *
         * This step also ensures that when we are pushing into a setop tree,
         * each component query gets its own copy of the qual.
         */
        if (levelsup == 0)
        {
            qual = ResolveNew(qual, rti, 0, rte, subquery->targetList, CMD_SELECT, 0, &subquery->hasSubLinks);
        }
        else
        {
            /* we only used it where push the predicate into subquery block */
            qual = trans_lateral_vars(subquery, rti, qual, levelsup);
        }

        /*
         * Now attach the qual to the proper place: normally WHERE, but if the
         * subquery uses grouping or aggregation, put it in HAVING (since the
         * qual really refers to the group-result rows).
         */
        if (subquery->hasAggs || subquery->groupClause || subquery->havingQual)
            subquery->havingQual = make_and_qual(subquery->havingQual, qual);
        else
            subquery->jointree->quals = make_and_qual(subquery->jointree->quals, qual);

        /*
         * We need not change the subquery's hasAggs or hasSublinks flags,
         * since we can't be pushing down any aggregates that weren't there
         * before, and we don't push down subselects at all.
         */
    }
}

/*
 * Helper routine to recurse through setOperations tree
 */
static void recurse_push_qual(Node* setOp, Query* topquery, RangeTblEntry* rte, Index rti, Node* qual, int filter)
{
    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        RangeTblEntry* subrte = rt_fetch(rtr->rtindex, topquery->rtable);
        Query* subquery = subrte->subquery;

        AssertEreport(subquery != NULL, MOD_OPT, "");
        subquery_push_qual(subquery, rte, rti, qual, filter);
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;
        recurse_push_qual(op->larg, topquery, rte, rti, qual, filter);
        recurse_push_qual(op->rarg, topquery, rte, rti, qual, filter);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type when recurse push qual through setOperations tree: %d",
                    (int)nodeTag(setOp))));
    }
}
/*
 * partIterator tries to inherit pathkeys from scan path
 *
 * Now we have done two things
 * 1. Build all access paths for partitioned relation
 * 2. Prune partitioned relation and know which partitions will be scaned
 *
 * We just use Partiterator to scan partitions from lowerboundary to upperboundary,
 * and we brutally believe that Partiterator's output is disordered，howerver we can
 * inherit pathkeys from scan path if the path's output is ordered by partiitonkeys, or
 * exactly the opposite. since partiitonkeys is ordered by ASC NULLS LAST
 *
 * Just refuse to inherit pathkeys if some index on selected partition is unusable
 *
 * Now we try to inherit pathkeys from scan path as following steps
 * 1) pruning result  <= 1, since we can treat it as an ordinary table scan,
 * 2) the path's output is is order by partkeys, or exactly the opposite.
 */
static void make_partiterator_pathkey(
    PlannerInfo* root, RelOptInfo* rel, Relation relation, PartIteratorPath* itrpath, List* pathkeys)
{
    int2vector* partitionKey = NULL;
    ListCell* pk_cell = NULL;
    ListCell* rt_ec_cell = NULL;
    IndexesUsableType usable_type;
    Oid indexOid = ((IndexPath*)itrpath->subPath)->indexinfo->indexoid;
    if (u_sess->attr.attr_sql.enable_hypo_index && ((IndexPath *)itrpath->subPath)->indexinfo->hypothetical) {
        /* hypothetical index does not support partition index unusable */
        usable_type = INDEXES_FULL_USABLE;
    } else {
        usable_type = eliminate_partition_index_unusable(indexOid, rel->pruning_result, NULL, NULL);
    }
    if (INDEXES_FULL_USABLE != usable_type) {
        /* some index partition is unusable */
        OPT_LOG(DEBUG2, "fail to inherit pathkeys since some index partition is unusable");
        return;
    }

    if (rel->partItrs <= 1) {
        /* inherit pathkeys if pruning result is <= 1 */
        itrpath->path.pathkeys = pathkeys;
        return;
    }

    partitionKey = ((RangePartitionMap*)relation->partMap)->partitionKey;
    pk_cell = (ListCell*)list_head(pathkeys);

    bool pk_state_init = false;  /* if all pathkey have same sort direction (ASC or DESC) */
    bool pk_nulls_first = false; /* if all pathkey have same NULLs sort direction  (FIRST or LAST) */
    int pk_strategy = InvalidStrategy;
    int partkey_index = 0;

    /*
     * check if partkeys' sort strategy is the same as pathkeys, or exactly the opposite
     *
     *		create table t(a int, b int, c text) partition by range(a, b)
     *
     * we can inherit pathkeys only as follwing query
     *		a) select * from t order by a,b,c;
     *		b) select * from t order by a,b;
     *		c) select * from t order by a;
     *		d) select * from t where a = 1 order by a, b;
     *		e) select * from t where a = 1 and b = 1 order by a,b,c;
     *		f) above case with opposite sort strategy
     *
     * skip ec_collation check since we check var info which contains collation info
     */
    for (partkey_index = 0; partkey_index < partitionKey->dim1; partkey_index++) {
        PathKey* pathkey = NULL;
        EquivalenceClass* pk_ec = NULL;
        ListCell* pk_em_cell = NULL;
        bool found_partkey = false;
        int partkey_varattno = partitionKey->values[partkey_index];

        if (!PointerIsValid(pk_cell)) {
            /* all pathkeys have been checked */
            break;
        }

        pathkey = (PathKey*)lfirst(pk_cell);
        pk_ec = pathkey->pk_eclass;

        if (pk_ec->ec_has_volatile) {
            /* refuse to inherit pathkeys if sortclause is a volatile expr */
            OPT_LOG(DEBUG2,
                "partiterator fails to inherit pathkeys since"
                " sortclauses contains volatile expr");
            return;
        }

        if (pk_ec->ec_collation != attnumCollationId(relation, partkey_varattno)) {
            OPT_LOG(DEBUG2,
                "partiterator fails to inherit pathkeys since sortclauses's"
                " collation mismatches corresponding partitonkey's collation");
            return;
        }

        foreach (pk_em_cell, pk_ec->ec_members) {
            EquivalenceMember* pk_em = (EquivalenceMember*)lfirst(pk_em_cell);
            Expr* pk_em_expr = pk_em->em_expr;

            /*
             * check current pathkey is current partitionkey
             * skip ec_below_outer_join check since
             */
            if (!bms_equal(pk_em->em_relids, rel->relids)) {
                /* skip if EquivalenceMember contains other table's column */
                continue;
            }

            if (pk_em->em_is_const) {
                OPT_LOG(DEBUG2,
                    "partiterator fails to inherit pathkeys since pathkey is"
                    " a const value which should never happen");
                return;
            }

            if (IsA(pk_em_expr, RelabelType)) {
                /*
                 * RelabelType represents a "dummy" type coercion between two
                 * binary-compatible datatypes. It is a no-op at runtime, we fetch
                 * inner expression to build index key Var, just as in ExecIndexBuildScanKeys
                 */
                pk_em_expr = ((RelabelType*)pk_em_expr)->arg;
            }

            if (!IsA(pk_em_expr, Var)) {
                /* skip if EquivalenceMember is not a var since partitionkey can be treat as a var */
                continue;
            }

            if (((Var*)pk_em_expr)->varattno == partkey_varattno) {
                /* EquivalenceMembers is equal to current partitionkey */
                found_partkey = true;

                if (pk_state_init) {
                    /*
                     * if we have found 1st pathkey and have record 1st pathkey's strategy( ASC
                     * or DESC, NULL FIRST or NULL LAST), just to check if current pathkey's strategy
                     * is equal to 1st pathkey's strategy
                     */
                    if (pk_nulls_first != pathkey->pk_nulls_first || pk_strategy != pathkey->pk_strategy) {
                        /* refuse to inherit pathkeys if it is not eqal */
                        OPT_LOG(DEBUG2,
                            "partiterator fails to inherit pathkeys since"
                            "pathkeys have different sort strategy");
                        return;
                    }
                } else {
                    /* record 1st pathkey's sort strategy */
                    pk_state_init = true;
                    pk_nulls_first = pathkey->pk_nulls_first;
                    pk_strategy = pathkey->pk_strategy;

                    if (pk_strategy != BTLessStrategyNumber && pk_strategy != BTGreaterStrategyNumber) {
                        /*
                         * refuse to inherit pathkeys if current sort strategy is nether ASC nor
                         * DESC, should never happen
                         */
                        OPT_LOG(DEBUG2,
                            "partiterator fails to inherit pathkeys since sort"
                            " strategy is nether ASC nor DESC");
                        return;
                    }

                    /*
                     * we do partitionkey comparison just as ''order by ASC NULL LAST'', and pathkey
                     * must follow the same rules. If sortcluase is ether ''ASC NULL FIRST'' or "DESC
                     * NULL LAST", just abandon pathkeys
                     */
                    if (!(pk_strategy == BTLessStrategyNumber && pk_nulls_first == false) &&
                        !(pk_strategy == BTGreaterStrategyNumber && pk_nulls_first == true)) {
                        OPT_LOG(DEBUG2,
                            "partiterator fails to inherit pathkeys since sort strategy is"
                            " nether ASC NULL LAST nor DESC DESC NULL FIRST");
                        return;
                    }
                }
            }
        }

        if (found_partkey) {
            /* find current partitionkey is equal to current pathkey */
            pk_cell = lnext(pk_cell);
            continue;
        }

        /*
         * if current partitionkey is unequal to current pathkey, check if current
         * partitionkey is equal to a const value, since 'order by col1, col2' is the
         * same as ''order by col1, const_value, col2'
         */
        foreach (rt_ec_cell, root->eq_classes) {
            EquivalenceClass* ec = (EquivalenceClass*)lfirst(rt_ec_cell);
            ListCell* rt_em_cell = NULL;

            if (ec->ec_has_volatile || ec->ec_below_outer_join || !ec->ec_has_const ||
                !bms_is_subset(rel->relids, ec->ec_relids)) {
                /*
                 * skip current EquivalenceMember if
                 * 1. ec_has_volatile = true which means it is a volatile expr
                 * 2. ec_has_const = false which means it is not a const value
                 * 3. doesnot contain current partitioned table
                 */
                continue;
            }

            foreach (rt_em_cell, ec->ec_members) {
                EquivalenceMember* rt_em = (EquivalenceMember*)lfirst(rt_em_cell);
                Expr* rt_em_expr = rt_em->em_expr;

                if (!bms_equal(rt_em->em_relids, rel->relids)) {
                    /* skip if EquivalenceMember contains other table's column */
                    continue;
                }

                if (IsA(rt_em_expr, RelabelType)) {
                    rt_em_expr = ((RelabelType*)rt_em_expr)->arg;
                }

                if (!IsA(rt_em_expr, Var)) {
                    continue;
                }

                if (((Var*)rt_em_expr)->varattno == partkey_varattno) {
                    /* find current partitionkey is equal to a const value */
                    found_partkey = true;
                    break;
                }
            }

            if (found_partkey) {
                /* current partkey is a const value */
                break;
            }
        }

        if (!found_partkey) {
            /*
             * refuse to inherit pathkeys if current partkey is nether a const value
             * nor equal to current pathkey
             */
            OPT_LOG(DEBUG2,
                "partiterator fails to inherit pathkeys since current"
                " partitionkey is nether a const value nor current pathkey");
            return;
        }
    }

    itrpath->path.pathkeys = pathkeys;

    /*
     * we will scan partition from HIGH Boundary to LOW Boundary if
     * pathkeys is "order by DESC NULLS FIRST", or scan partition
     * from LOW Boundary TO HIGH Boundary, we mark direction flag
     * here for iterator method
     */
    itrpath->direction = (BTLessStrategyNumber == pk_strategy ? ForwardScanDirection : BackwardScanDirection);
}

/*
 * Check scan path for partition table whether use global partition index
 */
bool CheckPathUseGlobalPartIndex(Path* path)
{
    if (path->pathtype == T_IndexScan || path->pathtype == T_IndexOnlyScan) {
        IndexPath* indexPath = (IndexPath*)path;
        if (indexPath->indexinfo->isGlobal) {
            return true;
        }
    } else if (path->pathtype == T_BitmapHeapScan) {
        BitmapHeapPath* bitmapHeapPath = (BitmapHeapPath*)path;
        if (CheckBitmapQualIsGlobalIndex(bitmapHeapPath->bitmapqual)) {
            return true;
        }
    } else {
        return false;
    }

    return false;
}

static Path* create_partiterator_path(PlannerInfo* root, RelOptInfo* rel, Path* path, Relation relation)
{
    Path* result = NULL;

    switch (path->pathtype) {
        case T_SeqScan:
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_IndexScan:
        case T_IndexOnlyScan: {
            PartIteratorPath* itrpath = makeNode(PartIteratorPath);

            itrpath->subPath = path;
            itrpath->path.pathtype = T_PartIterator;
            itrpath->path.parent = rel;
            itrpath->path.param_info = path->param_info;
            itrpath->path.pathkeys = NIL;
            itrpath->itrs = rel->partItrs;
            set_path_rows(&itrpath->path, path->rows, path->multiple);
            itrpath->path.startup_cost = path->startup_cost;
            itrpath->path.total_cost = path->total_cost;
            itrpath->path.dop = path->dop;

            /* scan parttition from lower boundary to upper boundary by default */
            itrpath->direction = ForwardScanDirection;

            if (NIL != path->pathkeys && (T_IndexOnlyScan == path->pathtype || T_IndexScan == path->pathtype)) {
                /* try to inherit pathkeys from IndexPath/IndexOnlyScan since only */
                make_partiterator_pathkey(root, rel, relation, itrpath, path->pathkeys);
            }

#ifdef STREAMPLAN
            if (IS_STREAM_PLAN)
                inherit_path_locator_info(&(itrpath->path), path);
#endif
            result = (Path*)itrpath;
        } break;
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type when create partiterator path: %d", (int)path->pathtype)));
        } break;
    }

    return result;
}

/*
 * try to add control operator PartIterator over scan operator, so we can
 * scan all selected partitions
 */
static void try_add_partiterator(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    ListCell* pathCell = NULL;
    Relation relation = NULL;

    if (false == rel->isPartitionedTable) {
        /* do nothing for non-partitioned table */
        return;
    }

    /* specified lock must have been obtained */
    relation = relation_open(rte->relid, NoLock);
    foreach (pathCell, rel->pathlist) {
        Path* path = (Path*)lfirst(pathCell);
        Path* itrPath = NULL;
        ListCell* ctPathCell = NULL;
        ListCell* cpPathCell = NULL;

        /* do not handle inlist2join path for Partition Table */
        if (path->parent->base_rel && T_SubqueryScan == path->pathtype) {
            Assert(path->parent->base_rel->alternatives != NIL);
            continue;
        }

        /* Use globa partition index */
        if (CheckPathUseGlobalPartIndex(path)) {
            continue;
        }

        itrPath = create_partiterator_path(root, rel, path, relation);

        /* replace entry in pathlist */
        lfirst(pathCell) = itrPath;

        if (path == rel->cheapest_startup_path) {
            /* replace cheapest_startup_path */
            rel->cheapest_startup_path = itrPath;
        }

        if (path == rel->cheapest_unique_path) {
            /* replace cheapest_unique_path */
            rel->cheapest_unique_path = itrPath;
        }

        /* replace entry in cheapest_total_path */
        foreach (ctPathCell, rel->cheapest_total_path) {
            if (lfirst(ctPathCell) == path) {
                lfirst(ctPathCell) = itrPath;
                break;
            }
        }

        /* replace entry in cheapest_parameterized_paths */
        foreach (cpPathCell, rel->cheapest_parameterized_paths) {
            /* we add cheapest total into cheapest_parameterized_paths in set_cheapest */
            if (lfirst(cpPathCell) == path) {
                lfirst(cpPathCell) = itrPath;
                break;
            }
        }
    }

    relation_close(relation, NoLock);
}

/*
 * passdown_itst_keys_to_rel:
 *		For single-table subquery, pass the interested keys to base rel,
 *		and later it can pass down to root of lower query level
 * Paramters:
 *	@in root: planner info of current query level
 *	@in brel: single-table subquery rel
 *	@in rte: corresponding range table entry of brel
 *	@in inherit_matching_key: if matching key should be passed down, only
 *			when matching key only comes from this table
 */
static void passdown_itst_keys_to_rel(
    PlannerInfo* root, RelOptInfo* brel, RangeTblEntry* rte, bool inherit_matching_key)
{
    ListCell* lc = NULL;

    /* for matching and superset key pass, there should only be one subquery rel */
    if (rte->rtekind != RTE_SUBQUERY)
        return;

    if (inherit_matching_key)
        brel->rel_dis_keys.matching_keys = root->dis_keys.matching_keys;

    brel->rel_dis_keys.superset_keys = build_superset_keys_for_rel(root, brel, NULL, NULL, JOIN_INNER);

    /* For inherit table (simple union all), we should pass keys down to all its children */
    if (rte->inh) {
        foreach (lc, root->append_rel_list) {
            AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(lc);
            int childRTindex;
            RelOptInfo* childrel = NULL;

            /* append_rel_list contains all append rels; ignore others */
            if (appinfo->parent_relid != brel->relid)
                continue;

            /* Re-locate the child RTE and RelOptInfo */
            childRTindex = appinfo->child_relid;
            childrel = root->simple_rel_array[childRTindex];
            /* only pass down for subqueries, or it will shared the same root as current */
            if (root->simple_rte_array[childRTindex]->rtekind == RTE_SUBQUERY)
                childrel->rel_dis_keys = brel->rel_dis_keys;
        }
    }
}

/*
 * is_single_baseresult_plan:
 *		check if the plan node is single baseresult without lefttree.
 * Paramters:
 *	@in plan: the plan node will be checked.
 */
bool is_single_baseresult_plan(Plan* plan)
{
    if (plan == NULL) {
        return false;
    }

    return IsA(plan, BaseResult) ? (plan->lefttree == NULL) : false;
}

/*****************************************************************************
 *			DEBUG SUPPORT
 *****************************************************************************/
#ifdef OPTIMIZER_DEBUG

static void print_relids(Relids relids, List* rtable)
{
    Relids tmprelids;
    int x;
    bool first = true;
    RangeTblEntry* rte = NULL;

    tmprelids = bms_copy(relids);
    while ((x = bms_first_member(tmprelids)) >= 0) {
        if (!first)
            printf(" ");
        printf("%d:", x);
        rte = rt_fetch(x, rtable);
        printf("%s ", rte->relname);
        first = false;
    }
    bms_free_ext(tmprelids);
}

static void print_restrictclauses(PlannerInfo* root, List* clauses)
{
    ListCell* l = NULL;

    foreach (l, clauses) {
        RestrictInfo* c = (RestrictInfo*)lfirst(l);

        print_expr((Node*)c->clause, root->parse->rtable);
        if (lnext(l))
            printf(", ");
    }
}

inline void print_tab(int indent)
{
    for (int i = 0; i < indent; i++)
        printf("\t");
}

static void print_path(PlannerInfo* root, Path* path, int indent)
{
    const char* ptype = NULL;
    bool join = false;
    Path* subpath = NULL;
    int i;

    ptype = nodeTagToString(path->pathtype);

    switch (path->pathtype) {
        case T_Material:
            subpath = ((MaterialPath*)path)->subpath;
            break;
        case T_Unique:
            subpath = ((UniquePath*)path)->subpath;
            break;
        case T_NestLoop:
            join = true;
            break;
        case T_MergeJoin:
            join = true;
            break;
        case T_HashJoin:
            join = true;
            break;
#ifdef STREAMPLAN
        case T_Stream: {
            ptype = StreamTypeToString(((StreamPath*)path)->type);
        } break;
#endif
        default:
            /* noop */
            break;
    }

    print_tab(indent);
    printf("%s", ptype);

    if (path->parent) {
        printf("(");
        print_relids(path->parent->relids, root->parse->rtable);
        printf(") rows=%.0f multiple = %lf", path->parent->rows, path->parent->multiple);
    }
    printf(" cost=%.2f..%.2f\n", path->startup_cost, path->total_cost);

    if (path->pathkeys) {
        print_tab(indent);
        printf("  pathkeys: ");
        print_pathkeys(path->pathkeys, root->parse->rtable);
    }

    if (join) {
        JoinPath* jp = (JoinPath*)path;
        
        print_tab(indent);
        printf("  clauses: ");
        print_restrictclauses(root, jp->joinrestrictinfo);
        printf("\n");

        if (IsA(path, MergePath)) {
            MergePath* mp = (MergePath*)path;

            print_tab(indent);
            printf("  sortouter=%d sortinner=%d materializeinner=%d\n",
                ((mp->outersortkeys) ? 1 : 0),
                ((mp->innersortkeys) ? 1 : 0),
                ((mp->materialize_inner) ? 1 : 0));
        }

        print_path(root, jp->outerjoinpath, indent + 1);
        print_path(root, jp->innerjoinpath, indent + 1);
    }

    if (subpath != NULL)
        print_path(root, subpath, indent + 1);
}

void debug_print_rel(PlannerInfo* root, RelOptInfo* rel)
{
    ListCell* l = NULL;

    printf("RELOPTINFO (");
    print_relids(rel->relids, root->parse->rtable);
    printf("): rows=%.0f multiple = %lf width=%d\n", rel->rows, rel->multiple, rel->width);

    if (rel->baserestrictinfo) {
        printf("\tbaserestrictinfo: ");
        print_restrictclauses(root, rel->baserestrictinfo);
        printf("\n");
    }

    if (rel->joininfo) {
        printf("\tjoininfo: ");
        print_restrictclauses(root, rel->joininfo);
        printf("\n");
    }

    printf("\tpath list:\n");
    foreach (l, rel->pathlist)
        print_path(root, (Path*)lfirst(l), 1);
    printf("\n\tcheapest startup path:\n");
    print_path(root, rel->cheapest_startup_path, 1);
    printf("\n\tcheapest total path:\n");
    foreach (l, rel->cheapest_total_path)
        print_path(root, (Path*)lfirst(l), 1);
    printf("\n");
    fflush(stdout);
}

#endif /* OPTIMIZER_DEBUG */

