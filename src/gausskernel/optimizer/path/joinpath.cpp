/* -------------------------------------------------------------------------
 *
 * joinpath.cpp
 *	  Routines to find all possible paths for processing a set of joins
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/joinpath.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "foreign/fdwapi.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "parser/parse_hint.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/nodegroups.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streampath.h"
#include "parser/parse_hint.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "optimizer/streamplan.h"
#include "pgxc/pgxc.h"
#include "parser/parsetree.h"
#include "optimizer/planmain.h"

#define PATH_PARAM_BY_REL(path, rel)  \
   ((path)->param_info && bms_overlap(PATH_REQ_OUTER(path), (rel)->relids))

static void copy_JoinCostWorkspace(JoinCostWorkspace* to, JoinCostWorkspace* from);
static void sort_inner_and_outer(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    List* restrictlist, List* mergeclause_list, JoinType jointype, JoinPathExtraData* extra, Relids param_source_rels);
static void match_unsorted_outer(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    List* restrictlist, List* mergeclause_list, JoinType jointype, JoinPathExtraData* extra, Relids param_source_rels);
static void hash_inner_and_outer(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    List* restrictlist, JoinType jointype, JoinPathExtraData* extra,
    Relids param_source_rels);
static List* select_mergejoin_clauses(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel,
    RelOptInfo* innerrel, List* restrictlist, JoinType jointype, bool* mergejoin_allowed);
static bool checkForPWJ(PlannerInfo* root, Path* outer_path, Path* inner_path, JoinType jointype, List* joinrestrict);
static bool checkJoinColumnForPWJ(PlannerInfo* root, Index varno, AttrNumber varattno);
static bool checkJoinClauseForPWJ(PlannerInfo* root, List* joinclause);
static bool checkPartitionkeyForPWJ(PlannerInfo* root, Path* outer_path, Path* inner_path);
static bool checkPathForPWJ(PlannerInfo* root, Path* path);
static bool checkIndexPathForPWJ(PartIteratorPath* pIterpath);
static bool checkPruningResultForPWJ(PartIteratorPath* outerpath, PartIteratorPath* innerpath);
static bool checkBoundary(List* outer_list, List* inner_list);
static bool checkBoundaryForPWJ(PlannerInfo* root, PartIteratorPath* outerpath, PartIteratorPath* innerpath);
static bool checkScanDirectionPWJ(PartIteratorPath* outerpath, PartIteratorPath* innerpath);
static Path* buildPartitionWiseJoinPath(Path* jpath, Path* outter_path, Path* inner_path);
static List* getPartitionkeyDataType(PlannerInfo* root, RelOptInfo* rel);
static PartIteratorPath* getPartIteratorPathForPWJ(Path* path);
static void getBoundaryFromBaseRel(PlannerInfo* root, PartIteratorPath* itrpath);
static void getBoundaryFromPartSeq(
    PartitionMap* map, int partitionSeq, Form_pg_attribute att, Const** upper, Const** lower);
static Path* fakePathForPWJ(Path* path);
static bool* calculate_join_georgraphy(
    PlannerInfo* root, RelOptInfo* outerrel, RelOptInfo* innerrel, List* joinclauses);
void debug1_print_outerrel_and_innerrel(PlannerInfo* root, RelOptInfo* outerrel, RelOptInfo* innerrel)
{
    if (log_min_messages > DEBUG1)
        return;
    if (root != NULL && root->parse != NULL) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfoString(&buf, "\n{\n");
        appendStringInfo(&buf, "\tOutter_rel:(%s)\n", debug1_print_relids(outerrel->relids, root->parse->rtable));
        appendStringInfo(&buf, "\tInner_rel :(%s)\n", debug1_print_relids(innerrel->relids, root->parse->rtable));
        ereport(DEBUG1,
            (errmodule(MOD_OPT),
                (errmsg("-----------------------Try to join these rels-----------------------%s}", buf.data))));
        pfree_ext(buf.data);
    }
    return;
}

/*
 * add_paths_to_joinrel
 *	  Given a join relation and two component rels from which it can be made,
 *	  consider all possible paths that use the two component rels as outer
 *	  and inner rel respectively.  Add these paths to the join rel's pathlist
 *	  if they survive comparison with other paths (and remove any existing
 *	  paths that are dominated by these paths).
 *
 * Modifies the pathlist field of the joinrel node to contain the best
 * paths found so far.
 *
 * jointype is not necessarily the same as sjinfo->jointype; it might be
 * "flipped around" if we are considering joining the rels in the opposite
 * direction from what's indicated in sjinfo.
 *
 * Also, this routine and others in this module accept the special JoinTypes
 * JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER to indicate that we should
 * unique-ify the outer or inner relation and then apply a regular inner
 * join.  These values are not allowed to propagate outside this module,
 * however.  Path cost estimation code may need to recognize that it's
 * dealing with such a case --- the combination of nominal jointype INNER
 * with sjinfo->jointype == JOIN_SEMI indicates that.
 */
void add_paths_to_joinrel(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    JoinType jointype, SpecialJoinInfo* sjinfo, List* restrictlist)
{
    JoinPathExtraData extra;
    List* mergeclause_list = NIL;
    bool mergejoin_allowed = true;
    Relids param_source_rels = NULL;
    ListCell* lc = NULL;
    List *mergejoin_hint = u_sess->attr.attr_sql.enable_mergejoin
                                ? NIL
                                : find_specific_join_hint(
                                    root->parse->hintState, joinrel->relids, NULL, HINT_KEYWORD_MERGEJOIN, false),
        *hashjoin_hint =
            u_sess->attr.attr_sql.enable_hashjoin
                ? NIL
                : find_specific_join_hint(root->parse->hintState, joinrel->relids, NULL, HINT_KEYWORD_HASHJOIN, false);

    /* print relation information in pg_log before looking for path */
    debug1_print_outerrel_and_innerrel(root, outerrel, innerrel);

    extra.sjinfo = sjinfo;
    if (u_sess->attr.attr_sql.enable_inner_unique_opt) {
        switch (jointype) {
            case JOIN_SEMI:
            case JOIN_ANTI:
                extra.inner_unique = false;
                break;
            case JOIN_UNIQUE_INNER:
                extra.inner_unique = bms_is_subset(sjinfo->min_lefthand, outerrel->relids);
                break;
            case JOIN_UNIQUE_OUTER:
                extra.inner_unique = innerrel_is_unique(root, outerrel, innerrel, JOIN_INNER, restrictlist);
                break;
            default:
                extra.inner_unique = innerrel_is_unique(root, outerrel, innerrel, jointype, restrictlist);
                break;
        }
    } else {
        extra.inner_unique = false;
    }

    /*
     * Find potential mergejoin clauses.  We can skip this if we are not
     * interested in doing a mergejoin.  However, mergejoin may be our only
     * way of implementing a full outer join, so override enable_mergejoin if
     * it's a full join.
     */
    if (u_sess->attr.attr_sql.enable_mergejoin || jointype == JOIN_FULL || mergejoin_hint != NIL)
        mergeclause_list =
            select_mergejoin_clauses(root, joinrel, outerrel, innerrel, restrictlist, jointype, &mergejoin_allowed);

    /*
     * If it's SEMI or ANTI join, compute correction factors for cost
     * estimation.	These will be the same for all paths.
     */
    if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
        compute_semi_anti_join_factors(root, outerrel, innerrel, jointype, sjinfo, restrictlist, &extra.semifactors);
    if (jointype == JOIN_RIGHT_SEMI) {
        SemiAntiJoinFactors sf;
        compute_semi_anti_join_factors(root, outerrel, innerrel, JOIN_SEMI, sjinfo, restrictlist, &extra.semifactors);
        compute_semi_anti_join_factors(root, innerrel, outerrel, JOIN_SEMI, sjinfo, restrictlist, &sf);
        extra.semifactors.match_count = sf.outer_match_frac;
    }
    if (jointype == JOIN_RIGHT_ANTI) {
        SemiAntiJoinFactors sf;
        compute_semi_anti_join_factors(root, outerrel, innerrel, JOIN_ANTI, sjinfo, restrictlist, &extra.semifactors);
        compute_semi_anti_join_factors(root, innerrel, outerrel, JOIN_ANTI, sjinfo, restrictlist, &sf);
        extra.semifactors.match_count = sf.outer_match_frac;
    }

    /*
     * Decide whether it's sensible to generate parameterized paths for this
     * joinrel, and if so, which relations such paths should require.  There
     * is usually no need to create a parameterized result path unless there
     * is a join order restriction that prevents joining one of our input rels
     * directly to the parameter source rel instead of joining to the other
     * input rel.  (But see allow_star_schema_join().)  This restriction
     * reduces the number of parameterized paths we have to deal with at
     * higher join levels, without compromising the quality of the resulting
     * plan.  We express the restriction as a Relids set that must overlap the
     * parameterization of any proposed join path.
     */
    Relids temp_relids = NULL;
    foreach (lc, root->join_info_list) {
        SpecialJoinInfo* root_sjinfo = (SpecialJoinInfo*)lfirst(lc);

        /*
         * SJ is relevant to this join if we have some part of its RHS
         * (possibly not all of it), and haven't yet joined to its LHS.  (This
         * test is pretty simplistic, but should be sufficient considering the
         * join has already been proven legal.)  If the SJ is relevant, it
         * presents constraints for joining to anything not in its RHS.
         */
        if (bms_overlap(joinrel->relids, root_sjinfo->min_righthand) &&
            !bms_overlap(joinrel->relids, root_sjinfo->min_lefthand))
            param_source_rels =
                bms_join(param_source_rels, bms_difference(root->all_baserels, root_sjinfo->min_righthand));

        /* full joins constrain both sides symmetrically */
        if (root_sjinfo->jointype == JOIN_FULL && bms_overlap(joinrel->relids, root_sjinfo->min_lefthand) &&
            !bms_overlap(joinrel->relids, root_sjinfo->min_righthand))
            param_source_rels =
                bms_join(param_source_rels, bms_difference(root->all_baserels, root_sjinfo->min_lefthand));
        /* Collect all rels on both sides. */
        temp_relids = bms_union(temp_relids, root_sjinfo->min_righthand);
        temp_relids = bms_union(temp_relids, root_sjinfo->min_lefthand);
    }
    /* Obtain the set of all rels in the inner join. */
    Relids total_available = bms_difference(root->all_baserels, temp_relids);
    /*
     * Try our best to obtain the optimal set of param_source_rels with 
     * the GUC PARAM_PATH_GEN on.
     */
    if (ENABLE_SQL_BETA_FEATURE(PARAM_PATH_GEN)) {
        Relids predpush_all = predpush_candidates_same_level(root);
        /*
         * If the GUC PREDPUSHFORCE is enabled, we just check the table appeared 
         * in the predpush hint, and add it into param_source_rels. Otherwise, 
         * we check all rels with param info for each outer table. 
         */
        if (ENABLE_PRED_PUSH_FORCE(root) && predpush_all != NULL) {
            total_available =
                bms_intersect(predpush_all, total_available);
        } else if (outerrel->ppilist != NIL) {
            ListCell *cc = NULL;
            Relids all_outer_param = NULL;
            foreach(cc, outerrel->ppilist) {
                ParamPathInfo *ppi = (ParamPathInfo*)lfirst(cc);
                if (ppi->ppi_req_outer != NULL) {
                    all_outer_param =
                        bms_union(all_outer_param, ppi->ppi_req_outer);
                }
            }
            total_available =
                bms_intersect(total_available, all_outer_param);
        } else {
            /* Do nothing with param_source_rels. */
            total_available = NULL;
        }
        param_source_rels =
                bms_union(param_source_rels, total_available);
    }

    /*
     * However, when a LATERAL subquery is involved, we have to be a bit
     * laxer, because there may simply not be any paths for the joinrel that
     * aren't parameterized by whatever the subquery is parameterized by.
     * Hence, add to param_source_rels anything that is in the minimum
     * parameterization of either input (and not in the other input).
     *
     * XXX need a more principled way of determining minimum parameterization.
     */
    foreach(lc, root->lateral_info_list)
    {
        LateralJoinInfo *ljinfo = (LateralJoinInfo *) lfirst(lc);

        if (bms_is_member(ljinfo->lateral_rhs, joinrel->relids)) {
            param_source_rels = bms_join(param_source_rels,
                bms_difference(ljinfo->lateral_lhs, joinrel->relids));
        }
    }

    /*
     * 1. Consider mergejoin paths where both relations must be explicitly
     * sorted.	Skip this if we can't mergejoin.
     */
    if (mergejoin_allowed)
        sort_inner_and_outer(
            root, joinrel, outerrel, innerrel, restrictlist, mergeclause_list, jointype, &extra, param_source_rels);

    /*
     * 2. Consider paths where the outer relation need not be explicitly
     * sorted. This includes both nestloops and mergejoins where the outer
     * path is already ordered.  Again, skip this if we can't mergejoin.
     * (That's okay because we know that nestloop can't handle right/full
     * joins at all, so it wouldn't work in the prohibited cases either.)
     */
    if (mergejoin_allowed)
        match_unsorted_outer(root,
            joinrel,
            outerrel,
            innerrel,
            restrictlist,
            mergeclause_list,
            jointype,
            &extra,
            param_source_rels);

#ifdef NOT_USED

    /*
     * 3. Consider paths where the inner relation need not be explicitly
     * sorted.	This includes mergejoins only (nestloops were already built in
     * match_unsorted_outer).
     *
     * Diked out as redundant 2/13/2000 -- tgl.  There isn't any really
     * significant difference between the inner and outer side of a mergejoin,
     * so match_unsorted_inner creates no paths that aren't equivalent to
     * those made by match_unsorted_outer when add_paths_to_joinrel() is
     * invoked with the two rels given in the other order.
     */
    if (mergejoin_allowed)
        match_unsorted_inner(root,
            joinrel,
            outerrel,
            innerrel,
            restrictlist,
            mergeclause_list,
            jointype,
            sjinfo,
            &semifactors,
            param_source_rels);
#endif

    /*
     * 4. Consider paths where both outer and inner relations must be hashed
     * before being joined.  As above, disregard enable_hashjoin for full
     * joins, because there may be no other alternative.
     */
    if (u_sess->attr.attr_sql.enable_hashjoin || jointype == JOIN_FULL || hashjoin_hint != NIL)
        hash_inner_and_outer(
            root, joinrel, outerrel, innerrel, restrictlist, jointype, &extra, param_source_rels);

#ifdef PGXC
    /*
     * Can not generate join path. It is not necessary to this branch, otherwise
     * can generate a invalid path.
     */
    if (joinrel->pathlist) {
        /*
         * If the inner and outer relations have RemoteQuery paths, check if this
         * JOIN can be pushed to the data-nodes. If so, create a RemoteQuery path
         * corresponding to the this JOIN.
         */
        create_joinrel_rqpath(root, joinrel, outerrel, innerrel, restrictlist, jointype, sjinfo);
    }
#endif /* PGXC */

    list_free_ext(mergejoin_hint);
    list_free_ext(hashjoin_hint);

    /*
     * 5. If inner and outer relations are foreign tables (or joins) belonging
     * to the same server and assigned to the same user to check access
     * permissions as, give the FDW a chance to push down joins.
     */
    if (joinrel->fdwroutine && joinrel->fdwroutine->GetForeignJoinPaths) {
        joinrel->fdwroutine->GetForeignJoinPaths(root, joinrel, outerrel, innerrel, jointype, sjinfo, restrictlist);
    }

    /*
     * Finally, give extensions a change to manipulate the path list.
     */
    if (set_join_pathlist_hook) {
        set_join_pathlist_hook(root, joinrel, outerrel, innerrel,
                               jointype, sjinfo, param_source_rels, &extra.semifactors, restrictlist);
    }
}

/*
 * We override the param_source_rels heuristic to accept nestloop paths in
 * which the outer rel satisfies some but not all of the inner path's
 * parameterization.  This is necessary to get good plans for star-schema
 * scenarios, in which a parameterized path for a large table may require
 * parameters from multiple small tables that will not get joined directly to
 * each other.  We can handle that by stacking nestloops that have the small
 * tables on the outside; but this breaks the rule the param_source_rels
 * heuristic is based on, namely that parameters should not be passed down
 * across joins unless there's a join-order-constraint-based reason to do so.
 * So we ignore the param_source_rels restriction when this case applies.
 *
 * However, there's a pitfall: suppose the inner rel (call it A) has a
 * parameter that is a PlaceHolderVar, and that PHV's minimum eval_at set
 * includes the outer rel (B) and some third rel (C).  If we treat this as a
 * star-schema case and create a B/A nestloop join that's parameterized by C,
 * we would end up with a plan in which the PHV's expression has to be
 * evaluated as a nestloop parameter at the B/A join; and the executor is only
 * set up to handle simple Vars as NestLoopParams.  Rather than add complexity
 * and overhead to the executor for such corner cases, it seems better to
 * forbid the join.  (Note that existence of such a PHV probably means there
 * is a join order constraint that will cause us to consider joining B and C
 * directly; so we can still make use of A's parameterized path, and there is
 * no need for the star-schema exception.) To implement this exception to the
 * exception, we check whether any PHVs used in the query could pose such a
 * hazard.  We don't have any simple way of checking whether a risky PHV would
 * actually be used in the inner plan, and the case is so unusual that it
 * doesn't seem worth working very hard on it.
 *
 * allow_star_schema_join() returns TRUE if the param_source_rels restriction
 * should be overridden, ie, it's okay to perform this join.
 */
static bool allow_star_schema_join(PlannerInfo* root, Path* outer_path, Path* inner_path)
{
    Relids innerparams = PATH_REQ_OUTER(inner_path);
    Relids outerrelids = outer_path->parent->relids;
    ListCell* lc = NULL;

    /*
     * It's not a star-schema case unless the outer rel provides some but not
     * all of the inner rel's parameterization.
     */
    if (!(bms_overlap(innerparams, outerrelids) && bms_nonempty_difference(innerparams, outerrelids)))
        return false;

    /* Check for hazardous PHVs */
    foreach (lc, root->placeholder_list) {
        PlaceHolderInfo* phinfo = (PlaceHolderInfo*)lfirst(lc);

        if (!bms_is_subset(phinfo->ph_eval_at, innerparams))
            continue; /* ignore, could not be a nestloop param */
        if (!bms_overlap(phinfo->ph_eval_at, outerrelids))
            continue; /* ignore, not relevant to this join */
        if (bms_is_subset(phinfo->ph_eval_at, outerrelids))
            continue; /* safe, it can be eval'd within outerrel */
        /* Otherwise, it's potentially unsafe, so reject the join */
        return false;
    }

    /* OK to perform the join */
    return true;
}

static void copy_JoinCostWorkspace(JoinCostWorkspace* to, JoinCostWorkspace* from)
{
    to->startup_cost = from->startup_cost;
    to->total_cost = from->total_cost;
    to->run_cost = from->run_cost;
    to->inner_rescan_run_cost = from->inner_rescan_run_cost;
    to->outer_matched_rows = from->outer_matched_rows;
    to->inner_scan_frac = from->inner_scan_frac;
    to->inner_run_cost = from->inner_run_cost;
    to->outer_rows = from->outer_rows;
    to->inner_rows = from->inner_rows;
    to->outer_skip_rows = from->outer_skip_rows;
    to->inner_skip_rows = from->inner_skip_rows;
    to->numbuckets = from->numbuckets;
    to->numbatches = from->numbatches;

    to->outer_mem_info.opMem = from->outer_mem_info.opMem;
    to->outer_mem_info.minMem = from->outer_mem_info.minMem;
    to->outer_mem_info.maxMem = from->outer_mem_info.maxMem;
    to->outer_mem_info.regressCost = from->outer_mem_info.regressCost;

    to->inner_mem_info.opMem = from->inner_mem_info.opMem;
    to->inner_mem_info.minMem = from->inner_mem_info.minMem;
    to->inner_mem_info.maxMem = from->inner_mem_info.maxMem;
    to->inner_mem_info.regressCost = from->inner_mem_info.regressCost;
}

/*
 * @Description: Judge this joinrelids if be hinted, and outer path or inner path if include
 * hint path.
 * @in hstate: Keep hint struct.
 * @in joinrelids: Join reliton ids.
 * @in outer_path: Outer path.
 * @in inner_path: Inner path.
 * @in keyWord: Hint keyword.
 * @return: If joinrelids be hinted, or outer path, inner path if include hin path return true, else return false.
 */
static bool add_path_hintcheck(
    HintState* hstate, Relids joinrelids, Path* outer_path, Path* inner_path, HintKeyword keyWord)
{
    List* hint = NIL;
    Path* path = NULL;

    hint = find_specific_join_hint(hstate, joinrelids, inner_path->parent->relids, keyWord);

    /* If this join be hinted, return true so that need generate this path. */
    if (hint != NIL) {
        list_free_ext(hint);
        return true;
    }

    path = find_hinted_path(outer_path);

    /* Outer path include hinted path. */
    if (path != NULL && path->hint_value > 0) {
        return true;
    }

    path = find_hinted_path(inner_path);

    /* Inner path include hinted path. */
    if (path->hint_value > 0) {
        return true;
    }

    return false;
}

DistrbutionPreferenceType get_join_distribution_perference_type(RelOptInfo* joinRel, Path* innerPath, Path* outerPath) 
{
    if (!u_sess->attr.attr_sql.enable_dngather || !u_sess->opt_cxt.is_dngather_support) {
        return DPT_SHUFFLE;
    }

    double dnGatherUpperRows = u_sess->attr.attr_sql.dngather_min_rows;
    if (innerPath->rows <= dnGatherUpperRows
        && outerPath->rows <= dnGatherUpperRows
        && joinRel->rows <= dnGatherUpperRows) {
        return DPT_SINGLE;
    }

    return DPT_SHUFFLE;
}

/*
 * try nestloop path single
 */
static void TryNestLoopPathSingle(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinPathExtraData* extra, Path* outerPath, Path* innerPath,
    Relids requiredOuter,List* restrictClauses, List* pathkeys, JoinCostWorkspace* workspace)
{
    /* try to use partitionwisejoin if need */
    if (checkForPWJ(root, outerPath, innerPath, jointype, restrictClauses)) {
        Path* outerpath = fakePathForPWJ(outerPath);
        Path* innerpath = fakePathForPWJ(innerPath);
        Path* jpath = NULL;

        jpath = (Path*)create_nestloop_path(root,
            joinrel,
            jointype,
            workspace,
            extra,
            outerpath,
            innerpath,
            restrictClauses,
            pathkeys,
            requiredOuter);
        add_path(root, joinrel, buildPartitionWiseJoinPath(jpath, outerPath, innerPath));
    } else {
        add_path(root,
            joinrel,
            (Path*)create_nestloop_path(root,
                joinrel,
                jointype,
                workspace,
                extra,
                outerPath,
                innerPath,
                restrictClauses,
                pathkeys,
                requiredOuter));
    }

    return;
}

/*
 * try_nestloop_path
 *	  Consider a nestloop join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void try_nestloop_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinPathExtraData* extra, Relids param_source_rels, Path* outer_path,
    Path* inner_path, List* restrict_clauses, List* pathkeys)
{
    bool execOnCoords = false;
    Relids required_outer;
    JoinCostWorkspace workspace;

    /* only work on CN Gather mode */
    if (IS_STREAM_PLAN && !IsSameJoinExecType(root, outer_path, inner_path)) {
        return;
    }

    /*
     * Check to see if proposed path is still parameterized, and reject if the
     * parameterization wouldn't be sensible --- unless allow_star_schema_join
     * says to allow it anyway.
     */
    required_outer = calc_nestloop_required_outer(outer_path, inner_path);
    if (required_outer && !bms_overlap(required_outer, param_source_rels) &&
        !allow_star_schema_join(root, outer_path, inner_path)) {
        /* Waste no memory when we reject a path here */
        bms_free_ext(required_outer);
        return;
    }

    /*
     * Do a precheck to quickly eliminate obviously-inferior paths.  We
     * calculate a cheap lower bound on the path's cost and then use
     * add_path_precheck() to see if the path is clearly going to be dominated
     * by some existing path for the joinrel.  If not, do the full pushup with
     * creating a fully valid path structure and submitting it to add_path().
     * The latter two steps are expensive enough to make this two-phase
     * methodology worthwhile.
     * Note: smp does not support parameterized paths.
     */
    int max_dop = (required_outer != NULL) ? 1 : u_sess->opt_cxt.query_dop;
    initial_cost_nestloop(root, &workspace, jointype, outer_path, inner_path, extra, max_dop);

    if (add_path_precheck(joinrel, workspace.startup_cost, workspace.total_cost, pathkeys, required_outer) ||
        add_path_hintcheck(root->parse->hintState, joinrel->relids, outer_path, inner_path, HINT_KEYWORD_NESTLOOP)) {

#ifdef STREAMPLAN
        /* check exec type of inner and outer path before generate join path. */
        if (IS_STREAM_PLAN && CheckJoinExecType(root, outer_path, inner_path)) {
            execOnCoords = true;
        }

        if (IS_STREAM_PLAN && !execOnCoords) {
            NestLoopPathGen* nlpgen = New(CurrentMemoryContext) NestLoopPathGen(root,
                joinrel,
                jointype,
                save_jointype,
                extra,
                outer_path,
                inner_path,
                restrict_clauses,
                pathkeys,
                required_outer);

            if (!canSeparateComputeAndStorageGroupForDelete(root)) {
                RangeTblEntry* rte =
                    rt_fetch(linitial_int(root->parse->resultRelations), root->parse->rtable); /* target udi relation */

                Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);

                JoinCostWorkspace cur_workspace;
                copy_JoinCostWorkspace(&cur_workspace, &workspace);

                nlpgen->addNestLoopPath(&cur_workspace, distribution, 1);

                if (u_sess->opt_cxt.query_dop > 1)
                    nlpgen->addNestLoopPath(&cur_workspace, distribution, u_sess->opt_cxt.query_dop);
            } else {
#ifdef ENABLE_MULTIPLE_NODES
                /*
                 * We choose candidate distribution list here with heuristic method,
                 * (1) for un-correlated query block, we will get a list of candidate Distribution
                 * (2) for correlated query block, we should shuffle them (inner and outer) to a correlated subplan node
                 * group
                 */
                List* candidate_distribution_list =
                    ng_get_join_candidate_distribution_list(outer_path, inner_path, root->is_correlated, 
                         get_join_distribution_perference_type(joinrel, inner_path, outer_path));

                /*
                 * For each candidate distribution (node group), we do nest loop join computing on it,
                 * if outer or inner is not in candidate node group, we should do shuffle.
                 */
                ListCell* lc = NULL;
                foreach (lc, candidate_distribution_list) {
                    Distribution* distribution = (Distribution*)lfirst(lc);
                    JoinCostWorkspace cur_workspace;
                    copy_JoinCostWorkspace(&cur_workspace, &workspace);

                    nlpgen->addNestLoopPath(&cur_workspace, distribution, 1);

                    if (u_sess->opt_cxt.query_dop > 1)
                        nlpgen->addNestLoopPath(&cur_workspace, distribution, u_sess->opt_cxt.query_dop);
                }
#else
                JoinCostWorkspace cur_workspace;
                copy_JoinCostWorkspace(&cur_workspace, &workspace);

                nlpgen->addNestLoopPath(&cur_workspace, NULL, 1);

                if (u_sess->opt_cxt.query_dop > 1)
                    nlpgen->addNestLoopPath(&cur_workspace, NULL, u_sess->opt_cxt.query_dop);
#endif
            }

            delete nlpgen;
        } else
#endif
        {
            /* try nestloop path single */
            TryNestLoopPathSingle(root,
                joinrel,
                jointype,
                extra,
                outer_path,
                inner_path,
                required_outer,
                restrict_clauses,
                pathkeys,
                &workspace);
        }

    } else {
        /* Waste no memory when we reject a path here */
        bms_free_ext(required_outer);
    }
}

/*
 * try_mergejoin_path for single node
 */
static void TryMergeJoinPathSingle(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinPathExtraData* extra, Path* outerPath, Path* innerPath, List* restrictClauses, Relids requiredOuter,
    List* pathkeys, List* mergeclauses, List* outersortkeys, List* innersortkeys, JoinCostWorkspace* workspace)
{
    /* try to use partitionwisejoin if need */
    if (checkForPWJ(root, outerPath, innerPath, jointype, restrictClauses)) {
        Path* outerpath = fakePathForPWJ(outerPath);
        Path* innerpath = fakePathForPWJ(innerPath);
        Path* jpath = NULL;

        jpath = (Path*)create_mergejoin_path(root,
            joinrel,
            jointype,
            workspace,
            extra,
            outerpath,
            innerpath,
            restrictClauses,
            pathkeys,
            requiredOuter,
            mergeclauses,
            outersortkeys,
            innersortkeys);

        add_path(root, joinrel, buildPartitionWiseJoinPath(jpath, outerPath, innerPath));
    } else {
        add_path(root,
            joinrel,
            (Path*)create_mergejoin_path(root,
                joinrel,
                jointype,
                workspace,
                extra,
                outerPath,
                innerPath,
                restrictClauses,
                pathkeys,
                requiredOuter,
                mergeclauses,
                outersortkeys,
                innersortkeys));
    }

    return;
}

static bool TryMergeJoinPreCheck(PlannerInfo* root, Relids paramSourceRels,
    Path* outerPath, Path* innerPath, Relids *requiredOuter)
{
    /* only work on CN Gather mode */
    if (IS_STREAM_PLAN && !IsSameJoinExecType(root, outerPath, innerPath)) {
        return false;
    }

    /*
     * Check to see if proposed path is still parameterized, and reject if the
     * parameterization wouldn't be sensible.
     */
    *requiredOuter = calc_non_nestloop_required_outer(outerPath, innerPath);
    if (*requiredOuter && !bms_overlap(*requiredOuter, paramSourceRels)) {
        /* Waste no memory when we reject a path here */
        bms_free_ext(*requiredOuter);
        return false;
    }

    /* SMP no support. */
    bool pathParalleled = (innerPath->dop > 1 || outerPath->dop > 1);
    if (pathParalleled) {
        return false;
    }

    return true;
}

/*
 * try_mergejoin_path
 *	  Consider a merge join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void try_mergejoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinPathExtraData *extra, Relids param_source_rels, Path* outer_path, Path* inner_path, List* restrict_clauses,
    List* pathkeys, List* mergeclauses, List* outersortkeys, List* innersortkeys)
{
    bool execOnCoords = false;
    Relids required_outer;
    JoinCostWorkspace workspace;

    if (!TryMergeJoinPreCheck(root, param_source_rels, outer_path, inner_path, &required_outer)) {
        return;
    }

    /*
     * If the given paths are already well enough ordered, we can skip doing
     * an explicit sort.
     */
    if (outersortkeys && pathkeys_contained_in(outersortkeys, outer_path->pathkeys))
        outersortkeys = NIL;
    if (innersortkeys && pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
        innersortkeys = NIL;

    /*
     * See comments in try_nestloop_path().
     */
    initial_cost_mergejoin(
        root, &workspace, jointype, mergeclauses, outer_path, inner_path, outersortkeys, innersortkeys, extra);

    if (add_path_precheck(joinrel, workspace.startup_cost, workspace.total_cost, pathkeys, required_outer) ||
        add_path_hintcheck(root->parse->hintState, joinrel->relids, outer_path, inner_path, HINT_KEYWORD_MERGEJOIN)) {
#ifdef STREAMPLAN
        /* check exec type of inner and outer path before generate join path. */
        if (IS_STREAM_PLAN && CheckJoinExecType(root, outer_path, inner_path)) {
            execOnCoords = true;
        }

        if (IS_STREAM_PLAN && !execOnCoords) {
            MergeJoinPathGen* mjpgen = New(CurrentMemoryContext) MergeJoinPathGen(root,
                joinrel,
                jointype,
                save_jointype,
                extra,
                outer_path,
                inner_path,
                restrict_clauses,
                pathkeys,
                required_outer,
                mergeclauses,
                outersortkeys,
                innersortkeys);

            if (!canSeparateComputeAndStorageGroupForDelete(root)) {
                RangeTblEntry* rte =
                    rt_fetch(linitial_int(root->parse->resultRelations), root->parse->rtable); /* target udi relation */

                Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);

                JoinCostWorkspace cur_workspace;
                copy_JoinCostWorkspace(&cur_workspace, &workspace);

                mjpgen->addMergeJoinPath(&cur_workspace, distribution, 1);
                if (u_sess->opt_cxt.query_dop > 1)
                    mjpgen->addMergeJoinPath(&cur_workspace, distribution, u_sess->opt_cxt.query_dop);
            } else {
#ifdef ENABLE_MULTIPLE_NODES
                /*
                 * We choose candidate distribution list here with heuristic method,
                 * (1) for un-correlated query block, we will get a list of candidate Distribution
                 * (2) for correlated query block, we should shuffle them (inner and outer) to a correlated subplan node
                 * group
                 */
                List* candidate_distribution_list =
                    ng_get_join_candidate_distribution_list(outer_path, inner_path, root->is_correlated, 
                         get_join_distribution_perference_type(joinrel, inner_path, outer_path));

                /*
                 * For each candidate distribution (node group), we do merge join computing on it,
                 * if outer or inner is not in candidate node group, we should do shuffle.
                 */
                ListCell* lc = NULL;
                foreach (lc, candidate_distribution_list) {
                    Distribution* distribution = (Distribution*)lfirst(lc);
                    JoinCostWorkspace cur_workspace;
                    copy_JoinCostWorkspace(&cur_workspace, &workspace);

                    mjpgen->addMergeJoinPath(&cur_workspace, distribution, 1);
                    if (u_sess->opt_cxt.query_dop > 1)
                        mjpgen->addMergeJoinPath(&cur_workspace, distribution, u_sess->opt_cxt.query_dop);
                }
#else
                JoinCostWorkspace cur_workspace;
                copy_JoinCostWorkspace(&cur_workspace, &workspace);

                mjpgen->addMergeJoinPath(&cur_workspace, NULL, 1);
                if (u_sess->opt_cxt.query_dop > 1)
                    mjpgen->addMergeJoinPath(&cur_workspace, NULL, u_sess->opt_cxt.query_dop);
#endif
            }

            delete mjpgen;
        } else
#endif
        {
            /* try mergejoin path for single */
            TryMergeJoinPathSingle(root,
                joinrel,
                jointype,
                extra,
                outer_path,
                inner_path,
                restrict_clauses,
                required_outer,
                pathkeys,
                mergeclauses,
                outersortkeys,
                innersortkeys,
                &workspace);
        }
    } else {
        /* Waste no memory when we reject a path here */
        bms_free_ext(required_outer);
    }
}

static void TryHashJoinPathSingle(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype,
    JoinPathExtraData* extra, Path* outerPath, Path* innerPath,
    List* restrictClauses, List* hashclauses, Relids requiredOuter, JoinCostWorkspace* workspace)
{
    /* try to use partitionwisejoin if need */
    if (checkForPWJ(root, outerPath, innerPath, jointype, restrictClauses)) {
        Path* outerpath = fakePathForPWJ(outerPath);
        Path* innerpath = fakePathForPWJ(innerPath);
        Path* jpath = NULL;

        jpath = (Path*)create_hashjoin_path(root,
            joinrel,
            jointype,
            workspace,
            extra,
            outerpath,
            innerpath,
            restrictClauses,
            requiredOuter,
            hashclauses);
        add_path(root, joinrel, buildPartitionWiseJoinPath(jpath, outerPath, innerPath));
    } else {
        add_path(root,
            joinrel,
            (Path*)create_hashjoin_path(root,
                joinrel,
                jointype,
                workspace,
                extra,
                outerPath,
                innerPath,
                restrictClauses,
                requiredOuter,
                hashclauses));
    }

    return;
}

/*
 * try_hashjoin_path
 *	  Consider a hash join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void try_hashjoin_path(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinPathExtraData* extra, Relids param_source_rels, Path* outer_path,
    Path* inner_path, List* restrict_clauses, List* hashclauses)
{
    bool execOnCoords = false;
    Relids required_outer;
    JoinCostWorkspace workspace;

    /* only work on CN Gather mode */
    if (IS_STREAM_PLAN && !IsSameJoinExecType(root, outer_path, inner_path)) {
        return;
    }

    /*
     * Check to see if proposed path is still parameterized, and reject if the
     * parameterization wouldn't be sensible.
     */
    required_outer = calc_non_nestloop_required_outer(outer_path, inner_path);
    if (required_outer && !bms_overlap(required_outer, param_source_rels)) {
        /* Waste no memory when we reject a path here */
        bms_free_ext(required_outer);
        return;
    }

    /*
     * See comments in try_nestloop_path().  Also note that hashjoin paths
     * never have any output pathkeys, per comments in create_hashjoin_path.
     * Note: smp does not support parameterized paths.
     */
    int max_dop = ((required_outer != NULL) || (u_sess->opt_cxt.query_dop <= 4 && u_sess->opt_cxt.max_query_dop >= 0))
                    ? 1
                    : u_sess->opt_cxt.query_dop;
    initial_cost_hashjoin(
        root, &workspace, jointype, hashclauses, outer_path, inner_path, extra, max_dop);

    if (add_path_precheck(joinrel, workspace.startup_cost, workspace.total_cost, NIL, required_outer) ||
        add_path_hintcheck(root->parse->hintState, joinrel->relids, outer_path, inner_path, HINT_KEYWORD_HASHJOIN)) {
#ifdef STREAMPLAN
        /* check exec type of inner and outer path before generate join path. */
        if (IS_STREAM_PLAN && CheckJoinExecType(root, outer_path, inner_path)) {
            execOnCoords = true;
        }

        if (IS_STREAM_PLAN && !execOnCoords) {
            HashJoinPathGen* hjpgen = New(CurrentMemoryContext) HashJoinPathGen(root,
                joinrel,
                jointype,
                save_jointype,
                extra,
                outer_path,
                inner_path,
                restrict_clauses,
                required_outer,
                hashclauses);

            if (!canSeparateComputeAndStorageGroupForDelete(root)) {
                RangeTblEntry* rte =
                    rt_fetch(linitial_int(root->parse->resultRelations), root->parse->rtable); /* target udi relation */

                Distribution* distribution = ng_get_baserel_data_distribution(rte->relid, rte->relkind);

                JoinCostWorkspace cur_workspace;
                copy_JoinCostWorkspace(&cur_workspace, &workspace);

                hjpgen->addHashJoinPath(&cur_workspace, distribution, 1);
                if (u_sess->opt_cxt.query_dop > 1)
                    hjpgen->addHashJoinPath(&cur_workspace, distribution, u_sess->opt_cxt.query_dop);
            } else {
#ifdef ENABLE_MULTIPLE_NODES
                /*
                 * We choose candidate distribution list here with heuristic method,
                 * (1) for un-correlated query block, we will get a list of candidate Distribution
                 * (2) for correlated query block, we should shuffle them (inner and outer) to a correlated subplan node
                 * group
                 */
                List* candidate_distribution_list =
                    ng_get_join_candidate_distribution_list(outer_path, inner_path, root->is_correlated, 
                         get_join_distribution_perference_type(joinrel, inner_path, outer_path));

                /*
                 * For each candidate distribution (node group), we do hash join computing on it,
                 * if outer or inner is not in candidate node group, we should do shuffle.
                 */
                ListCell* lc = NULL;
                foreach (lc, candidate_distribution_list) {
                    Distribution* distribution = (Distribution*)lfirst(lc);
                    JoinCostWorkspace cur_workspace;
                    copy_JoinCostWorkspace(&cur_workspace, &workspace);

                    hjpgen->addHashJoinPath(&cur_workspace, distribution, 1);
                    if (u_sess->opt_cxt.query_dop > 1)
                        hjpgen->addHashJoinPath(&cur_workspace, distribution, u_sess->opt_cxt.query_dop);
                }
#else
                JoinCostWorkspace cur_workspace;
                copy_JoinCostWorkspace(&cur_workspace, &workspace);
                hjpgen->addHashJoinPath(&cur_workspace, NULL, 1);
                if (u_sess->opt_cxt.query_dop > 1)
                    hjpgen->addHashJoinPath(&cur_workspace, NULL, u_sess->opt_cxt.query_dop);
#endif
            }

            delete hjpgen;
        } else
#endif
        {
            /* try hash join single */
            TryHashJoinPathSingle(root,
                joinrel,
                jointype,
                extra,
                outer_path,
                inner_path,
                restrict_clauses,
                hashclauses,
                required_outer,
                &workspace);
        }
    } else {
        /* Waste no memory when we reject a path here */
        bms_free_ext(required_outer);
    }
}

/*
 * clause_sides_match_join
 *	  Determine whether a join clause is of the right form to use in this join.
 *
 * We already know that the clause is a binary opclause referencing only the
 * rels in the current join.  The point here is to check whether it has the
 * form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
 * rather than mixing outer and inner vars on either side.	If it matches,
 * we set the transient flag outer_is_left to identify which side is which.
 */
bool clause_sides_match_join(RestrictInfo* rinfo, RelOptInfo* outerrel, RelOptInfo* innerrel)
{
    if (bms_is_subset(rinfo->left_relids, outerrel->relids) && bms_is_subset(rinfo->right_relids, innerrel->relids)) {
        /* lefthand side is outer */
        rinfo->outer_is_left = true;
        return true;
    } else if (bms_is_subset(rinfo->left_relids, innerrel->relids) &&
               bms_is_subset(rinfo->right_relids, outerrel->relids)) {
        /* righthand side is outer */
        rinfo->outer_is_left = false;
        return true;
    }
    return false; /* no good for these input relations */
}

/*
 * sort_inner_and_outer
 *	  Create mergejoin join paths by explicitly sorting both the outer and
 *	  inner join relations on each available merge ordering.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'restrictlist' contains all of the RestrictInfo nodes for restriction
 *		clauses that apply to this join
 * 'mergeclause_list' is a list of RestrictInfo nodes for available
 *		mergejoin clauses in this join
 * 'jointype' is the type of join to do
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'param_source_rels' are OK targets for parameterization of result paths
 */
static void sort_inner_and_outer(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    List* restrictlist, List* mergeclause_list, JoinType jointype, JoinPathExtraData *extra, Relids param_source_rels)
{
    JoinType save_jointype = jointype;
    List* all_pathkeys = NIL;
    ListCell* l = NULL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    int i, j;
    bool* join_used = NULL;
    int num_inner = list_length(innerrel->cheapest_total_path) - 1;

    /*
     * We only consider the cheapest-total-cost input paths, since we are
     * assuming here that a sort is required.  We will consider
     * cheapest-startup-cost input paths later, and only if they don't need a
     * sort.
     *
     * This function intentionally does not consider parameterized input paths
     * (implicit in the fact that it only looks at cheapest_total_path, which
     * is always unparameterized).	If we did so, we'd have a combinatorial
     * explosion of mergejoin paths of dubious value.  This interacts with
     * decisions elsewhere that also discriminate against mergejoins with
     * parameterized inputs; see comments in src/backend/optimizer/README.
     *
     * If unique-ification is requested, do it and then handle as a plain
     * inner join.
     */
    join_used = calculate_join_georgraphy(root, outerrel, innerrel, mergeclause_list);
    i = 0;
    foreach (lc1, outerrel->cheapest_total_path) {
        Path* outer_path_orig = (Path*)lfirst(lc1);
        Path* outer_path = NULL;
        j = 0;
        foreach (lc2, innerrel->cheapest_total_path) {
            Path* inner_path = (Path*)lfirst(lc2);
            outer_path = outer_path_orig;

            /*
             * If either cheapest-total path is parameterized by the other rel, we
             * can't use a mergejoin.  (There's no use looking for alternative input
             * paths, since these should already be the least-parameterized available
             * paths.)
             */
            if (PATH_PARAM_BY_REL(outer_path, innerrel) ||
                PATH_PARAM_BY_REL(inner_path, outerrel))
                return;

            /* we always accept join combination if one of path is cheapest path */
            if (outer_path != linitial(outerrel->cheapest_total_path) &&
                inner_path != linitial(innerrel->cheapest_total_path)) {
                if (!join_used[(i - 1) * num_inner + j - 1]) {
                    j++;
                    continue;
                }
            }

            jointype = save_jointype;
            if (jointype == JOIN_UNIQUE_OUTER) {
                outer_path = (Path*)create_unique_path(root, outerrel, outer_path, extra->sjinfo);
                AssertEreport(outer_path != NULL, MOD_OPT_JOIN, "Outer path is NULL");
                jointype = JOIN_INNER;
            } else if (jointype == JOIN_UNIQUE_INNER) {
                inner_path = (Path*)create_unique_path(root, innerrel, inner_path, extra->sjinfo);
                AssertEreport(inner_path != NULL, MOD_OPT_JOIN, "Inner path is NULL");
                jointype = JOIN_INNER;
            }

            /*
             * Each possible ordering of the available mergejoin clauses will generate
             * a differently-sorted result path at essentially the same cost.  We have
             * no basis for choosing one over another at this level of joining, but
             * some sort orders may be more useful than others for higher-level
             * mergejoins, so it's worth considering multiple orderings.
             *
             * Actually, it's not quite true that every mergeclause ordering will
             * generate a different path order, because some of the clauses may be
             * partially redundant (refer to the same EquivalenceClasses).	Therefore,
             * what we do is convert the mergeclause list to a list of canonical
             * pathkeys, and then consider different orderings of the pathkeys.
             *
             * Generating a path for *every* permutation of the pathkeys doesn't seem
             * like a winning strategy; the cost in planning time is too high. For
             * now, we generate one path for each pathkey, listing that pathkey first
             * and the rest in random order.  This should allow at least a one-clause
             * mergejoin without re-sorting against any other possible mergejoin
             * partner path.  But if we've not guessed the right ordering of secondary
             * keys, we may end up evaluating clauses as qpquals when they could have
             * been done as mergeclauses.  (In practice, it's rare that there's more
             * than two or three mergeclauses, so expending a huge amount of thought
             * on that is probably not worth it.)
             *
             * The pathkey order returned by select_outer_pathkeys_for_merge() has
             * some heuristics behind it (see that function), so be sure to try it
             * exactly as-is as well as making variants.
             */
            all_pathkeys = select_outer_pathkeys_for_merge(root, mergeclause_list, joinrel);

            foreach (l, all_pathkeys) {
                List* front_pathkey = (List*)lfirst(l);
                List* cur_mergeclauses = NIL;
                List* outerkeys = NIL;
                List* innerkeys = NIL;
                List* merge_pathkeys = NIL;

                /* Make a pathkey list with this guy first */
                if (l != list_head(all_pathkeys))
                    outerkeys = lcons(front_pathkey, list_delete_ptr(list_copy(all_pathkeys), front_pathkey));
                else
                    outerkeys = all_pathkeys; /* no work at first one... */

                /* Sort the mergeclauses into the corresponding ordering */
                cur_mergeclauses = find_mergeclauses_for_outer_pathkeys(root, outerkeys, mergeclause_list);

                /* Should have used them all... */
                AssertEreport(list_length(cur_mergeclauses) == list_length(mergeclause_list),
                    MOD_OPT_JOIN,
                    "mergeclause_list are not equal in length");

                /* Build sort pathkeys for the inner side */
                innerkeys = make_inner_pathkeys_for_merge(root, cur_mergeclauses, outerkeys);

                /* Build pathkeys representing output sort order */
                merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, outerkeys);

                /*
                 * And now we can make the path.
                 *
                 * Note: it's possible that the cheapest paths will already be sorted
                 * properly.  try_mergejoin_path will detect that case and suppress an
                 * explicit sort step, so we needn't do so here.
                 */
                try_mergejoin_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    extra,
                    param_source_rels,
                    outer_path,
                    inner_path,
                    restrictlist,
                    merge_pathkeys,
                    cur_mergeclauses,
                    outerkeys,
                    innerkeys);
            }
            j++;
        }
        i++;
    }
    if (join_used != NULL)
        pfree_ext(join_used);
}

/*
 * match_unsorted_outer
 *	  Creates possible join paths for processing a single join relation
 *	  'joinrel' by employing either iterative substitution or
 *	  mergejoining on each of its possible outer paths (considering
 *	  only outer paths that are already ordered well enough for merging).
 *
 * We always generate a nestloop path for each available outer path.
 * In fact we may generate as many as five: one on the cheapest-total-cost
 * inner path, one on the same with materialization, one on the
 * cheapest-startup-cost inner path (if different), one on the
 * cheapest-total inner-indexscan path (if any), and one on the
 * cheapest-startup inner-indexscan path (if different).
 *
 * We also consider mergejoins if mergejoin clauses are available.	We have
 * two ways to generate the inner path for a mergejoin: sort the cheapest
 * inner path, or use an inner path that is already suitably ordered for the
 * merge.  If we have several mergeclauses, it could be that there is no inner
 * path (or only a very expensive one) for the full list of mergeclauses, but
 * better paths exist if we truncate the mergeclause list (thereby discarding
 * some sort key requirements).  So, we consider truncations of the
 * mergeclause list as well as the full list.  (Ideally we'd consider all
 * subsets of the mergeclause list, but that seems way too expensive.)
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'restrictlist' contains all of the RestrictInfo nodes for restriction
 *		clauses that apply to this join
 * 'mergeclause_list' is a list of RestrictInfo nodes for available
 *		mergejoin clauses in this join
 * 'jointype' is the type of join to do
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 * 'param_source_rels' are OK targets for parameterization of result paths
 */
static void match_unsorted_outer(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    List* restrictlist, List* mergeclause_list, JoinType jointype, JoinPathExtraData* extra, Relids param_source_rels)
{
    JoinType save_jointype = jointype;
    bool nestjoinOK = false;
    bool useallclauses = false;
    Path* matpath = NULL;
    ListCell* l = NULL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    int i, j;
    bool* join_used = NULL;
    int num_inner = list_length(innerrel->cheapest_total_path) - 1;

    /*
     * Nestloop only supports inner, left, semi, and anti joins.  Also, if we
     * are doing a right or full mergejoin, we must use *all* the mergeclauses
     * as join clauses, else we will not have a valid plan.  (Although these
     * two flags are currently inverses, keep them separate for clarity and
     * possible future changes.)
     */
    switch (jointype) {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_SEMI:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            nestjoinOK = true;
            useallclauses = false;
            break;
        case JOIN_RIGHT:
        case JOIN_FULL:
        case JOIN_RIGHT_SEMI:
        case JOIN_RIGHT_ANTI:
        case JOIN_RIGHT_ANTI_FULL:
            nestjoinOK = false;
            useallclauses = true;
            break;
        case JOIN_UNIQUE_OUTER:
        case JOIN_UNIQUE_INNER:
            jointype = JOIN_INNER;
            nestjoinOK = true;
            useallclauses = false;
            break;
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized join type when match unsorted outer: %d", (int)jointype)));

            nestjoinOK = false; /* keep compiler quiet */
            useallclauses = false;
        } break;
    }

    join_used = calculate_join_georgraphy(root, outerrel, innerrel, restrictlist);
    i = 0;
    foreach (lc1, outerrel->cheapest_total_path) {
        Path* outer_cheapest_total = (Path*)lfirst(lc1);
        j = 0;
        foreach (lc2, innerrel->cheapest_total_path) {
            Path* inner_cheapest_total = (Path*)lfirst(lc2);
            Path* inner_cheapest_total_orig = inner_cheapest_total;

            /* we always accept join combination if one of path is cheapest path */
            if (outer_cheapest_total != linitial(outerrel->cheapest_total_path) &&
                inner_cheapest_total != linitial(innerrel->cheapest_total_path)) {
                if (!join_used[(i - 1) * num_inner + j - 1]) {
                    j++;
                    continue;
                }
            }

            /*
             * If inner_cheapest_total is parameterized by the outer rel, ignore it;
             * we will consider it below as a member of cheapest_parameterized_paths,
             * but the other possibilities considered in this routine aren't usable.
             */
            if (PATH_PARAM_BY_REL(inner_cheapest_total, outerrel))
                inner_cheapest_total = NULL;

            /*
             * If we need to unique-ify the inner path, we will consider only the
             * cheapest-total inner.
             */
            if (save_jointype == JOIN_UNIQUE_INNER) {
                if (inner_cheapest_total == NULL)
                    return;

                inner_cheapest_total = (Path*)create_unique_path(root, innerrel, inner_cheapest_total, extra->sjinfo);
                AssertEreport(inner_cheapest_total != NULL, MOD_OPT_JOIN, "inner cheapest path is NULL");
            } else if (nestjoinOK && inner_cheapest_total != NULL ) {
                /*
                 * Consider materializing the cheapest inner path, unless
                 * enable_material is off or the path in question materializes its
                 * output anyway.
                 */
                if (u_sess->attr.attr_sql.enable_material &&
                    !ExecMaterializesOutput(inner_cheapest_total->pathtype)) {
                    matpath = (Path*)create_material_path(inner_cheapest_total);
                } else if (ExecMaterializesOutput(inner_cheapest_total->pathtype))
                    /* if inner is already materialized, we accept it */
                    matpath = inner_cheapest_total;
            }

            foreach (l, outerrel->pathlist) {
                Path* outerpath = (Path*)lfirst(l);
                List* merge_pathkeys = NIL;
                List* mergeclauses = NIL;
                List* innersortkeys = NIL;
                List* trialsortkeys = NIL;
                Path* cheapest_startup_inner = NULL;
                Path* cheapest_total_inner = NULL;
                int num_sortkeys;
                int sortkeycnt;

                /* for non-optimal inner, we only try outer path with the same distributed key */
                if (inner_cheapest_total_orig != linitial(innerrel->cheapest_total_path) &&
                    outerpath != outer_cheapest_total)
                    continue;

                /*
                 * We cannot use an outer path that is parameterized by the inner rel.
                 */
                if (PATH_PARAM_BY_REL(outerpath, innerrel))
                    continue;

                /*
                 * If we need to unique-ify the outer path, it's pointless to consider
                 * any but the cheapest outer.	(XXX we don't consider parameterized
                 * outers, nor inners, for unique-ified cases.	Should we?)
                 */
                if (save_jointype == JOIN_UNIQUE_OUTER) {
                    if (outerpath != outer_cheapest_total)
                        continue;
                    outerpath = (Path*)create_unique_path(root, outerrel, outerpath, extra->sjinfo);
                    AssertEreport(outerpath != NULL, MOD_OPT_JOIN, "outer path is NULL");
                }

                /*
                 * The result will have this sort order (even if it is implemented as
                 * a nestloop, and even if some of the mergeclauses are implemented by
                 * qpquals rather than as true mergeclauses):
                 */
                merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, outerpath->pathkeys);

                if (save_jointype == JOIN_UNIQUE_INNER) {
                    /*
                     * Consider nestloop join, but only with the unique-ified cheapest
                     * inner path
                     */
                    try_nestloop_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        extra,
                        param_source_rels,
                        outerpath,
                        inner_cheapest_total,
                        restrictlist,
                        merge_pathkeys);
                } else if (nestjoinOK) {
                    /*
                     * Consider nestloop joins using this outer path and various
                     * available paths for the inner relation.	We consider the
                     * cheapest-total paths for each available parameterization of the
                     * inner relation, including the unparameterized case.
                     */
                    List* all_paths =
                        list_union_ptr(innerrel->cheapest_parameterized_paths, innerrel->cheapest_total_path);
                    ListCell* llc2 = NULL;

                    foreach (llc2, all_paths) {
                        Path* innerpath = (Path*)lfirst(llc2);

                        try_nestloop_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            extra,
                            param_source_rels,
                            outerpath,
                            innerpath,
                            restrictlist,
                            merge_pathkeys);
                    }

                    list_free_ext(all_paths);

                    /* Also consider materialized form of the cheapest inner path */
                    if (matpath != NULL)
                        try_nestloop_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            extra,
                            param_source_rels,
                            outerpath,
                            matpath,
                            restrictlist,
                            merge_pathkeys);
                }

                /* Can't do anything else if outer path needs to be unique'd */
                if (save_jointype == JOIN_UNIQUE_OUTER)
                    continue;

                /* Can't do anything else if inner rel is parameterized by outer */
                if (inner_cheapest_total == NULL)
                    continue;

                /* Look for useful mergeclauses (if any) */
                mergeclauses = find_mergeclauses_for_outer_pathkeys(root, outerpath->pathkeys, mergeclause_list);

                /*
                 * Done with this outer path if no chance for a mergejoin.
                 *
                 * Special corner case: for "x FULL JOIN y ON true", there will be no
                 * join clauses at all.  Ordinarily we'd generate a clauseless
                 * nestloop path, but since mergejoin is our only join type that
                 * supports FULL JOIN without any join clauses, it's necessary to
                 * generate a clauseless mergejoin path instead.
                 */
                if (mergeclauses == NIL) {
                    if (jointype == JOIN_FULL)
                        /* okay to try for mergejoin */;
                    else
                        continue;
                }
                if (useallclauses && list_length(mergeclauses) != list_length(mergeclause_list))
                    continue;

                /* Compute the required ordering of the inner path */
                innersortkeys = make_inner_pathkeys_for_merge(root, mergeclauses, outerpath->pathkeys);

                /*
                 * Generate a mergejoin on the basis of sorting the cheapest inner.
                 * Since a sort will be needed, only cheapest total cost matters. (But
                 * try_mergejoin_path will do the right thing if inner_cheapest_total
                 * is already correctly sorted.)
                 */
                try_mergejoin_path(root,
                    joinrel,
                    jointype,
                    save_jointype,
                    extra,
                    param_source_rels,
                    outerpath,
                    inner_cheapest_total,
                    restrictlist,
                    merge_pathkeys,
                    mergeclauses,
                    NIL,
                    innersortkeys);

                /* Can't do anything else if inner path needs to be unique'd */
                if (save_jointype == JOIN_UNIQUE_INNER)
                    continue;

                /*
                 * Look for presorted inner paths that satisfy the innersortkey list
                 * --- or any truncation thereof, if we are allowed to build a
                 * mergejoin using a subset of the merge clauses.  Here, we consider
                 * both cheap startup cost and cheap total cost.
                 *
                 * Currently we do not consider parameterized inner paths here. This
                 * interacts with decisions elsewhere that also discriminate against
                 * mergejoins with parameterized inputs; see comments in
                 * src/backend/optimizer/README.
                 *
                 * As we shorten the sortkey list, we should consider only paths that
                 * are strictly cheaper than (in particular, not the same as) any path
                 * found in an earlier iteration.  Otherwise we'd be intentionally
                 * using fewer merge keys than a given path allows (treating the rest
                 * as plain joinquals), which is unlikely to be a good idea.  Also,
                 * eliminating paths here on the basis of compare_path_costs is a lot
                 * cheaper than building the mergejoin path only to throw it away.
                 *
                 * If inner_cheapest_total is well enough sorted to have not required
                 * a sort in the path made above, we shouldn't make a duplicate path
                 * with it, either.  We handle that case with the same logic that
                 * handles the previous consideration, by initializing the variables
                 * that track cheapest-so-far properly.  Note that we do NOT reject
                 * inner_cheapest_total if we find it matches some shorter set of
                 * pathkeys.  That case corresponds to using fewer mergekeys to avoid
                 * sorting inner_cheapest_total, whereas we did sort it above, so the
                 * plans being considered are different.
                 */
                if (pathkeys_contained_in(innersortkeys, inner_cheapest_total->pathkeys)) {
                    /* inner_cheapest_total didn't require a sort */
                    cheapest_startup_inner = inner_cheapest_total;
                    cheapest_total_inner = inner_cheapest_total;
                } else {
                    /* it did require a sort, at least for the full set of keys */
                    cheapest_startup_inner = NULL;
                    cheapest_total_inner = NULL;
                }
                num_sortkeys = list_length(innersortkeys);
                if (num_sortkeys > 1 && !useallclauses)
                    trialsortkeys = list_copy(innersortkeys); /* need modifiable copy */
                else
                    trialsortkeys = innersortkeys; /* won't really truncate */

                for (sortkeycnt = num_sortkeys; sortkeycnt > 0; sortkeycnt--) {
                    Path* innerpath = NULL;
                    List* newclauses = NIL;

                    /*
                     * Look for an inner path ordered well enough for the first
                     * 'sortkeycnt' innersortkeys.	NB: trialsortkeys list is modified
                     * destructively, which is why we made a copy...
                     */
                    trialsortkeys = list_truncate(trialsortkeys, sortkeycnt);
                    innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist, trialsortkeys, NULL, TOTAL_COST);
                    if (innerpath != NULL && (cheapest_total_inner == NULL ||
                                                 compare_path_costs(innerpath, cheapest_total_inner, TOTAL_COST) < 0)) {
                        /* Found a cheap (or even-cheaper) sorted path */
                        /* Select the right mergeclauses, if we didn't already */
                        if (sortkeycnt < num_sortkeys) {
                            newclauses = trim_mergeclauses_for_inner_pathkeys(root, mergeclauses, trialsortkeys);
                            AssertEreport(newclauses != NIL, MOD_OPT_JOIN, "newclauses list is NIL");
                        } else
                            newclauses = mergeclauses;
                        try_mergejoin_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            extra,
                            param_source_rels,
                            outerpath,
                            innerpath,
                            restrictlist,
                            merge_pathkeys,
                            newclauses,
                            NIL,
                            NIL);
                        cheapest_total_inner = innerpath;
                    }
                    /* Same on the basis of cheapest startup cost ... */
                    innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist, trialsortkeys, NULL, STARTUP_COST);
                    if (innerpath != NULL &&
                        (cheapest_startup_inner == NULL ||
                            compare_path_costs(innerpath, cheapest_startup_inner, STARTUP_COST) < 0)) {
                        /* Found a cheap (or even-cheaper) sorted path */
                        if (innerpath != cheapest_total_inner) {
                            /*
                             * Avoid rebuilding clause list if we already made one;
                             * saves memory in big join trees...
                             */
                            if (newclauses == NIL) {
                                if (sortkeycnt < num_sortkeys) {
                                    newclauses =
                                        trim_mergeclauses_for_inner_pathkeys(root, mergeclauses, trialsortkeys);
                                    AssertEreport(newclauses != NIL, MOD_OPT_JOIN, "newclauses list is NIL");
                                } else
                                    newclauses = mergeclauses;
                            }
                            try_mergejoin_path(root,
                                joinrel,
                                jointype,
                                save_jointype,
                                extra,
                                param_source_rels,
                                outerpath,
                                innerpath,
                                restrictlist,
                                merge_pathkeys,
                                newclauses,
                                NIL,
                                NIL);
                        }
                        cheapest_startup_inner = innerpath;
                    }

                    /*
                     * Don't consider truncated sortkeys if we need all clauses.
                     */
                    if (useallclauses)
                        break;
                }
            }
            j++;
        }
        i++;
    }
    if (join_used != NULL)
        pfree_ext(join_used);
}

/*
 * hash_inner_and_outer
 *	  Create hashjoin join paths by explicitly hashing both the outer and
 *	  inner keys of each available hash clause.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'restrictlist' contains all of the RestrictInfo nodes for restriction
 *		clauses that apply to this join
 * 'jointype' is the type of join to do
 * 'sjinfo' is extra info about the join for selectivity estimation
 * 'semifactors' contains valid data if jointype is SEMI or ANTI
 * 'param_source_rels' are OK targets for parameterization of result paths
 */
static void hash_inner_and_outer(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel,
    List* restrictlist, JoinType jointype, JoinPathExtraData* extra,
    Relids param_source_rels)
{
    JoinType save_jointype = jointype;
    bool isouterjoin = IS_OUTER_JOIN((uint32)jointype);
    List* hashclauses = NIL;
    ListCell* l = NULL;

    /*
     * We need to build only one hashclauses list for any given pair of outer
     * and inner relations; all of the hashable clauses will be used as keys.
     *
     * Scan the join's restrictinfo list to find hashjoinable clauses that are
     * usable with this pair of sub-relations.
     */
    hashclauses = NIL;
    foreach (l, restrictlist) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);

        /*
         * If processing an outer join, only use its own join clauses for
         * hashing.  For inner joins we need not be so picky.
         */
        if (isouterjoin && restrictinfo->is_pushed_down)
            continue;

        if (!restrictinfo->can_join || restrictinfo->hashjoinoperator == InvalidOid)
            continue; /* not hashjoinable */

        /*
         * Check if clause has the form "outer op inner" or "inner op outer".
         */
        if (!clause_sides_match_join(restrictinfo, outerrel, innerrel))
            continue; /* no good for these input relations */

        /*
         * skip qual that contains sublink
         */
        if (contain_subplans((Node*)restrictinfo->clause))
            continue;

        hashclauses = lappend(hashclauses, restrictinfo);
    }

    /* If we found any usable hashclauses, make paths */
    if (hashclauses != NULL) {
        ListCell* lc1 = NULL;
        ListCell* lc2 = NULL;
        Path* cheapest_startup_outer = outerrel->cheapest_startup_path;
        int i, j;
        bool* join_used = NULL;
        int num_inner = list_length(innerrel->cheapest_total_path) - 1;

        /*
         * We consider both the cheapest-total-cost and cheapest-startup-cost
         * outer paths.  There's no need to consider any but the
         * cheapest-total-cost inner path, however.
         */
        join_used = calculate_join_georgraphy(root, outerrel, innerrel, hashclauses);
        i = 0;
        foreach (lc1, outerrel->cheapest_total_path) {
            Path* cheapest_total_outer_orig = (Path*)lfirst(lc1);
            Path* cheapest_total_outer = NULL;
            j = 0;
            foreach (lc2, innerrel->cheapest_total_path) {
                Path* cheapest_total_inner = (Path*)lfirst(lc2);
                cheapest_total_outer = cheapest_total_outer_orig;

                /*
                 * If either cheapest-total path is parameterized by the other rel, we
                 * can't use a hashjoin.  (There's no use looking for alternative
                 * input paths, since these should already be the least-parameterized
                 * available paths.)
                 */
                if (PATH_PARAM_BY_REL(cheapest_total_outer, innerrel) ||
                    PATH_PARAM_BY_REL(cheapest_total_inner, outerrel) ||
                    (cheapest_startup_outer != NULL && PATH_PARAM_BY_REL(cheapest_startup_outer, innerrel)))
                    return;

                /* we always accept join combination if one of path is cheapest path */
                if (cheapest_total_outer != linitial(outerrel->cheapest_total_path) &&
                    cheapest_total_inner != linitial(innerrel->cheapest_total_path)) {
                    if (!join_used[(i - 1) * num_inner + j - 1]) {
                        j++;
                        continue;
                    }
                }

                jointype = save_jointype;

                /* Unique-ify if need be; we ignore parameterized possibilities */
                if (jointype == JOIN_UNIQUE_OUTER) {
                    cheapest_total_outer = (Path*)create_unique_path(root, outerrel, cheapest_total_outer, extra->sjinfo);
                    AssertEreport(cheapest_total_outer != NULL, MOD_OPT_JOIN, "outer cheapest path is NULL");
                    jointype = JOIN_INNER;
                    try_hashjoin_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        extra,
                        param_source_rels,
                        cheapest_total_outer,
                        cheapest_total_inner,
                        restrictlist,
                        hashclauses);
                    /* no possibility of cheap startup here */
                } else if (jointype == JOIN_UNIQUE_INNER) {
                    cheapest_total_inner = (Path*)create_unique_path(root, innerrel, cheapest_total_inner, extra->sjinfo);
                    AssertEreport(cheapest_total_inner != NULL, MOD_OPT_JOIN, "inner cheapest path is NULL");
                    jointype = JOIN_INNER;
                    try_hashjoin_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        extra,
                        param_source_rels,
                        cheapest_total_outer,
                        cheapest_total_inner,
                        restrictlist,
                        hashclauses);
                    if (cheapest_startup_outer != NULL &&
                        cheapest_startup_outer != cheapest_total_outer)
                        try_hashjoin_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            extra,
                            param_source_rels,
                            cheapest_startup_outer,
                            cheapest_total_inner,
                            restrictlist,
                            hashclauses);
                } else {
                    /*
                     * For other jointypes, we consider the following paths:
                     *    1. cheapest_total_outer   and cheapest_total_inner
                     *    2. cheapest_startup_outer and cheapest_total_inner
                     *    3. cheapest_parameterized_paths
                     * There is no use in generating parameterized paths on the basis
                     * of possibly cheap startup cost, so this is sufficient.
                     */
                    ListCell* llc1 = NULL;
                    ListCell* llc2 = NULL;

                    try_hashjoin_path(root,
                        joinrel,
                        jointype,
                        save_jointype,
                        extra,
                        param_source_rels,
                        cheapest_total_outer,
                        cheapest_total_inner,
                        restrictlist,
                        hashclauses);


                    if (cheapest_startup_outer != NULL &&
                        cheapest_startup_outer != cheapest_total_outer)
                        try_hashjoin_path(root,
                            joinrel,
                            jointype,
                            save_jointype,
                            extra,
                            param_source_rels,
                            cheapest_startup_outer,
                            cheapest_total_inner,
                            restrictlist,
                            hashclauses);

                    foreach (llc1, outerrel->cheapest_parameterized_paths) {
                        Path* outerpath = (Path*)lfirst(llc1);

                        /*
                         * We cannot use an outer path that is parameterized by the
                         * inner rel.
                         */
                        if (PATH_PARAM_BY_REL(outerpath, innerrel))
                            continue;

                        foreach (llc2, innerrel->cheapest_parameterized_paths) {
                            Path* innerpath = (Path*)lfirst(llc2);

                            /*
                             * We cannot use an inner path that is parameterized by
                             * the outer rel, either.
                             */
                            if (PATH_PARAM_BY_REL(innerpath, outerrel))
                                continue;

                            if (outerpath == cheapest_startup_outer && innerpath == cheapest_total_inner)
                                continue; /* already tried it */

                            if (outerpath == cheapest_total_outer && innerpath == cheapest_total_inner)
                                continue; /* already tried it */

                            try_hashjoin_path(root,
                                joinrel,
                                jointype,
                                save_jointype,
                                extra,
                                param_source_rels,
                                outerpath,
                                innerpath,
                                restrictlist,
                                hashclauses);
                        }
                    }
                }
                j++;
            }
            i++;
        }
        if (join_used != NULL)
            pfree_ext(join_used);
    }
}

/*
 * select_mergejoin_clauses
 *	  Select mergejoin clauses that are usable for a particular join.
 *	  Returns a list of RestrictInfo nodes for those clauses.
 *
 * *mergejoin_allowed is normally set to TRUE, but it is set to FALSE if
 * this is a right/full join and there are nonmergejoinable join clauses.
 * The executor's mergejoin machinery cannot handle such cases, so we have
 * to avoid generating a mergejoin plan.  (Note that this flag does NOT
 * consider whether there are actually any mergejoinable clauses.  This is
 * correct because in some cases we need to build a clauseless mergejoin.
 * Simply returning NIL is therefore not enough to distinguish safe from
 * unsafe cases.)
 *
 * We also mark each selected RestrictInfo to show which side is currently
 * being considered as outer.  These are transient markings that are only
 * good for the duration of the current add_paths_to_joinrel() call!
 *
 * We examine each restrictinfo clause known for the join to see
 * if it is mergejoinable and involves vars from the two sub-relations
 * currently of interest.
 */
static List* select_mergejoin_clauses(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel,
    RelOptInfo* innerrel, List* restrictlist, JoinType jointype, bool* mergejoin_allowed)
{
    List* result_list = NIL;
    bool isouterjoin = IS_OUTER_JOIN((uint32)jointype);
    bool have_nonmergeable_joinclause = false;
    ListCell* l = NULL;

    foreach (l, restrictlist) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);

        /*
         * If processing an outer join, only use its own join clauses in the
         * merge.  For inner joins we can use pushed-down clauses too. (Note:
         * we don't set have_nonmergeable_joinclause here because pushed-down
         * clauses will become otherquals not joinquals.)
         */
        if (isouterjoin && restrictinfo->is_pushed_down)
            continue;

        /* Check that clause is a mergeable operator clause */
        if (!restrictinfo->can_join || restrictinfo->mergeopfamilies == NIL) {
            /*
             * The executor can handle extra joinquals that are constants, but
             * not anything else, when doing right/full merge join.  (The
             * reason to support constants is so we can do FULL JOIN ON
             * FALSE.)
             */
            if (!restrictinfo->clause || !IsA(restrictinfo->clause, Const))
                have_nonmergeable_joinclause = true;
            continue; /* not mergejoinable */
        }

        /*
         * Check if clause has the form "outer op inner" or "inner op outer".
         */
        if (!clause_sides_match_join(restrictinfo, outerrel, innerrel)) {
            have_nonmergeable_joinclause = true;
            continue; /* no good for these input relations */
        }

        /*
         * Insist that each side have a non-redundant eclass.  This
         * restriction is needed because various bits of the planner expect
         * that each clause in a merge be associatable with some pathkey in a
         * canonical pathkey list, but redundant eclasses can't appear in
         * canonical sort orderings.  (XXX it might be worth relaxing this,
         * but not enough time to address it for 8.3.)
         *
         * Note: it would be bad if this condition failed for an otherwise
         * mergejoinable FULL JOIN clause, since that would result in
         * undesirable planner failure.  I believe that is not possible
         * however; a variable involved in a full join could only appear in
         * below_outer_join eclasses, which aren't considered redundant.
         *
         * This case *can* happen for left/right join clauses: the outer-side
         * variable could be equated to a constant.  Because we will propagate
         * that constant across the join clause, the loss of ability to do a
         * mergejoin is not really all that big a deal, and so it's not clear
         * that improving this is important.
         */
        update_mergeclause_eclasses(root, restrictinfo);

        if (EC_MUST_BE_REDUNDANT(restrictinfo->left_ec) || EC_MUST_BE_REDUNDANT(restrictinfo->right_ec)) {
            have_nonmergeable_joinclause = true;
            continue; /* can't handle redundant eclasses */
        }

        result_list = lappend(result_list, restrictinfo);
    }

    /*
     * Report whether mergejoin is allowed (see comment at top of function).
     */
    switch (jointype) {
        case JOIN_RIGHT:
        case JOIN_FULL:
            *mergejoin_allowed = !have_nonmergeable_joinclause;
            break;
        case JOIN_RIGHT_SEMI:
        case JOIN_RIGHT_ANTI:
        case JOIN_RIGHT_ANTI_FULL:
            *mergejoin_allowed = false;
            break;
        default:
            *mergejoin_allowed = true;
            break;
    }

    return result_list;
}

static bool checkForPWJ(PlannerInfo* root, Path* outer_path, Path* inner_path, JoinType jointype, List* joinrestrict)
{
    /* Validate configuration */
    if (!u_sess->attr.attr_sql.enable_partitionwise) {
        return false;
    }

    /* SMP not support. */
    if (inner_path->dop > 1 || outer_path->dop > 1) {
        return false;
    }

    /* only support JOIN_INNER */
    if (jointype != JOIN_INNER) {
        return false;
    }

    /* Validate path */
    if (!checkPathForPWJ(root, outer_path) || !checkPathForPWJ(root, inner_path)) {
        return false;
    }

    /* Validate the partitionkey */
    if (!checkPartitionkeyForPWJ(root, outer_path, inner_path)) {
        return false;
    }

    /* Validate the join-clause */
    if (!checkJoinClauseForPWJ(root, joinrestrict)) {
        return false;
    }

    PartIteratorPath* outerpath = getPartIteratorPathForPWJ(outer_path);
    PartIteratorPath* innerpath = getPartIteratorPathForPWJ(inner_path);

    /* Validate the partiiton number */
    if (!checkPruningResultForPWJ(outerpath, innerpath)) {
        return false;
    }

    /* Validate the partiiton number */
    if (!checkScanDirectionPWJ(outerpath, innerpath)) {
        return false;
    }

    /*
     * build boundary for partitionwisejoin
     * if fails ,return false;
     * if sucess , return true
     */
    if (!checkBoundaryForPWJ(root, outerpath, innerpath)) {
        return false;
    }

    return true;
}

static bool checkIndexPathForPWJ(PartIteratorPath* pIterpath)
{
    bool result = false;

    if (is_partitionIndex_Subpath(pIterpath->subPath)) {
        Path* index_path = pIterpath->subPath;

        /* Get the usable type of the index subpath. */
        IndexesUsableType usable_type = eliminate_partition_index_unusable(
            ((IndexPath*)index_path)->indexinfo->indexoid, index_path->parent->pruning_result, NULL, NULL);

        if (usable_type == INDEXES_FULL_USABLE)
            result = true;
    } else {
        result = true;
    }
    return result;
}

static bool checkPathForPWJ(PlannerInfo* root, Path* path)
{
    bool result = false;
    Path* subpath = NULL;

    if (T_PartIterator == path->pathtype) {
        PartIteratorPath* pIterpath = (PartIteratorPath*)path;

        result = checkIndexPathForPWJ(pIterpath);
    } else if (T_Material == path->pathtype) {
        subpath = ((MaterialPath*)path)->subpath;

        if (T_PartIterator == subpath->pathtype) {
            PartIteratorPath* pIterpath = (PartIteratorPath*)subpath;

            result = checkIndexPathForPWJ(pIterpath);
        }
    } else if (T_Unique == path->pathtype) {
        /* May need deal with UniquePath */
        result = false;
    } else {
        result = false;
    }

    return result;
}

static bool checkPruningResultForPWJ(PartIteratorPath* outerpath, PartIteratorPath* innerpath)
{
    if (outerpath->itrs > 0 && innerpath->itrs > 0 && outerpath->itrs == innerpath->itrs) {
        return true;
    }

    return false;
}

static bool checkScanDirectionPWJ(PartIteratorPath* outerpath, PartIteratorPath* innerpath)
{
    if (outerpath->direction != innerpath->direction)
        return false;

    return true;
}

static bool checkPartitionkeyForPWJ(PlannerInfo* root, Path* outer_path, Path* inner_path)
{
    bool result = true;
    List* outerpk = NIL;
    List* innerpk = NIL;
    ListCell* outercell = NULL;
    ListCell* innercell = NULL;
    RelOptInfo* outerel = outer_path->parent;
    RelOptInfo* innerel = inner_path->parent;

    outerpk = getPartitionkeyDataType(root, outerel);
    /*
     * check partitionkey num
     * the number of the partitionkey must be 1
     */
    if (outerpk->length != 1) {
        list_free_ext(outerpk);

        return false;
    }

    innerpk = getPartitionkeyDataType(root, innerel);

    /*
     * check partitionkey num
     * the number of the partitionkey must be 1
     */
    if (innerpk->length != 1) {
        list_free_ext(innerpk);

        return false;
    }

    /*
     * check partitionkey datatype
     * partitionkey's datatype must be identical
     */
    forboth(outercell, outerpk, innercell, innerpk)
    {
        if (lfirst_oid(outercell) != lfirst_oid(innercell)) {
            result = false;
            break;
        }
    }

    list_free_ext(outerpk);
    list_free_ext(innerpk);

    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: check if the attribute pertained to a partitioned table's partitionkey
 * Description	: inpuit
 *			: 'root': Per-query information for planning/optimization
 *			: 'varno': the sn for the relation that the attribute pertained to
 *			: 'varattno': the sn for the attribute in the relation it pertained to
 * Input		:
 * Output	:
 * Notes		:
 */
static bool checkJoinColumnForPWJ(PlannerInfo* root, Index varno, AttrNumber varattno)
{
    Oid relid = InvalidOid;
    Relation relation = NULL;
    RangePartitionMap* partitionmap = NULL;
    int keypos = -1;

    /* Skip non-realtion such as subquery, joinrel, functionrel and cte */
    if (root->simple_rte_array[varno]->rtekind != RTE_RELATION) {
        return false;
    }

    relid = root->simple_rte_array[varno]->relid;
    AssertEreport(OidIsValid(relid), MOD_OPT_JOIN, "Relid is invalid");

    relation = heap_open(relid, NoLock);

    /* Must be a partitioned table */
    AssertEreport(RELATION_IS_PARTITIONED(relation), MOD_OPT_JOIN, "Relation is not partitioned");
    if (relation->partMap->type == PART_TYPE_LIST || 
        relation->partMap->type == PART_TYPE_HASH) {
        heap_close(relation, NoLock);
        return false;
    }
    partitionmap = (RangePartitionMap*)(relation->partMap);

    /* get the position for the special attribute in partitionkey */
    keypos = varIsInPartitionKey(varattno, partitionmap->base.partitionKey, partitionmap->base.partitionKey->dim1);
    heap_close(relation, NoLock);

    /* if keypos = -1, join_column is not a subset of the partitionkey */
    if (keypos < 0) {
        return false;
    }

    return true;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Validate the join_clause
 * Description	: 1. More than one join_condition
 *			: 2. At least one join_condition matches 'join_column1 = oin_column2'
 *			: 3. join_column1 is the partitionkey of the outer_path->parent
 *			:     join_column2 is the partitionkey of the inner_path->parent
 * Input		:
 * Output	:
 * Notes		:
 */
static bool checkJoinClauseForPWJ(PlannerInfo* root, List* joinclause)
{
    bool result = false;
    ListCell* rinfocell = NULL;
    RestrictInfo* rinfo = NULL;
    OpExpr* rinfoexpr = NULL;
    Var* leftarg = NULL;
    Var* rightarg = NULL;
    char* opname = NULL;

    if (!PointerIsValid(joinclause)) {
        return result;
    }

    /* Check if the join-clause match the partitionwisejoin */
    foreach (rinfocell, joinclause) {
        rinfo = (RestrictInfo*)lfirst(rinfocell);

        /*
         * Skip
         * It is non-OpExpr
         */
        if (T_OpExpr != nodeTag(rinfo->clause)) {
            continue;
        }

        rinfoexpr = (OpExpr*)(rinfo->clause);

        /*
         * Skip
         * It is not an equal expression as 'join_column1 = join_column2'
         */
        opname = get_opname(rinfoexpr->opno);
        if (PointerIsValid(opname)) {
            if (0 != strcmp("=", opname)) {
                pfree_ext(opname);
                continue;
            }

            pfree_ext(opname);
        }

        /*
         * Skip
         * Join_condition can not match 'join_column1 = join_column2'
         * if the number of the args is not equal to 2
         */
        if (2 != list_length(rinfoexpr->args)) {
            continue;
        }

        /*
         * Skip
         * Join_condition can not match 'join_column1 = join_column2'
         * if the one of the arg is not var
         */
        if ((T_Var != nodeTag((Expr*)list_nth(rinfoexpr->args, 0))) ||
            (T_Var != nodeTag((Expr*)list_nth(rinfoexpr->args, 1)))) {
            continue;
        }

        /*
         * Skip
         * Join_column is not a subset of the partitionkey
         */
        leftarg = (Var*)list_nth(rinfoexpr->args, 0);
        rightarg = (Var*)list_nth(rinfoexpr->args, 1);
        if (!checkJoinColumnForPWJ(root, leftarg->varno, leftarg->varattno) ||
            !checkJoinColumnForPWJ(root, rightarg->varno, rightarg->varattno)) {
            continue;
        }

        result = true;
        break;
    }

    return result;
}

static bool checkBoundaryForPWJ(PlannerInfo* root, PartIteratorPath* outerpath, PartIteratorPath* innerpath)
{
    RelOptInfo* outerrel = outerpath->path.parent;
    RelOptInfo* innerrel = innerpath->path.parent;
    bool result = true;

    if (outerrel->reloptkind == RELOPT_BASEREL) {
        getBoundaryFromBaseRel(root, outerpath);
    }

    if (innerrel->reloptkind == RELOPT_BASEREL) {
        getBoundaryFromBaseRel(root, innerpath);
    }

    if (!checkBoundary(outerpath->upperboundary, innerpath->upperboundary)) {
        result = false;
    } else if (!checkBoundary(outerpath->lowerboundary, innerpath->lowerboundary)) {
        result = false;
    } else {
        result = true;
    }

    return result;
}

static bool checkBoundary(List* outer_list, List* inner_list)
{
    ListCell* outer_cell = NULL;
    ListCell* inner_cell = NULL;
    bool result = true;

    forboth(outer_cell, outer_list, inner_cell, inner_list)
    {
        Const* outer_const = (Const*)lfirst(outer_cell);
        Const* inner_const = (Const*)lfirst(inner_cell);

        if (partitonKeyCompare(&outer_const, &inner_const, 1)) {
            result = false;
            break;
        }
    }

    return result;
}

static List* getPartitionkeyDataType(PlannerInfo* root, RelOptInfo* rel)
{
    List* result = NIL;
    Oid relid = InvalidOid;
    int counter = -1;
    Index relindex = 0;
    Relation relation = NULL;
    RangePartitionMap* partitionmap = NULL;
    AssertEreport(PointerIsValid(rel) && PointerIsValid(root), MOD_OPT_JOIN, "Either root or rel pointer is NULL");

    /*
     * get partitioned table's rangetable indexe for partitionekey checking
     * if it is a baserel, fetch its index (rel->relid)
     * if it is a joinrel, fetch the first rangetable indexe in set of base relids(rel->relids)
     */
    if (rel->reloptkind == RELOPT_BASEREL) {
        Assert(rel->isPartitionedTable);

        relindex = rel->relid;
    } else {
        Bitmapset* temp = NULL;

        AssertEreport(!bms_is_empty(rel->relids), MOD_OPT_JOIN, "base relid set is empty");
        temp = bms_copy(rel->relids);
        relindex = bms_first_member(temp);
        bms_free_ext(temp);
    }

    AssertEreport(relindex > 0, MOD_OPT_JOIN, "rel index is smaller than 0");
    relid = root->simple_rte_array[relindex]->relid;

    relation = heap_open(relid, NoLock);

    /* get partitionekey datatype's oid as a list */
    partitionmap = (RangePartitionMap*)(relation->partMap);
    AssertEreport(PointerIsValid(partitionmap->base.partitionKeyDataType), MOD_OPT_JOIN,
        "Partition key data type is NULL");
    AssertEreport(PointerIsValid(partitionmap->base.partitionKey), MOD_OPT_JOIN, "Partition key is NULL");

    for (counter = 0; counter < partitionmap->base.partitionKey->dim1; counter++) {
        result = lappend_oid(result, partitionmap->base.partitionKeyDataType[counter]);
    }

    heap_close(relation, NoLock);

    return result;
}

static PartIteratorPath* getPartIteratorPathForPWJ(Path* path)
{
    Path* result = NULL;

    if (T_PartIterator == path->pathtype) {
        result = path;
    } else if (T_Material == path->pathtype) {
        result = ((MaterialPath*)path)->subpath;
        AssertEreport(T_PartIterator == result->pathtype, MOD_OPT_JOIN, "Path type is not part iterator");
    } else if (T_Unique == path->pathtype) {
        /* May need deal with UniquePath */
        Assert(false);
    } else { /* never happen; just to be self-contained */
        Assert(false);
    }

    return (PartIteratorPath*)result;
}

static void getBoundaryFromBaseRel(PlannerInfo* root, PartIteratorPath* itrpath)
{
    ListCell* cell = NULL;
    List* part_seqs = NIL;
    Oid partitionedtableid = InvalidOid;
    Relation relation = NULL;
    HeapTuple tuple = NULL;
    Form_pg_attribute att = NULL;
    RangePartitionMap* map = NULL;
    Const* upper = NULL;
    Const* lower = NULL;
    RelOptInfo* rel = NULL;

    AssertEreport(PointerIsValid(itrpath) && PointerIsValid(root), MOD_OPT_JOIN, "Either itrparh or root is NULL");
    rel = itrpath->path.parent;
    AssertEreport(rel->reloptkind == RELOPT_BASEREL, MOD_OPT_JOIN, "Rel opt kind is not base rel");
    AssertEreport(PointerIsValid(rel->pruning_result), MOD_OPT_JOIN, "pruning_result pointer is NULL");

    if (PointerIsValid(itrpath->upperboundary)) {
        return;
    }

    if (1 > itrpath->itrs) {
        itrpath->upperboundary = NIL;
        itrpath->lowerboundary = NIL;

        return;
    }

    partitionedtableid = root->simple_rte_array[rel->relid]->relid;
    relation = heap_open(partitionedtableid, NoLock);
    /*
     * before access partition map, increace partition refcount preventing rebuilding the partition map
     */
    incre_partmap_refcount(relation->partMap);
    map = (RangePartitionMap*)(relation->partMap);
    AssertEreport(map->base.partitionKey->dim1 == 1, MOD_OPT_JOIN, "partition key dim1 is incorrect");

    tuple = SearchSysCache2((int)ATTNUM, ObjectIdGetDatum(partitionedtableid), Int16GetDatum(map->base.partitionKey->values[0]));
    if (!HeapTupleIsValid(tuple)) {
        decre_partmap_refcount(relation->partMap);
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u",
                    map->base.partitionKey->values[0], partitionedtableid)));
    }
    att = (Form_pg_attribute)GETSTRUCT(tuple);

    part_seqs = rel->pruning_result->ls_rangeSelectedPartitions;
    AssertEreport(rel->partItrs == rel->pruning_result->ls_rangeSelectedPartitions->length,
        MOD_OPT_JOIN,
        "partition length is not equal");

    foreach (cell, part_seqs) {
        int partSeq = lfirst_int(cell);

        getBoundaryFromPartSeq((PartitionMap*)map, partSeq, att, &upper, &lower);
        itrpath->upperboundary = lappend(itrpath->upperboundary, upper);
        if (PointerIsValid(lower)) {
            itrpath->lowerboundary = lappend(itrpath->lowerboundary, lower);
        }
    }

    ReleaseSysCache(tuple);
    /*
     * after access partition map, decreace partition refcount
     */
    decre_partmap_refcount(relation->partMap);
    heap_close(relation, NoLock);
}

static void getBoundaryFromPartSeq(
    PartitionMap* map, int partitionSeq, Form_pg_attribute att, Const** upper, Const** lower)
{
    AssertEreport(PointerIsValid(map) && PointerIsValid(att), MOD_OPT_JOIN, "Either map or att pointer is NULL");

    if (map->type == PART_TYPE_RANGE ) {
        RangePartitionMap* rangemap = (RangePartitionMap*)map;
        RangeElement* pre_element = NULL;
        RangeElement* cur_element = NULL;

        AssertEreport(partitionSeq <= rangemap->rangeElementsNum, MOD_OPT_JOIN, "range partition number is incorrect");

        pre_element = cur_element;
        if (partitionSeq >= 1)
            pre_element = &(rangemap->rangeElements[partitionSeq - 1]);
        cur_element = &(rangemap->rangeElements[partitionSeq]);

        AssertEreport(PointerIsValid(cur_element), MOD_OPT_JOIN, "current range map is NULL");
        *upper = (Const*)copyObject(cur_element->boundary[0]);

        if (PointerIsValid(pre_element)) {
            *lower = (Const*)copyObject(pre_element->boundary[0]);
        } else {
            *lower = NULL;
        }

    } else {
        /* never happen; just to be self-contained */
        Assert(false);
    }
}

static Path* buildPartitionWiseJoinPath(Path* jpath, Path* outter_path, Path* inner_path)
{
    PartIteratorPath* pwjpath = NULL;
    PartIteratorPath* outer = getPartIteratorPathForPWJ(outter_path);
    PartIteratorPath* inner = getPartIteratorPathForPWJ(inner_path);
    double factor = 1.0;

    if (!PointerIsValid(jpath) || !PointerIsValid(outter_path) || !PointerIsValid(inner_path) ||
        !PointerIsValid(outer) || !PointerIsValid(inner)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Null value error for building partitionwise join")));
    }

    AssertEreport(outer->itrs == inner->itrs, MOD_OPT_JOIN, "the itrs of inner and outer are mismatched");

    factor = (double)1.0 / outer->itrs;

    pwjpath = makeNode(PartIteratorPath);
    pwjpath->itrs = outer->itrs;
    pwjpath->path.pathtype = T_PartIterator;
    pwjpath->path.parent = jpath->parent;
    pwjpath->path.pathtarget = jpath->parent->reltarget;
    pwjpath->path.pathkeys = jpath->pathkeys;
    pwjpath->direction = outer->direction;
    pwjpath->path.param_info = jpath->param_info;
    set_path_rows(&pwjpath->path, jpath->rows * factor, jpath->multiple);
    pwjpath->path.startup_cost = jpath->startup_cost * factor;
    pwjpath->path.total_cost = jpath->total_cost * factor;
    pwjpath->ispwj = true;
    pwjpath->subPath = jpath;
    pwjpath->upperboundary = outer->upperboundary;
    pwjpath->lowerboundary = outer->lowerboundary;

    return (Path*)pwjpath;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static Path* fakePathForPWJ(Path* path)
{
    errno_t rc = EOK;
    if (T_PartIterator == path->pathtype) {
        return ((PartIteratorPath*)path)->subPath;
    } else if (T_Material == path->pathtype) {
        MaterialPath* mpath = (MaterialPath*)path;
        MaterialPath* newpath = makeNode(MaterialPath);

        AssertEreport(
            T_PartIterator == mpath->subpath->pathtype, MOD_OPT_JOIN, "subpath pathtype is not part iterator");
        rc = memcpy_s(newpath, sizeof(MaterialPath), mpath, sizeof(MaterialPath));
        securec_check_c(rc, "\0", "\0");
        newpath->subpath = ((PartIteratorPath*)(mpath->subpath))->subPath;

        return (Path*)newpath;
    } else {
        /* never happen; just to be self-contained */
        Assert(false);
        return NULL;
    }
}

/*
 * calculate_join_georgraphy
 *	For multiple cheapest total paths, find which two from different rels can be cheaply joined due to
 *	unnecessary redistribution.
 *	For non-local-optimial paths, we form an two-dimension array to indicates if two paths from outer
 *	and inner side can join cheaply, that is, redistribution is unnecesary for these two paths. If so, we
 *	set corresponding array item to true.
 *	Since cheapest total path should always joined with all other cheap paths, so no need to judge them.
 *	The target array is 1 size less for each dimension.
 *
 * Parameters:
 *	@in root: Plannerinfo structure for current query level
 *	@in outerrel: outer join relation
 *	@in innerrel: inner join relation
 *	@in joinclauses: join clauses between two relations
 * Returns:
 *	2-dimensional join georgraphy array with size 1 less than actural path number of each relation.
 *	If either relation has only one path, return NULL.
 */
static bool* calculate_join_georgraphy(PlannerInfo* root, RelOptInfo* outerrel, RelOptInfo* innerrel, List* joinclauses)
{
    int num_outer = list_length(outerrel->cheapest_total_path) - 1;
    int num_inner = list_length(innerrel->cheapest_total_path) - 1;
    List **outer_rinfo, **inner_rinfo; /* rinfo related to distribute key for each path */
    bool* join_used = NULL;
    int i, j;
    ListCell* lc = NULL;

    if (num_outer > 0 && num_inner > 0) {
        List* rinfo = NIL;
        bool need_redistribute = false;
        outer_rinfo = (List**)palloc0(sizeof(List*) * num_outer);
        inner_rinfo = (List**)palloc0(sizeof(List*) * num_inner);
        join_used = (bool*)palloc0(sizeof(bool) * num_inner * num_outer);
        /* check if redistribute is necessary for each non-local-optimal path for outerrel */
        i = 0;
        foreach (lc, outerrel->cheapest_total_path) {
            Path* path = (Path*)lfirst(lc);
            if (lc == list_head(outerrel->cheapest_total_path))
                continue;
            need_redistribute =
                is_distribute_need_on_joinclauses(root, path->distribute_keys, joinclauses, outerrel, innerrel, &rinfo);
            if (!need_redistribute)
                outer_rinfo[i] = rinfo;
            i++;
        }
        /* check if redistribute is necessary for each non-local-optimal path for innerrel */
        i = 0;
        foreach (lc, innerrel->cheapest_total_path) {
            Path* path = (Path*)lfirst(lc);
            if (lc == list_head(innerrel->cheapest_total_path))
                continue;
            need_redistribute =
                is_distribute_need_on_joinclauses(root, path->distribute_keys, joinclauses, innerrel, outerrel, &rinfo);
            if (!need_redistribute)
                inner_rinfo[i] = rinfo;
            i++;
        }
        /* we set the array if rinfo from both sides are equal, that means, we don't need redistribute on it */
        for (i = 0; i < num_outer; i++) {
            for (j = 0; j < num_inner; j++) {
                if (outer_rinfo[i] != NIL && inner_rinfo[j] != NIL) {
                    List* diff1 = list_difference_ptr(outer_rinfo[i], inner_rinfo[j]);
                    List* diff2 = list_difference_ptr(inner_rinfo[j], outer_rinfo[i]);
                    if (diff1 == NIL && diff2 == NIL)
                        join_used[i * num_inner + j] = true;
                    list_free_ext(diff1);
                    list_free_ext(diff2);
                }
            }
        }
        for (i = 0; i < num_outer; i++) {
            if (outer_rinfo[i] != NIL)
                list_free_ext(outer_rinfo[i]);
        }
        pfree_ext(outer_rinfo);
        for (i = 0; i < num_inner; i++) {
            if (inner_rinfo[i] != NIL)
                list_free_ext(inner_rinfo[i]);
        }
        pfree_ext(inner_rinfo);
    }

    /* debug info for join info array */
    if (log_min_messages <= DEBUG1 && join_used) {
        StringInfoData ds;
        initStringInfo(&ds);
        appendStringInfo(&ds, "outerrel: ");
        appendBitmapsetToString(&ds, outerrel->relids);
        appendStringInfo(&ds, " innerrel: ");
        appendBitmapsetToString(&ds, innerrel->relids);
        appendStringInfo(&ds, "\njoin georgraphy:\n");
        for (i = 0; i < num_outer; i++) {
            for (j = 0; j < num_inner; j++) {
                appendStringInfo(&ds, " %d", join_used[i * num_inner + j]);
                appendStringInfo(&ds, "\n");
            }
        }
        ereport(DEBUG1, (errmodule(MOD_OPT), (errmsg("%s", ds.data))));
        pfree_ext(ds.data);
    }

    return join_used;
}

