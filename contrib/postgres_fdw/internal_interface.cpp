/* -------------------------------------------------------------------------
 *
 * postgres_fdw.c
 * 		  Foreign-data wrapper for remote openGauss servers
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		  contrib/postgres_fdw/postgres_fdw.c
 *
 * -------------------------------------------------------------------------
 */

#ifndef FRONTEND

#include "postgres.h"
#include "utils/memutils.h"
#else
#include "postgres_fe.h"
/* It's possible we could use a different value for this in frontend code */
#define MaxAllocSize ((Size)0x3fffffff) /* 1 gigabyte - 1 */
#endif

#include "internal_interface.h"
#include "optimizer/pathnode.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"

/*
 * Get a copy of an existing local path for a given join relation.
 *
 * This function is usually helpful to obtain an alternate local path for EPQ
 * checks.
 *
 * Right now, this function only supports unparameterized foreign joins, so we
 * only search for unparameterized path in the given list of paths. Since we
 * are searching for a path which can be used to construct an alternative local
 * plan for a foreign join, we look for only MergeJoin, HashJoin or NestLoop
 * paths.
 *
 * If the inner or outer subpath of the chosen path is a ForeignScan, we
 * replace it with its outer subpath.  For this reason, and also because the
 * planner might free the original path later, the path returned by this
 * function is a shallow copy of the original.  There's no need to copy
 * the substructure, so we don't.
 *
 * Since the plan created using this path will presumably only be used to
 * execute EPQ checks, efficiency of the path is not a concern. But since the
 * path list in RelOptInfo is anyway sorted by total cost we are likely to
 * choose the most efficient path, which is all for the best.
 */
Path *GetExistingLocalJoinPath(RelOptInfo *joinrel)
{
    ListCell *lc = NULL;
    errno_t rc;

    Assert(IS_JOIN_REL(joinrel));

    foreach (lc, joinrel->pathlist) {
        Path *path = (Path *)lfirst(lc);
        JoinPath *joinpath = NULL;

        /* Skip parameterized paths. */
        if (path->param_info != NULL) {
            continue;
        }

        switch (path->pathtype) {
            case T_HashJoin: {
                HashPath *hash_path = makeNode(HashPath);
                rc = memcpy_s(hash_path, sizeof(HashPath), path, sizeof(HashPath));
                securec_check(rc, "\0", "\0");
                joinpath = (JoinPath *)hash_path;
            } break;

            case T_NestLoop: {
                NestPath *nest_path = makeNode(NestPath);
                rc = memcpy_s(nest_path, sizeof(NestPath), path, sizeof(NestPath));
                securec_check(rc, "\0", "\0");
                joinpath = (JoinPath *)nest_path;
            } break;

            case T_MergeJoin: {
                MergePath *merge_path = makeNode(MergePath);
                rc = memcpy_s(merge_path, sizeof(MergePath), path, sizeof(MergePath));
                securec_check(rc, "\0", "\0");
                joinpath = (JoinPath *)merge_path;
            } break;

            default:

                /*
                 * Just skip anything else. We don't know if corresponding
                 * plan would build the output row from whole-row references
                 * of base relations and execute the EPQ checks.
                 */
                break;
        }

        /* This path isn't good for us, check next. */
        if (!joinpath) {
            continue;
        }

        /*
         * If either inner or outer path is a ForeignPath corresponding to a
         * pushed down join, replace it with the fdw_outerpath, so that we
         * maintain path for EPQ checks built entirely of local join
         * strategies.
         */
        if (IsA(joinpath->outerjoinpath, ForeignPath)) {
            ForeignPath *foreign_path;

            foreign_path = (ForeignPath *)joinpath->outerjoinpath;
            if (IS_JOIN_REL(foreign_path->path.parent)) {
                joinpath->outerjoinpath = foreign_path->fdw_outerpath;
            }
        }

        if (IsA(joinpath->innerjoinpath, ForeignPath)) {
            ForeignPath *foreign_path;

            foreign_path = (ForeignPath *)joinpath->innerjoinpath;
            if (IS_JOIN_REL(foreign_path->path.parent)) {
                joinpath->innerjoinpath = foreign_path->fdw_outerpath;
            }
        }

        return (Path *)joinpath;
    }
    return NULL;
}

/*
 * psprintf
 *
 * Format text data under the control of fmt (an sprintf-style format string)
 * and return it in an allocated-on-demand buffer.  The buffer is allocated
 * with palloc in the backend, or malloc in frontend builds.  Caller is
 * responsible to free the buffer when no longer needed, if appropriate.
 *
 * Errors are not returned to the caller, but are reported via elog(ERROR)
 * in the backend, or printf-to-stderr-and-exit() in frontend builds.
 * One should therefore think twice about using this in libpq.
 */
char *psprintf(const char *fmt, ...)
{
    int save_errno = errno;
    size_t len = 128; /* initial assumption about buffer size */

    for (;;) {
        char *result = NULL;
        va_list args;
        size_t newlen;

        /*
         * Allocate result buffer.  Note that in frontend this maps to malloc
         * with exit-on-error.
         */
        result = (char *)palloc(len);

        /* Try to format the data. */
        errno = save_errno;
        va_start(args, fmt);
        newlen = pvsnprintf(result, len, fmt, args);
        va_end(args);

        if (newlen < len) {
            return result; /* success */
        }

        /* Release buffer and loop around to try again with larger len. */
        pfree(result);
        len = newlen;
    }
}

/*
 * pvsnprintf
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and insert it into buf (which has length len).
 *
 * If successful, return the number of bytes emitted, not counting the
 * trailing zero byte.  This will always be strictly less than len.
 *
 * If there's not enough space in buf, return an estimate of the buffer size
 * needed to succeed (this *must* be more than the given len, else callers
 * might loop infinitely).
 *
 * Other error cases do not return, but exit via elog(ERROR) or exit().
 * Hence, this shouldn't be used inside libpq.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 *
 * Note that the semantics of the return value are not exactly C99's.
 * First, we don't promise that the estimated buffer size is exactly right;
 * callers must be prepared to loop multiple times to get the right size.
 * (Given a C99-compliant vsnprintf, that won't happen, but it is rumored
 * that some implementations don't always return the same value ...)
 * Second, we return the recommended buffer size, not one less than that;
 * this lets overflow concerns be handled here rather than in the callers.
 */
size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
{
    int nprinted = 0;

    nprinted = vsnprintf_s(buf, len, len, fmt, args);
    securec_check_ss(nprinted, "\0", "\0");

    /* We assume failure means the fmt is bogus, hence hard failure is OK */
    if (unlikely(nprinted < 0)) {
#ifndef FRONTEND
        elog(ERROR, "vsnprintf failed: %m with format string \"%s\"", fmt);
#else
        fprintf(stderr, "vsnprintf failed: %s with format string \"%s\"\n", strerror(errno), fmt);
        exit(EXIT_FAILURE);
#endif
    }

    if ((size_t)nprinted < len) {
        /* Success.  Note nprinted does not include trailing null. */
        return (size_t)nprinted;
    }

    /*
     * We assume a C99-compliant vsnprintf, so believe its estimate of the
     * required space, and add one for the trailing null.  (If it's wrong, the
     * logic will still work, but we may loop multiple times.)
     *
     * Choke if the required space would exceed MaxAllocSize.  Note we use
     * this palloc-oriented overflow limit even when in frontend.
     */
    if (unlikely((size_t)nprinted > MaxAllocSize - 1)) {
#ifndef FRONTEND
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("out of memory")));
#else
        fprintf(stderr, _("out of memory\n"));
        exit(EXIT_FAILURE);
#endif
    }

    return nprinted + 1;
}

/*
 * create_foreign_join_path
 * 	  Creates a path corresponding to a scan of a foreign join,
 * 	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignJoinPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any way
 * to calculate them in core.  However, there is a usually-sane default for
 * the pathtarget (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *create_foreign_join_path(PlannerInfo *root, RelOptInfo *rel, List *target, double rows,
    Cost startup_cost, Cost total_cost, List *pathkeys, Relids required_outer, Path *fdw_outerpath, List *fdw_private)
{
    ForeignPath *pathnode = makeNode(ForeignPath);

    /*
     * We should use get_joinrel_parampathinfo to handle parameterized paths,
     * but the API of this function doesn't support it, and existing
     * extensions aren't yet trying to build such paths anyway.  For the
     * moment just throw an error if someone tries it; eventually we should
     * revisit this.
     */
    if (!bms_is_empty(required_outer) || !bms_is_empty(rel->lateral_relids)) {
        elog(ERROR, "parameterized foreign joins are not supported yet");
    }

    pathnode->path.pathtype = T_ForeignScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = NULL; /* XXX see above */
    pathnode->path.rows = rows;
    pathnode->path.startup_cost = startup_cost;
    pathnode->path.total_cost = total_cost;
    pathnode->path.pathkeys = pathkeys;
    pathnode->path.dop = 1;

    pathnode->fdw_outerpath = fdw_outerpath;
    pathnode->fdw_private = fdw_private;

    return pathnode;
}

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List *ExecInitExprList(List *nodes, PlanState *parent)
{
    List *result = NIL;
    ListCell *lc = NULL;

    foreach (lc, nodes) {
        Expr *e = (Expr *)lfirst(lc);
        result = lappend(result, ExecInitExpr(e, parent));
    }

    return result;
}

/*
 * get_namespace_name_or_temp
 * 		As above, but if it is this backend's temporary namespace, return
 * 		"pg_temp" instead.
 */
char *get_namespace_name_or_temp(Oid nspid)
{
    if (isTempNamespace(nspid)) {
        return pstrdup("pg_temp");
    } else {
        return get_namespace_name(nspid);
    }
}

/*
 * fetch_upper_rel
 * 		Build a RelOptInfo describing some post-scan/join query processing,
 * 		or return a pre-existing one if somebody already built it.
 *
 * An "upper" relation is identified by an UpperRelationKind and a Relids set.
 * The meaning of the Relids set is not specified here, and very likely will
 * vary for different relation kinds.
 *
 * Most of the fields in an upper-level RelOptInfo are not used and are not
 * set here (though makeNode should ensure they're zeroes).  We basically only
 * care about fields that are of interest to add_path() and set_cheapest().
 */
RelOptInfo *fetch_upper_rel(FDWUpperRelCxt *ufdwCxt, UpperRelationKind kind)
{
    /*
     * For the moment, our indexing data structure is just a List for each
     * relation kind.  If we ever get so many of one kind that this stops
     * working well, we can improve it.  No code outside this function should
     * assume anything about how to find a particular upperrel.
     */
    if (ufdwCxt->upperRels[kind] != NULL) {
        return ufdwCxt->upperRels[kind];
    }

    RelOptInfo *upperrel = makeNode(RelOptInfo);
    upperrel->reloptkind = RELOPT_UPPER_REL;
    upperrel->relids = NULL;

    upperrel->reltarget = makeNode(PathTarget);
    upperrel->pathlist = NIL;
    upperrel->cheapest_startup_path = NULL;
    upperrel->cheapest_total_path = NULL;
    upperrel->cheapest_unique_path = NULL;
    upperrel->cheapest_parameterized_paths = NIL;

    ufdwCxt->upperRels[kind] = upperrel;

    return upperrel;
}

List* extract_target_from_tel(FDWUpperRelCxt *ufdw_cxt, PgFdwRelationInfo *fpinfo)
{
    List* tel = NIL;
    switch (fpinfo->stage) {
        case UPPERREL_INIT:
            tel = ufdw_cxt->spjExtra->targetList;
            break;
        case UPPERREL_GROUP_AGG:
            tel = ufdw_cxt->groupExtra->targetList;
            break;
        case UPPERREL_ORDERED:
            tel = ufdw_cxt->orderExtra->targetList;
            break;
        case UPPERREL_FINAL:
            tel = ufdw_cxt->finalExtra->targetList;
            break;
        default:
            Assert(false);
            break;
    }

    fpinfo->complete_tlist = tel;
    
    List* tl = NULL;
    ListCell* lc = NULL;
    foreach(lc, tel) {
        Assert(IsA(lfirst(lc), TargetEntry));
        TargetEntry* te = (TargetEntry*)lfirst(lc);

        tl = lappend(tl, copyObject(te->expr));
    }

    return tl;
}

/*
 * make_upper_rel
 *
 * Create a new grouping rel and set basic properties.
 *
 * input_rel represents the underlying scan/join relation.
 * target is the output expected from the grouping relation.
 */
RelOptInfo *make_upper_rel(FDWUpperRelCxt *ufdwCxt, PgFdwRelationInfo *fpinfo)
{
    RelOptInfo *upper_rel = fetch_upper_rel(ufdwCxt, fpinfo->stage);

    /* Set target. */
    upper_rel->reltarget->exprs = extract_target_from_tel(ufdwCxt, fpinfo);

    /*
     * If the input rel belongs to a single FDW, so does the grouped rel.
     */
    upper_rel->serverid = ufdwCxt->currentRel->serverid;
    upper_rel->userid = ufdwCxt->currentRel->userid;
    upper_rel->useridiscurrent = ufdwCxt->currentRel->useridiscurrent;
    upper_rel->fdwroutine = ufdwCxt->currentRel->fdwroutine;
    upper_rel->fdw_private = fpinfo;

    return upper_rel;
}

/*
 * get_sortgroupref_clause_noerr
 * 		As above, but return NULL rather than throwing an error if not found.
 */
SortGroupClause *get_sortgroupref_clause_noerr(Index sortref, List *clauses)
{
    ListCell *l = NULL;

    foreach (l, clauses) {
        SortGroupClause *cl = (SortGroupClause *)lfirst(l);

        if (cl->tleSortGroupRef == sortref) {
            return cl;
        }
    }

    return NULL;
}

/*
 * create_foreign_upper_path
 * 	  Creates a path corresponding to an upper relation that's computed
 * 	  directly by an FDW, returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected to
 * be called by the GetForeignUpperPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any way
 * to calculate them in core.  However, there is a usually-sane default for
 * the pathtarget (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *create_foreign_upper_path(PlannerInfo *root, RelOptInfo *rel, List *target, double rows,
    Cost startup_cost, Cost total_cost, List *pathkeys, Path *fdw_outerpath, List *fdw_private)
{
    ForeignPath *pathnode = makeNode(ForeignPath);

    /*
     * Upper relations should never have any lateral references, since joining
     * is complete.
     */
    Assert(bms_is_empty(rel->lateral_relids));

    pathnode->path.pathtype = T_ForeignScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->path.param_info = NULL;
    pathnode->path.rows = rows;
    pathnode->path.startup_cost = startup_cost;
    pathnode->path.total_cost = total_cost;
    pathnode->path.pathkeys = pathkeys;
    pathnode->path.dop = 1;

    pathnode->fdw_outerpath = fdw_outerpath;
    pathnode->fdw_private = fdw_private;

    return pathnode;
}

/*
 * adjust_limit_rows_costs
 * 	  Adjust the size and cost estimates for a LimitPath node according to the
 * 	  offset/limit.
 *
 * This is only a cosmetic issue if we are at top level, but if we are
 * building a subquery then it's important to report correct info to the outer
 * planner.
 *
 * When the offset or count couldn't be estimated, use 10% of the estimated
 * number of rows emitted from the subpath.
 *
 * XXX we don't bother to add eval costs of the offset/limit expressions
 * themselves to the path costs.  In theory we should, but in most cases those
 * expressions are trivial and it's just not worth the trouble.
 */
void adjust_limit_rows_costs(double *rows, /* in/out parameter */
    Cost *startup_cost,                    /* in/out parameter */
    Cost *total_cost,                      /* in/out parameter */
    int64 offset_est, int64 count_est)
{
    double input_rows = *rows;
    Cost input_startup_cost = *startup_cost;
    Cost input_total_cost = *total_cost;

    if (offset_est != 0) {
        double offset_rows;

        if (offset_est > 0) {
            offset_rows = (double)offset_est;
        } else {
            offset_rows = clamp_row_est(input_rows * 0.10);
        }
        if (offset_rows > *rows) {
            offset_rows = *rows;
        }
        if (input_rows > 0) {
            *startup_cost += (input_total_cost - input_startup_cost) * offset_rows / input_rows;
        }
        *rows -= offset_rows;
        if (*rows < 1) {
            *rows = 1;
        }
    }

    if (count_est != 0) {
        double count_rows;

        if (count_est > 0) {
            count_rows = (double)count_est;
        } else {
            count_rows = clamp_row_est(input_rows * 0.10);
        }
        if (count_rows > *rows) {
            count_rows = *rows;
        }
        if (input_rows > 0) {
            *total_cost = *startup_cost + (input_total_cost - input_startup_cost) * count_rows / input_rows;
        }
        *rows = count_rows;
        if (*rows < 1) {
            *rows = 1;
        }
    }
}

/*
 * tlist_same_exprs
 * 		Check whether two target lists contain the same expressions
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
static bool tlist_same_exprs(List *tlist1, List *tlist2)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;

    if (list_length(tlist1) != list_length(tlist2)) {
        return false; /* not same length, so can't match */
    }

    forboth(lc1, tlist1, lc2, tlist2) {
        TargetEntry *tle1 = (TargetEntry *)lfirst(lc1);
        TargetEntry *tle2 = (TargetEntry *)lfirst(lc2);

        if (!equal(tle1->expr, tle2->expr)) {
            return false;
        }
    }

    return true;
}

/*
 * inject_projection_plan
 * 	  Insert a Result node to do a projection step.
 *
 * This is used in a few places where we decide on-the-fly that we need a
 * projection step as part of the tree generated for some Path node.
 * We should try to get rid of this in favor of doing it more honestly.
 *
 * One reason it's ugly is we have to be told the right parallel_safe marking
 * to apply (since the tlist might be unsafe even if the child plan is safe).
 */
static Plan *inject_projection_plan(PlannerInfo *root, Plan *subplan, List *tlist)
{
    Plan *plan = NULL;

    plan = (Plan *)make_result(root, tlist, NULL, subplan);

    /*
     * In principle, we should charge tlist eval cost plus cpu_per_tuple per
     * row for the Result node.  But the former has probably been factored in
     * already and the latter was not accounted for during Path construction,
     * so being formally correct might just make the EXPLAIN output look less
     * consistent not more so.  Hence, just copy the subplan's cost.
     */
    copy_plan_costsize(plan, subplan);

    return plan;
}

/*
 * change_plan_targetlist
 * 	  Externally available wrapper for inject_projection_plan.
 *
 * This is meant for use by FDW plan-generation functions, which might
 * want to adjust the tlist computed by some subplan tree.  In general,
 * a Result node is needed to compute the new tlist, but we can optimize
 * some cases.
 *
 * In most cases, tlist_parallel_safe can just be passed as the parallel_safe
 * flag of the FDW's own Path node.
 */
Plan *change_plan_targetlist(PlannerInfo *root, Plan *subplan, List *tlist)
{
    /*
     * If the top plan node can't do projections and its existing target list
     * isn't already what we need, we need to add a Result node to help it
     * along.
     */
    if (!is_projection_capable_plan(subplan) && !tlist_same_exprs(tlist, subplan->targetlist)) {
        subplan = inject_projection_plan(root, subplan, tlist);
    } else {
        /* Else we can just replace the plan node's tlist */
        subplan->targetlist = tlist;
    }
    return subplan;
}

/*
 * apply_tlist_labeling
 * 		Apply the TargetEntry labeling attributes of src_tlist to dest_tlist
 *
 * This is useful for reattaching column names etc to a plan's final output
 * targetlist.
 */
void apply_tlist_labeling(List *dest_tlist, List *src_tlist)
{
    ListCell *ld = NULL;
    ListCell *ls = NULL;

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
