/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * optimizerdebug.cpp
 *	  Generate random plan according to random mode and seed.
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/optimizerdebug.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include "fmgr.h"
#include "access/printtup.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/pathnode.h"
#include "optimizer/streamplan.h"
#include "parser/parsetree.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

static char* free_string_info_return_data(StringInfo buf)
{
    char* ret = buf->data;
    pfree_ext(buf);
    return ret;
}

/*****************************************************************************
 *		FUNCTION
 *****************************************************************************/
char* debug1_print_relids(Relids relids, List* rtable)
{
    Relids tmprelids;
    int x;
    bool first = true;
    RangeTblEntry* rte = NULL;
    StringInfo buf = makeStringInfo();

    tmprelids = bms_copy(relids);
    while ((x = bms_first_member(tmprelids)) >= 0) {
        if (!first) {
            appendStringInfoString(buf, " ");
        }
        appendStringInfo(buf, "%d:", x);
        rte = rt_fetch(x, rtable);
        appendStringInfo(buf, "%s ", rte->relname);
        first = false;
    }
    bms_free_ext(tmprelids);

    return free_string_info_return_data(buf);
}

/* print rel join ratio for debug. */
static char* debug1_print_reljoinratio(PlannerInfo* root, const Relids relids, const List* rtable)
{
    Relids tmprelids;
    int x;
    bool first = true;
    RangeTblEntry* rte = NULL;
    RelOptInfo* rel = NULL;
    StringInfo buf = makeStringInfo();
    ListCell* lc = NULL;

    tmprelids = bms_copy(relids);
    while ((x = bms_first_member(tmprelids)) >= 0) {
        if (!first) {
            appendStringInfoString(buf, "; ");
        }
        appendStringInfo(buf, "%d:", x);
        rte = rt_fetch(x, rtable);
        appendStringInfo(buf, "%s", rte->relname);

        rel = find_base_rel(root, x);
        Assert(rel);

        if (rel->reloptkind != RELOPT_BASEREL)
            continue;

        /* print join ratio info if the rel have. */
        if (rel->varratio) {
            int i = 0;
            appendStringInfoString(buf, "(");
            foreach (lc, rel->varratio) {
                VarRatio* vr = (VarRatio*)lfirst(lc);
                if (RatioType_Filter == vr->ratiotype) {
                    appendStringInfoString(buf, "filter,");
                } else {
                    appendStringInfo(buf, "%u,", *(uint32*)vr->joinrelids->words);
                }

                char* attribute_name = get_rte_attribute_name(rte, ((Var*)vr->var)->varattno);
                appendStringInfo(buf, "%s,", attribute_name);
                if (attribute_name != NULL)
                    pfree_ext(attribute_name);
                appendStringInfo(buf, "%lf", vr->ratio);
                i++;

                if (i != list_length(rel->varratio))
                    appendStringInfoString(buf, "; ");
            }
            appendStringInfoString(buf, ")");
        }
        first = false;
    }
    bms_free_ext(tmprelids);

    appendStringInfoString(buf, "\n");

    return free_string_info_return_data(buf);
}

static char* debug1_print_restrictclauses(const PlannerInfo* root, const List* clauses)
{
    ListCell* l = NULL;
    StringInfo buf = makeStringInfo();
    foreach (l, clauses) {
        RestrictInfo* c = (RestrictInfo*)lfirst(l);
        char* expr = ExprToString((Node*)c->clause, root->parse->rtable);

        appendStringInfoString(buf, expr);
        appendStringInfo(buf, "(norm_selec=%lf, outer_selec=%lf)", c->norm_selec, c->outer_selec);
        pfree_ext(expr);
        if (lnext(l))
            appendStringInfoString(buf, ", ");
    }

    return free_string_info_return_data(buf);
}

void debug1_print_jointype(const Path* path, const char** ptype)
{
    switch (((JoinPath*)path)->jointype) {
        case JOIN_SEMI:
            *ptype = "SemiHashJoin";
            break;
        case JOIN_ANTI:
            *ptype = "AntiHashJoin";
            break;
        case JOIN_RIGHT_SEMI:
            *ptype = "RightSemiHashJoin";
            break;
        case JOIN_RIGHT_ANTI:
            *ptype = "RightAntiHashJoin";
            break;
        default:
            *ptype = "HashJoin";
            break;
    }
    return;
}
static const char* debug1_print_pathtype(Path* path, NodeTag pathtype, Path** subpath, bool* join)
{
    const char* ptype = NULL;

    ptype = nodeTagToString(pathtype);

    switch (pathtype) {
        case T_Material:
            *subpath = ((MaterialPath*)path)->subpath;
            break;
        case T_Unique:
            *subpath = ((UniquePath*)path)->subpath;
            break;
        case T_NestLoop:
            *join = true;
            break;
        case T_MergeJoin:
            *join = true;
            break;
        case T_HashJoin:
            debug1_print_jointype(path, &ptype);
            *join = true;
            break;
#ifdef STREAMPLAN
        case T_Stream:
        case T_VecStream: {
            ptype = GetStreamTypeStrOf((StreamPath*)path);
        } break;
#endif
        default:
            /* noop */
            break;
    }

    return ptype;
}

char* debug1_print_path(PlannerInfo* root, Path* path, int indent)
{
    check_stack_depth();
    bool join = false;
    Path* subpath = NULL;
    char* pathBufPtr = NULL;
    int i;
    StringInfo buf = makeStringInfo();

    for (i = 0; i < indent; i++) {
        appendStringInfoString(buf, "\t");
    }
    appendStringInfo(buf, "%s", debug1_print_pathtype(path, path->pathtype, &subpath, &join));

    if (path->parent) {
        char* RelidBufPtr = NULL;
        appendStringInfoString(buf, "(");

        if (root != NULL && root->parse != NULL) {
            RelidBufPtr = debug1_print_relids(path->parent->relids, root->parse->rtable);
            appendStringInfoString(buf, RelidBufPtr);
            pfree_ext(RelidBufPtr);
        }

        if (IsA(path, IndexPath)) {
            appendStringInfo(buf, ") rows=%.0f multiple=%lf", path->rows, path->multiple);
        } else {
            appendStringInfo(buf, ") rows=%.0f multiple=%lf", path->parent->rows, path->parent->multiple);
        }
    }
    appendStringInfo(
        buf, " dop=%d cost=%.2f..%.2f hint %d\n", path->dop, path->startup_cost, path->total_cost, path->hint_value);

    if (join) {
        JoinPath* jp = (JoinPath*)path;
        char* restrictclauses = NULL;

        for (i = 0; i < indent; i++) {
            appendStringInfoString(buf, "\t");
        }
        appendStringInfoString(buf, "  clauses: ");
        restrictclauses = debug1_print_restrictclauses(root, jp->joinrestrictinfo);
        appendStringInfoString(buf, restrictclauses);
        pfree_ext(restrictclauses);
        appendStringInfoString(buf, "\n");

        if (IsA(path, MergePath)) {
            MergePath* mp = (MergePath*)path;

            for (i = 0; i < indent; i++) {
                appendStringInfoString(buf, "\t");
            }

            appendStringInfo(buf,
                "  sortouter=%d sortinner=%d materializeinner=%d\n",
                ((mp->outersortkeys) ? 1 : 0),
                ((mp->innersortkeys) ? 1 : 0),
                ((mp->materialize_inner) ? 1 : 0));
        }

        pathBufPtr = debug1_print_path(root, jp->outerjoinpath, indent + 1);
        appendStringInfoString(buf, pathBufPtr);
        pfree_ext(pathBufPtr);

        pathBufPtr = debug1_print_path(root, jp->innerjoinpath, indent + 1);
        appendStringInfoString(buf, pathBufPtr);
        pfree_ext(pathBufPtr);
    }

    if (subpath != NULL) {
        pathBufPtr = debug1_print_path(root, subpath, indent + 1);
        appendStringInfoString(buf, pathBufPtr);
        pfree_ext(pathBufPtr);
    }

    return free_string_info_return_data(buf);
}

void debug1_print_rel(PlannerInfo* root, RelOptInfo* rel)
{
    check_stack_depth();
    ListCell* l = NULL;
    char* tmpBufPtr = NULL;
    char* restrictclauses = NULL;

    if (log_min_messages > DEBUG1)
        return;

    StringInfo buf = makeStringInfo();
    appendStringInfoChar(buf, '\n');
    appendStringInfoChar(buf, '{');
    appendStringInfoChar(buf, '\n');
    appendStringInfoString(buf, "\t");
    appendStringInfoString(buf, "RELOPTINFO (");
    tmpBufPtr = debug1_print_relids(rel->relids, root->parse->rtable);
    appendStringInfoString(buf, tmpBufPtr);
    pfree_ext(tmpBufPtr);

    appendStringInfo(buf, "): rows=%.0f, width=%d, multiple=%lf\n", rel->rows, rel->reltarget->width, rel->multiple);

    if (rel->baserestrictinfo) {
        appendStringInfoString(buf, "\tbaserestrictinfo: ");
        restrictclauses = debug1_print_restrictclauses(root, rel->baserestrictinfo);
        appendStringInfoString(buf, restrictclauses);
        pfree_ext(restrictclauses);
        appendStringInfoString(buf, "\n");
    }

    if (rel->joininfo) {
        appendStringInfoString(buf, "\tjoininfo: ");
        restrictclauses = debug1_print_restrictclauses(root, rel->joininfo);
        appendStringInfoString(buf, restrictclauses);
        pfree_ext(restrictclauses);
        appendStringInfoString(buf, "\n");
    }

    appendStringInfoString(buf, "\tpath list:\n");
    foreach (l, rel->pathlist) {
        tmpBufPtr = debug1_print_path(root, (Path*)lfirst(l), 1);
        appendStringInfoString(buf, tmpBufPtr);
        pfree_ext(tmpBufPtr);
    }

    if (rel->cheapest_startup_path) {
        appendStringInfoString(buf, "\n\tcheapest startup path:\n");
        tmpBufPtr = debug1_print_path(root, rel->cheapest_startup_path, 1);
        appendStringInfoString(buf, tmpBufPtr);
        pfree_ext(tmpBufPtr);
    }

    if (rel->cheapest_total_path) {
        appendStringInfoString(buf, "\n\tcheapest total path:\n");
        foreach (l, rel->cheapest_total_path) {
            tmpBufPtr = debug1_print_path(root, (Path*)lfirst(l), 1);
            appendStringInfoString(buf, tmpBufPtr);
            pfree_ext(tmpBufPtr);
        }
    }

    if (rel->cheapest_gather_path) {
        appendStringInfoString(buf, "\n\tcheapest gather path:\n");
        tmpBufPtr = debug1_print_path(root, rel->cheapest_gather_path, 1);
        appendStringInfoString(buf, tmpBufPtr);
        pfree_ext(tmpBufPtr);
    }

    appendStringInfoChar(buf, '\n');
    appendStringInfoChar(buf, '}');
    appendStringInfoChar(buf, '\n');

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("%s", buf->data), errhidestmt(true))));
    DestroyStringInfo(buf);
    return;
}

void debug_print_hashjoin_detail(PlannerInfo* root, HashPath* path, double virtualbuckets, Selectivity innerbucketsize,
    double outer_scan_ratio, Cost startup_cost, Cost total_cost)
{
    char* RelidBufPtr = NULL;
    Path* subpath = NULL;
    bool join = false;
    const char* ptype = NULL;

    if (log_min_messages > DEBUG1)
        return;

    StringInfo buf = makeStringInfo();

    appendStringInfoChar(buf, '\n');
    appendStringInfoChar(buf, '{');
    appendStringInfoChar(buf, '\n');
    appendStringInfoChar(buf, '\t');
    debug1_print_jointype((Path*)path, &ptype);
    appendStringInfo(buf, "%s", ptype);

    if (path->jpath.path.parent) {
        appendStringInfoString(buf, "(");

        RelidBufPtr = debug1_print_relids(path->jpath.path.parent->relids, root->parse->rtable);
        appendStringInfoString(buf, RelidBufPtr);
        pfree_ext(RelidBufPtr);

        appendStringInfo(
            buf, ") rows=%.0f, multiple=%lf, ", path->jpath.path.parent->rows, path->jpath.path.parent->multiple);
    }
    appendStringInfo(
        buf, "dop: %d, startup_cost: %lf, total_cost: %lf\n", path->jpath.path.dop, startup_cost, total_cost);

    if (path->jpath.joinrestrictinfo) {
        char* restrictclauses = NULL;
        appendStringInfoString(buf, "\tjoinrestrictinfo: ");
        restrictclauses = debug1_print_restrictclauses(root, path->jpath.joinrestrictinfo);
        appendStringInfoString(buf, restrictclauses);
        pfree_ext(restrictclauses);
        appendStringInfoString(buf, "\n");
    }

    if (path->jpath.path.parent) {
        char* reljoinratio_string =
            debug1_print_reljoinratio(root, path->jpath.path.parent->relids, root->parse->rtable);
        appendStringInfo(buf, "\treljoinratioinfo: %s", reljoinratio_string);
        pfree_ext(reljoinratio_string);
    }

    /* print outer path info. */
    appendStringInfo(buf,
        "\touter_path = %s",
        debug1_print_pathtype(path->jpath.outerjoinpath, path->jpath.outerjoinpath->pathtype, &subpath, &join));
    RelidBufPtr = debug1_print_relids(path->jpath.outerjoinpath->parent->relids, root->parse->rtable);
    appendStringInfo(buf, "(%s)\n", RelidBufPtr);
    pfree_ext(RelidBufPtr);

    /* print inner path info. */
    appendStringInfo(buf,
        "\tinner_path = %s",
        debug1_print_pathtype(path->jpath.innerjoinpath, path->jpath.innerjoinpath->pathtype, &subpath, &join));
    RelidBufPtr = debug1_print_relids(path->jpath.innerjoinpath->parent->relids, root->parse->rtable);
    appendStringInfo(buf, "(%s)\n", RelidBufPtr);
    pfree_ext(RelidBufPtr);

    appendStringInfo(buf,
        "\tdop: %d, outer_path_rows: %lf, outer_path_global_rows: %lf\n",
        path->jpath.outerjoinpath->dop,
        PATH_LOCAL_ROWS(path->jpath.outerjoinpath),
        path->jpath.outerjoinpath->rows);
    appendStringInfo(buf,
        "\tdop: %d, inner_path_rows: %lf, inner_path_global_rows: %lf\n",
        path->jpath.innerjoinpath->dop,
        PATH_LOCAL_ROWS(path->jpath.innerjoinpath),
        path->jpath.innerjoinpath->rows);
    appendStringInfo(buf,
        "\tinnerbucketsize: %lf, outerscanratio: %lf, virtualbucket: %lf\n",
        innerbucketsize,
        outer_scan_ratio,
        virtualbuckets);

    appendStringInfoChar(buf, '}');
    appendStringInfoChar(buf, '\n');
    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("%s", buf->data), errhidestmt(true))));
    DestroyStringInfo(buf);

    return;
}

void debug_print_agg_detail(PlannerInfo* root, AggStrategy aggstrategy, SAggMethod aggmethod, Path* path1, Path* path2,
    Path* path3, Path* path4, Path* path5)
{
    bool join = false;
    ListCell* lc = NULL;
    int indent = 1;
    const char* aggstgname = NULL;
    const char* aggmthname = NULL;
    Path* subpath = NULL;
    List* pathlist = NULL;

    if (log_min_messages > DEBUG1)
        return;

    StringInfo buf = makeStringInfo();
    appendStringInfoChar(buf, '\n');
    appendStringInfoChar(buf, '{');
    appendStringInfoChar(buf, '\n');
    appendStringInfoString(buf, "\t");

    /* print agg type. */
    switch (aggstrategy) {
        case AGG_PLAIN:
            aggstgname = "Aggregate";
            aggmthname = "DN_AGG_CN";
            break;
        case AGG_SORTED:
        case AGG_SORT_GROUP:
            aggstgname = "GroupAggregate";
            break;
        case AGG_HASHED:
            aggstgname = "HashAggregate";
            break;
    }

#ifdef STREAMPLAN
    if (aggstrategy != AGG_PLAIN) {
        switch (aggmethod) {
            case OPTIMAL_AGG:
                Assert(false);
                break;
            case DN_AGG_CN_AGG:
                aggmthname = "DN_AGG_CN_AGG";
                break;
            case DN_REDISTRIBUTE_AGG:
                aggmthname = "DN_REDISTRIBUTE_AGG";
                break;
            case DN_AGG_REDISTRIBUTE_AGG:
                aggmthname = "DN_AGG_REDISTRIBUTE_AGG";
                break;
            case DN_REDISTRIBUTE_AGG_CN_AGG:
                aggmthname = "DN_REDISTRIBUTE_AGG_CN_AGG";
                break;
            case DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG:
                aggmthname = "DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG";
                break;
        }

        pathlist = lappend(pathlist, path1);
        pathlist = NULL == path2 ? pathlist : lappend(pathlist, path2);
        pathlist = NULL == path3 ? pathlist : lappend(pathlist, path3);
        pathlist = NULL == path4 ? pathlist : lappend(pathlist, path4);
        pathlist = NULL == path5 ? pathlist : lappend(pathlist, path5);
    }
#endif

    appendStringInfo(buf, "AggStrategy: %s, SAggMethod: %s.\n", aggstgname, aggmthname);

    /* print agg detail info, include distinct,cost,distribute_key. */
    foreach (lc, pathlist) {
        Path* path = (Path*)lfirst(lc);

        for (int i = 0; i < indent; i++)
            appendStringInfoString(buf, "\t");
        appendStringInfo(buf, "%s", debug1_print_pathtype(path, path->pathtype, &subpath, &join));
        appendStringInfo(buf, " (rows=%.0f, global_rows=%.0f,", PATH_LOCAL_ROWS(path), path->rows);
        appendStringInfo(buf, " cost=%.2f..%.2f)\n", path->startup_cost, path->total_cost);
        indent++;
    }

    appendStringInfoChar(buf, '\n');
    appendStringInfoChar(buf, '}');
    appendStringInfoChar(buf, '\n');
    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("%s", buf->data), errhidestmt(true))));

    if (pathlist != NULL) {
        list_free_ext(pathlist);
        pathlist = NULL;
    }

    DestroyStringInfo(buf);
    return;
}
