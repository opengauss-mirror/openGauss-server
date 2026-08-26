/* -------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * src/gausskernel/optimizer/path/hybridRecallPath.cpp
 *
 * [P2] HybridRecall planner recognition and branch metadata construction
 * (hybrid recall design doc §3.3/§7).  V1 recognizes only a deliberately
 * narrow top-level shape, removes the reserved marker, and records the
 * common filter for identical branch pushdown plus fusion-layer recheck.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include <math.h>
#include "access/skey.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "optimizer/clauses.h"
#include "optimizer/hybrid_recall_path.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static bool is_ann_am(Oid relam)
{
    return relam == HNSW_AM_OID || relam == IVFFLAT_AM_OID || relam == DISKANN_AM_OID;
}

/*
 * [P2-6] Initial latency cost model (design doc section 11).
 *
 * HybridRecall is blocking: neither alternative can return its first tuple
 * before both ranked candidate sets have been fused, so startup_cost equals
 * total_cost.  The serial alternative pays both branch estimates, while the
 * task-stream alternative pays the slower branch plus local stream startup
 * and transfer.  The square-root terms approximate increasing index search
 * work without pretending either AM scans the whole relation; P3 benchmarks
 * will calibrate the coefficients and worker-pressure penalties.
 */
static void cost_hybrid_recall_path(HybridRecallPath* path, RelOptInfo* rel)
{
    double table_rows = rel->tuples;
    if (table_rows < 1.0)
        table_rows = rel->rows;
    if (table_rows < 1.0)
        table_rows = 1.0;

    double sqrt_rows = sqrt(table_rows);
    double log2_rows = log(table_rows + 1.0) / log(2.0);
    double text_rows = (double)path->text_candidates_est;
    double vector_rows = (double)path->vector_candidates_est;
    double candidate_rows = text_rows + vector_rows;
    double heap_levels = log((double)path->final_topk_est + 1.0) / log(2.0);

    Cost text_cost = u_sess->attr.attr_sql.random_page_cost +
        log2_rows * u_sess->attr.attr_sql.cpu_operator_cost * 25.0 +
        sqrt_rows * u_sess->attr.attr_sql.cpu_operator_cost * 6.0 +
        text_rows * u_sess->attr.attr_sql.cpu_index_tuple_cost;
    Cost vector_cost = u_sess->attr.attr_sql.random_page_cost * 2.0 +
        log2_rows * u_sess->attr.attr_sql.cpu_operator_cost * 50.0 +
        sqrt_rows * u_sess->attr.attr_sql.cpu_operator_cost * 12.0 +
        vector_rows * u_sess->attr.attr_sql.cpu_index_tuple_cost;
    Cost fusion_cost = candidate_rows *
        (u_sess->attr.attr_sql.cpu_tuple_cost + 2.0 * u_sess->attr.attr_sql.cpu_operator_cost) +
        candidate_rows * heap_levels * u_sess->attr.attr_sql.cpu_operator_cost;
    Cost stream_startup_cost = u_sess->attr.attr_sql.random_page_cost +
        150.0 * u_sess->attr.attr_sql.cpu_tuple_cost;
    Cost stream_transfer_cost = candidate_rows * u_sess->attr.attr_sql.cpu_tuple_cost * 0.5;

    Cost serial_cost = text_cost + vector_cost + fusion_cost;
    Cost parallel_cost = stream_startup_cost + Max(text_cost, vector_cost) +
        stream_transfer_cost + fusion_cost;
    Cost chosen_cost = path->parallel_branches ? parallel_cost : serial_cost;

    path->path.startup_cost = chosen_cost;
    path->path.total_cost = chosen_cost;
}

static HybridRecallPath* make_hybrid_recall_path(RelOptInfo* rel,
                                                 Index relid,
                                                 AttrNumber id_attno,
                                                 AttrNumber text_attno,
                                                 AttrNumber vector_attno,
                                                 Node* query_text,
                                                 Node* query_vector,
                                                 List* common_quals,
                                                 int64 final_topk,
                                                 bool parallel_branches,
                                                 bool require_parallel)
{
    HybridRecallPath* path = (HybridRecallPath*)palloc0(sizeof(HybridRecallPath));
    path->path.type = T_HybridRecallPath;
    path->path.pathtype = T_HybridRecall;
    path->path.parent = rel;
    path->path.pathkeys = NIL; /* fused order is delivered, const sort dropped */
    path->path.rows = (double)final_topk;
    if (path->path.rows < 1)
        path->path.rows = 1;

    path->text_path = NULL;  /* branches are planned recursively at plan time */
    path->vector_path = NULL;
    path->relid = relid; /* RT index, not relation OID */
    path->id_attno = id_attno;
    path->text_attno = text_attno;
    path->vector_attno = vector_attno;
    path->query_text = (Expr*)copyObject(query_text);
    path->query_vector = (Expr*)copyObject(query_vector);
    path->common_quals = (List*)copyObject(common_quals);
    path->final_topk_est = final_topk;
    path->text_candidates_est = 100;
    path->vector_candidates_est = 100;
    path->fusion = HYBRID_FUSION_RRF;
    path->rrf_k = 60.0;
    path->text_weight = 0.5;
    path->vector_weight = 0.5;
    path->parallel_branches = parallel_branches;
    path->require_parallel = require_parallel;
    cost_hybrid_recall_path(path, rel);
    return path;
}

void TryAddHybridRecallPath(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Query* parse = root->parse;

    /* V1 is a top-level, plain SELECT over one ordinary table.  In
     * particular, do not inject task streams below an upper query level:
     * rescan/generation semantics for correlated or otherwise nested plans
     * are deliberately deferred (design doc §1.3/§8.8 and test P06). */
    if (parse == NULL || root->parent_root != NULL || parse->commandType != CMD_SELECT ||
        parse->hasAggs || parse->hasWindowFuncs || parse->hasSubLinks || parse->hasForUpdate ||
        parse->groupClause != NIL || parse->groupingSets != NIL || parse->havingQual != NULL ||
        parse->distinctClause != NIL || parse->setOperations != NULL ||
        list_length(parse->rtable) != 1 || rel->relid != 1 ||
        rte->rtekind != RTE_RELATION || rel->indexlist == NIL)
        return;

    /* shape: ORDER BY <first key> DESC with a finite LIMIT */
    if (list_length(parse->sortClause) != 1 || parse->limitCount == NULL ||
        parse->limitOffset != NULL)
        return;
    SortGroupClause* sgc = (SortGroupClause*)linitial(parse->sortClause);
    Oid sort_opfamily = InvalidOid;
    Oid sort_input_type = InvalidOid;
    int16 sort_strategy = 0;
    if (!OidIsValid(sgc->sortop) ||
        !get_ordering_op_properties(sgc->sortop, &sort_opfamily, &sort_input_type, &sort_strategy) ||
        sort_strategy != BTGreaterStrategyNumber)
        return;

    /* first sort target must be the marker call */
    TargetEntry* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, parse->targetList);
    if (tle == NULL || !IsA(tle->expr, FuncExpr))
        return;
    FuncExpr* func = (FuncExpr*)tle->expr;
    /* Identify the reserved marker by both name and namespace.  A public
     * look-alike must never acquire HybridRecall semantics. */
    const char* proname = get_func_name(func->funcid);
    Oid ogai_namespace = get_namespace_oid("ogai", true);
    if (proname == NULL || strcmp(proname, "hybrid_rrf_marker") != 0 ||
        !OidIsValid(ogai_namespace) || get_func_namespace(func->funcid) != ogai_namespace)
        return;

    /* marker args: (id_var, text_var, vector_var, query_text, query_vector,
     * options) - first three must be plain Vars of THIS relation */
    if (list_length(func->args) != 6)
        return;
    Var* id_var = (Var*)linitial(func->args);
    Var* text_var = (Var*)lsecond(func->args);
    Var* vec_var = (Var*)lthird(func->args);
    if (!IsA(id_var, Var) || !IsA(text_var, Var) || !IsA(vec_var, Var))
        return;
    if (id_var->varno != rel->relid || text_var->varno != rel->relid ||
        vec_var->varno != rel->relid || id_var->varlevelsup != 0 ||
        text_var->varlevelsup != 0 || vec_var->varlevelsup != 0 ||
        id_var->varattno <= 0 || text_var->varattno <= 0 || vec_var->varattno <= 0)
        return;

    /* whitelist: a BM25 index on the text column and an ANN index on the
     * vector column must both exist */
    bool has_bm25 = false;
    bool has_ann = false;
    ListCell* lc = NULL;
    foreach (lc, rel->indexlist) {
        IndexOptInfo* idx = (IndexOptInfo*)lfirst(lc);
        if (idx->indexkeys == NULL || idx->indexkeys[0] == 0)
            continue;
        if (idx->relam == BM25_AM_OID && idx->indexkeys[0] == text_var->varattno)
            has_bm25 = true;
        if (is_ann_am(idx->relam) && idx->indexkeys[0] == vec_var->varattno)
            has_ann = true;
    }

    if (!has_bm25 || !has_ann)
        return;

    /* First cut requires a finite int8 constant final K and no OFFSET
     * (P2-7 lifts this).  LIMIT NULL means ALL, so it is not finite. */
    if (!IsA(parse->limitCount, Const) || ((Const*)parse->limitCount)->constisnull ||
        ((Const*)parse->limitCount)->consttype != INT8OID)
        return;
    int64 final_topk = DatumGetInt64(((Const*)parse->limitCount)->constvalue);

    Node* q_text_raw = (Node*)list_nth(func->args, 3);
    Node* q_vec_raw = (Node*)list_nth(func->args, 4);
    if (IsA(q_text_raw, TargetEntry))
        q_text_raw = (Node*)((TargetEntry*)q_text_raw)->expr;
    if (IsA(q_vec_raw, TargetEntry))
        q_vec_raw = (Node*)((TargetEntry*)q_vec_raw)->expr;
    if (contain_volatile_functions(q_text_raw) || contain_volatile_functions(q_vec_raw) ||
        contain_subplans(q_text_raw) || contain_subplans(q_vec_raw))
        return;

    /*
     * [P2-4] Normalize the public filter before mutating the marker query.
     * Every safe expression is copied into both synthesized branch queries
     * and retained on HybridRecall for a defensive recheck after dedup.
     * A volatile expression or SubPlan cannot be evaluated independently in
     * two task workers without changing query semantics, so V1 does not
     * generate the parallel path for that shape.
     */
    List* common_quals = NIL;
    {
        ListCell* qc = NULL;
        Node* quals = (Node*)parse->jointree->quals;
        List* qual_list = quals == NULL ? NIL : (IsA(quals, List) ? (List*)quals : list_make1(quals));
        foreach (qc, qual_list) {
            Node* item = (Node*)lfirst(qc);
            if (item == NULL)
                continue;
            if (IsA(item, TargetEntry))
                item = (Node*)((TargetEntry*)item)->expr;
            else if (IsA(item, RestrictInfo))
                item = (Node*)((RestrictInfo*)item)->clause;
            if (contain_volatile_functions(item) || contain_subplans(item))
                return;
            common_quals = lappend(common_quals, copyObject(item));
        }
    }

    elog(DEBUG1,
         "hybrid_recall P2-2 path added: rel=%u sortop=%u bm25=%d ann=%d",
         rte->relid, sgc->sortop, (int)has_bm25, (int)has_ann);

    /*
     * [P2-2] Eliminate the marker from the query (design doc §7.7): replace
     * the FuncExpr with a Const float8 so nothing ever evaluates it. The
     * fused score column is produced by the HybridRecall node; the visible
     * hybrid_score is recovered by the Result projection built in createplan.
     */
    tle->expr = (Expr*)makeConst(FLOAT8OID, -1, InvalidOid, sizeof(float8),
                                 Float8GetDatum(0.0), false, true);

    /*
     * The fused order IS the requested order; drop the sort machinery that
     * preprocessing built for the marker (pathkeys were computed before
     * this hook ran - clearing them prevents a Sort that would otherwise
     * try to locate the now-replaced marker item).
     */
    parse->sortClause = NIL;
    root->query_pathkeys = NIL;
    root->sort_pathkeys = NIL;
    root->group_pathkeys = NIL;

    /*
     * The reserved marker is a planner directive, not a function with a
     * meaningful scalar fallback.  Once recognized we have also replaced
     * the marker and removed its sort clause, so allowing an ordinary base
     * path to win would silently return score 0 in heap order (most visible
     * for a small LIMIT).  Make the semantic rewrite atomic by replacing the
     * relation's ordinary paths with the HybridRecall path.  Costing still
     * describes this path internally; it must not decide whether marker
     * semantics apply.
     */
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;
    switch (u_sess->attr.attr_sql.hybrid_recall_parallel_mode) {
        case HYBRID_RECALL_PARALLEL_OFF:
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, false, false));
            break;
        case HYBRID_RECALL_PARALLEL_FORCE:
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, true, true));
            break;
        case HYBRID_RECALL_PARALLEL_AUTO:
        default:
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, false, false));
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, true, false));
            break;
    }
    set_cheapest(rel);
}
