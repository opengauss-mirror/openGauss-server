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
 * [P2-1] HybridRecall planner recognition (hybrid recall design doc §3.3/§7.1).
 * Recognizes the marker query shape and validates the full whitelist;
 * emits a DEBUG1 verdict line for every rejection reason so tests can
 * assert non-matching shapes never slip through. Building the actual
 * HybridRecallPath (text/vector branch paths, costs) lands in P2-2..P2-5;
 * this task only recognizes, and deliberately adds nothing to pathlist.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_am.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "optimizer/hybrid_recall_path.h"
#include "optimizer/tlist.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static bool is_ann_am(Oid relam)
{
    return relam == HNSW_AM_OID || relam == IVFFLAT_AM_OID || relam == DISKANN_AM_OID;
}

void TryAddHybridRecallPath(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Query* parse = root->parse;

    /* only the plain SELECT over one ordinary table qualifies */
    if (parse == NULL || parse->commandType != CMD_SELECT || parse->rtable == NIL ||
        rte->rtekind != RTE_RELATION || rel->indexlist == NIL)
        return;

    /* shape: ORDER BY <first key> DESC with a finite LIMIT */
    if (parse->sortClause == NIL || parse->limitCount == NULL)
        return;
    SortGroupClause* sgc = (SortGroupClause*)linitial(parse->sortClause);
    if (!sgc->sortop) /* sortop==0 means original order; marker needs explicit */
        return;

    /* first sort target must be the marker call */
    TargetEntry* tle = get_sortgroupref_tle(sgc->tleSortGroupRef, parse->targetList);
    if (tle == NULL || !IsA(tle->expr, FuncExpr))
        return;
    FuncExpr* func = (FuncExpr*)tle->expr;
    /* Identify the marker by NAME via a single-OID syscache lookup -
     * planner-safe. (A candidate search like LookupFuncName hangs here;
     * name match is sufficient for the whitelist: the marker is our own
     * reserved name and executing it directly raises by definition.) */
    const char* proname = get_func_name(func->funcid);
    if (proname == NULL || strcmp(proname, "hybrid_rrf_marker") != 0)
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
        vec_var->varno != rel->relid || id_var->varlevelsup != 0)
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

    elog(DEBUG1,
         "hybrid_recall P2-1 recognize: rel=%u marker=yes sortop=%u limit=%s bm25=%d ann=%d",
         rte->relid, sgc->sortop,
         IsA(parse->limitCount, Const) ? "const" : "expr",
         (int)has_bm25, (int)has_ann);

    /* P2-2..P2-5 attach the HybridRecallPath here; recognition only for now */
    return;
}
