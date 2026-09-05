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
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

typedef struct HybridRecallOptions {
    int64 text_candidates;
    int64 vector_candidates;
    double rrf_k;
    double text_weight;
    double vector_weight;
    bool require_parallel;
} HybridRecallOptions;

static bool hybrid_jsonb_string_equals(const JsonbValue* value, const char* expected)
{
    int expected_len = strlen(expected);
    return value != NULL && value->type == jbvString &&
        value->string.len == expected_len &&
        strncmp(value->string.val, expected, expected_len) == 0;
}

static JsonbValue* hybrid_jsonb_get(Jsonb* options, const char* name)
{
    JsonbValue key;
    key.type = jbvString;
    key.string.val = (char*)name;
    key.string.len = strlen(name);
    return findJsonbValueFromSuperHeader(VARDATA_ANY(options), JB_FOBJECT, NULL, &key);
}

static double hybrid_jsonb_number(JsonbValue* value, const char* name)
{
    if (value == NULL || value->type != jbvNumeric)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"%s\" must be a number", name)));

    double result = DatumGetFloat8(
        DirectFunctionCall1(numeric_float8, PointerGetDatum(value->numeric)));
    if (!isfinite(result))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"%s\" must be finite", name)));
    return result;
}

static int64 hybrid_jsonb_integer(JsonbValue* value, const char* name)
{
    if (value == NULL || value->type != jbvNumeric)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"%s\" must be an integer", name)));

    Datum numeric = PointerGetDatum(value->numeric);
    Datum truncated = DirectFunctionCall2(numeric_trunc, numeric, Int32GetDatum(0));
    if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, numeric, truncated)) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"%s\" must be an integer", name)));

    /* numeric_int8 performs the range check before conversion, avoiding an
     * undefined out-of-range float-to-int cast for hostile JSONB input. */
    return DatumGetInt64(DirectFunctionCall1(numeric_int8, numeric));
}

static bool hybrid_option_key_supported(const JsonbValue* key)
{
    static const char* supported[] = {
        "strategy", "text_candidates", "vector_candidates", "rrf_k",
        "weights", "on_worker_shortage", "filters", "query_vector"
    };

    for (uint32 i = 0; i < lengthof(supported); i++) {
        if (hybrid_jsonb_string_equals(key, supported[i]))
            return true;
    }
    return false;
}

static void hybrid_parse_options(Node* options_expr, HybridRecallOptions* result)
{
    result->text_candidates = 100;
    result->vector_candidates = 100;
    result->rrf_k = (double)u_sess->datavec_ctx.hybrid_recall_rrf_k;
    result->text_weight = 0.5;
    result->vector_weight = 0.5;
    result->require_parallel = false;

    /* P3-1 keeps options a planner constant.  The OGAI wrapper quotes a
     * validated JSONB value into its generated marker query, so prepared
     * query text/vector/final-K parameters remain independent. */
    if (!IsA(options_expr, Const))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("hybrid recall options must be a JSONB constant")));

    Const* options_const = (Const*)options_expr;
    if (options_const->constisnull || options_const->consttype != JSONBOID)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall options must be a non-NULL JSONB object")));

    Jsonb* options = DatumGetJsonb(options_const->constvalue);
    if (!JB_ROOT_IS_OBJECT(options))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall options must be a JSONB object")));

    JsonbIterator* iterator = JsonbIteratorInit(VARDATA_ANY(options));
    JsonbValue item;
    int token;
    while ((token = JsonbIteratorNext(&iterator, &item, true)) != WJB_DONE) {
        if (token == WJB_KEY && !hybrid_option_key_supported(&item)) {
            char* key = pnstrdup(item.string.val, item.string.len);
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unsupported hybrid recall option \"%s\"", key)));
        }
    }

    JsonbValue* value = hybrid_jsonb_get(options, "strategy");
    if (value != NULL && !hybrid_jsonb_string_equals(value, "parallel_rrf"))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"strategy\" must be \"parallel_rrf\"")));

    value = hybrid_jsonb_get(options, "text_candidates");
    if (value != NULL)
        result->text_candidates = hybrid_jsonb_integer(value, "text_candidates");
    value = hybrid_jsonb_get(options, "vector_candidates");
    if (value != NULL)
        result->vector_candidates = hybrid_jsonb_integer(value, "vector_candidates");
    value = hybrid_jsonb_get(options, "rrf_k");
    if (value != NULL)
        result->rrf_k = hybrid_jsonb_number(value, "rrf_k");

    value = hybrid_jsonb_get(options, "weights");
    if (value != NULL) {
        if (value->type != jbvBinary || !JsonbSuperHeaderIsArray(value->binary.data))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("hybrid recall option \"weights\" must be a two-number array")));
        JsonbIterator* weights = JsonbIteratorInit(value->binary.data);
        JsonbValue weight;
        int weight_token;
        int weight_count = 0;
        while ((weight_token = JsonbIteratorNext(&weights, &weight, true)) != WJB_DONE) {
            if (weight_token != WJB_ELEM)
                continue;
            if (weight_count == 0)
                result->text_weight = hybrid_jsonb_number(&weight, "weights[0]");
            else if (weight_count == 1)
                result->vector_weight = hybrid_jsonb_number(&weight, "weights[1]");
            weight_count++;
        }
        if (weight_count != 2)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("hybrid recall option \"weights\" must contain exactly two numbers")));
    }

    value = hybrid_jsonb_get(options, "on_worker_shortage");
    if (value != NULL) {
        if (hybrid_jsonb_string_equals(value, "error"))
            result->require_parallel = true;
        else if (!hybrid_jsonb_string_equals(value, "serial"))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("hybrid recall option \"on_worker_shortage\" must be \"serial\" or \"error\"")));
    }

    value = hybrid_jsonb_get(options, "filters");
    if (value != NULL &&
        (value->type != jbvBinary || !JsonbSuperHeaderIsObject(value->binary.data)))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"filters\" must be a JSONB object")));

    value = hybrid_jsonb_get(options, "query_vector");
    if (value != NULL &&
        (value->type != jbvBinary || !JsonbSuperHeaderIsArray(value->binary.data)))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall option \"query_vector\" must be a JSON array")));

    int max_branch = u_sess->attr.attr_sql.hybrid_recall_max_candidates_branch;
    int max_total = u_sess->attr.attr_sql.hybrid_recall_max_candidates_total;
    if (result->text_candidates < 1 || result->text_candidates > max_branch ||
        result->vector_candidates < 1 || result->vector_candidates > max_branch ||
        result->text_candidates + result->vector_candidates > max_total)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("hybrid recall candidates exceed configured limits"),
                 errdetail("text=%ld vector=%ld; each must be 1..%d and total must be <= %d",
                           result->text_candidates, result->vector_candidates,
                           max_branch, max_total)));
    if (result->rrf_k <= 0.0 || result->text_weight < 0.0 || result->vector_weight < 0.0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("hybrid recall RRF options are invalid")));
}

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
    /* Stream startup calibration (2026-09-05, 100k-row hybrid bench, release):
     * the fixed per-query cost of one task-stream producer/consumer pair
     * measured ~6 ms (serial off 3.6 ms vs force 9.5 ms at cand=100), while
     * the model's serial_cost for that query is ~33 units -> 1 ms ~ 9 units,
     * so the startup term must be ~54 units.  The old 150 * cpu_tuple_cost
     * (~1.5 units) made auto pick parallel on small-candidate queries where
     * it is 2.6x slower in practice.  Revisit after any stream-path cost
     * fix (e.g. protocol handshaking), see OPT-2 half-force PoC notes. */
    Cost stream_startup_cost = u_sess->attr.attr_sql.random_page_cost +
        5000.0 * u_sess->attr.attr_sql.cpu_tuple_cost;
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
                                                 const HybridRecallOptions* options,
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
    path->text_candidates_est = options->text_candidates;
    path->vector_candidates_est = options->vector_candidates;
    path->fusion = HYBRID_FUSION_RRF;
    path->rrf_k = options->rrf_k;
    path->text_weight = options->text_weight;
    path->vector_weight = options->vector_weight;
    path->parallel_branches = parallel_branches;
    path->require_parallel = require_parallel || options->require_parallel;
    cost_hybrid_recall_path(path, rel);
    return path;
}

void TryAddHybridRecallPath(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    Query* parse = root->parse;

    /* P3-2 session-level production rollback switch.  The OGAI v2 wrapper checks the same
     * GUC before emitting marker SQL and invokes the legacy API while off. */
    if (!u_sess->attr.attr_sql.enable_hybrid_recall)
        return;

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

    /*
     * [P2-7] A custom plan normally substitutes an external LIMIT parameter
     * with a Const before this hook.  A generic plan deliberately retains the
     * PARAM_EXTERN node, so keep it live for executor-time evaluation and use
     * a conservative estimate for Path rows/cost only.  Arbitrary LIMIT
     * expressions and LIMIT NULL/ALL remain outside the V1 whitelist.
     */
    int64 final_topk = 10;
    bool final_topk_is_param = false;
    if (IsA(parse->limitCount, Const)) {
        Const* limit_const = (Const*)parse->limitCount;
        if (limit_const->constisnull || limit_const->consttype != INT8OID)
            return;
        final_topk = DatumGetInt64(limit_const->constvalue);
    } else if (IsA(parse->limitCount, Param)) {
        Param* limit_param = (Param*)parse->limitCount;
        if (limit_param->paramkind != PARAM_EXTERN || limit_param->paramtype != INT8OID ||
            limit_param->paramid <= 0)
            return;
        final_topk_is_param = true;
    } else {
        return;
    }

    HybridRecallOptions options;
    hybrid_parse_options((Node*)list_nth(func->args, 5), &options);

    if (!final_topk_is_param) {
        int max_final_topk = u_sess->attr.attr_sql.hybrid_recall_max_final_topk;
        if (final_topk < 1 || final_topk > max_final_topk)
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("HybridRecall final_topk exceeds configured limit"),
                     errdetail("final_topk=%ld; allowed range is 1..%d",
                               final_topk, max_final_topk)));
        if (options.text_candidates < final_topk || options.vector_candidates < final_topk)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("HybridRecall branch candidate caps must be >= final_topk"),
                     errdetail("final_topk=%ld text=%ld vector=%ld",
                               final_topk, options.text_candidates, options.vector_candidates)));
    }

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
                q_text_raw, q_vec_raw, common_quals, final_topk, &options, false, false));
            break;
        case HYBRID_RECALL_PARALLEL_FORCE:
        case HYBRID_RECALL_PARALLEL_VECTOR:
            /* VECTOR enumerates the parallel path exactly like FORCE; the
             * flavour difference (text branch left inlined) is applied at
             * create_hybrid_plan time from the session GUC. */
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, &options, true, true));
            break;
        case HYBRID_RECALL_PARALLEL_AUTO:
        default:
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, &options, false, false));
            add_path(root, rel, (Path*)make_hybrid_recall_path(rel,
                rel->relid, id_var->varattno, text_var->varattno, vec_var->varattno,
                q_text_raw, q_vec_raw, common_quals, final_topk, &options, true, false));
            break;
    }
    set_cheapest(rel);
}
