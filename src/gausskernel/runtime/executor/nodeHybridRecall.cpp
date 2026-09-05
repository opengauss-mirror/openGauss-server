/* -------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/Mulan PSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * src/gausskernel/runtime/executor/nodeHybridRecall.cpp
 *
 * [P0-4] HybridRecall - two-branch recall fusion parent (hybrid recall
 * design doc §8). Serial execution only in this phase: BUILD drains the
 * text branch (lefttree, BM25 Top-K) and the vector branch (righttree,
 * ANN Top-K) into a doc-key dedup hash, computes RRF, keeps the final
 * Top-K in a bounded binary heap with the full deterministic tie-break,
 * then OUTPUT returns one fused row per call. Parallel branch workers
 * (P1) replace the drain source only - the fusion semantics live here
 * and must keep matching the S0 golden (hybrid_recall_baseline.sql).
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/exec/localStreamPoll.h"
#include "executor/node/nodeHybridRecall.h"
#include "fmgr.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

/*
 * Keep the key type inside the dynahash key.  Custom hash/match callbacks do
 * not receive a caller context, so a self-describing key avoids process- or
 * thread-global callback state when concurrent sessions use different key
 * types.
 */
typedef struct HybridCandKey {
    Datum id;
    Oid type_id;
} HybridCandKey;

/* hash table entry: self-describing doc key first, then payload */
typedef struct HybridCandEntry {
    HybridCandKey key;
    HybridCandidate cand;
} HybridCandEntry;

static TupleTableSlot* ExecHybridRecall(PlanState* state);
static void hybrid_validate_options(HybridRecallState* node);

static double hybrid_elapsed_ms(instr_time start, instr_time end)
{
    instr_time elapsed;

    INSTR_TIME_SET_ZERO(elapsed);
    INSTR_TIME_ACCUM_DIFF(elapsed, end, start);
    return INSTR_TIME_GET_MILLISEC(elapsed);
}

static void hybrid_check_candidate_memory(HybridRecallState* node)
{
    Size used = AllocSetContextUsedSpace((AllocSet)node->candidate_cxt);
    uint64 limit = (uint64)u_sess->datavec_ctx.hybrid_recall_max_candidate_memory_kb * 1024;

    if ((uint64)used > limit)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("HybridRecall candidate memory exceeds configured limit"),
                 errdetail("used=%lu bytes limit=%lu bytes (%d kB)",
                           (unsigned long)used, (unsigned long)limit,
                           u_sess->datavec_ctx.hybrid_recall_max_candidate_memory_kb)));
}

/*
 * Planner-generated HybridRecall targetlists deliberately retain base-table
 * Vars through planning, because openGauss plan-copy/cache processing can
 * restore varattno from the original-column metadata.  Once ExecInit starts,
 * the Plan is final: map each distinct payload column to the positional branch
 * schema (key=1, score=2, payload=3..N) exactly once.  Handmade P0 vehicle
 * plans have a NIL targetlist and continue to use id_resno as a branch resno.
 */
typedef struct HybridVarMapContext {
    HybridRecall* node;
    List* payload_attnos;
} HybridVarMapContext;

static AttrNumber hybrid_map_branch_attno(HybridVarMapContext* context, Var* var)
{
    AttrNumber original_attno = var->varoattno > 0 ? var->varoattno : var->varattno;
    if (var->varlevelsup != 0 || original_attno <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("hybrid recall V1 cannot map a nonlocal or system-column Var")));

    if (original_attno == context->node->id_resno)
        return 1;

    int payload_index = 0;
    ListCell* lc = NULL;
    foreach (lc, context->payload_attnos) {
        if ((AttrNumber)lfirst_int(lc) == original_attno)
            return (AttrNumber)(3 + payload_index);
        payload_index++;
    }

    context->payload_attnos = lappend_int(context->payload_attnos, original_attno);
    return (AttrNumber)(3 + payload_index);
}

static bool hybrid_map_recheck_vars_walker(Node* expr, HybridVarMapContext* context)
{
    if (expr == NULL)
        return false;
    if (IsA(expr, Var)) {
        Var* var = (Var*)expr;
        var->varno = OUTER_VAR;
        var->varattno = hybrid_map_branch_attno(context, var);
        /* varoattno remains the immutable base-table identity for another
         * ExecInit of the same cached plan. */
        return false;
    }
    return expression_tree_walker(expr, (bool (*)())hybrid_map_recheck_vars_walker, (void*)context);
}

static void hybrid_map_planner_vars(HybridRecall* node)
{
    if (node->plan.targetlist == NIL)
        return;

    HybridVarMapContext context;
    context.node = node;
    context.payload_attnos = NIL;

    ListCell* lc = NULL;
    foreach (lc, node->plan.targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (!IsA(tle->expr, Var))
            continue;

        Var* var = (Var*)tle->expr;
        var->varno = OUTER_VAR;
        var->varattno = hybrid_map_branch_attno(&context, var);
        /* Keep varoattno as the immutable base-table identity.  Cached plans
         * can be executed more than once; the next ExecInit must rebuild the
         * same mapping from original coordinates, not from a prior run's
         * branch position. */
    }

    /* Visible output Vars establish the first payload positions; recursively
     * append and map any columns referenced only by the P2-4 recheck quals. */
    (void)hybrid_map_recheck_vars_walker((Node*)node->recheck_quals, &context);
    list_free(context.payload_attnos);
}

static uint32 hybrid_id_hash(const void* key, Size keysize)
{
    const HybridCandKey* cand_key = (const HybridCandKey*)key;
    TypeCacheEntry* tcache = lookup_type_cache(cand_key->type_id, TYPECACHE_HASH_PROC);
    FmgrInfo fi;

    Assert(OidIsValid(tcache->hash_proc));
    /* hash_proc is a support-function OID; fill a local FmgrInfo rather
     * than assuming the cache keeps a finfo (openGauss TypeCacheEntry
     * only maintains finfos for eq/cmp). */
    fmgr_info(tcache->hash_proc, &fi);
    return DatumGetUInt32(FunctionCall1Coll(&fi, InvalidOid, cand_key->id));
}

static int hybrid_id_match(const void* key1, const void* key2, Size keysize)
{
    const HybridCandKey* cand_key1 = (const HybridCandKey*)key1;
    const HybridCandKey* cand_key2 = (const HybridCandKey*)key2;
    TypeCacheEntry* tcache;
    FmgrInfo fi;

    if (cand_key1->type_id != cand_key2->type_id)
        return 1;

    tcache = lookup_type_cache(cand_key1->type_id, TYPECACHE_EQ_OPR);
    Assert(OidIsValid(tcache->eq_opr));
    fmgr_info(get_opcode(tcache->eq_opr), &fi);
    return !DatumGetBool(FunctionCall2Coll(&fi, InvalidOid,
                                           cand_key1->id, cand_key2->id));
}

/* three-way doc-key compare via the type's btree procs (never byte order
 * or pointer compares - design doc §3.4) */
static int hybrid_id_cmp(HybridRecallState* node, Datum a, Datum b)
{
    TypeCacheEntry* tcache = node->id_tcache;
    FmgrInfo fi;

    fmgr_info(get_opcode(tcache->lt_opr), &fi);
    if (DatumGetBool(FunctionCall2Coll(&fi, InvalidOid, a, b)))
        return -1;
    if (DatumGetBool(FunctionCall2Coll(&fi, InvalidOid, b, a)))
        return 1;
    return 0;
}

/*
 * Full deterministic tie-break, "better" returns positive:
 *   hybrid_score DESC, source_mask DESC, best_branch_rank ASC, doc_id ASC
 * NaN scores are treated as lowest relevance (fixed policy, design doc
 * §8.5; exercised by P0-6 tests).
 */
static int hybrid_cand_better(HybridRecallState* node, const HybridCandidate* a, const HybridCandidate* b)
{
    if (a->hybrid_score > b->hybrid_score)
        return 1;
    if (a->hybrid_score < b->hybrid_score)
        return -1;

    if (a->source_mask != b->source_mask)
        return a->source_mask > b->source_mask ? 1 : -1;

    /* best_branch_rank = the better (smaller) of the ranks actually hit
     * (design doc §3.4); 0 ranks mean the branch missed and never count. */
    uint32 ra = (a->text_rank != 0 && a->vector_rank != 0)
                    ? Min(a->text_rank, a->vector_rank)
                    : (a->text_rank != 0 ? a->text_rank : a->vector_rank);
    uint32 rb = (b->text_rank != 0 && b->vector_rank != 0)
                    ? Min(b->text_rank, b->vector_rank)
                    : (b->text_rank != 0 ? b->text_rank : b->vector_rank);
    if (ra != rb)
        return ra < rb ? 1 : -1;

    return -hybrid_id_cmp(node, a->id, b->id);
}

/* binaryheap keeps the WORST candidate on top so a better challenger can
 * evict it in O(log K); the comparator therefore inverts "better". */
static int hybrid_heap_compare(Datum a, Datum b, void* arg)
{
    HybridRecallState* node = (HybridRecallState*)arg;
    const HybridCandidate* ca = (const HybridCandidate*)DatumGetPointer(a);
    const HybridCandidate* cb = (const HybridCandidate*)DatumGetPointer(b);

    return -hybrid_cand_better(node, ca, cb);
}

/* ----------------------------------------------------------------
 *		ExecInitHybridRecall
 * ----------------------------------------------------------------
 */
HybridRecallState* ExecInitHybridRecall(HybridRecall* node, EState* estate, int eflags)
{
    /* Cursor mark/restore and backward scan are out of scope for V1
     * (design doc §1.3); reject them here rather than misbehave later. */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /* A generic/cached plan can outlive a session-level SET enable_hybrid_recall=off.  Do not
     * execute its branch workers after rollback; callers must replan, while
     * hybrid_search_v2 itself selects legacy SQL before creating a marker. */
    if (!u_sess->attr.attr_sql.enable_hybrid_recall)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("HybridRecall is disabled by enable_hybrid_recall"),
                 errhint("Discard the cached plan or enable HybridRecall before retrying.")));

    if (node->plan.lefttree == NULL || node->plan.righttree == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall requires both text and vector branch plans")));

    HybridRecallState* state = makeNode(HybridRecallState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;
    state->ps.type = T_HybridRecallState;
    state->ps.ExecProcNode = ExecHybridRecall;

    /* lefttree = text branch, righttree = vector branch (design doc §6.2) */
    state->text_state = ExecInitNode(node->plan.lefttree, estate, eflags);
    state->vector_state = ExecInitNode(node->plan.righttree, estate, eflags);
    /* standard PlanState child pointers so explain/planstate walkers
     * (outerPlanState/innerPlanState) see both branches */
    state->ps.lefttree = state->text_state;
    state->ps.righttree = state->vector_state;

    ExecAssignExprContext(estate, &state->ps);
    hybrid_map_planner_vars(node);

    /* P2-4 defensive common-filter recheck.  Vars now address the common
     * branch tuple through OUTER_VAR, including hidden WHERE-only payload. */
    if (estate->es_is_flt_frame)
        state->ps.qual = (List*)ExecInitQualByFlatten(node->recheck_quals, (PlanState*)state);
    else
        state->ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->recheck_quals, (PlanState*)state);

    /*
     * Branch schema contract: children output (doc_id, branch score,
     * payload...); the Plan pins positions via resnos, and the executor
     * validates the actual runtime types instead of guessing by name.
     */
    TupleDesc text_desc = state->text_state->ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleDesc vector_desc = state->vector_state->ps_ResultTupleSlot->tts_tupleDescriptor;

    AttrNumber branch_id_resno = node->plan.targetlist == NIL ? node->id_resno : 1;
    if (branch_id_resno < 1 || branch_id_resno > text_desc->natts || branch_id_resno > vector_desc->natts)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall id_resno %d is outside the branch targetlists", branch_id_resno)));

    Oid id_type = text_desc->attrs[branch_id_resno - 1].atttypid;
    if (vector_desc->attrs[branch_id_resno - 1].atttypid != id_type)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall branches disagree on the doc key type")));

    /* Score columns are diagnostic only (rank drives fusion), so float4 or
     * float8 are both fine and a missing column (resno 0) is tolerated. */
    if (node->text_score_resno > text_desc->natts ||
        (node->text_score_resno >= 1 &&
         text_desc->attrs[node->text_score_resno - 1].atttypid != FLOAT8OID &&
         text_desc->attrs[node->text_score_resno - 1].atttypid != FLOAT4OID))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall text branch must supply a float4/float8 score column")));
    if (node->vector_distance_resno > vector_desc->natts ||
        (node->vector_distance_resno >= 1 &&
         vector_desc->attrs[node->vector_distance_resno - 1].atttypid != FLOAT8OID &&
         vector_desc->attrs[node->vector_distance_resno - 1].atttypid != FLOAT4OID))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall vector branch must supply a float4/float8 distance column")));

    state->id_tcache = lookup_type_cache(id_type,
                                         TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR);
    if (!OidIsValid(state->id_tcache->hash_proc) || !OidIsValid(state->id_tcache->eq_opr) ||
        !OidIsValid(state->id_tcache->lt_opr))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall doc key type %u lacks hash/eq/lt operators", id_type)));

    if (node->final_topk == NULL || node->text_candidates == NULL || node->vector_candidates == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall requires final_topk and both candidate limit expressions")));

    /*
     * Candidate limits are Plan Exprs and must be re-evaluated on every
     * execution (parameterized LIMIT, design doc §7.5); fused values are
     * read at BUILD start.
     */
    state->final_topk_state = ExecInitExpr((Expr*)node->final_topk, (PlanState*)state);
    state->text_candidates_state = ExecInitExpr((Expr*)node->text_candidates, (PlanState*)state);
    state->vector_candidates_state = ExecInitExpr((Expr*)node->vector_candidates, (PlanState*)state);

    /* Result layout: the PLAN targetlist (query tlist, marker already a
     * Const). The executor materializes each entry: Var -> stored candidate
     * tuple attribute, Const -> constant. The vehicle's 5-column contract
     * remains supported when plan->targetlist is NIL (handmade plans). */
    if (node->plan.targetlist != NIL) {
        TupleDesc result_desc = ExecTypeFromTL(node->plan.targetlist, false);
        ExecInitResultTupleSlot(estate, &state->ps);
        ExecSetSlotDescriptor(state->ps.ps_ResultTupleSlot, result_desc);
    } else {
        TupleDesc result_desc;
        result_desc = CreateTemplateTupleDesc(5, false);
        TupleDescInitEntry(result_desc, (AttrNumber)1, "doc_id", id_type, -1, 0);
        TupleDescInitEntry(result_desc, (AttrNumber)2, "source_mask", INT4OID, -1, 0);
        TupleDescInitEntry(result_desc, (AttrNumber)3, "text_rank", INT4OID, -1, 0);
        TupleDescInitEntry(result_desc, (AttrNumber)4, "vector_rank", INT4OID, -1, 0);
        TupleDescInitEntry(result_desc, (AttrNumber)5, "hybrid_score", FLOAT8OID, -1, 0);
        ExecInitResultTupleSlot(estate, &state->ps);
        ExecSetSlotDescriptor(state->ps.ps_ResultTupleSlot, result_desc);
    }
    state->ps.ps_ProjInfo = NULL;

    /* Candidates live in a dedicated context so that End/Rescan can drop
     * dedup hash, payload copies and the output array in one shot. */
    state->candidate_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                                 "HybridRecallCandidates",
                                                 ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE);

    state->phase = HYBRID_PHASE_BUILD;
    state->by_doc_id = NULL;
    state->final_heap = NULL;
    state->output = NULL;
    state->output_count = 0;
    state->output_pos = 0;
    state->text_rows = 0;
    state->vector_rows = 0;
    state->dedup_rows = 0;
    state->final_rows = 0;
    state->actual_parallel = node->parallel_branches &&
        IsA(node->plan.lefttree, Stream) && IsA(node->plan.righttree, Stream);
    state->serial_fallback = false;

    /* Validate during ExecutorStart as well as BUILD.  An outer LIMIT 0 does
     * not call its child, but a parameterized HybridRecall limit must still
     * obey the same non-NULL, positive and candidate-cap contract. */
    hybrid_validate_options(state);

    return state;
}

/* Evaluate one candidate-limit Expr; validate the resource contract.
 * Const limits (the common shape) take a direct fast path - no econtext. */
static int64 hybrid_eval_limit(HybridRecallState* node, Expr* expr, ExprState** expr_state, const char* name)
{
    int64 result;

    if (expr != NULL && IsA(expr, Const) && !((Const*)expr)->constisnull) {
        result = DatumGetInt64(((Const*)expr)->constvalue);
    } else {
        bool is_null = false;
        /* PARAM_EXTERN needs the owning PlanState expression context to
         * reach EState::es_param_list_info.  Passing NULL happened to work
         * for Const-only plans but is invalid for P2-7 generic plans. */
        Datum val = ExecEvalExpr(*expr_state, node->ps.ps_ExprContext, &is_null);

        if (is_null)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("HybridRecall %s must not be NULL", name)));
        result = DatumGetInt64(val);
    }
    if (result < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall %s must be non-negative, got %ld", name, result)));
    return result;
}

static void hybrid_validate_options(HybridRecallState* node)
{
    HybridRecall* plan = (HybridRecall*)node->ps.plan;

    node->final_topk = hybrid_eval_limit(node, plan->final_topk, &node->final_topk_state, "final_topk");
    node->text_limit = hybrid_eval_limit(node, plan->text_candidates, &node->text_candidates_state, "text_candidates");
    node->vector_limit = hybrid_eval_limit(node, plan->vector_candidates, &node->vector_candidates_state, "vector_candidates");

    if (node->final_topk < 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall final_topk must be >= 1")));
    if (node->final_topk > u_sess->attr.attr_sql.hybrid_recall_max_final_topk)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("HybridRecall final_topk exceeds configured limit"),
                 errdetail("final_topk=%ld limit=%d",
                           node->final_topk,
                           u_sess->attr.attr_sql.hybrid_recall_max_final_topk)));
    /* A branch cap below the final K cannot produce the final K rows. */
    if (node->text_limit < node->final_topk || node->vector_limit < node->final_topk)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall branch candidate caps must be >= final_topk")));
    if (node->text_limit > u_sess->attr.attr_sql.hybrid_recall_max_candidates_branch ||
        node->vector_limit > u_sess->attr.attr_sql.hybrid_recall_max_candidates_branch)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("HybridRecall branch candidates exceed configured limit"),
                 errdetail("text=%ld vector=%ld branch_limit=%d",
                           node->text_limit, node->vector_limit,
                           u_sess->attr.attr_sql.hybrid_recall_max_candidates_branch)));
    if (node->text_limit + node->vector_limit >
        u_sess->attr.attr_sql.hybrid_recall_max_candidates_total)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("HybridRecall total candidates exceed configured limit"),
                 errdetail("text=%ld vector=%ld total_limit=%d",
                           node->text_limit, node->vector_limit,
                           u_sess->attr.attr_sql.hybrid_recall_max_candidates_total)));
    if (!isfinite(plan->rrf_k) || !isfinite(plan->text_weight) || !isfinite(plan->vector_weight) ||
        plan->rrf_k <= 0.0 || plan->text_weight < 0.0 || plan->vector_weight < 0.0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall RRF parameters are invalid")));
}

static double hybrid_get_float_column(TupleTableSlot* slot, AttrNumber resno, bool* isnull)
{
    Datum value = heap_slot_getattr(slot, resno, isnull);
    if (*isnull)
        return 0.0;

    Oid type = slot->tts_tupleDescriptor->attrs[resno - 1].atttypid;
    if (type == FLOAT4OID)
        return (double)DatumGetFloat4(value);
    Assert(type == FLOAT8OID);
    return DatumGetFloat8(value);
}

/* Insert-or-merge one branch row. Rank is the 1-based position inside the
 * branch stream (index order == recall order, design doc §3.4). */
static void hybrid_upsert_candidate(HybridRecallState* node, TupleTableSlot* slot, bool is_text, uint32 rank)
{
    HybridRecall* plan = (HybridRecall*)node->ps.plan;
    /* Stream-delivered slots are lazily deformed (tts_nvalid==0); the
     * canonical accessor deforms on demand - direct tts_values reads
     * are only valid for materialized slots. */
    bool id_isnull = false;
    AttrNumber branch_id_resno = plan->plan.targetlist == NIL ? plan->id_resno : 1;
    Datum id = heap_slot_getattr(slot, branch_id_resno, &id_isnull);
    if (id_isnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("HybridRecall doc key must not be NULL (V1 requires a stable non-null key)"),
                        errdetail("branch=%s natts=%d nvalid=%d id_resno=%d",
                                  is_text ? "text" : "vector",
                                  slot->tts_tupleDescriptor ? slot->tts_tupleDescriptor->natts : -1,
                                  slot->tts_nvalid, branch_id_resno)));

    bool found = false;
    HybridCandKey key;
    key.id = id;
    key.type_id = node->id_tcache->type_id;
    HybridCandEntry* entry = (HybridCandEntry*)hash_search(node->by_doc_id, &key, HASH_ENTER, &found);

    if (!found) {
        /* first hit owns the payload copy and the doc-key copy */
        MemoryContext old = MemoryContextSwitchTo(node->candidate_cxt);
        entry->cand.id = !node->id_tcache->typbyval
                             ? datumCopy(id, false, node->id_tcache->typlen)
                             : id;
        entry->cand.id_isnull = false;
        entry->cand.source_mask = 0;
        entry->cand.text_rank = 0;
        entry->cand.vector_rank = 0;
        entry->cand.text_score = 0.0;
        entry->cand.vector_distance = 0.0;
        entry->cand.hybrid_score = 0.0;
        entry->cand.tuple = ExecCopySlotMinimalTuple(slot);
        /* Dynahash copied the transient slot Datum into its key.  Point a
         * pass-by-reference key at the durable candidate-context copy before
         * the branch slot can be reused.  The value and therefore hash bucket
         * are unchanged. */
        entry->key.id = entry->cand.id;
        (void)MemoryContextSwitchTo(old);
        node->dedup_rows++;
        hybrid_check_candidate_memory(node);
    } else {
        /* same-branch duplicate doc key: keep the better (earlier) rank */
        if ((is_text && entry->cand.text_rank != 0) || (!is_text && entry->cand.vector_rank != 0))
            return;
    }

    if (is_text) {
        entry->cand.text_rank = rank;
        entry->cand.source_mask |= 1;
        bool score_null = false;
        if (plan->text_score_resno >= 1) {
            entry->cand.text_score = hybrid_get_float_column(slot, plan->text_score_resno, &score_null);
        }
    } else {
        entry->cand.vector_rank = rank;
        entry->cand.source_mask |= 2;
        bool dist_null = false;
        if (plan->vector_distance_resno >= 1) {
            entry->cand.vector_distance = hybrid_get_float_column(slot, plan->vector_distance_resno, &dist_null);
        }
    }
}

static bool hybrid_drain_next(HybridRecallState* node, PlanState* branch, bool is_text,
                              uint32* rank, int64 cap)
{
    TupleTableSlot* slot = ExecProcNode(branch);
    if (TupIsNull(slot))
        return false;

    (*rank)++;
    /* Bounded candidates are a hard resource contract (design doc
     * §1.4): a branch overflowing its cap is an error, never a silent
     * growth. */
    if ((int64)(*rank) > cap)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall %s branch exceeded its candidate cap %ld",
                               is_text ? "text" : "vector", cap)));

    ExecMaterializeSlot(slot);
    hybrid_upsert_candidate(node, slot, is_text, *rank);
    ResetExprContext(node->ps.ps_ExprContext);
    return true;
}

static void hybrid_consume_polled_slot(HybridRecallState* node, PlanState* branch,
                                       bool is_text, uint32* rank, int64 cap)
{
    TupleTableSlot* slot = branch->ps_ResultTupleSlot;

    (*rank)++;
    if ((int64)(*rank) > cap)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall %s branch exceeded its candidate cap %ld",
                               is_text ? "text" : "vector", cap)));

    ExecMaterializeSlot(slot);
    hybrid_upsert_candidate(node, slot, is_text, *rank);
    ResetExprContext(node->ps.ps_ExprContext);
}

static void hybrid_drain_branch(HybridRecallState* node, PlanState* branch, bool is_text, uint64* row_counter)
{
    uint32 rank = 0;
    int64 cap = is_text ? node->text_limit : node->vector_limit;

    while (hybrid_drain_next(node, branch, is_text, &rank, cap))
        CHECK_FOR_INTERRUPTS();

    *row_counter = rank;
}

/* ----------------------------------------------------------------
 * OPT-2: vector-branch fetcher.
 *
 * The serial drain executes the two independent branches strictly one
 * after another (text first, then vector), so the shorter branch's
 * wall time is pure additive overhead.  The fetcher decouples branch
 * execution from candidate-hash upserts: rows are pulled from the
 * vector branch and copied as minimal tuples into a fetcher-private
 * memory context; after the fetch completes, the owning thread
 * replays them through the existing upsert path.
 *
 * Invariants (design doc: OPT-2_支路并行执行详细设计.md §3/§6/§8):
 * - the fetcher never writes any shared structure (single-writer
 *   hash table / EXPLAIN counters stay on the driving thread);
 * - rank is assigned in replay order == pull order, therefore the
 *   fused output is byte-identical to the serial drain;
 * - cap contract: cap_rows = vector_limit + 1 so a branch producing
 *   more rows than its limit is rejected with the same error as the
 *   serial path instead of being silently truncated.
 * ---------------------------------------------------------------- */
typedef struct HybridVectorFetcher {
    PlanState* vector_state;      /* only the fetching thread touches this subtree */
    MemoryContext fetch_cxt;      /* private AllocSet: tuple copies and array */
    MinimalTuple* tuples;         /* fixed-size array in fetch_cxt */
    int n_rows;
    int cap_rows;                 /* vector_limit + 1 (overflow detection) */
    volatile bool done;
    volatile bool failed;
    volatile bool cancel_requested;
    instr_time started_at;
} HybridVectorFetcher;

static HybridVectorFetcher* hybrid_fetcher_create(HybridRecallState* node)
{
    EState* estate = node->ps.state;
    HybridVectorFetcher* f = (HybridVectorFetcher*)MemoryContextAllocZero(estate->es_query_cxt, sizeof(HybridVectorFetcher));

    f->vector_state = node->vector_state;
    f->cap_rows = (int)node->vector_limit + 1;
    f->fetch_cxt = AllocSetContextCreate(estate->es_query_cxt, "HybridVectorFetcher", ALLOCSET_DEFAULT_SIZES);
    f->tuples = (MinimalTuple*)MemoryContextAllocZero(f->fetch_cxt, sizeof(MinimalTuple) * f->cap_rows);
    return f;
}

/* Pull one row from the vector branch into the private array.
 * Returns false on EOF or when the cap buffer is full. */
static bool hybrid_fetcher_pull(HybridVectorFetcher* f)
{
    TupleTableSlot* slot = ExecProcNode(f->vector_state);

    if (TupIsNull(slot))
        return false;

    MemoryContext old = MemoryContextSwitchTo(f->fetch_cxt);
    ExecMaterializeSlot(slot);
    f->tuples[f->n_rows++] = ExecCopySlotMinimalTuple(slot);
    (void)MemoryContextSwitchTo(old);
    return f->n_rows < f->cap_rows;
}

/* T1 data-plane vehicle: run the fetch loop on the calling thread. */
static void hybrid_fetcher_run_serial(HybridVectorFetcher* f)
{
    INSTR_TIME_SET_CURRENT(f->started_at);
    while (hybrid_fetcher_pull(f))
        CHECK_FOR_INTERRUPTS();
    f->done = true;
}

/* Replay fetched tuples through the regular upsert path.  Rank is
 * 1..n_rows in pull order; the cap contract is enforced here so the
 * error text matches the serial drain exactly. */
static void hybrid_fetcher_replay(HybridRecallState* node, HybridVectorFetcher* f)
{
    TupleTableSlot* replay_slot = node->vector_state->ps_ResultTupleSlot;
    int rank;

    if (f->n_rows > (int)node->vector_limit)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall vector branch exceeded its candidate cap %ld",
                        node->vector_limit)));

    for (rank = 1; rank <= f->n_rows; rank++) {
        ExecStoreMinimalTuple(f->tuples[rank - 1], replay_slot, false);
        hybrid_upsert_candidate(node, replay_slot, false, (uint32)rank);
        ResetExprContext(node->ps.ps_ExprContext);
    }
    node->vector_rows = (uint64)f->n_rows;
}

static void hybrid_fetcher_destroy(HybridVectorFetcher* f)
{
    if (f == NULL)
        return;
    if (f->fetch_cxt != NULL)
        MemoryContextDelete(f->fetch_cxt);
    pfree(f);
}

/*
 * Both local Stream producers are already running in separate task workers.
 * Draining one Stream to EOF before touching the other lets the second
 * producer fill its bounded queue and block, turning real worker overlap into
 * nearly serial parent latency.  Consume one candidate from each live Stream
 * per round so both queues keep making progress.  Rank remains independent
 * per branch, therefore RRF and deterministic tie-breaking are unchanged.
 */
static void hybrid_drain_parallel_branches(HybridRecallState* node)
{
    bool text_done = false;
    bool vector_done = false;
    uint32 text_rank = 0;
    uint32 vector_rank = 0;

    if (!IsA(node->text_state, StreamState) || !IsA(node->vector_state, StreamState))
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("parallel HybridRecall requires two local Stream inputs")));

    while (!text_done || !vector_done) {
        bool made_progress = false;

        if (!text_done) {
            StreamPollStatus status = ExecPollLocalStream((StreamState*)node->text_state);
            if (status == STREAM_POLL_DATA) {
                hybrid_consume_polled_slot(
                    node, node->text_state, true, &text_rank, node->text_limit);
                made_progress = true;
            } else if (status == STREAM_POLL_FINISH) {
                text_done = true;
                INSTR_TIME_SET_CURRENT(node->text_end);
                made_progress = true;
            }
        }

        if (!vector_done) {
            StreamPollStatus status = ExecPollLocalStream((StreamState*)node->vector_state);
            if (status == STREAM_POLL_DATA) {
                hybrid_consume_polled_slot(
                    node, node->vector_state, false, &vector_rank, node->vector_limit);
                made_progress = true;
            } else if (status == STREAM_POLL_FINISH) {
                vector_done = true;
                INSTR_TIME_SET_CURRENT(node->vector_end);
                made_progress = true;
            }
        }

        if (!made_progress)
            pg_usleep(50L);
        CHECK_FOR_INTERRUPTS();
    }

    node->text_rows = text_rank;
    node->vector_rows = vector_rank;
}

static bool hybrid_recheck_candidate(HybridRecallState* node, HybridCandidate* cand)
{
    if (node->ps.qual == NIL)
        return true;

    TupleTableSlot* payload_slot = node->text_state->ps_ResultTupleSlot;
    ExecStoreMinimalTuple(cand->tuple, payload_slot, false);

    ExprContext* econtext = node->ps.ps_ExprContext;
    TupleTableSlot* saved_outer = econtext->ecxt_outertuple;
    econtext->ecxt_outertuple = payload_slot;
    bool passed = ExecQual(node->ps.qual, econtext, false);
    econtext->ecxt_outertuple = saved_outer;
    ResetExprContext(econtext);
    return passed;
}

/* RRF (design doc §3.4): score = sum of weight / (rrf_k + rank) over the
 * branches that hit the doc; missed branches contribute nothing. */
static void hybrid_fuse(HybridRecallState* node)
{
    HybridRecall* plan = (HybridRecall*)node->ps.plan;
    HASH_SEQ_STATUS seq;
    HybridCandEntry* entry = NULL;

    MemoryContext old = MemoryContextSwitchTo(node->candidate_cxt);
    node->final_heap = binaryheap_allocate((int)node->final_topk, hybrid_heap_compare, node);
    (void)MemoryContextSwitchTo(old);
    hybrid_check_candidate_memory(node);

    hash_seq_init(&seq, node->by_doc_id);
    while ((entry = (HybridCandEntry*)hash_seq_search(&seq)) != NULL) {
        HybridCandidate* cand = &entry->cand;

        /* Both branches already applied the filter.  Rechecking the retained
         * expression here is intentionally defensive: it protects semantics
         * across lossy/in-filter AM behavior and future pushdown changes, and
         * runs before the bounded heap so a rejected candidate is replaced by
         * the next valid one rather than shrinking the final Top-K. */
        if (!hybrid_recheck_candidate(node, cand))
            continue;

        cand->hybrid_score = 0.0;
        if (cand->text_rank != 0)
            cand->hybrid_score += plan->text_weight / (plan->rrf_k + (double)cand->text_rank);
        if (cand->vector_rank != 0)
            cand->hybrid_score += plan->vector_weight / (plan->rrf_k + (double)cand->vector_rank);
        /* NaN policy: lowest relevance, never propagates as NaN. */
        if (isnan(cand->hybrid_score))
            cand->hybrid_score = -1.0;

        if (node->final_heap->bh_size < node->final_topk) {
            binaryheap_add(node->final_heap, PointerGetDatum(cand));
        } else if (node->final_heap->bh_size > 0 &&
                   hybrid_cand_better(node, cand,
                                      (HybridCandidate*)DatumGetPointer(binaryheap_first(node->final_heap))) > 0) {
            (void)binaryheap_remove_first(node->final_heap);
            binaryheap_add(node->final_heap, PointerGetDatum(cand));
        }
    }

    /* Pop order runs worst-to-best; reverse into the output array so
     * output_pos walks best-first. */
    int count = (int)node->final_heap->bh_size;
    node->output = (HybridCandidate**)MemoryContextAlloc(node->candidate_cxt,
                                                         count * sizeof(HybridCandidate*));
    hybrid_check_candidate_memory(node);
    for (int i = count - 1; i >= 0; i--)
        node->output[i] = (HybridCandidate*)DatumGetPointer(binaryheap_remove_first(node->final_heap));
    node->output_count = (uint32)count;
    node->final_rows = (uint64)count;
}

static TupleTableSlot* hybrid_store_result(HybridRecallState* node, HybridCandidate* cand)
{
    TupleTableSlot* slot = node->ps.ps_ResultTupleSlot;
    HybridRecall* plan = (HybridRecall*)node->ps.plan;

    ExecClearTuple(slot);
    if (plan->plan.targetlist == NIL) {
        /* vehicle 5-column contract */
        slot->tts_values[0] = cand->id;
        slot->tts_isnull[0] = false;
        slot->tts_values[1] = Int32GetDatum((int32)cand->source_mask);
        slot->tts_isnull[1] = false;
        slot->tts_values[2] = Int32GetDatum((int32)cand->text_rank);
        slot->tts_isnull[2] = false;
        slot->tts_values[3] = Int32GetDatum((int32)cand->vector_rank);
        slot->tts_isnull[3] = false;
        slot->tts_values[4] = Float8GetDatum(cand->hybrid_score);
        slot->tts_isnull[4] = false;
        return ExecStoreVirtualTuple(slot);
    }

    /* Planner path: Vars address the common branch tuple contract; Consts
     * pass through.  The ORDER BY marker TLE keeps its ressortgroupref after
     * becoming a float8 Const sentinel, so only that exact column is replaced
     * by the fused score (never an unrelated user-selected 0::float8). */
    {
        TupleTableSlot* payload_slot = node->text_state->ps_ResultTupleSlot;
        ExecStoreMinimalTuple(cand->tuple, payload_slot, false);

        ListCell* lc = NULL;
        int i = 0;
        foreach (lc, plan->plan.targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            if (IsA(tle->expr, Var)) {
                Var* v = (Var*)tle->expr;
                bool isnull = false;
                slot->tts_values[i] = heap_slot_getattr(payload_slot, v->varattno, &isnull);
                slot->tts_isnull[i] = isnull;
            } else if (IsA(tle->expr, Const)) {
                Const* c = (Const*)tle->expr;
                if (tle->ressortgroupref != 0 && c->consttype == FLOAT8OID &&
                    c->constbyval && !c->constisnull &&
                    DatumGetFloat8(c->constvalue) == 0.0) {
                    /* the eliminated marker position */
                    slot->tts_values[i] = Float8GetDatum(cand->hybrid_score);
                    slot->tts_isnull[i] = false;
                } else {
                    slot->tts_values[i] = c->constvalue;
                    slot->tts_isnull[i] = c->constisnull;
                }
            } else {
                slot->tts_isnull[i] = true;
            }
            i++;
        }
    }
    /* Candidate memory is owned by candidate_cxt, which lives until End;
     * no per-row copy is needed. */
    return ExecStoreVirtualTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecHybridRecall
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecHybridRecall(PlanState* state)
{
    HybridRecallState* node = (HybridRecallState*)state;

    if (node->phase == HYBRID_PHASE_BUILD) {
        HASHCTL ctl;

        hybrid_validate_options(node);

        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.keysize = sizeof(HybridCandKey);
        ctl.entrysize = sizeof(HybridCandEntry);
        ctl.hash = hybrid_id_hash;
        ctl.match = hybrid_id_match;
        ctl.hcxt = node->candidate_cxt;
        node->by_doc_id = hash_create("HybridRecallCandidates",
                                      256,
                                      &ctl,
                                      HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
        hybrid_check_candidate_memory(node);

        if (node->actual_parallel) {
            INSTR_TIME_SET_CURRENT(node->text_first_tuple);
            node->vector_first_tuple = node->text_first_tuple;
            hybrid_drain_parallel_branches(node);
        } else {
            INSTR_TIME_SET_CURRENT(node->text_first_tuple);
            hybrid_drain_branch(node, node->text_state, true, &node->text_rows);
            INSTR_TIME_SET_CURRENT(node->text_end);
            /* OPT-2 [T1]: vector branch through the fetcher (serial for
             * data-plane verification; T3 replaces run_serial with the
             * background thread).  Output must stay byte-identical. */
            INSTR_TIME_SET_CURRENT(node->vector_first_tuple);
            {
                HybridVectorFetcher* fetcher = hybrid_fetcher_create(node);
                hybrid_fetcher_run_serial(fetcher);
                INSTR_TIME_SET_CURRENT(node->vector_end);
                hybrid_fetcher_replay(node, fetcher);
                hybrid_fetcher_destroy(fetcher);
            }
        }
        hybrid_fuse(node);
        INSTR_TIME_SET_CURRENT(node->fusion_time);

        if (u_sess->attr.attr_sql.hybrid_recall_log_summary) {
            /* Deliberately exclude query text, vector values, filters and
             * document keys.  The opt-in GUC prevents per-query log noise;
             * errhidestmt prevents the log collector attaching the SQL text. */
            ereport(LOG,
                    (errmsg("HybridRecall execution summary"),
                     errdetail("strategy=%s final_topk=%ld text_rows=%lu vector_rows=%lu "
                               "deduplicated=%lu output_rows=%lu text_drain_ms=%.3f "
                               "vector_drain_ms=%.3f fusion_ms=%.3f",
                               node->actual_parallel ? "parallel" : "serial",
                               node->final_topk,
                               (unsigned long)node->text_rows,
                               (unsigned long)node->vector_rows,
                               (unsigned long)node->dedup_rows,
                               (unsigned long)node->final_rows,
                               hybrid_elapsed_ms(node->text_first_tuple, node->text_end),
                               hybrid_elapsed_ms(node->vector_first_tuple, node->vector_end),
                               hybrid_elapsed_ms(
                                   INSTR_TIME_GET_MICROSEC(node->text_end) >
                                           INSTR_TIME_GET_MICROSEC(node->vector_end) ?
                                       node->text_end : node->vector_end,
                                   node->fusion_time)),
                     errhidestmt(true)));
        }

        node->phase = HYBRID_PHASE_OUTPUT;
    }

    if (node->phase == HYBRID_PHASE_OUTPUT && node->output_pos < node->output_count)
        return hybrid_store_result(node, node->output[node->output_pos++]);

    node->phase = HYBRID_PHASE_DONE;
    return ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

/* ----------------------------------------------------------------
 *		ExecEndHybridRecall
 * ----------------------------------------------------------------
 */
void ExecEndHybridRecall(HybridRecallState* node)
{
    /* End both branch subtrees; Stream-owned shared contexts are released
     * by the StreamNodeGroup lifecycle, not here (design doc §8.7). */
    ExecEndNode(node->text_state);
    ExecEndNode(node->vector_state);

    if (node->candidate_cxt != NULL) {
        MemoryContextDelete(node->candidate_cxt);
        node->candidate_cxt = NULL;
        node->by_doc_id = NULL;
        node->final_heap = NULL;
        node->output = NULL;
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanHybridRecall
 *
 * Full rebuild on rescan: limits are re-evaluated at BUILD start and the
 * candidate context is recreated, so no state from the previous round
 * survives. The generation-id protocol against late stream tuples lands
 * with P1 (design doc §8.8).
 * ----------------------------------------------------------------
 */
void ExecReScanHybridRecall(HybridRecallState* node)
{
    if (node->candidate_cxt != NULL) {
        MemoryContextDelete(node->candidate_cxt);
        node->candidate_cxt = NULL;
    }
    /* Keep rescans under this operator's node context so WLM/operator memory
     * accounting observes the same ownership tree as the first execution. */
    node->candidate_cxt = AllocSetContextCreate(node->ps.nodeContext,
                                                "HybridRecallCandidates",
                                                ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE);
    node->by_doc_id = NULL;
    node->final_heap = NULL;
    node->output = NULL;
    node->output_count = 0;
    node->output_pos = 0;
    node->text_rows = 0;
    node->vector_rows = 0;
    node->dedup_rows = 0;
    node->final_rows = 0;
    INSTR_TIME_SET_ZERO(node->text_first_tuple);
    INSTR_TIME_SET_ZERO(node->text_end);
    INSTR_TIME_SET_ZERO(node->vector_first_tuple);
    INSTR_TIME_SET_ZERO(node->vector_end);
    INSTR_TIME_SET_ZERO(node->fusion_time);
    node->phase = HYBRID_PHASE_BUILD;

    ExecReScan(node->text_state);
    ExecReScan(node->vector_state);
}
