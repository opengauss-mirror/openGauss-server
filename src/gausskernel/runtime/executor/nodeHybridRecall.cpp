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

/* hash table entry: doc-key Datum first (hash_create key), then payload */
typedef struct HybridCandEntry {
    Datum id;
    HybridCandidate cand;
} HybridCandEntry;

/*
 * hash_create's custom hash/match callbacks receive only the key, so the
 * doc-key TypeCacheEntry is passed through this thread-local pointer.  A
 * gaussdb process hosts multiple session threads; process-global state here
 * would race concurrent HybridRecall queries using different key types.
 * Stream workers never execute the fusion parent (design doc §5.1).
 */
static THR_LOCAL TypeCacheEntry* g_hybrid_id_tcache = NULL;

static TupleTableSlot* ExecHybridRecall(PlanState* state);
static void hybrid_validate_options(HybridRecallState* node);

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
    Datum id = *(const Datum*)key;
    FmgrInfo fi;

    Assert(g_hybrid_id_tcache != NULL);
    /* hash_proc is a support-function OID; fill a local FmgrInfo rather
     * than assuming the cache keeps a finfo (openGauss TypeCacheEntry
     * only maintains finfos for eq/cmp). */
    fmgr_info(g_hybrid_id_tcache->hash_proc, &fi);
    return DatumGetUInt32(FunctionCall1Coll(&fi, InvalidOid, id));
}

static int hybrid_id_match(const void* key1, const void* key2, Size keysize)
{
    Datum id1 = *(const Datum*)key1;
    Datum id2 = *(const Datum*)key2;
    FmgrInfo fi;

    Assert(g_hybrid_id_tcache != NULL);
    fmgr_info(get_opcode(g_hybrid_id_tcache->eq_opr), &fi);
    return !DatumGetBool(FunctionCall2Coll(&fi, InvalidOid, id1, id2));
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
    state->actual_parallel = false;
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
    /* A branch cap below the final K cannot produce the final K rows. */
    if (node->text_limit < node->final_topk || node->vector_limit < node->final_topk)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("HybridRecall branch candidate caps must be >= final_topk")));
    if (plan->rrf_k <= 0.0 || plan->text_weight < 0.0 || plan->vector_weight < 0.0)
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

    g_hybrid_id_tcache = node->id_tcache;
    HybridCandEntry* entry = (HybridCandEntry*)hash_search(node->by_doc_id, &id, HASH_ENTER, &found);
    g_hybrid_id_tcache = NULL;

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
        (void)MemoryContextSwitchTo(old);
        node->dedup_rows++;
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

static void hybrid_drain_branch(HybridRecallState* node, PlanState* branch, bool is_text, uint64* row_counter)
{
    uint32 rank = 0;
    int64 cap = is_text ? node->text_limit : node->vector_limit;

    for (;;) {
        TupleTableSlot* slot = ExecProcNode(branch);
        if (TupIsNull(slot))
            break;

        rank++;
        /* Bounded candidates are a hard resource contract (design doc
         * §1.4): a branch overflowing its cap is an error, never a silent
         * growth. */
        if ((int64)rank > cap)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("HybridRecall %s branch exceeded its candidate cap %ld",
                                   is_text ? "text" : "vector", cap)));

        ExecMaterializeSlot(slot);
        hybrid_upsert_candidate(node, slot, is_text, rank);
        ResetExprContext(node->ps.ps_ExprContext);
    }

    *row_counter = rank;
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

    node->final_heap = binaryheap_allocate((int)node->final_topk, hybrid_heap_compare, node);

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
        ctl.keysize = sizeof(Datum);
        ctl.entrysize = sizeof(HybridCandEntry);
        ctl.hash = hybrid_id_hash;
        ctl.match = hybrid_id_match;
        ctl.hcxt = node->candidate_cxt;
        node->by_doc_id = hash_create("HybridRecallCandidates",
                                      256,
                                      &ctl,
                                      HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

        INSTR_TIME_SET_CURRENT(node->text_first_tuple);
        hybrid_drain_branch(node, node->text_state, true, &node->text_rows);
        INSTR_TIME_SET_CURRENT(node->text_end);
        hybrid_drain_branch(node, node->vector_state, false, &node->vector_rows);
        INSTR_TIME_SET_CURRENT(node->vector_end);
        hybrid_fuse(node);
        INSTR_TIME_SET_CURRENT(node->fusion_time);

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
    node->candidate_cxt = AllocSetContextCreate(node->ps.state->es_query_cxt,
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
    node->phase = HYBRID_PHASE_BUILD;

    ExecReScan(node->text_state);
    ExecReScan(node->vector_state);
}
