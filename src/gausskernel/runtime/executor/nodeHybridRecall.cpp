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
 * doc-key TypeCacheEntry is passed through this process-local pointer.
 * Safe because the fusion parent always runs in the query's main thread
 * (design doc §5.1 - stream workers never execute this node), one query
 * at a time per backend; stream worker threads in this process never
 * touch this path. Asserted below.
 */
static TypeCacheEntry* g_hybrid_id_tcache = NULL;

static TupleTableSlot* ExecHybridRecall(PlanState* state);

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

    /*
     * Branch schema contract: children output (doc_id, branch score,
     * payload...); the Plan pins positions via resnos, and the executor
     * validates the actual runtime types instead of guessing by name.
     */
    TupleDesc text_desc = state->text_state->ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleDesc vector_desc = state->vector_state->ps_ResultTupleSlot->tts_tupleDescriptor;

    if (node->id_resno < 1 || node->id_resno > text_desc->natts || node->id_resno > vector_desc->natts)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall id_resno %d is outside the branch targetlists", node->id_resno)));

    Oid id_type = text_desc->attrs[node->id_resno - 1].atttypid;
    if (vector_desc->attrs[node->id_resno - 1].atttypid != id_type)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall branches disagree on the doc key type")));

    if (node->text_score_resno < 1 || node->text_score_resno > text_desc->natts ||
        text_desc->attrs[node->text_score_resno - 1].atttypid != FLOAT8OID)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall text branch must supply a float8 score column")));
    if (node->vector_distance_resno < 1 || node->vector_distance_resno > vector_desc->natts ||
        vector_desc->attrs[node->vector_distance_resno - 1].atttypid != FLOAT8OID)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall vector branch must supply a float8 distance column")));

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

    /* Result layout (V1): doc_id, source_mask, text_rank, vector_rank,
     * hybrid_score. Full projection against a planner targetlist arrives
     * with P2; the debug/vehicle path (P0-4b) consumes exactly this. */
    TupleDesc result_desc;
    result_desc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(result_desc, (AttrNumber)1, "doc_id", id_type, -1, 0);
    TupleDescInitEntry(result_desc, (AttrNumber)2, "source_mask", INT4OID, -1, 0);
    TupleDescInitEntry(result_desc, (AttrNumber)3, "text_rank", INT4OID, -1, 0);
    TupleDescInitEntry(result_desc, (AttrNumber)4, "vector_rank", INT4OID, -1, 0);
    TupleDescInitEntry(result_desc, (AttrNumber)5, "hybrid_score", FLOAT8OID, -1, 0);

    ExecInitResultTupleSlot(estate, &state->ps);
    ExecSetSlotDescriptor(state->ps.ps_ResultTupleSlot, result_desc);
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
        Datum val = ExecEvalExpr(*expr_state, NULL, &is_null);

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

/* Insert-or-merge one branch row. Rank is the 1-based position inside the
 * branch stream (index order == recall order, design doc §3.4). */
static void hybrid_upsert_candidate(HybridRecallState* node, TupleTableSlot* slot, bool is_text, uint32 rank)
{
    HybridRecall* plan = (HybridRecall*)node->ps.plan;
    bool id_isnull = slot->tts_isnull[plan->id_resno - 1];
    if (id_isnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("HybridRecall doc key must not be NULL (V1 requires a stable non-null key)")));

    Datum id = slot->tts_values[plan->id_resno - 1];
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
        entry->cand.text_score = DatumGetFloat8(slot->tts_values[plan->text_score_resno - 1]);
    } else {
        entry->cand.vector_rank = rank;
        entry->cand.source_mask |= 2;
        entry->cand.vector_distance = DatumGetFloat8(slot->tts_values[plan->vector_distance_resno - 1]);
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

    ExecClearTuple(slot);
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
