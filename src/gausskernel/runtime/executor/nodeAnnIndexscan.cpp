/* -------------------------------------------------------------------------
 *
 * nodeAnnIndexscan.cpp
 *	  Routines to support indexed scans of relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecAnnIndexScan			scans a relation using an ann index
 *		AnnIndexNext				retrieve next tuple using ann index
 *		ExecInitAnnIndexScan		creates and initializes state info.
 *		ExecReScanAnnIndexScan		rescans the ann indexed relation.
 *		ExecEndAnnIndexScan		releases all storage.
 *		ExecAnnIndexMarkPos		marks scan position.
 *		ExecAnnIndexRestrPos		restores scan position.
 */
#include "postgres.h"
#include <math.h>

#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/heapam.h"
#include "access/datavec/diskann.h"
#include "access/tableam.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "commands/cluster.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeIndexscan.h"
#include "executor/node/nodeSubplan.h"
#include "optimizer/clauses.h"
#include "storage/tcap.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/tuplesort.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/executer_gstrace.h"
#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"
#include "executor/node/nodeAnnIndexscan.h"

static TupleTableSlot* ExecAnnIndexScan(PlanState* state);
static TupleTableSlot* AnnIndexNext(AnnIndexScanState* node);
static void ExecInitNextPartitionForAnnIndexScan(AnnIndexScanState* node);

struct DiskAnnQueryFilterState {
    DiskAnnFilterSpec spec;
    DiskAnnFilterCounters counters;
    MemoryContext epoch_context;
    ItemPointerData* candidate_tid; /* EState query lifetime, including global SubPlan End */
    SubPlanState* probe;
    ExprContext* probe_econtext;
    TupleTableSlot* probe_slot; /* EState owns final slot cleanup */
    HTAB* eligibility_cache;
    HTAB* rejected_members;
    bool prepared;
    bool sealed;
    bool in_probe;
    HTAB* pre_roots;
    HTAB* delivered;
    MemoryContext probe_context;
    ExprState* limit_state;
    int64 final_k;
    DiskAnnFilterCounters total_counters;
};

struct AnnQueryEligibilityEntry {
    uint64 visible_tid;
    bool qualified;
};

/* Index relation/file, snapshot and parameters are implicit in the bound
 * scan descriptor and epoch. Encode identity fields without struct padding. */
struct AnnQueryMemberKey {
    uint64 node_block;
    uint64 owner_tid;
    uint64 root_tid;
};

static Size AnnQueryOwnedMemory(void* context);
static bool AnnQueryQualify(void* context, const DiskAnnQueryMember* member,
    ItemPointerData* visible);
static AnnOrderByDistanceQuality AnnIndexRuntimeDistanceQuality(IndexScanDesc scan);

static void AnnQueryFilterInit(AnnIndexScanState* node, AnnIndexScan* plan, int eflags)
{
    if (!plan->ann_filter_enabled)
        return;
#ifdef USE_ASSERT_CHECKING
    /* Exercise the actual planned-node protocols, including the generated
     * correlated SubPlan, in the Debug acceptance build. */
    for (int withCosts = 0; withCosts <= 1; withCosts++) {
        bool savedStats = u_sess->opt_cxt.out_plan_stat;
        u_sess->opt_cxt.out_plan_stat = withCosts != 0;
        char* encoded = nodeToString(plan);
        u_sess->opt_cxt.out_plan_stat = savedStats;
        if (withCosts == 0) {
            /* The native statistics-free output is a plan hash image.
             * Check that filtering semantics participate in that image;
             * readable plan serialization uses the native full format. */
            Assert(strstr(encoded, ":ann_filter_enabled true") != NULL);
            Assert(strstr(encoded, ":ann_query_vector ") != NULL);
            Assert(strstr(encoded, ":ann_limit_expr ") != NULL);
            Assert(strstr(encoded, ":ann_in_filter ") != NULL);
            Assert(strstr(encoded, ":ann_candidate_tid_param ") != NULL);
            pfree(encoded);
            continue;
        }
        AnnIndexScan* decoded = (AnnIndexScan*)stringToNode(encoded);
        AnnIndexScan* copied = (AnnIndexScan*)copyObject(plan);
        Assert(decoded->ann_filter_enabled && copied->ann_filter_enabled);
        Assert(decoded->filterMode == plan->filterMode && copied->filterMode == plan->filterMode);
        Assert(decoded->ann_request_id == plan->ann_request_id);
        Assert(decoded->ann_initial_search_nodes == plan->ann_initial_search_nodes);
        Assert(decoded->ann_filter_work_mem == plan->ann_filter_work_mem);
        Assert(decoded->ann_candidate_tid_param == plan->ann_candidate_tid_param);
        Assert(decoded->ann_eligibility_tid_resno == plan->ann_eligibility_tid_resno);
        Assert(equal(decoded->ann_query_vector, plan->ann_query_vector));
        Assert(equal(decoded->ann_distance_expr, plan->ann_distance_expr));
        Assert(equal(decoded->ann_limit_expr, plan->ann_limit_expr));
        if (plan->ann_in_filter != NULL) {
            Assert(decoded->ann_in_filter != NULL);
            /* Native SubPlan output intentionally rounds optional planner
             * costs to two decimals. Verify execution fields independently. */
            if (withCosts != 0) {
                Assert(fabs(decoded->ann_in_filter->startup_cost -
                    plan->ann_in_filter->startup_cost) <= 0.005001);
                Assert(fabs(decoded->ann_in_filter->per_call_cost -
                    plan->ann_in_filter->per_call_cost) <= 0.005001);
            }
            decoded->ann_in_filter->startup_cost = plan->ann_in_filter->startup_cost;
            decoded->ann_in_filter->per_call_cost = plan->ann_in_filter->per_call_cost;
        }
        Assert(equal(decoded->ann_in_filter, plan->ann_in_filter));
        Assert(equal(copied->ann_in_filter, plan->ann_in_filter));
        pfree(encoded);
    }
#endif
    int mode = u_sess->datavec_ctx.diskann_filter_mode;
    int expected = plan->filterMode == ANN_FILTER_PRE ? DISKANN_FILTER_PRE :
        plan->filterMode == ANN_FILTER_IN ? DISKANN_FILTER_IN : DISKANN_FILTER_POST;
    if (!u_sess->datavec_ctx.enable_diskann_filter ||
        (mode != DISKANN_FILTER_AUTO && mode != expected))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cached DiskANN filtering plan no longer matches the current mode")));
    EState* estate = node->ss.ps.state;
    MemoryContext old = MemoryContextSwitchTo(estate->es_query_cxt);
    DiskAnnQueryFilterState* state = (DiskAnnQueryFilterState*)palloc0(sizeof(DiskAnnQueryFilterState));
    node->iss_QueryFilter = state;
    state->epoch_context = AllocSetContextCreate(estate->es_query_cxt,
        "DiskANN query filter epoch", ALLOCSET_DEFAULT_SIZES);
    /* ExecInitNode temporarily makes es_query_cxt the node context.  The
     * correlated global SubPlan outlives that context during ExecEndPlan. */
    state->candidate_tid = (ItemPointerData*)MemoryContextAllocZero(
        estate->es_const_query_cxt, sizeof(ItemPointerData));
    state->spec.mode = plan->filterMode;
    state->spec.search_nodes = plan->ann_initial_search_nodes;
    int memory_kb = plan->ann_filter_work_mem > 0 ? plan->ann_filter_work_mem :
        u_sess->attr.attr_memory.work_mem;
    state->spec.memory_limit = (Size)memory_kb * 1024;
    state->spec.counters = &state->counters;
    state->spec.context = node;
    state->spec.qualify = AnnQueryQualify;
    state->spec.owned_memory = AnnQueryOwnedMemory;
    state->spec.epoch = 1;
    state->limit_state = ExecInitExpr(plan->ann_limit_expr, (PlanState*)node);
    if (plan->filterMode == ANN_FILTER_PRE)
        outerPlanState(node) = ExecInitNode(outerPlan(plan), estate, eflags);
    if (plan->filterMode == ANN_FILTER_IN) {
        state->probe = ExecInitSubPlan(plan->ann_in_filter, (PlanState*)node);
        node->ss.ps.subPlan = lappend(node->ss.ps.subPlan, state->probe);
        state->probe_econtext = CreateExprContext(estate);
        state->probe_slot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(state->probe_slot, RelationGetDescr(node->ss.ss_currentRelation));
        state->probe_econtext->ecxt_scantuple = state->probe_slot;
        ParamExecData* param = &estate->es_param_exec_vals[plan->ann_candidate_tid_param];
        param->value = PointerGetDatum(state->candidate_tid);
        param->isnull = true;
    }
    MemoryContextSwitchTo(old);
}

static void AnnQueryAccumulate(DiskAnnFilterCounters* dst, const DiskAnnFilterCounters* src)
{
#define ADD_COUNTER(name) dst->name += src->name
    ADD_COUNTER(visited_nodes);
    ADD_COUNTER(candidate_members);
    ADD_COUNTER(candidate_callbacks);
    ADD_COUNTER(accepted_members);
    ADD_COUNTER(rejected_members);
    ADD_COUNTER(visible_rows);
    ADD_COUNTER(probe_calls);
    ADD_COUNTER(probe_cache_hits);
    ADD_COUNTER(member_cache_hits);
    ADD_COUNTER(qualified_target_rows);
    ADD_COUNTER(fetch_attempts);
    ADD_COUNTER(key_mismatches);
    ADD_COUNTER(search_rounds);
    ADD_COUNTER(pre_build_runs);
    ADD_COUNTER(pre_source_rows);
    ADD_COUNTER(pre_seals);
    dst->memory_peak = Max(dst->memory_peak, src->memory_peak);
#undef ADD_COUNTER
}

bool ExecAnnIndexFilterStats(AnnIndexScanState* node, DiskAnnFilterCounters* counters,
    uint64* epoch, bool* prepared)
{
    if (node == NULL || node->iss_QueryFilter == NULL)
        return false;
    DiskAnnQueryFilterState* state = node->iss_QueryFilter;
    *counters = state->total_counters;
    AnnQueryAccumulate(counters, &state->counters);
    *epoch = state->spec.epoch;
    *prepared = state->prepared;
    return true;
}

static Size AnnQueryOwnedMemory(void* context)
{
    DiskAnnQueryFilterState* state = ((AnnIndexScanState*)context)->iss_QueryFilter;
    Size owned = GetMemoryTotalSpace(state->epoch_context, true) +
        GetMemoryChunkSpace(state) + GetMemoryChunkSpace(state->candidate_tid);
    if (state->probe_econtext != NULL) {
        owned += GetMemoryChunkSpace(state->probe_econtext) +
            GetMemoryTotalSpace(state->probe_econtext->ecxt_per_tuple_memory, true);
        owned += GetMemoryChunkSpace(state->probe_slot) +
            GetMemoryChunkSpace(state->probe_slot->tts_values) +
            GetMemoryChunkSpace(state->probe_slot->tts_isnull) +
            GetMemoryChunkSpace(state->probe_slot->tts_lobPointers);
    }
    return owned;
}

static HTAB* AnnQueryTidSet(MemoryContext context, const char* name)
{
    HASHCTL ctl = {};
    ctl.keysize = sizeof(uint64);
    ctl.entrysize = sizeof(uint64);
    ctl.hcxt = context;
    return hash_create(name, 128, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Encode fields, never hash padding from a storage-specific TID struct.
 * The relation, snapshot and parameters are fixed by this scan epoch. */
static uint64 AnnQueryTidKey(const ItemPointerData* tid)
{
    return ((uint64)ItemPointerGetBlockNumber(tid) << 16) |
        ItemPointerGetOffsetNumber(tid);
}

static void AnnQueryCheckMemory(AnnIndexScanState* node)
{
    DiskAnnQueryFilterState* state = node->iss_QueryFilter;
    Size owned = AnnQueryOwnedMemory(node);
    state->counters.memory_peak = Max(state->counters.memory_peak, owned);
    if (owned > state->spec.memory_limit)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("DiskANN filtering exceeded diskann_filter_work_mem")));
    if (node->iss_ScanDesc != NULL)
        diskann_query_check_memory(GetIndexScanDesc(node->iss_ScanDesc));
}

/* The copied heap row owns every in-line datum. TOAST and normalization
 * happen after the heap content lock has been released. */
static bool AnnQueryKeyMatches(Datum value, Datum key, bool normalized)
{
    Vector* heap_vector = DatumGetVector(PointerGetDatum(PG_DETOAST_DATUM(value)));
    if (normalized)
        heap_vector = DatumGetVector(DirectFunctionCall1(l2_normalize, PointerGetDatum(heap_vector)));
    Vector* stored = DatumGetVector(key);
    return heap_vector->dim == stored->dim &&
        memcmp(heap_vector->x, stored->x, stored->dim * sizeof(float)) == 0;
}

static bool AnnQueryQualify(void* context, const DiskAnnQueryMember* member,
    ItemPointerData* visible)
{
    AnnIndexScanState* node = (AnnIndexScanState*)context;
    DiskAnnQueryFilterState* state = node->iss_QueryFilter;
    AnnIndexScan* plan = (AnnIndexScan*)node->ss.ps.plan;
    EState* estate = node->ss.ps.state;
    IndexScanDesc scan = GetIndexScanDesc(node->iss_ScanDesc);
    const ItemPointerData* root = &member->root_tid;
    Datum key = member->key;
    bool normalized = member->normalized;
    if (state->in_probe)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("recursive DiskANN candidate probe")));
    state->counters.candidate_callbacks++;
    if (!ItemPointerIsValid(root) || ItemPointerGetBlockNumber(root) >=
        RelationGetNumberOfBlocks(node->ss.ss_currentRelation))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
            errmsg("DiskANN candidate has an invalid target TID")));
    if (plan->filterMode == ANN_FILTER_PRE) {
        Assert(state->sealed);
        uint64 root_key = AnnQueryTidKey(root);
        if (hash_search(state->pre_roots, &root_key, HASH_FIND, NULL) == NULL) {
            state->counters.rejected_members++;
            return false;
        }
    }
    if (plan->filterMode == ANN_FILTER_IN)
        Assert(diskann_query_search_active(scan));
    /* VACUUM may invalidate the original vector owner while other members
     * survive. The copied canonical key is still checked against each row. */
    uint64 owner_key = ItemPointerIsValid(&member->owner_tid) ?
        AnnQueryTidKey(&member->owner_tid) : ~(uint64)0;
    AnnQueryMemberKey member_key = {(uint64)member->node_block, owner_key, AnnQueryTidKey(root)};
    if (hash_search(state->rejected_members, &member_key, HASH_FIND, NULL) != NULL) {
        state->counters.member_cache_hits++;
        state->counters.rejected_members++;
        return false;
    }
    MemoryContext old = MemoryContextSwitchTo(state->probe_context);
    bool qualified = false;
    bool valid_identity = false;
    state->in_probe = true;
    PG_TRY();
    {
        Relation table = node->ss.ss_currentRelation;
        state->counters.fetch_attempts++;
        Buffer buffer = ReadBuffer(table, ItemPointerGetBlockNumber(root));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        HeapTupleData tuple = {};
        tuple.tupTableType = HEAP_TUPLE;
        ItemPointerData tid = *root;
        HeapTuple owned = NULL;
        if (heap_hot_search_buffer(&tid, table, buffer, estate->es_snapshot, &tuple,
            NULL, NULL, true, &estate->have_current_xact_date))
            owned = heap_copytuple(&tuple);
        UnlockReleaseBuffer(buffer);
        if (owned != NULL) {
            state->counters.visible_rows++;
            bool isnull = false;
            AttrNumber attno = scan->indexRelation->rd_index->indkey.values[0];
            Datum value = heap_getattr(owned, attno, RelationGetDescr(table), &isnull);
            if (!isnull && AnnQueryKeyMatches(value, key, normalized)) {
                valid_identity = true;
                *visible = owned->t_self;
                qualified = true;
                if (plan->filterMode == ANN_FILTER_IN) {
                    /* Cache only a business verdict for an identity whose
                     * snapshot-visible row and canonical vector just matched.
                     * All parameters and the snapshot are fixed by this epoch. */
                    uint64 visible_key = AnnQueryTidKey(visible);
                    AnnQueryEligibilityEntry* entry = (AnnQueryEligibilityEntry*)hash_search(
                        state->eligibility_cache, &visible_key, HASH_FIND, NULL);
                    if (entry != NULL) {
                        state->counters.probe_cache_hits++;
                        qualified = entry->qualified;
                    } else {
                        ResetExprContext(state->probe_econtext);
                        ExecStoreTuple(owned, state->probe_slot, InvalidBuffer, false);
                        *state->candidate_tid = *visible;
                        ParamExecData* param = &estate->es_param_exec_vals[plan->ann_candidate_tid_param];
                        param->isnull = false;
                        bool result_isnull = false;
                        state->counters.probe_calls++;
                        Datum verdict = ExecSubPlan(state->probe, state->probe_econtext, &result_isnull);
                        qualified = !result_isnull && DatumGetBool(verdict);
                        entry = (AnnQueryEligibilityEntry*)hash_search(
                            state->eligibility_cache, &visible_key, HASH_ENTER, NULL);
                        entry->qualified = qualified;
                    }
                }
            } else {
                state->counters.key_mismatches++;
            }
        }
        /* Identity/key failures are local to this full member identity,
         * never a node-wide verdict or a business qualification verdict. */
        if (!valid_identity)
            (void)hash_search(state->rejected_members, &member_key, HASH_ENTER, NULL);
        AnnQueryCheckMemory(node);
    }
    PG_CATCH();
    {
        state->in_probe = false;
        MemoryContextSwitchTo(old);
        PG_RE_THROW();
    }
    PG_END_TRY();
    state->in_probe = false;
    if (state->probe_slot != NULL) {
        ExecClearTuple(state->probe_slot);
        ResetExprContext(state->probe_econtext);
    }
    if (qualified)
        state->counters.accepted_members++;
    else
        state->counters.rejected_members++;
    MemoryContextSwitchTo(old);
    MemoryContextReset(state->probe_context);
    return qualified;
}

static void AnnQueryBuildPre(AnnIndexScanState* node)
{
    DiskAnnQueryFilterState* state = node->iss_QueryFilter;
    AnnIndexScan* plan = (AnnIndexScan*)node->ss.ps.plan;
    Relation table = node->ss.ss_currentRelation;
    state->counters.pre_build_runs++;
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        TupleTableSlot* slot = ExecProcNode(outerPlanState(node));
        if (TupIsNull(slot))
            break;
        state->counters.pre_source_rows++;
        bool isnull = false;
        Datum value = tableam_tslot_getattr(slot, plan->ann_eligibility_tid_resno, &isnull);
        if (isnull)
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("PRE source returned a NULL target TID")));
        ItemPointerData tid = *(ItemPointer)DatumGetPointer(value);
        if (!ItemPointerIsValid(&tid) || ItemPointerGetBlockNumber(&tid) >=
            RelationGetNumberOfBlocks(table))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("PRE source returned an invalid target TID")));
        Buffer buffer = ReadBuffer(table, ItemPointerGetBlockNumber(&tid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        OffsetNumber offset = ItemPointerGetOffsetNumber(&tid);
        OffsetNumber roots[MaxHeapTuplesPerPage];
        heap_get_root_tuples(BufferGetPage(buffer), roots);
        OffsetNumber root = offset > 0 && offset <= MaxHeapTuplesPerPage ? roots[offset - 1] : InvalidOffsetNumber;
        UnlockReleaseBuffer(buffer);
        if (!OffsetNumberIsValid(root))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("PRE source identity cannot be resolved to a HOT root")));
        ItemPointerSetOffsetNumber(&tid, root);
        uint64 root_key = AnnQueryTidKey(&tid);
        (void)hash_search(state->pre_roots, &root_key, HASH_ENTER, NULL);
        AnnQueryCheckMemory(node);
    }
    state->sealed = true; /* even an empty source must still enter ANN */
    state->counters.pre_seals++;
}

static void AnnQueryPrepare(AnnIndexScanState* node)
{
    DiskAnnQueryFilterState* state = node->iss_QueryFilter;
    AnnIndexScan* plan = (AnnIndexScan*)node->ss.ps.plan;
    IndexScanDesc scan = GetIndexScanDesc(node->iss_ScanDesc);
    bool isnull = false;
    Datum k = ExecEvalExprSwitchContext(state->limit_state, node->ss.ps.ps_ExprContext, &isnull);
    if (isnull || DatumGetInt64(k) <= 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cached DiskANN filtering plan has an unsupported LIMIT")));
    state->final_k = DatumGetInt64(k); /* SQL output bound, never the beam capacity */
    AnnOrderByDistanceQuality quality = AnnIndexRuntimeDistanceQuality(scan);
    if (quality == ANN_ORDERBY_DISTANCE_UNAVAILABLE ||
        RelationIsUstoreFormat(node->ss.ss_currentRelation) ||
        RowRelationIsCompressed(node->ss.ss_currentRelation))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("opened index or table cannot execute DiskANN filtering")));
    MemoryContext old = MemoryContextSwitchTo(state->epoch_context);
    state->delivered = AnnQueryTidSet(state->epoch_context, "DiskANN delivered targets");
    HASHCTL rejected_ctl = {};
    rejected_ctl.keysize = sizeof(AnnQueryMemberKey);
    rejected_ctl.entrysize = sizeof(AnnQueryMemberKey);
    rejected_ctl.hcxt = state->epoch_context;
    state->rejected_members = hash_create("DiskANN rejected member identities", 128,
        &rejected_ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    if (state->probe_context == NULL)
        state->probe_context = AllocSetContextCreate(state->epoch_context,
            "DiskANN candidate probe", ALLOCSET_DEFAULT_SIZES);
    if (plan->filterMode == ANN_FILTER_PRE) {
        state->pre_roots = AnnQueryTidSet(state->epoch_context, "DiskANN PRE roots");
        AnnQueryBuildPre(node); /* runtime keys were evaluated before this call */
    } else if (plan->filterMode == ANN_FILTER_IN) {
        HASHCTL ctl = {};
        ctl.keysize = sizeof(uint64);
        ctl.entrysize = sizeof(AnnQueryEligibilityEntry);
        ctl.hcxt = state->epoch_context;
        state->eligibility_cache = hash_create("DiskANN qualified visible identities", 128,
            &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }
    diskann_set_query_filter(scan, &state->spec);
    state->prepared = true;
    MemoryContextSwitchTo(old);
}

static bool AnnQueryValidateDelivery(AnnIndexScanState* node, IndexScanDesc scan, TupleTableSlot* slot)
{
    DiskAnnQueryFilterState* state = node->iss_QueryFilter;
    DiskAnnQueryMember member;
    if (!diskann_current_query_member(scan, &member) ||
        !ItemPointerEquals(&member.visible_tid, &scan->xs_ctup.t_self))
        return false;
    MemoryContext old = MemoryContextSwitchTo(state->probe_context);
    bool isnull = false;
    Datum value = tableam_tslot_getattr(slot, scan->indexRelation->rd_index->indkey.values[0], &isnull);
    bool matches = !isnull && AnnQueryKeyMatches(value, member.key, member.normalized);
    MemoryContextSwitchTo(old);
    MemoryContextReset(state->probe_context);
    if (!matches) {
        state->counters.key_mismatches++;
        return false;
    }
    bool found = false;
    uint64 visible_key = AnnQueryTidKey(&member.visible_tid);
    (void)hash_search(state->delivered, &visible_key, HASH_ENTER, &found);
    AnnQueryCheckMemory(node);
    return !found;
}

static AnnOrderByRef* AnnIndexFindOrderByRef(AnnIndexScan* plan)
{
    ListCell* lc = NULL;

    foreach (lc, plan->scan.plan.targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (IsA(tle->expr, AnnOrderByRef)) {
            return (AnnOrderByRef*)tle->expr;
        }
    }
    return NULL;
}

static bool AnnIndexReuseModeAllows(int mode, AnnOrderByDistanceQuality quality)
{
    if (quality == ANN_ORDERBY_DISTANCE_EXACT_L2) {
        return (mode & ANN_ORDERBY_REUSE_L2) != 0;
    }
    if (quality == ANN_ORDERBY_DISTANCE_APPROX_COSINE) {
        return (mode & ANN_ORDERBY_REUSE_COSINE) != 0;
    }
    return false;
}

/* Revalidate the physical index opened for this execution.  A saved plan's
 * AnnOrderByRef is only a request; it must not authorize an unrelated ANN
 * AM, PQ index, non-vector opclass, or changed support procedure. */
static AnnOrderByDistanceQuality AnnIndexRuntimeDistanceQuality(IndexScanDesc scan)
{
    if (scan == NULL || scan->indexRelation == NULL ||
        scan->indexRelation->rd_rel->relam != DISKANN_AM_OID ||
        TupleDescAttr(scan->indexRelation->rd_att, 0)->atttypid != VECTOROID ||
        scan->indexRelation->rd_index->indkey.values[0] <= 0 ||
        RELATION_HAS_BUCKET(scan->indexRelation)) {
        return ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    }

    DiskAnnOptions* opts = (DiskAnnOptions*)scan->indexRelation->rd_options;
    if (opts != NULL && opts->enablePQ) {
        return ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    }

    Oid distanceProc = index_getprocid(scan->indexRelation, 1, DISKANN_DISTANCE_PROC);
    Oid normProc = index_getprocid(scan->indexRelation, 1, DISKANN_NORM_PROC);
    if (distanceProc == L2_FUNC_OID && !OidIsValid(normProc)) {
        return ANN_ORDERBY_DISTANCE_EXACT_L2;
    }
    if (distanceProc == IP_FUNC_OID && OidIsValid(normProc)) {
        return ANN_ORDERBY_DISTANCE_APPROX_COSINE;
    }
    return ANN_ORDERBY_DISTANCE_UNAVAILABLE;
}

static void AnnIndexInvalidateOrderByValue(AnnIndexScanState* node)
{
    node->iss_AnnOrderByValid = false;
    node->iss_AnnOrderByHeapRecheck = false;
    node->iss_AnnOrderByIsNull = true;
    node->iss_AnnOrderByValue = (Datum)0;
    node->iss_AnnOrderByQuality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;
}

/* Bind the AM channel only after the heap access layer has produced a visible
 * tuple.  Epoch/token checks prevent rescan and stale-row reuse; HOT members
 * intentionally do not require physical TID equality with the index root. */
static void AnnIndexBindOrderByValue(AnnIndexScanState* node, IndexScanDesc scan)
{
    AnnOrderByDistanceChannel* channel = &scan->xs_ann_orderby;
    uint64 previousEpoch = node->iss_AnnOrderByEpoch;
    uint64 previousToken = node->iss_AnnOrderByRowToken;

    AnnIndexInvalidateOrderByValue(node);
    /* A cached plan may be executed after the GUC has been lowered.  That
     * choice is made once in ExecInitAnnIndexScan; every row then uses the
     * fallback expression and the AM channel stays unrequested. */
    if (node->iss_AnnOrderByExpectedQuality == ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
        return;
    }

    /* Once reuse is selected for this scan, a missing or stale publication
     * is an AM/executor contract violation. The sole row-local exception
     * is an explicit L2 heap recheck: both sources return exact SQL L2.
     * Never silently fall back from an approximate cosine publication. */
    if (!channel->requested || !channel->valid || channel->isnull ||
        channel->quality != node->iss_AnnOrderByExpectedQuality ||
        (channel->heap_recheck && channel->quality != ANN_ORDERBY_DISTANCE_EXACT_L2) ||
        channel->scan_epoch == 0 || channel->row_token == 0 ||
        !ItemPointerIsValid(&channel->heap_tid) ||
        !ItemPointerIsValid(&scan->xs_ctup.t_self) ||
        ItemPointerGetBlockNumber(&channel->heap_tid) !=
            ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) ||
        (channel->scan_epoch == previousEpoch && channel->row_token <= previousToken)) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("DiskANN did not publish a valid ORDER BY distance for the current row")));
    }

    node->iss_AnnOrderByValue = channel->value;
    node->iss_AnnOrderByIsNull = false;
    node->iss_AnnOrderByValid = true;
    node->iss_AnnOrderByHeapRecheck = channel->heap_recheck;
    node->iss_AnnOrderByQuality = channel->quality;
    node->iss_AnnOrderByEpoch = channel->scan_epoch;
    node->iss_AnnOrderByRowToken = channel->row_token;

    if (!node->iss_AnnOrderByHeapRecheck) {
        AttrNumber attno = scan->indexRelation->rd_index->indkey.values[0];
        bool isnull = false;
        Datum vector = tableam_tslot_getattr(node->ss.ss_ScanTupleSlot, attno, &isnull);
        if (isnull || !diskann_orderby_vector_matches(scan, vector)) {
            if (channel->quality != ANN_ORDERBY_DISTANCE_EXACT_L2) {
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("DiskANN distance belongs to a different heap vector"),
                    errhint("REINDEX this legacy index before reusing approximate cosine distances.")));
            }
            node->iss_AnnOrderByHeapRecheck = true;
        }
    }
}

/* ----------------------------------------------------------------
 *		AnnIndexNext
 *
 *		Retrieve a tuple from the AnnIndexScan node's current_relation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* AnnIndexNext(AnnIndexScanState* node);

static TupleTableSlot* AnnIndexNext(AnnIndexScanState* node)
{
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    ScanDirection direction;
    IndexScanDesc scandesc;
    HeapTuple tuple;
    TupleTableSlot* slot = NULL;
    bool isUstore = false;

    /*
     * extract necessary information from index scan node
     */
    estate = node->ss.ps.state;
    direction = estate->es_direction;
    /* flip direction if this is an overall backward scan */
    if (ScanDirectionIsBackward(((AnnIndexScan*)node->ss.ps.plan)->indexorderdir)) {
        if (ScanDirectionIsForward(direction))
            direction = BackwardScanDirection;
        else if (ScanDirectionIsBackward(direction))
            direction = ForwardScanDirection;
    }
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    scandesc = node->iss_ScanDesc;
    /*
     * annCount is a planner-side costing estimate. Do not push it down as the
     * initial HNSW search size; iterative scan expands candidates on demand.
     */
    scandesc->count = 0;
    scandesc->limitk = (int64_t)node->limitCount;
    isUstore = RelationIsUstoreFormat(node->ss.ss_currentRelation);
    /*
     * ok, now that we have what we need, fetch the next tuple.
     */
    // we should change abs_idx_getnext to call IdxScanAm(scan)->idx_getnext and channge .idx_getnext in g_HeapIdxAm to
    // IndexGetnextSlot
    while (true) {
        CHECK_FOR_INTERRUPTS();

        AnnIndexInvalidateOrderByValue(node);

        IndexScanDesc indexScan = GetIndexScanDesc(scandesc);
        if (isUstore) {
            if (!IndexGetnextSlot(scandesc, direction, slot, &node->ss.ps.state->have_current_xact_date)) {
                break;
            }
        } else {
            if ((tuple = scan_handler_idx_getnext(scandesc, direction, InvalidOid, InvalidBktId,
                                                  &node->ss.ps.state->have_current_xact_date)) == NULL) {
                break;
            }
            /* Update indexScan, because hashbucket may switch current index in scan_handler_idx_getnext */
            indexScan = GetIndexScanDesc(scandesc);
            /*
             * Store the scanned tuple in the scan tuple slot of the scan state.
             * Note: we pass 'false' because tuples returned by amgetnext are
             * pointers onto disk pages and must not be pfree_ext()'d.
             */
            (void)ExecStoreTuple(tuple, /* tuple to store */
                slot,                   /* slot to store in */
                indexScan->xs_cbuf,     /* buffer containing tuple */
                false);                 /* don't pfree */
        }

        if (node->iss_QueryFilter != NULL && !AnnQueryValidateDelivery(node, indexScan, slot))
            continue;
        AnnIndexBindOrderByValue(node, indexScan);

        /*
         * If the index was lossy, we have to recheck the index quals using
         * the fetched tuple.
         */
        if (indexScan->xs_recheck) {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);
            if (!ExecQual(node->indexqualorig, econtext, false)) {
                /* Fails recheck, so drop it and loop back for another */
                AnnIndexInvalidateOrderByValue(node);
                InstrCountFiltered2(node, 1);
                continue;
            }
        }

        return slot;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    return ExecClearTuple(slot);
}

/* Keep the original heap header (ctid, xmin/base, tableoid, etc.) rather
 * than reforming a row from user attributes. The bytea envelope is private
 * to this sorter, never a SQL result; no process pointer is persisted. */
static void AnnIndexSortPut(AnnIndexScanState* node, TupleTableSlot* slot, Datum distance, bool isnull)
{
    HeapTuple tuple = ExecCopySlotTuple(slot);
    Size bytes = HEAPTUPLESIZE + tuple->t_len;
    bytea* payload = (bytea*)palloc0(VARHDRSZ + bytes);
    SET_VARSIZE(payload, VARHDRSZ + bytes);
    HeapTupleData header;
    errno_t rc = memset_s(&header, sizeof(header), 0, sizeof(header));
    securec_check(rc, "\0", "\0");
    header.tupTableType = HEAP_TUPLE;
    header.t_len = tuple->t_len;
    header.t_self = tuple->t_self;
    header.t_tableOid = tuple->t_tableOid;
    header.t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(&header, tuple);
#ifdef ENABLE_MULTIPLE_NODES
    header.t_xc_node_id = tuple->t_xc_node_id;
#endif
    rc = memcpy_s(VARDATA(payload), bytes, &header, sizeof(header));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(VARDATA(payload) + HEAPTUPLESIZE, tuple->t_len, tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    TupleTableSlot* saved = node->iss_OrderedSlot;
    ExecClearTuple(saved);
    saved->tts_values[0] = distance;
    saved->tts_isnull[0] = isnull;
    saved->tts_values[1] = PointerGetDatum(payload);
    saved->tts_isnull[1] = false;
    ExecStoreVirtualTuple(saved);
    tuplesort_puttupleslot(node->iss_OrderedSort, saved);
    ExecClearTuple(saved);
    pfree(payload);
    heap_freetuple(tuple);
}

static TupleTableSlot* AnnIndexOrderedNext(AnnIndexScanState* node)
{
    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    IndexScanDesc scan = GetIndexScanDesc(node->iss_ScanDesc);
    if (!node->iss_OrderedReady) {
        MemoryContext oldCtx = MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
        AttrNumber key = 1;
        Oid sortop = FLOAT8LTOID;
        Oid collation = InvalidOid;
        bool nullsFirst = false;
        node->iss_OrderedSort = tuplesort_begin_heap(node->iss_OrderedSlot->tts_tupleDescriptor,
            1, &key, &sortop, &collation, &nullsFirst, u_sess->attr.attr_memory.work_mem, false);
        MemoryContextSwitchTo(oldCtx);
        ((DiskAnnScanOpaque)scan->opaque)->orderedBatch = true;
        uint64 qualified = 0;
        /* A target SRF can emit zero rows, so a base-row bound is unsafe. */
        int64 bound = node->iss_OrderedHasSRF ? 0 : node->limitCount;
        while (diskann_advance_ordered_batch(scan)) {
            for (;;) {
                CHECK_FOR_INTERRUPTS();
                ResetExprContext(econtext);
                TupleTableSlot* next = AnnIndexNext(node);
                if (TupIsNull(next)) {
                    break;
                }
                econtext->ecxt_scantuple = next;
                if (node->iss_OrderedQual != NIL && !ExecQual(node->iss_OrderedQual, econtext)) {
                    InstrCountFiltered1(node, 1);
                    continue;
                }
                bool isnull = false;
                Datum distance;
                if (node->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE &&
                    !node->iss_AnnOrderByHeapRecheck) {
                    distance = node->iss_AnnOrderByValue;
                    isnull = node->iss_AnnOrderByIsNull;
                } else {
                    distance = ExecEvalExprSwitchContext(node->iss_OrderedExpr, econtext, &isnull, NULL);
                    if (node->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
                        node->iss_AnnOrderByFallbacks++;
                    }
                }
                AnnIndexSortPut(node, next, distance, isnull);
                qualified++;
            }
            /* Complete the entire last batch, including merged TIDs, before
             * deciding to stop. No later expansion may follow publication. */
            if (bound > 0 && qualified >= (uint64)bound) {
                break;
            }
        }
        tuplesort_performsort(node->iss_OrderedSort);
        node->iss_OrderedReady = true;
    }

    ExecClearTuple(slot);
    if (!tuplesort_gettupleslot(node->iss_OrderedSort, true, node->iss_OrderedSlot, NULL)) {
        AnnIndexInvalidateOrderByValue(node);
        return slot;
    }
    bool isnull = false;
    Datum distance = tableam_tslot_getattr(node->iss_OrderedSlot, 1, &isnull);
    bool payloadNull = false;
    Datum payloadDatum = tableam_tslot_getattr(node->iss_OrderedSlot, 2, &payloadNull);
    if (payloadNull) {
        elog(ERROR, "missing DiskANN sort tuple");
    }
    bytea* payload = DatumGetByteaP(payloadDatum);
    Size bytes = VARSIZE(payload) - VARHDRSZ;
    if (bytes < HEAPTUPLESIZE) {
        elog(ERROR, "invalid DiskANN sort tuple");
    }
    HeapTupleData header;
    errno_t rc = memcpy_s(&header, sizeof(header), VARDATA(payload), sizeof(header));
    securec_check(rc, "\0", "\0");
    if (bytes != HEAPTUPLESIZE + header.t_len) {
        elog(ERROR, "invalid DiskANN sort tuple length");
    }
    HeapTuple tuple = heaptup_alloc(bytes);
    *tuple = header;
    tuple->t_data = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);
    rc = memcpy_s(tuple->t_data, tuple->t_len, VARDATA(payload) + HEAPTUPLESIZE, tuple->t_len);
    securec_check(rc, "\0", "\0");
    if ((Pointer)payload != DatumGetPointer(payloadDatum)) {
        pfree(payload);
    }
    ExecStoreTuple(tuple, slot, InvalidBuffer, true);
    if (node->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
        node->iss_AnnOrderByValue = distance;
        node->iss_AnnOrderByIsNull = isnull;
        node->iss_AnnOrderByValid = true;
        node->iss_AnnOrderByHeapRecheck = false;
        node->iss_AnnOrderByQuality = node->iss_AnnOrderByExpectedQuality;
    }
    return slot;
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool AnnIndexRecheck(AnnIndexScanState* node, TupleTableSlot* slot)
{
    ExprContext* econtext = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;

    /* Does the tuple meet the indexqual condition? */
    econtext->ecxt_scantuple = slot;

    ResetExprContext(econtext);

    return ExecQual(node->indexqualorig, econtext, false);
}

/* ----------------------------------------------------------------
 *		ExecAnnIndexScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecAnnIndexScan(PlanState* state)
{
    AnnIndexScanState* node = castNode(AnnIndexScanState, state);
    if (node->iss_QueryFilter != NULL && node->iss_QueryFilter->in_probe)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("cannot fetch DiskANN while its candidate probe is active")));
    /*
     * If we have runtime keys and they've not already been set up, do it now.
     */
    if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady) {
        /*
         * set a flag for partitioned table, so we can deal with it specially
         * when we rescan the partitioned table
         */
        if (node->ss.isPartTbl) {
            if (PointerIsValid(node->ss.partitions)) {
                node->ss.ss_ReScan = true;
                ExecReScan((PlanState*)node);
            }
        } else {
            ExecReScan((PlanState*)node);
        }
    } else if (DB_IS_CMPT(B_FORMAT) && node->iss_NumRuntimeKeys != 0 && u_sess->parser_cxt.has_set_uservar) {
        ExprContext* econtext = node->iss_RuntimeContext;
        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }

    if (node->iss_QueryFilter != NULL && !node->iss_QueryFilter->prepared)
        AnnQueryPrepare(node);

    if (node->iss_DiskannOrdered && node->ss.ps.state->es_epqTuple == NULL) {
        /* OrderedNext checks quals once while collecting candidates. Keep
         * projection here so volatile targets and SRFs run on emitted rows.
         * EPQ substitute tuples must still go through normal qual checking. */
        TupleTableSlot* result = NULL;
        node->ss.ps.qual = NIL;
        PG_TRY();
        {
            result = ExecScan(&node->ss, (ExecScanAccessMtd)AnnIndexOrderedNext,
                (ExecScanRecheckMtd)AnnIndexRecheck);
        }
        PG_CATCH();
        {
            node->ss.ps.qual = node->iss_OrderedQual;
            PG_RE_THROW();
        }
        PG_END_TRY();
        node->ss.ps.qual = node->iss_OrderedQual;
        return result;
    }
    TupleTableSlot* result = ExecScan(&node->ss, (ExecScanAccessMtd)AnnIndexNext, (ExecScanRecheckMtd)AnnIndexRecheck);
    if (node->iss_QueryFilter != NULL && !TupIsNull(result))
        node->iss_QueryFilter->counters.qualified_target_rows++;
    return result;
}

/* ----------------------------------------------------------------
 *		ExecReScanAnnIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void ExecReScanAnnIndexScan(AnnIndexScanState* node)
{
    if (node->iss_QueryFilter != NULL) {
        DiskAnnQueryFilterState* state = node->iss_QueryFilter;
        bool was_prepared = state->prepared;
        if (state->in_probe)
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("cannot rescan DiskANN while its candidate probe is active")));
        if (node->iss_ScanDesc != NULL)
            diskann_clear_query_filter(GetIndexScanDesc(node->iss_ScanDesc));
        AnnQueryAccumulate(&state->total_counters, &state->counters);
        errno_t rc = memset_s(&state->counters, sizeof(state->counters), 0, sizeof(state->counters));
        securec_check(rc, "", "");
        state->prepared = false;
        state->sealed = false;
        state->pre_roots = NULL;
        state->delivered = NULL;
        state->eligibility_cache = NULL;
        state->rejected_members = NULL;
        if (state->probe_slot != NULL) {
            ExecClearTuple(state->probe_slot);
            ReScanExprContext(state->probe_econtext);
        }
        /* Initial runtime-key evaluation can call ReScan before a search
         * has borrowed this state. Start epoch 1 for that first execution;
         * every rescan of a prepared scan still gets a fresh identity. */
        if (was_prepared)
            state->spec.epoch++;
        MemoryContextReset(state->epoch_context);
        if (outerPlanState(node) != NULL && outerPlanState(node)->chgParam == NULL)
            ExecReScan(outerPlanState(node));
        AnnIndexScan* plan = (AnnIndexScan*)node->ss.ps.plan;
        if (plan->filterMode == ANN_FILTER_IN)
            node->ss.ps.state->es_param_exec_vals[plan->ann_candidate_tid_param].isnull = true;
    }
    if (node->iss_OrderedSort != NULL) {
        ExecClearTuple(node->iss_OrderedSlot);
        tuplesort_end(node->iss_OrderedSort);
        node->iss_OrderedSort = NULL;
    }
    node->iss_OrderedReady = false;
    AnnIndexInvalidateOrderByValue(node);
    /*
     * For recursive-stream rescan, if number of RuntimeKeys not euqal zero,
     * just return without rescan.
     *
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.  But first,
     * reset the context so we don't leak memory as each outer tuple is
     * scanned.  Note this assumes that we will recalculate *all* runtime keys
     * on each call.
     */
    if (node->iss_NumRuntimeKeys != 0) {
        if (node->ss.ps.state->es_recursive_next_iteration) {
            node->iss_RuntimeKeysReady = false;
            return;
        }

        ExprContext* econtext = node->iss_RuntimeContext;

        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }
    node->iss_RuntimeKeysReady = true;

    /*
     * deal with partitioned table
     */
    bool partpruning = !RelationIsSubPartitioned(node->ss.ss_currentRelation) &&
        ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && list_length(node->ss.partitions) == 1;
    /* if only one partition is scaned in indexscan, we don't need do rescan for partition */
    if (node->ss.isPartTbl && !partpruning) {
        /*
         * if node->ss.ss_ReScan = true, just do rescaning as non-partitioned
         * table; else switch to next partition for scaning.
         */
        if (node->ss.ss_ReScan ||
            (((Scan *)node->ss.ps.plan)->partition_iterator_elimination)) {
            /* reset the rescan falg */
            node->ss.ss_ReScan = false;
        } else {
            if (!PointerIsValid(node->ss.partitions)) {
                /*
                 * give up rescaning the index if there is no partition to scan
                 */
                return;
            }

            /* switch to next partition for scaning */
            Assert(PointerIsValid(node->iss_ScanDesc));
            scan_handler_idx_endscan(node->iss_ScanDesc);
            /*  initialize Scan for the next partition */
            ExecInitNextPartitionForAnnIndexScan(node);
            ExecScanReScan(&node->ss);
            return;
        }
    }

    /* reset index scan */
    scan_handler_idx_rescan(
        node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys, node->iss_OrderByKeys, node->iss_NumOrderByKeys);

    scan_handler_idx_rescan_parallel(node->iss_ScanDesc);

    ExecScanReScan(&node->ss);
}

/* ----------------------------------------------------------------
 *		ExecEndAnnIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndAnnIndexScan(AnnIndexScanState* node)
{
    if (node->iss_QueryFilter != NULL) {
        if (node->iss_ScanDesc != NULL)
            diskann_clear_query_filter(GetIndexScanDesc(node->iss_ScanDesc));
        if (outerPlanState(node) != NULL)
            ExecEndNode(outerPlanState(node));
        if (node->iss_QueryFilter->probe_slot != NULL)
            ExecClearTuple(node->iss_QueryFilter->probe_slot);
        if (node->iss_QueryFilter->probe_econtext != NULL) {
            FreeExprContext(node->iss_QueryFilter->probe_econtext, true);
            node->iss_QueryFilter->probe_econtext = NULL;
        }
        /* Candidate storage survives for EState's global SubPlan cleanup. */
        MemoryContextDelete(node->iss_QueryFilter->epoch_context);
        node->iss_QueryFilter->epoch_context = NULL;
    }
    if (node->iss_OrderedSort != NULL) {
        ExecClearTuple(node->iss_OrderedSlot);
        tuplesort_end(node->iss_OrderedSort);
        node->iss_OrderedSort = NULL;
    }
    Relation index_relation_desc;
    IndexScanDesc index_scan_desc;
    Relation relation;

    /*
     * extract information from the node
     */
    index_relation_desc = node->iss_RelationDesc;
    index_scan_desc = node->iss_ScanDesc;
    relation = node->ss.ss_currentRelation;

    /*
     * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
     */
#ifdef NOT_USED
    ExecFreeExprContext(&node->ss.ps);
    if (node->iss_RuntimeContext)
        FreeExprContext(node->iss_RuntimeContext, true);
#endif

    /*
     * clear out tuple table slots
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if (index_scan_desc) {
        scan_handler_idx_endscan(index_scan_desc);
        if (WorkerThreadAmI() && node->ss.ps.plan->dop > 1) {
            u_sess->stream_cxt.global_obj->DestroyStreamDesc(
                node->ss.ps.state->es_plannedstmt->queryId, node->ss.ps.plan);
        }
    }

    /*
     * close the index relation (no-op if we didn't open it)
     * close the index relation if the relation is non-partitioned table
     * close the index partitions and table partitions if the relation is
     * non-partitioned table
     */
    if (node->ss.isPartTbl) {
        if (PointerIsValid(node->iss_IndexPartitionList)) {
            Assert(PointerIsValid(index_relation_desc));
            Assert(PointerIsValid(node->ss.partitions));
            Assert(node->ss.partitions->length == node->iss_IndexPartitionList->length);

            Assert(PointerIsValid(node->iss_CurrentIndexPartition));
            releaseDummyRelation(&(node->iss_CurrentIndexPartition));

            Assert(PointerIsValid(node->ss.ss_currentPartition));
            releaseDummyRelation(&(node->ss.ss_currentPartition));

            if (RelationIsSubPartitioned(relation)) {
                releaseSubPartitionList(index_relation_desc, &(node->iss_IndexPartitionList), NoLock);
                releaseSubPartitionList(node->ss.ss_currentRelation, &(node->ss.subpartitions), NoLock);
            } else {
                /* close index partition */
                releasePartitionList(node->iss_RelationDesc, &(node->iss_IndexPartitionList), NoLock);
            }

            /* close table partition */
            releasePartitionList(node->ss.ss_currentRelation, &(node->ss.partitions), NoLock);
        }
    }

    if (index_relation_desc)
        index_close(index_relation_desc, NoLock);

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecAnnIndexMarkPos
 * ----------------------------------------------------------------
 */
void ExecAnnIndexMarkPos(AnnIndexScanState* node)
{
    scan_handler_idx_markpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecAnnIndexRestrPos
 * ----------------------------------------------------------------
 */
void ExecAnnIndexRestrPos(AnnIndexScanState* node)
{
    scan_handler_idx_restrpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecAnnInitIndexScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
void ExecInitAnnIndexRelation(AnnIndexScanState* node, EState* estate, int eflags)
{
    AnnIndexScanState* index_state = node;
    Snapshot scanSnap;
    Relation current_relation = index_state->ss.ss_currentRelation;
    AnnIndexScan *index_scan = (AnnIndexScan *)node->ss.ps.plan;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(index_state->iss_RelationDesc, &index_scan->scan, &index_state->ss);

    /* deal with partition info */
    if (index_state->ss.isPartTbl) {
        index_state->iss_ScanDesc = NULL;

        if (index_scan->scan.itrs > 0) {
            Partition current_partition = NULL;
            Partition currentindex = NULL;

            /* Initialize table partition list and index partition list for following scan */
            ExecInitPartitionForAnnIndexScan(index_state, estate);

            if (index_state->ss.partitions != NIL) {
                /* construct a dummy relation with the first table partition for following scan */
                if (RelationIsSubPartitioned(current_relation)) {
                    Partition subOnePart = (Partition)list_nth(index_state->ss.partitions, index_state->ss.currentSlot);
                    List *currentSubpartList = (List *)list_nth(index_state->ss.subpartitions, 0);
                    List *currentindexlist = (List *)list_nth(index_state->iss_IndexPartitionList, 0);
                    current_partition = (Partition)list_nth(currentSubpartList, 0);
                    currentindex = (Partition)list_nth(currentindexlist, 0);
                    Relation subOnePartRel = partitionGetRelation(index_state->ss.ss_currentRelation, subOnePart);
                    index_state->ss.ss_currentPartition =
                        partitionGetRelation(subOnePartRel, current_partition);
                    releaseDummyRelation(&subOnePartRel);
                } else {
                    current_partition = (Partition)list_nth(index_state->ss.partitions, 0);
                    currentindex = (Partition)list_nth(index_state->iss_IndexPartitionList, 0);
                    index_state->ss.ss_currentPartition =
                        partitionGetRelation(index_state->ss.ss_currentRelation, current_partition);
                }

                index_state->iss_CurrentIndexPartition =
                    partitionGetRelation(index_state->iss_RelationDesc, currentindex);

                /*
                 * Verify if a DDL operation that froze all tuples in the relation
                 * occured after taking the snapshot.
                 */
                if (RelationIsUstoreFormat(index_state->ss.ss_currentPartition)) {
                    TransactionId relfrozenxid64 = InvalidTransactionId;
                    getPartitionRelxids(index_state->ss.ss_currentPartition, &relfrozenxid64);
                    if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                        !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                        TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                        ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                            (errmsg("Snapshot too old, IndexRelation is  PartTbl, the info: snapxmax is %lu, "
                                "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                                scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                                g_instance.undo_cxt.globalRecycleXid))));
                    }
                }

                /* Initialize scan descriptor for partitioned table */
                index_state->iss_ScanDesc = scan_handler_idx_beginscan(index_state->ss.ss_currentPartition,
                    index_state->iss_CurrentIndexPartition,
                    scanSnap,
                    index_state->iss_NumScanKeys,
                    index_state->iss_NumOrderByKeys,
                    (ScanState*)index_state);
            }
        }
    } else {
        /*
         * Verify if a DDL operation that froze all tuples in the relation
         * occured after taking the snapshot.
         */
        if (RelationIsUstoreFormat(current_relation)) {
            TransactionId relfrozenxid64 = InvalidTransactionId;
            getRelationRelxids(current_relation, &relfrozenxid64);
            if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                    (errmsg("Snapshot too old, IndexRelation is not  PartTbl, the info: snapxmax is %lu, "
                        "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                        scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                        g_instance.undo_cxt.globalRecycleXid))));
            }
        }

        ParallelIndexScanDescData *paralleDesc = NULL;
        if (u_sess->stream_cxt.global_obj && node->ss.ps.plan->dop > 1) {
            if (WorkerThreadAmI()) {
                u_sess->stream_cxt.global_obj->BuildStreamDesc(
                    estate->es_plannedstmt->queryId, index_state->ss.ps.plan);
            }
            paralleDesc = (ParallelIndexScanDescData*)u_sess->stream_cxt.global_obj->GetParalleDesc(
                estate->es_plannedstmt->queryId, index_state->ss.ps.plan->plan_node_id);
            if (WorkerThreadAmI())
                scan_handler_idx_parallelscan_initialize(current_relation, index_state->iss_RelationDesc, paralleDesc);
        }

        /*
         * Initialize scan descriptor.
         */
        index_state->iss_ScanDesc = scan_handler_idx_beginscan(current_relation,
            index_state->iss_RelationDesc,
            scanSnap,
            index_state->iss_NumScanKeys,
            index_state->iss_NumOrderByKeys,
            (ScanState*)index_state,
            paralleDesc);
    }

    return;
}

AnnIndexScanState* ExecInitAnnIndexScan(AnnIndexScan* node, EState* estate, int eflags)
{
    AnnIndexScanState* index_state = NULL;
    Relation current_relation;
    bool relisTarget = false;

    gstrace_entry(GS_TRC_ID_ExecInitIndexScan);
    /*
     * create state structure
     */
    index_state = makeNode(AnnIndexScanState);
    index_state->ss.ps.plan = (Plan*)node;
    index_state->ss.ps.state = estate;
    index_state->ss.isPartTbl = node->scan.isPartTbl;
    index_state->ss.currentSlot = 0;
    index_state->ss.partScanDirection = node->indexorderdir;
    index_state->ss.ps.ExecProcNode = ExecAnnIndexScan;
    index_state->annCount = node->annCount;
    index_state->iss_AnnOrderByExpectedQuality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    index_state->iss_AnnOrderByEpoch = 0;
    index_state->iss_AnnOrderByRowToken = 0;
    AnnIndexInvalidateOrderByValue(index_state);
    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &index_state->ss.ps);

    index_state->ss.ps.ps_vec_TupFromTlist = false;

    /*
     * initialize child expressions
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).  Likewise for
     * indexorderby, if any.  But the indexqualorig expression is always
     * initialized even though it will only be used in some uncommon cases ---
     * would be nice to improve that.  (Problem is that any SubPlans present
     * in the expression must be found now...)
     */
    if (estate->es_is_flt_frame) {
        index_state->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig = (List*)ExecInitQualByFlatten(node->indexqualorig, (PlanState*)index_state);
    } else {
        index_state->ss.ps.targetlist = (List*)ExecInitExprByRecursion(
            (Expr*)node->scan.plan.targetlist, (PlanState*)index_state);
        index_state->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig =
            (List*)ExecInitExprByRecursion((Expr*)node->indexqualorig, (PlanState*)index_state);
    }

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    index_state->ss.ss_currentRelation = current_relation;
    index_state->ss.ss_currentScanDesc = NULL; /* no heap scan here */
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &index_state->ss.ps, current_relation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, &index_state->ss, current_relation->rd_tam_ops);

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&index_state->ss, CreateTupleDescCopy(RelationGetDescr(current_relation)));
    index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops = current_relation->rd_tam_ops;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&index_state->ss.ps);

    index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops =
            index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops;

    ExecAssignScanProjectionInfo(&index_state->ss);

    Assert(index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    AnnQueryFilterInit(index_state, node, eflags);
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
        return index_state;
    }

    /*
     * Open the index relation.
     *
     * If the parent table is one of the target relations of the query, then
     * InitPlan already opened and write-locked the index, so we can avoid
     * taking another lock here.  Otherwise we need a normal reader's lock.
     */
    relisTarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    index_state->iss_RelationDesc = index_open(node->indexid, relisTarget ? NoLock : AccessShareLock);
    if (!IndexIsUsable(index_state->iss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("can't initialize index scans using unusable index \"%s\"",
                    RelationGetRelationName(index_state->iss_RelationDesc))));
    }
    /*
     * Initialize index-specific scan state
     */
    index_state->iss_RuntimeKeysReady = false;
    index_state->iss_RuntimeKeys = NULL;
    index_state->iss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexqual,
        false,
        &index_state->iss_ScanKeys,
        &index_state->iss_NumScanKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexorderby,
        true,
        &index_state->iss_OrderByKeys,
        &index_state->iss_NumOrderByKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (index_state->iss_NumRuntimeKeys != 0) {
        ExprContext* stdecontext = index_state->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &index_state->ss.ps);
        index_state->iss_RuntimeContext = index_state->ss.ps.ps_ExprContext;
        index_state->ss.ps.ps_ExprContext = stdecontext;
    } else {
        index_state->iss_RuntimeContext = NULL;
    }

    /* deal with partition info */
    ExecInitAnnIndexRelation(index_state, estate, eflags);

    /* Only DiskANN heap scans use this reorder protocol. HNSW, index-only,
     * bucket and Ustore execution retain their own access contracts. */
    if (index_state->iss_RelationDesc->rd_rel->relam == DISKANN_AM_OID &&
        !node->ann_filter_enabled && !node->index_only_scan && !node->is_ustore &&
        !RELATION_HAS_BUCKET(current_relation) && list_length(node->indexorderbyorig) == 1 &&
        exprType((Node*)linitial(node->indexorderbyorig)) == FLOAT8OID) {
        index_state->iss_DiskannOrdered = true;
        index_state->iss_OrderedQual = index_state->ss.ps.qual;
        index_state->iss_OrderedHasSRF = expression_returns_set((Node*)node->scan.plan.targetlist);
        index_state->iss_OrderedExpr = ExecInitExprByRecursion(
            (Expr*)linitial(node->indexorderbyorig), (PlanState*)index_state);
        TupleDesc desc = CreateTemplateTupleDesc(2, false);
        TupleDescInitEntry(desc, 1, "distance", FLOAT8OID, -1, 0);
        TupleDescInitEntry(desc, 2, "heap_tuple", BYTEAOID, -1, 0);
        index_state->iss_OrderedSlot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(index_state->iss_OrderedSlot, desc);
    }

    /* Request the dedicated AM channel only when this execution's GUC still
     * satisfies the quality encoded by the post-setrefs reference. */
    AnnOrderByRef* orderByRef = AnnIndexFindOrderByRef(node);
    if (orderByRef != NULL && index_state->iss_ScanDesc != NULL) {
        AnnOrderByDistanceQuality quality = (AnnOrderByDistanceQuality)orderByRef->quality;
        IndexScanDesc scanDesc = GetIndexScanDesc(index_state->iss_ScanDesc);
        AnnOrderByDistanceQuality runtimeQuality = AnnIndexRuntimeDistanceQuality(scanDesc);
        bool runtimeShapeSupported = !node->is_ustore &&
            !node->scan.isPartTbl && node->scan.plan.dop <= 1 &&
            !ScanDirectionIsBackward(node->indexorderdir) &&
            index_state->ss.ss_currentRelation != NULL &&
            !RELATION_HAS_BUCKET(index_state->ss.ss_currentRelation);

        if (runtimeShapeSupported && runtimeQuality == quality &&
            AnnIndexReuseModeAllows(u_sess->attr.attr_sql.ann_orderby_distance_reuse, quality)) {
            scanDesc->xs_ann_orderby.requested = true;
            scanDesc->xs_ann_orderby.requested_mode =
                u_sess->attr.attr_sql.ann_orderby_distance_reuse;
            index_state->iss_AnnOrderByExpectedQuality = quality;
        }
    }

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if (index_state->iss_ScanDesc == NULL) {
        index_state->ss.ps.stubType = PST_Scan;
    } else if (index_state->iss_NumRuntimeKeys == 0) {
        scan_handler_idx_rescan_local(index_state->iss_ScanDesc,
            index_state->iss_ScanKeys,
            index_state->iss_NumScanKeys,
            index_state->iss_OrderByKeys,
            index_state->iss_NumOrderByKeys);
    }

    /*
     * all done.
     */
    gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
    return index_state;
}

/*
 * @@GaussDB@@
 * Target	: data partition
 * Brief	: construt a dummy relation with the next partition and the partitiobed
 *			: table for the following AnnIndexscan, and swith the scaning relation to
 *			: the dummy relation
 * Description	: input a AnnIndexScanState node, to construct a dummy relation
 *              : with the next partition.
 * Input		: AnnIndexScanState *node
 * Output	    : void
 * Notes		: NULL
 */
static void ExecInitNextPartitionForAnnIndexScan(AnnIndexScanState* node)
{
    Partition current_partition = NULL;
    Relation current_partition_rel = NULL;
    Partition current_index_partition = NULL;
    Relation current_index_partition_rel = NULL;
    AnnIndexScan* plan = NULL;
    int paramNo = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* SubPrtParam = NULL;

    AnnIndexScanState* indexState = node;
    AnnIndexScan *indexScan = (AnnIndexScan *)node->ss.ps.plan;
    Snapshot scanSnap = TvChooseScanSnap(indexState->iss_RelationDesc, &indexScan->scan, &indexState->ss);

    plan = (AnnIndexScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    paramNo = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[paramNo]);
    node->ss.currentSlot = (int)param->value;

    subPartParamno = plan->scan.plan.subparamno;
    SubPrtParam = &(node->ss.ps.state->es_param_exec_vals[subPartParamno]);

    Oid heapOid = node->iss_RelationDesc->rd_index->indrelid;
    Relation heapRelation = heap_open(heapOid, AccessShareLock);

    /* no heap scan here */
    node->ss.ss_currentScanDesc = NULL;

    /* construct a dummy relation with the next index partition */
    if (RelationIsSubPartitioned(heapRelation)) {
        Partition subOnePart = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
        List *subPartList = (List *)list_nth(node->ss.subpartitions, node->ss.currentSlot);
        List *subIndexList = (List *)list_nth(node->iss_IndexPartitionList,
                                              node->ss.currentSlot);
        current_partition = (Partition)list_nth(subPartList, (int)SubPrtParam->value);

        Relation subOnePartRel = partitionGetRelation(node->ss.ss_currentRelation, subOnePart);

        current_partition_rel = partitionGetRelation(subOnePartRel, current_partition);
        current_index_partition = (Partition)list_nth(subIndexList, (int)SubPrtParam->value);
        releaseDummyRelation(&subOnePartRel);
    } else {
        current_partition = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
        current_partition_rel = partitionGetRelation(node->ss.ss_currentRelation, current_partition);
        current_index_partition = (Partition)list_nth(node->iss_IndexPartitionList, node->ss.currentSlot);
    }

    current_index_partition_rel = partitionGetRelation(node->iss_RelationDesc, current_index_partition);

    Assert(PointerIsValid(node->iss_CurrentIndexPartition));
    releaseDummyRelation(&(node->iss_CurrentIndexPartition));
    node->iss_CurrentIndexPartition = current_index_partition_rel;

    /* update scan-related partition */
    releaseDummyRelation(&(node->ss.ss_currentPartition));
    node->ss.ss_currentPartition = current_partition_rel;

    /* Initialize scan descriptor. */
    node->iss_ScanDesc = scan_handler_idx_beginscan(node->ss.ss_currentPartition,
        node->iss_CurrentIndexPartition,
        scanSnap,
        node->iss_NumScanKeys,
        node->iss_NumOrderByKeys,
        (ScanState*)node);

    if (node->iss_ScanDesc != NULL) {
        scan_handler_idx_rescan_local(
            node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys,
            node->iss_OrderByKeys, node->iss_NumOrderByKeys);
    }

    heap_close(heapRelation, AccessShareLock);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get index partitions list and table partitions list for the
 *			    : the following AnnIndexScan
 * Description	: Init partitions list in AnnIndexScanState.
 * Input		: AnnIndexScanState* index_state, EState* estate
 * Output	    : void
 * Notes		: NULL
 */
void ExecInitPartitionForAnnIndexScan(AnnIndexScanState* index_state, EState* estate)
{
    AnnIndexScan* plan = NULL;
    Relation current_relation = NULL;
    Partition table_partition = NULL;
    Partition index_partition = NULL;

    index_state->ss.partitions = NIL;
    index_state->ss.ss_currentPartition = NULL;
    index_state->iss_IndexPartitionList = NIL;
    index_state->iss_CurrentIndexPartition = NULL;

    plan = (AnnIndexScan*)index_state->ss.ps.plan;
    current_relation = index_state->ss.ss_currentRelation;

    if (plan->scan.itrs > 0) {
        Oid indexid = plan->indexid;
        bool relisTarget = false;
        Partition indexpartition = NULL;
        LOCKMODE lock;

        /*
         * get relation's lockmode that hangs on whether
         * it's one of the target relations of the query
         */
        relisTarget = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relisTarget ? RowExclusiveLock : AccessShareLock);
        index_state->ss.lockMode = lock;
        index_state->lockMode = lock;
        
        PruningResult* resultPlan = NULL;
        
        if (plan->scan.pruningInfo->expr != NULL) {
            resultPlan = GetPartitionInfo(plan->scan.pruningInfo, estate, current_relation);
        } else {
            resultPlan = plan->scan.pruningInfo;
        }
        
        if (resultPlan->ls_rangeSelectedPartitions != NULL) {
            index_state->ss.part_id = resultPlan->ls_rangeSelectedPartitions->length;
        } else {
            index_state->ss.part_id = 0;
        }

        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
        List* partitionnos = resultPlan->ls_selectedPartitionnos;
        Assert(list_length(part_seqs) == list_length(partitionnos));
        StringInfo partNameInfo = makeStringInfo();
        StringInfo partOidInfo = makeStringInfo();

        forboth (cell1, part_seqs, cell2, partitionnos) {
            Oid tablepartitionid = InvalidOid;
            Oid indexpartitionid = InvalidOid;
            List* partitionIndexOidList = NIL;
            int partSeq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);

            /* get table partition and add it to a list for following scan */
            tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq, partitionno);
            table_partition =
                PartitionOpenWithPartitionno(current_relation, tablepartitionid, partitionno, lock, true);
            /* Skip concurrent dropped partitions */
            if (table_partition == NULL) {
                continue;
            }
            index_state->ss.partitions = lappend(index_state->ss.partitions, table_partition);

            appendStringInfo(partNameInfo, "%s ", table_partition->pd_part->relname.data);
            appendStringInfo(partOidInfo, "%u ", tablepartitionid);

            if (RelationIsSubPartitioned(current_relation)) {
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                SubPartitionPruningResult* subPartPruningResult =
                    GetSubPartitionPruningResult(resultPlan->ls_selectedSubPartitions, partSeq, partitionno);
                if (subPartPruningResult == NULL) {
                    continue;
                }
                List *subpartList = subPartPruningResult->ls_selectedSubPartitions;
                List *subpartitionnos = subPartPruningResult->ls_selectedSubPartitionnos;
                Assert(list_length(subpartList) == list_length(subpartitionnos));
                List *subIndexList = NULL;
                List *subRelationList = NULL;

                forboth (lc1, subpartList, lc2, subpartitionnos)
                {
                    int subpartSeq = lfirst_int(lc1);
                    int subpartitionno = lfirst_int(lc2);
                    Relation tablepartrel = partitionGetRelation(current_relation, table_partition);
                    Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subpartSeq, subpartitionno);
                    Partition subpart = PartitionOpenWithPartitionno(tablepartrel, subpartitionid, subpartitionno,
                                                                     AccessShareLock, true);
                    /* Skip concurrent dropped partitions */
                    if (subpart == NULL) {
                        continue;
                    }

                    partitionIndexOidList = PartitionGetPartIndexList(subpart);

                    Assert(partitionIndexOidList != NULL);
                    if (!PointerIsValid(partitionIndexOidList)) {
                        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                        errmsg("no local indexes found for partition %s",
                                               PartitionGetPartitionName(subpart))));
                    }

                    indexpartitionid = searchPartitionIndexOid(indexid, partitionIndexOidList);
                    list_free_ext(partitionIndexOidList);
                    indexpartition = partitionOpen(index_state->iss_RelationDesc, indexpartitionid, AccessShareLock);

                    releaseDummyRelation(&tablepartrel);

                    if (indexpartition->pd_part->indisusable == false) {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED), errmodule(MOD_EXECUTOR),
                             errmsg(
                                 "can't initialize bitmap index scans using unusable local index \"%s\" for partition",
                                 PartitionGetPartitionName(indexpartition))));
                    }

                    subIndexList = lappend(subIndexList, indexpartition);
                    subRelationList = lappend(subRelationList, subpart);
                }

                index_state->iss_IndexPartitionList = lappend(index_state->iss_IndexPartitionList,
                                                              subIndexList);
                index_state->ss.subpartitions = lappend(index_state->ss.subpartitions, subRelationList);
                index_state->ss.subPartLengthList =
                    lappend_int(index_state->ss.subPartLengthList, list_length(subIndexList));
            } else {
                /* get index partition and add it to a list for following scan */
                partitionIndexOidList = PartitionGetPartIndexList(table_partition);
                Assert(PointerIsValid(partitionIndexOidList));
                if (!PointerIsValid(partitionIndexOidList)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("no local indexes found for partition %s",
                                                                        PartitionGetPartitionName(table_partition))));
                }
                indexpartitionid = searchPartitionIndexOid(indexid, partitionIndexOidList);
                list_free_ext(partitionIndexOidList);

                index_partition = partitionOpen(index_state->iss_RelationDesc, indexpartitionid, lock);
                if (index_partition->pd_part->indisusable == false) {
                    ereport(ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED),
                             errmsg("can't initialize index scans using unusable local index \"%s\"",
                                    PartitionGetPartitionName(index_partition))));
                }
                index_state->iss_IndexPartitionList = lappend(index_state->iss_IndexPartitionList, index_partition);
            }
        }
        /*
         * Set the total scaned num of partition from level 1 partition, subpartition
         * list is drilled down into node->subpartitions for each node_partition entry;
         *
         * Note: we do not set is value from select partittins from pruning-result as some of
         *       pre-pruned partitions could be dropped from conecurrent DDL, node->partitions
         *       is refreshed partition list to be scanned;
         */
        if (index_state->ss.partitions != NULL) {
            index_state->ss.part_id = list_length(index_state->ss.partitions);
        } else {
            index_state->ss.part_id = 0;
        }
    }
}
