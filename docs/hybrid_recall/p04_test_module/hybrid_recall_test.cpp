/*
 * hybrid_recall_test.cpp - P0-4 verification vehicle (test only, never shipped)
 *
 * Loads into a debug backend via LOAD and exposes
 *   hybrid_recall_debug_execute(text_branch_sql, vector_branch_sql,
 *       id_resno, text_score_resno, vector_distance_resno,
 *       final_topk, text_candidates, vector_candidates,
 *       rrf_k, text_weight, vector_weight)
 * which plans each branch SQL with the real planner, wraps them under a
 * hand-built HybridRecall parent and runs the executor, returning the
 * fused (doc_id, source_mask, text_rank, vector_rank, hybrid_score) rows.
 *
 * Constraints (asserted): each branch SQL is a single-table SELECT over
 * the same relation - V1's same-base-table rule (design doc D-006) means
 * both branch plans' varno 1 legitimately share one parent range table.
 *
 * This module exists because the executor lands before the planner
 * (P2); the handwritten S0 golden SQL defines the semantics to match.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "commands/explain.h"

#include "executor/exec/execdesc.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/stream_cost.h"
#include "optimizer/streamplan.h"
#include "nodes/plannodes.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

typedef struct CaptureState {
    DestReceiver pub;
    Tuplestorestate* store;
    uint64 rows;
} CaptureState;

static void capture_receiveSlot(TupleTableSlot* slot, DestReceiver* self)
{
    CaptureState* cap = (CaptureState*)self;

    tuplestore_puttupleslot(cap->store, slot);
    cap->rows++;
}

static void capture_noop(DestReceiver* self)
{
}

static void capture_startup(DestReceiver* self, int operation, TupleDesc typeinfo)
{
}

static Plan* plan_branch_sql(const char* sql, const char* branch_name)
{
    List* raw_list = NIL;
    Node* parsetree = NULL;
    Query* query = NULL;
    PlannedStmt* planned = NULL;

    raw_list = raw_parser(sql, NULL);
    if (list_length(raw_list) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("%s branch SQL must be exactly one statement", branch_name)));

    parsetree = (Node*)linitial(raw_list);
    if (!IsA(parsetree, SelectStmt))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("%s branch SQL must be a SELECT", branch_name)));

    query = parse_analyze(parsetree, sql, NULL, 0);
    /* exactly one RTE_RELATION required; CTE entries are fine alongside
     * (e.g. WITH d AS (SELECT pg_sleep(n)) used as a one-shot branch delay
     * for overlap wall-clock tests - the CTE materializes once inside the
     * stream worker). */
    int rel_count = 0;
    ListCell* lc = NULL;
    foreach (lc, query->rtable) {
        if (((RangeTblEntry*)lfirst(lc))->rtekind == RTE_RELATION)
            rel_count++;
    }
    if (query->commandType != CMD_SELECT || rel_count != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("%s branch SQL must reference exactly one base relation", branch_name)));

    planned = pg_plan_query(query, 0, NULL);
    return planned->planTree;
}

static Expr* make_int8_const(int64 value)
{
    return (Expr*)makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(value), false, true);
}

static List* passthrough_tlist(void)
{
    List* tlist = NIL;
    Var* id_var = makeVar(1, 1, INT4OID, -1, InvalidOid, 0);
    Var* score_var = makeVar(1, 2, FLOAT8OID, -1, InvalidOid, 0);
    tlist = lappend(tlist, makeTargetEntry((Expr*)id_var, 1, NULL, false));
    tlist = lappend(tlist, makeTargetEntry((Expr*)score_var, 2, NULL, false));
    return tlist;
}

/*
 * [P1-3] Wrap a branch plan in a 1:1 local task Stream, mirroring the
 * recursive LOCAL GATHER construction (createplan.cpp make_recursive_local
 * precedent): STREAM_REDISTRIBUTE + LOCAL_ROUNDROBIN + dop 1/1. The producer
 * executes copyObject(stream->scan.plan.lefttree) in its own stream worker;
 * consumer_nodes come from the branch plan's exec_nodes.
 */
static Plan* make_task_stream(Plan* branch, int branch_id)
{
    Stream* stream = makeNode(Stream);
    stream->type = STREAM_REDISTRIBUTE;
    stream->is_sorted = false;
    stream->sort = NULL;
    stream->smpDesc.consumerDop = 1;
    stream->smpDesc.producerDop = 1;
    stream->smpDesc.distriType = LOCAL_ROUNDROBIN;
    stream->origin_consumer_nodes = NULL;
    stream->distribute_keys = NIL;
    stream->consumer_nodes = (ExecNodes*)copyObject(branch->exec_nodes);
    stream->is_task_parallel = true;
    stream->task_group_id = 1;
    stream->task_branch_id = branch_id;

    Plan* plan = &stream->scan.plan;
    plan->dop = 1;
    plan->targetlist = passthrough_tlist();
    plan->lefttree = branch;
    plan->righttree = NULL;
    inherit_plan_locator_info(plan, branch);
    copy_plan_costsize(plan, branch);
    return plan;
}

/* assign unique plan_node_ids over the whole handmade tree (StreamKey
 * uniqueness, design doc 7.8) */
static int renumber_plan_node_ids(Plan* plan, int next_id)
{
    if (plan == NULL)
        return next_id;
    plan->plan_node_id = next_id++;
    next_id = renumber_plan_node_ids(plan->lefttree, next_id);
    next_id = renumber_plan_node_ids(plan->righttree, next_id);
    return next_id;
}

static PlannedStmt* build_serial_hybrid_stmt(const char* text_sql,
                                             const char* vector_sql,
                                             int32 id_resno,
                                             int32 text_resno,
                                             int32 vector_resno,
                                             int64 final_topk,
                                             int64 text_candidates,
                                             int64 vector_candidates,
                                             float8 rrf_k,
                                             float8 text_weight,
                                             float8 vector_weight)
{
    Plan* text_plan = plan_branch_sql(text_sql, "text");
    Plan* vector_plan = plan_branch_sql(vector_sql, "vector");

    /* Attach the branch Limit plans DIRECTLY: Limit is not a scan node,
     * so tlist_matches_tupdesc never asserts on its setrefs varnos, and
     * the SubqueryScan wrapper layer (init quirks) is eliminated.
     * Both subplans reference the relation as varno 1; the parent range
     * table below carries that same relation once. */
    HybridRecall* hybrid = makeNode(HybridRecall);
    hybrid->plan.lefttree = text_plan;
    hybrid->plan.righttree = vector_plan;
    hybrid->plan.targetlist = NIL;
    hybrid->plan.qual = NIL;
    hybrid->id_resno = (AttrNumber)id_resno;
    hybrid->text_score_resno = (AttrNumber)text_resno;
    hybrid->vector_distance_resno = (AttrNumber)vector_resno;
    hybrid->final_topk = make_int8_const(final_topk);
    hybrid->text_candidates = make_int8_const(text_candidates);
    hybrid->vector_candidates = make_int8_const(vector_candidates);
    hybrid->fusion = HYBRID_FUSION_RRF;
    hybrid->rrf_k = rrf_k;
    hybrid->text_weight = text_weight;
    hybrid->vector_weight = vector_weight;
    hybrid->parallel_branches = false;
    hybrid->require_parallel = false;
    hybrid->recheck_quals = NIL;

    PlannedStmt* stmt = makeNode(PlannedStmt);
    stmt->commandType = CMD_SELECT;
    stmt->canSetTag = true;
    (void)renumber_plan_node_ids((Plan*)hybrid, 1);
    stmt->planTree = (Plan*)hybrid;
    stmt->num_streams = 0;

    /* one relation RTE: the single shared base table, whose Var numbering
     * both sub-plans use */
    List* raw_list = raw_parser(text_sql, NULL);
    Node* parsetree = (Node*)linitial(raw_list);
    Query* query = parse_analyze(parsetree, text_sql, NULL, 0);
    stmt->rtable = query->rtable;

    return stmt;
}

/* fmgr.h 的宏只对 pg_finfo_* 自动加 extern "C"；SQL 可调用主符号需要
 * 单独的 C 链接声明（与 contrib/pg_stat_statements.cpp 的写法一致）。 */
extern "C" Datum hybrid_recall_debug_execute(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hybrid_recall_debug_execute);

Datum hybrid_recall_debug_execute(PG_FUNCTION_ARGS)
{
    char* text_sql = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* vector_sql = text_to_cstring(PG_GETARG_TEXT_P(1));
    int32 arg_id_resno = PG_GETARG_INT32(2);
    int32 arg_text_resno = PG_GETARG_INT32(3);
    int32 arg_vector_resno = PG_GETARG_INT32(4);
    int64 final_topk = PG_GETARG_INT64(5);
    int64 text_candidates = PG_GETARG_INT64(6);
    int64 vector_candidates = PG_GETARG_INT64(7);
    float8 rrf_k = PG_GETARG_FLOAT8(8);
    float8 text_weight = PG_GETARG_FLOAT8(9);
    float8 vector_weight = PG_GETARG_FLOAT8(10);

    FuncCallContext* funcctx = NULL;
    MemoryContext old_cxt = NULL;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        old_cxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        PlannedStmt* stmt = build_serial_hybrid_stmt(text_sql,
                                                     vector_sql,
                                                     arg_id_resno,
                                                     arg_text_resno,
                                                     arg_vector_resno,
                                                     final_topk,
                                                     text_candidates,
                                                     vector_candidates,
                                                     rrf_k,
                                                     text_weight,
                                                     vector_weight);

        CaptureState* cap = (CaptureState*)palloc0(sizeof(CaptureState));
        cap->pub.receiveSlot = capture_receiveSlot;
        cap->pub.rStartup = capture_startup;
        cap->pub.rShutdown = capture_noop;
        cap->pub.rDestroy = capture_noop;
        cap->store = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);

        QueryDesc* qd = CreateQueryDesc(stmt,
                                        "hybrid_recall_debug",
                                        GetActiveSnapshot(),
                                        InvalidSnapshot,
                                        &cap->pub,
                                        NULL,
                                        0);
        ExecutorStart(qd, 0);
    if (qd->tupDesc != NULL) {
            funcctx->tuple_desc = CreateTupleDescCopy(qd->tupDesc);
            /* hand-built descs carry no registered composite type until
             * blessed; ExecFetchSlotTupleDatum requires one */
            BlessTupleDesc(funcctx->tuple_desc);
        }
        ExecutorRun(qd, ForwardScanDirection, 0L);
        ExecutorFinish(qd);
        ExecutorEnd(qd);
        FreeQueryDesc(qd);

        funcctx->user_fctx = cap->store;

        (void)MemoryContextSwitchTo(old_cxt);
    }

    funcctx = SRF_PERCALL_SETUP();
    Tuplestorestate* store = (Tuplestorestate*)funcctx->user_fctx;

    {
        TupleTableSlot* slot = funcctx->slot;
        if (slot == NULL) {
            slot = MakeSingleTupleTableSlot(funcctx->tuple_desc);
            funcctx->slot = slot;
        }
        /* natural termination: the store runs dry */
        if (tuplestore_gettupleslot(store, true, false, slot))
            SRF_RETURN_NEXT(funcctx, ExecFetchSlotTupleDatum(slot));
    }

    SRF_RETURN_DONE(funcctx);
}

extern "C" Datum hybrid_recall_debug_rescan(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(hybrid_recall_debug_rescan);

/* P3-5 test-only entry point: execute one serial HybridRecall QueryDesc,
 * rewind it in place and execute again.  This reaches
 * ExecutorRewind -> ExecReScanHybridRecall without cursor materialization. */
Datum hybrid_recall_debug_rescan(PG_FUNCTION_ARGS)
{
    char* text_sql = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* vector_sql = text_to_cstring(PG_GETARG_TEXT_P(1));
    int32 loops = PG_GETARG_INT32(11);

    if (loops < 1 || loops > 10000)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("HybridRecall rescan loops must be between 1 and 10000, got %d", loops)));

    PlannedStmt* stmt = build_serial_hybrid_stmt(text_sql,
                                                 vector_sql,
                                                 PG_GETARG_INT32(2),
                                                 PG_GETARG_INT32(3),
                                                 PG_GETARG_INT32(4),
                                                 PG_GETARG_INT64(5),
                                                 PG_GETARG_INT64(6),
                                                 PG_GETARG_INT64(7),
                                                 PG_GETARG_FLOAT8(8),
                                                 PG_GETARG_FLOAT8(9),
                                                 PG_GETARG_FLOAT8(10));

    CaptureState* cap = (CaptureState*)palloc0(sizeof(CaptureState));
    cap->pub.receiveSlot = capture_receiveSlot;
    cap->pub.rStartup = capture_startup;
    cap->pub.rShutdown = capture_noop;
    cap->pub.rDestroy = capture_noop;
    cap->store = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);

    QueryDesc* qd = CreateQueryDesc(stmt,
                                    "hybrid_recall_rescan",
                                    GetActiveSnapshot(),
                                    InvalidSnapshot,
                                    &cap->pub,
                                    NULL,
                                    0);
    ExecutorStart(qd, 0);
    for (int32 i = 0; i < loops; i++) {
        if (i > 0)
            ExecutorRewind(qd);
        ExecutorRun(qd, ForwardScanDirection, 0L);
    }
    ExecutorFinish(qd);
    ExecutorEnd(qd);
    FreeQueryDesc(qd);

    PG_RETURN_INT64((int64)cap->rows);
}


extern "C" Datum hybrid_recall_debug_plan(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(hybrid_recall_debug_plan);

/*
 * [P1-3] EXPLAIN-only mode: build the same parallel HybridRecall plan but
 * never execute it - prints nodeToString(planTree). Decouples plan-shape
 * verification from the stream-startup debugging (which hangs in
 * ExecutorStart and is tracked separately).
 */
Datum hybrid_recall_debug_plan(PG_FUNCTION_ARGS)
{
    char* text_sql = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* vector_sql = text_to_cstring(PG_GETARG_TEXT_P(1));

    Plan* text_plan = plan_branch_sql(text_sql, "text");
    Plan* vector_plan = plan_branch_sql(vector_sql, "vector");
    Plan* text_stream = make_task_stream(text_plan, 0);
    Plan* vector_stream = make_task_stream(vector_plan, 1);

    HybridRecall* hybrid = makeNode(HybridRecall);
    hybrid->plan.lefttree = text_stream;
    hybrid->plan.righttree = vector_stream;
    hybrid->plan.targetlist = NIL;
    hybrid->plan.qual = NIL;
    hybrid->id_resno = 1;
    hybrid->text_score_resno = 2;
    hybrid->vector_distance_resno = 2;
    hybrid->final_topk = make_int8_const(PG_GETARG_INT64(5));
    hybrid->text_candidates = make_int8_const(PG_GETARG_INT64(6));
    hybrid->vector_candidates = make_int8_const(PG_GETARG_INT64(7));
    hybrid->fusion = HYBRID_FUSION_RRF;
    hybrid->rrf_k = PG_GETARG_FLOAT8(8);
    hybrid->text_weight = PG_GETARG_FLOAT8(9);
    hybrid->vector_weight = PG_GETARG_FLOAT8(10);
    hybrid->parallel_branches = true;
    hybrid->require_parallel = false;
    hybrid->recheck_quals = NIL;

    PlannedStmt* stmt = makeNode(PlannedStmt);
    stmt->commandType = CMD_SELECT;
    stmt->canSetTag = true;
    (void)renumber_plan_node_ids((Plan*)hybrid, 1);
    stmt->planTree = (Plan*)hybrid;
    stmt->num_streams = 2;
    {
        List* raw_list = raw_parser(text_sql, NULL);
        Node* parsetree = (Node*)linitial(raw_list);
        Query* query = parse_analyze(parsetree, text_sql, NULL, 0);
        stmt->rtable = query->rtable;
    }

    PG_RETURN_TEXT_P(cstring_to_text(nodeToString(stmt->planTree)));
}


extern "C" Datum hybrid_recall_debug_explain(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(hybrid_recall_debug_explain);

/*
 * [P1-3] EXPLAIN ANALYZE of the REAL executed serial plan (the proven P0
 * path: branch Limit plans attached directly, no streams). Runs through
 * openGauss's own explain machinery so the output is the genuine format
 * with actual rows/timings.
 */
Datum hybrid_recall_debug_explain(PG_FUNCTION_ARGS)
{
    char* text_sql = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* vector_sql = text_to_cstring(PG_GETARG_TEXT_P(1));

    Plan* text_plan = plan_branch_sql(text_sql, "text");
    Plan* vector_plan = plan_branch_sql(vector_sql, "vector");
    Plan* text_stream = make_task_stream(text_plan, 0);
    Plan* vector_stream = make_task_stream(vector_plan, 1);

    HybridRecall* hybrid = makeNode(HybridRecall);
    hybrid->plan.lefttree = text_stream;
    hybrid->plan.righttree = vector_stream;
    hybrid->plan.targetlist = NIL;
    hybrid->plan.qual = NIL;
    hybrid->id_resno = 1;
    hybrid->text_score_resno = 2;
    hybrid->vector_distance_resno = 2;
    hybrid->final_topk = make_int8_const(PG_GETARG_INT64(5));
    hybrid->text_candidates = make_int8_const(PG_GETARG_INT64(6));
    hybrid->vector_candidates = make_int8_const(PG_GETARG_INT64(7));
    hybrid->fusion = HYBRID_FUSION_RRF;
    hybrid->rrf_k = PG_GETARG_FLOAT8(8);
    hybrid->text_weight = PG_GETARG_FLOAT8(9);
    hybrid->vector_weight = PG_GETARG_FLOAT8(10);
    hybrid->parallel_branches = true;
    hybrid->require_parallel = false;
    hybrid->recheck_quals = NIL;
    hybrid->plan.exec_nodes = (ExecNodes*)copyObject(text_plan->exec_nodes);

    PlannedStmt* stmt = makeNode(PlannedStmt);
    stmt->commandType = CMD_SELECT;
    stmt->canSetTag = true;
    (void)renumber_plan_node_ids((Plan*)hybrid, 1);
    stmt->planTree = (Plan*)hybrid;
    stmt->num_streams = 2;
    {
        List* raw_list = raw_parser(text_sql, NULL);
        Node* parsetree = (Node*)linitial(raw_list);
        Query* query = parse_analyze(parsetree, text_sql, NULL, 0);
        stmt->rtable = query->rtable;
    }

    ExplainState* es = (ExplainState*)palloc0(sizeof(ExplainState));
    es->str = makeStringInfo();
    es->analyze = false;
    es->timing = false;
    es->costs = false;
    es->format = EXPLAIN_FORMAT_TEXT;
    DestReceiver* none_dest = None_Receiver;
    ExplainOnePlan(stmt, NULL, es, "hybrid_recall_explain", none_dest, NULL);
    PG_RETURN_TEXT_P(cstring_to_text(es->str->data));
}


extern "C" int g_hybrid_test_producer_delay_ms;

extern "C" Datum hybrid_recall_debug_parallel(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(hybrid_recall_debug_parallel);

/* Parallel variant: branches wrapped in 1:1 task streams.  The optional
 * producer delay is used by P3-5 to make statement-timeout cleanup
 * deterministic while both workers are active. */
Datum hybrid_recall_debug_parallel(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    MemoryContext old_cxt = NULL;

    if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    old_cxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    char* text_sql = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* vector_sql = text_to_cstring(PG_GETARG_TEXT_P(1));
    Plan* text_plan = plan_branch_sql(text_sql, "text");
    Plan* vector_plan = plan_branch_sql(vector_sql, "vector");
    Plan* text_stream = make_task_stream(text_plan, 0);
    Plan* vector_stream = make_task_stream(vector_plan, 1);
    HybridRecall* hybrid = makeNode(HybridRecall);
    hybrid->plan.lefttree = text_stream;
    hybrid->plan.righttree = vector_stream;
    hybrid->id_resno = 1;
    hybrid->text_score_resno = 2;
    hybrid->vector_distance_resno = 2;
    hybrid->final_topk = make_int8_const(PG_GETARG_INT64(5));
    hybrid->text_candidates = make_int8_const(PG_GETARG_INT64(6));
    hybrid->vector_candidates = make_int8_const(PG_GETARG_INT64(7));
    hybrid->fusion = HYBRID_FUSION_RRF;
    hybrid->rrf_k = PG_GETARG_FLOAT8(8);
    hybrid->text_weight = PG_GETARG_FLOAT8(9);
    hybrid->vector_weight = PG_GETARG_FLOAT8(10);
    hybrid->parallel_branches = true;
    hybrid->require_parallel = false;
    hybrid->recheck_quals = NIL;
    /* SetCheckInfo (execStream.h) dereferences planTree->exec_nodes->nodeList
     * during BuildStreamFlow - NULL here crashes the backend inside
     * HOLD_INTERRUPTS and wedges the pool worker (the "hang"). */
    hybrid->plan.exec_nodes = (ExecNodes*)copyObject(text_plan->exec_nodes);
    PlannedStmt* stmt = makeNode(PlannedStmt);
    stmt->commandType = CMD_SELECT;
    stmt->canSetTag = true;
    (void)renumber_plan_node_ids((Plan*)hybrid, 1);
    stmt->planTree = (Plan*)hybrid;
    stmt->num_streams = 2;
    {
        List* raw_list = raw_parser(text_sql, NULL);
        Node* parsetree = (Node*)linitial(raw_list);
        Query* query = parse_analyze(parsetree, text_sql, NULL, 0);
        stmt->rtable = query->rtable;
    }
    CaptureState* cap = (CaptureState*)palloc0(sizeof(CaptureState));
    cap->pub.receiveSlot = capture_receiveSlot;
    cap->pub.rStartup = capture_startup;
    cap->pub.rShutdown = capture_noop;
    cap->pub.rDestroy = capture_noop;
    cap->store = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);
    /* 12th arg semantics: producer delay ms for the overlap wall-clock
     * proof (design doc 12.3); passed through the same-process global. */
    g_hybrid_test_producer_delay_ms = (int)PG_GETARG_INT32(11);
    PG_TRY();
    {
        QueryDesc* qd = CreateQueryDesc(stmt, "hybrid_recall_parallel", GetActiveSnapshot(),
                                        InvalidSnapshot, &cap->pub, NULL, 0);
        ExecutorStart(qd, 0);
        if (qd->tupDesc != NULL) {
            funcctx->tuple_desc = CreateTupleDescCopy(qd->tupDesc);
            BlessTupleDesc(funcctx->tuple_desc);
        }
        ExecutorRun(qd, ForwardScanDirection, 0L);
        ExecutorFinish(qd);
        ExecutorEnd(qd);
        FreeQueryDesc(qd);
    }
    PG_CATCH();
    {
        /* A timeout/cancel must never leave the process-wide test delay set
         * for later unrelated stream queries. Executor resources are owned
         * by the statement ResourceOwner and unwind through the normal error
         * path; this catch only restores the test hook before rethrowing. */
        g_hybrid_test_producer_delay_ms = 0;
        PG_RE_THROW();
    }
    PG_END_TRY();
    g_hybrid_test_producer_delay_ms = 0;

    funcctx->user_fctx = cap->store;
    (void)MemoryContextSwitchTo(old_cxt);
    }

    funcctx = SRF_PERCALL_SETUP();
    Tuplestorestate* store = (Tuplestorestate*)funcctx->user_fctx;
    {
        TupleTableSlot* slot = funcctx->slot;
        if (slot == NULL) {
            slot = MakeSingleTupleTableSlot(funcctx->tuple_desc);
            funcctx->slot = slot;
        }
        if (tuplestore_gettupleslot(store, true, false, slot))
            SRF_RETURN_NEXT(funcctx, ExecFetchSlotTupleDatum(slot));
    }
    SRF_RETURN_DONE(funcctx);
}
