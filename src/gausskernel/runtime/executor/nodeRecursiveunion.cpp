/* -------------------------------------------------------------------------
 *
 * nodeRecursiveunion.cpp
 *	  routines to handle RecursiveUnion nodes.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeRecursiveunion.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeAgg.h"
#include "executor/node/nodeCtescan.h"
#include "executor/node/nodeHashjoin.h"
#include "executor/node/nodeMaterial.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/node/nodeSetOp.h"
#include "executor/node/nodeSort.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/elog.h"

#ifdef USE_ASSERT_CHECKING
#define LOOP_ELOG(elevel, format, ...)                \
    do {                                              \
        if (loop_count >= 20) {                       \
            ereport(elevel, (errmodule(MOD_EXECUTOR), \
                    (errmsg(format, ##__VA_ARGS__)))); \
        }                                             \
    } while (0)
#else
#define LOOP_ELOG(elevel, format, ...)                \
    do {                                              \
        if (loop_count >= 20) {                       \
            ereport(DEBUG1, (errmodule(MOD_EXECUTOR), \
                    (errmsg(format, ##__VA_ARGS__)))); \
        }                                             \
    } while (0)
#endif

#define INSTR (node->ps.instrument)

THR_LOCAL int global_iteration = 0;

static SyncController* create_stream_synccontroller(Stream* stream_node);
static SyncController* create_recursiveunion_synccontroller(RecursiveUnion* ru_node);
static List* getSpecialSubPlanStateNodes(const PlanState* node);
template <bool isNonRecursive>
static void recordRecursiveInfo(RecursiveUnionState* node, int controller_plannodeid);

#ifdef ENABLE_MULTIPLE_NODES
/*
 * MPP with-recursive union support
 */
static void FindSyncUpStream(RecursiveUnionController* controller, PlanState* state, List** initplans);
static void StartNextRecursiveIteration(RecursiveUnionController* controller, int step);
static void ExecInitRecursiveResultTupleSlot(EState* estate, PlanState* planstate);
#endif

static inline bool IsUnderStartWith(RecursiveUnion *ruplan)
{
    return (ruplan->internalEntryList != NIL);
}

/*
 * To implement UNION (without ALL), we need a hashtable that stores tuples
 * already seen.  The hash key is computed from the grouping columns.
 */
typedef struct RUHashEntryData {
    TupleHashEntryData shared; /* common header for hash table entries */
} RUHashEntryData;

/*
 * Initialize the hash table to empty.
 */
static void build_hash_table(RecursiveUnionState* rustate)
{
    RecursiveUnion* node = (RecursiveUnion*)rustate->ps.plan;

    Assert(node->numCols > 0);
    Assert(node->numGroups > 0);

    rustate->hashtable = BuildTupleHashTable(node->numCols,
        node->dupColIdx,
        rustate->eqfunctions,
        rustate->hashfunctions,
        node->numGroups,
        sizeof(RUHashEntryData),
        rustate->tableContext,
        rustate->tempContext,
        u_sess->attr.attr_memory.work_mem);
}

/*
 * @Function: RecursiveUnionWaitCondNegtive() **INLINE**
 *
 * @Brief: wait given value becomes false
 *
 * @Input true_value: watching condition values that expected to FALSE
 * @Input executor_stop: stop waiting condition values
 *
 * @Return: void
 */
static inline void RecursiveUnionWaitCondNegtive(const bool* true_cond, const bool* executor_stop)
{
    Assert(true_cond != NULL && executor_stop != NULL);

    /* return immediately if the watching value already *negtive* */
    if (*true_cond == false) {
        return;
    }

    /* loop-wait the watching value become *negtive* */
    while (*true_cond) {
        if (*executor_stop) {
            u_sess->exec_cxt.executorStopFlag = true;
            break;
        }

        WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
    }

    return;
}

static void markIterationStats(RecursiveUnionState* node, bool isSW)
{
    if (node->ps.instrument == NULL) {
        return;
    }
    if (isSW) {
        markSWLevelEnd(node->swstate, node->swstate->sw_numtuples);
        markSWLevelBegin(node->swstate);
    }
}

/* ----------------------------------------------------------------
 *		ExecRecursiveUnion(node)
 *
 *		Scans the recursive query sequentially and returns the next
 *		qualifying tuple.
 *
 * 1. evaluate non recursive term and assign the result to RT
 *
 * 2. execute recursive terms
 *
 * 2.1 WT := RT
 * 2.2 while WT is not empty repeat 2.3 to 2.6. if WT is empty returns RT
 * 2.3 replace the name of recursive term with WT
 * 2.4 evaluate the recursive term and store into WT
 * 2.5 append WT to RT
 * 2.6 go back to 2.2
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecRecursiveUnion(RecursiveUnionState* node)
{
    PlanState* outer_plan = outerPlanState(node);
    PlanState* inner_plan = innerPlanState(node);
    RecursiveUnion* plan = (RecursiveUnion*)node->ps.plan;
    TupleTableSlot* slot = NULL;
    TupleTableSlot* swSlot = NULL;
    bool is_new = false;
    bool isSW = IsUnderStartWith((RecursiveUnion *)node->ps.plan);

    /* 0. build hash table if it is NULL */
    if (plan->numCols > 0) {
        if (unlikely(node->hashtable == NULL)) {
            build_hash_table(node);
        }
    }

    /* 1. Evaluate non-recursive term */
    if (!node->recursing) {
        for (;;) {
            slot = ExecProcNode(outer_plan);
            if (TupIsNull(slot)) {
                markIterationStats(node, isSW);
                break;
            }
            if (plan->numCols > 0) {
                /* Find or build hashtable entry for this tuple's group */
                LookupTupleHashEntry(node->hashtable, slot, &is_new);
                /* Must reset temp context after each hashtable lookup */
                MemoryContextReset(node->tempContext);
                /* Ignore tuple if already seen */
                if (!is_new) {
                    continue;
                }
            }

            /*
             * For START WITH CONNECT BY, create converted tuple with pseudo columns.
             */
            slot = isSW ? ConvertRuScanOutputSlot(node, slot, false) : slot;
            swSlot = isSW ? GetStartWithSlot(node, slot) : NULL;
            if (isSW && swSlot == NULL) {
                /*
                 * SWCB terminal condition met. Time to stop.
                 * Discarding the last tuple.
                 */
                markSWLevelEnd(node->swstate, node->swstate->sw_numtuples - 1);
                break;
            }

            /* Each non-duplicate tuple goes to the working table ... */
            tuplestore_puttupleslot(node->working_table, slot);

            /* counting tuple produced in current step */
            node->step_tuple_produced++;

            /* ... and to the caller */
            return (isSW ? swSlot : slot);
        }

        /* Mark none-recursive part is down */
        node->recursing = true;

#ifdef ENABLE_MULTIPLE_NODES
        /*
         * With-Recursive sync-up point 1:
         *
         * To Sync-all datanodes that we've done with none-recursive part
         */
        if (NeedSyncUpRecursiveUnionStep(node->ps.plan)) {
            StreamNodeGroup::SyncConsumerNextPlanStep(node->ps.plan->plan_node_id, WITH_RECURSIVE_SYNC_NONERQ);

            recordRecursiveInfo<true>(node, node->ps.plan->plan_node_id);

            /* Kick-Off next step */
            StartNextRecursiveIteration(node->rucontroller, WITH_RECURSIVE_SYNC_NONERQ);
        }
#endif
        node->iteration = 1;

        /* Need reset sw_tuple_idx to 1 when non-recursive term finish */
        node->sw_tuple_idx = 1;
    }

    /* 2. Execute recursive term */
    /* Inner plan of RecursiveUnion need rescan, skip early free. */
    bool orig_early_free = inner_plan->state->es_skip_early_free;
    inner_plan->state->es_skip_early_free = true;

    for (;;) {
        slot = ExecProcNode(inner_plan);
        if (TupIsNull(slot)) {
            /* debug information for SWCBcase */
            if (IsUnderStartWith((RecursiveUnion *)node->ps.plan) &&
                !node->intermediate_empty) {
                ereport(DEBUG1, (errmodule(MOD_EXECUTOR),
                        errmsg("[SWCB DEBUG] current iteration is done: level:%d rownum_current:%d rownum_total:%lu",
                        node->iteration + 1,
                        node->swstate->sw_numtuples,
                        node->swstate->sw_rownum)));
                markSWLevelEnd(node->swstate, node->swstate->sw_numtuples);
                markSWLevelBegin(node->swstate);
            }
#ifdef ENABLE_MULTIPLE_NODES
            /*
             * Check the recursive union is run out of max allowed iterations
             */
            if (IS_PGXC_DATANODE && !IS_SINGLE_NODE && node->iteration > u_sess->attr.attr_sql.max_recursive_times) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("max iteration times %d hit when looping over right plan tree.",
                            u_sess->attr.attr_sql.max_recursive_times)));
            }

            /*
             * With-Recursive sync-up point 2:
             *
             * To Sync-all datanodes that we've done with current recursive iteration.
             */
            if (NeedSyncUpRecursiveUnionStep(node->ps.plan)) {
                StreamNodeGroup::SyncConsumerNextPlanStep(node->ps.plan->plan_node_id, WITH_RECURSIVE_SYNC_RQSTEP);
            }

            recordRecursiveInfo<false>(node, node->ps.plan->plan_node_id);

            /* Done if there's nothing in the intermediate table */
            if (node->intermediate_empty && StreamNodeGroup::IsRecursiveUnionDone(node)) {
                break;
            }
#else
            /* Done if there's nothing in the intermediate table */
            if (node->intermediate_empty) {
                break;
            }
#endif
            /* Need reset sw_tuple_idx to 1 when current iteration finish */
            node->sw_tuple_idx = 1;

            /* done with old working table ... */
            tuplestore_end(node->working_table);

            /* intermediate table becomes working table */
            node->working_table = node->intermediate_table;

#ifdef ENABLE_MULTIPLE_NODES
            /* create new empty intermediate table */
            if (NeedSyncUpRecursiveUnionStep(node->ps.plan)) {
                if (node->shareContext == NULL) {
                    elog(ERROR, "MPP with-recursive in node->shareContext is NULL in distributed mode ");
                }

                MemoryContext old_memctx = MemoryContextSwitchTo(node->shareContext);
                node->intermediate_table = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);
                MemoryContextSwitchTo(old_memctx);

                /* reset the recursive plan tree */
                ExecReScanRecursivePlanTree(inner_plan);
            } else {
                node->intermediate_table = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);

                /* reset the recursive term */
                inner_plan->chgParam = bms_add_member(inner_plan->chgParam, plan->wtParam);
            }

            node->intermediate_empty = true;

            /*
             * @Distributed RecursiveCTE Support
             *
             * Note! At this point the producer on current datanode is still blocked on
             * condition "(recursive_finished == false)" and not run into next round of
             * iteration, we have to put marking it to true after working_table is reset
             * as WorkTableScan is shared across different stream threads
             */
            node->iteration++;
            if (NeedSyncUpRecursiveUnionStep(node->ps.plan)) {
                /* Kick-Off next step */
                StartNextRecursiveIteration(node->rucontroller, WITH_RECURSIVE_SYNC_RQSTEP);
            }
#else
            node->iteration++;
            node->intermediate_table = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);
            /* reset the recursive term */
            inner_plan->chgParam = bms_add_member(inner_plan->chgParam, plan->wtParam);
            node->intermediate_empty = true;
#endif

            /* and continue fetching from recursive term */
            continue;
        }

        if (plan->numCols > 0) {
            /* Find or build hashtable entry for this tuple's group */
            LookupTupleHashEntry(node->hashtable, slot, &is_new);
            /* Must reset temp context after each hashtable lookup */
            MemoryContextReset(node->tempContext);
            /* Ignore tuple if already seen */
            if (!is_new) {
                continue;
            }
        }

        /* Else, tuple is good; stash it in intermediate table ... */
        node->intermediate_empty = false;


        /* For start-with, reason ditto */
        bool isSW = IsUnderStartWith((RecursiveUnion*)node->ps.plan);
        if (isSW) {
            int max_times = u_sess->attr.attr_sql.max_recursive_times;
            StartWithOp *swplan = (StartWithOp *)node->swstate->ps.plan;

            /*
             * Cannot exceed max_recursive_times
             * The reason we also keep iteration check here is
             * avoid order siblings by exist.
             * */
            if (node->iteration > max_times) {
                ereport(ERROR,
                        (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("Current Start With...Connect by has exceeded max iteration times %d", max_times),
                        errhint("Please check your connect by clause carefully")));
            }

            slot = ConvertRuScanOutputSlot(node, slot, true);
            swSlot = GetStartWithSlot(node, slot);
            if (isSW && swSlot == NULL) {
                /*
                 * SWCB terminal condition met. Time to stop.
                 * Discarding the last tuple.
                 */
                markSWLevelEnd(node->swstate, node->swstate->sw_numtuples - 1);
                break;
            }

            /*
             * In ORDER SIBLINGS case, as we add SORT-Operator(material) on top of
             * RecursiveUnion, so we have to do nocycle check here
             */
            if (swplan->swoptions->siblings_orderby_clause) {
                StartWithOpState *swstate = (StartWithOpState *)node->swstate;
                if (swstate->sw_nocycleStopOrderSiblings) {
                    return (TupleTableSlot*)NULL;
                }

                if (CheckCycleExeception(swstate, slot)) {
                    /*
                     * Mark execution stop for order siblings, note we let the cycle-causing
                     * tuple return to upper node and stop next one
                     */
                    swstate->sw_nocycleStopOrderSiblings = true;
                    elog(DEBUG1, "nocycle option take effect on RecursiveUnion for Order Siblings! %s",
                            swstate->sw_curKeyArrayStr);
                }
            }
        }

        tuplestore_puttupleslot(node->intermediate_table, slot);

        /* ... and return it */
        /* it is okay to point slot to swSlot and return now, if necessary */
        slot = isSW ? swSlot : slot;
        inner_plan->state->es_skip_early_free = orig_early_free;

#ifdef ENABLE_MULTIPLE_NODES
        /* counting produced tuple */
        node->step_tuple_produced++;

        return slot;
    }

    inner_plan->state->es_skip_early_free = orig_early_free;

    /*
     * With-Recursive sync-up point 3:
     *
     * To sync-all datanodes that we've done with current recursive union evaluation.
     */
    if (NeedSyncUpRecursiveUnionStep(node->ps.plan)) {
        StreamNodeGroup::SyncConsumerNextPlanStep(node->ps.plan->plan_node_id, WITH_RECURSIVE_SYNC_DONE);

        /* Check final statistics */
        StartNextRecursiveIteration(node->rucontroller, WITH_RECURSIVE_SYNC_DONE);
    }
#else
        return slot;
    }
#endif

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitRecursiveUnionScan
 * ----------------------------------------------------------------
 */
RecursiveUnionState* ExecInitRecursiveUnion(RecursiveUnion* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

#ifdef ENABLE_MULTIPLE_NODES
    bool need_sync_controller = NeedSetupSyncUpController((Plan*)node);

    /*
     * For distributed recursive processing, we should have RecursiveUnionState as
     * well as its underlying working_table intermidiate_table are allocated in a
     * shared memory context
     */
    MemoryContext current_memctx = NULL;
    MemoryContext recursive_runtime_memctx = NULL;
    if (need_sync_controller) {
        MemoryContext stream_runtime_memctx = u_sess->stream_cxt.global_obj->m_streamRuntimeContext;
        Assert(stream_runtime_memctx != NULL);
        recursive_runtime_memctx = AllocSetContextCreate(stream_runtime_memctx,
            "RecursiveRuntimeContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);

        current_memctx = MemoryContextSwitchTo(recursive_runtime_memctx);
    }
#endif

    /*
     * create state structure
     */
    RecursiveUnionState* rustate = makeNode(RecursiveUnionState);
    rustate->ps.plan = (Plan*)node;
    rustate->ps.state = estate;

    rustate->eqfunctions = NULL;
    rustate->hashfunctions = NULL;
    rustate->hashtable = NULL;
    rustate->tempContext = NULL;
    rustate->tableContext = NULL;

    /* initialize processing state */
    rustate->recursing = false;
    rustate->intermediate_empty = true;
    rustate->working_table = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);
    rustate->intermediate_table = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);
    rustate->rucontroller = NULL;
    rustate->shareContext = NULL;
    rustate->step_tuple_produced = 0;

    /*
     * If hashing, we need a per-tuple memory context for comparisons, and a
     * longer-lived context to store the hash table.  The table can't just be
     * kept in the per-query context because we want to be able to throw it
     * away when rescanning.
     */
    if (node->numCols > 0) {
#ifdef ENABLE_MULTIPLE_NODES
        /* it can't be hashing when we have to do step-syncup across the whole cluster */
        if (need_sync_controller) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Unsupported hashing for recursive union")));
        }
#endif
        rustate->tempContext = AllocSetContextCreate(CurrentMemoryContext,
            "RecursiveUnion",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        rustate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
            "RecursiveUnion hash table",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    /*
     * Make the state structure available to descendant WorkTableScan nodes
     * via the Param slot reserved for it.
     */
    ParamExecData* prmdata = &(estate->es_param_exec_vals[node->wtParam]);
    Assert(prmdata->execPlan == NULL);
    prmdata->value = PointerGetDatum(rustate);
    prmdata->isnull = false;

    /*
     * Miscellaneous initialization
     *
     * RecursiveUnion plans don't have expression contexts because they never
     * call ExecQual or ExecProject.
     */
    Assert(node->plan.qual == NIL);

    /*
     * RecursiveUnion nodes still have Result slots, which hold pointers to
     * tuples, so we have to initialize them.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (need_sync_controller) {
        ExecInitRecursiveResultTupleSlot(estate, &rustate->ps);

        /* Bind share context on RecursiveUninoState */
        rustate->shareContext = recursive_runtime_memctx;
    } else {
        ExecInitResultTupleSlot(estate, &rustate->ps);
        rustate->shareContext = NULL;
    }
#else
    ExecInitResultTupleSlot(estate, &rustate->ps);
#endif

    /*
     * Initialize result tuple type and projection info.  (Note: we have to
     * set up the result type before initializing child nodes, because
     * nodeWorktablescan.c expects it to be valid.)
     */
    ExecAssignResultTypeFromTL(&rustate->ps, TAM_HEAP);
    rustate->ps.ps_ProjInfo = NULL;

    /*
     * initialize child nodes
     */
    outerPlanState(rustate) = ExecInitNode(outerPlan(node), estate, eflags);
    innerPlanState(rustate) = ExecInitNode(innerPlan(node), estate, eflags);

    /*
     * If hashing, precompute fmgr lookup data for inner loop, and create the
     * hash table.
     */
    if (node->numCols > 0) {
        execTuplesHashPrepare(node->numCols, node->dupOperators, &rustate->eqfunctions, &rustate->hashfunctions);
    }

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * For RecursiveUnon executed on datanodes(distributed-minor), we need setup
     * recursive "Controller" to for with-recursive execution
     */
    if (need_sync_controller) {
        RecursiveUnionController* controller =
            (RecursiveUnionController*)u_sess->stream_cxt.global_obj->GetSyncController(rustate->ps.plan->plan_node_id);
        if (controller == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("MPP With-Recursive sync controller for RecursiveUnion[%d] is not found",
                        node->plan.plan_node_id)));
        }

        Assert(controller->controller.controller_type == T_RecursiveUnion);
        controller->controller.controller_planstate = (PlanState*)rustate;
        /* create stream operator list belongs to current recursive cte */
        List* initplans = NIL;
        FindSyncUpStream(controller, (PlanState*)rustate, &initplans);
        list_free_ext(initplans);

        if (controller->syncup_streamnode == NULL) {
            elog(ERROR, "SycUP stream node is not found");
        }

        rustate->rucontroller = controller;

        /*
         * Save the THR_LOCAL VFD information into controller,
         * for the WorkTableScanNext get the next tuple from tuplestore when it's state is TSS_WRITEFILE.
         */
        GetFdGlobalVariables((void***)&rustate->rucontroller->recursive_vfd.recursive_VfdCache,
            &rustate->rucontroller->recursive_vfd.recursive_SizeVfdCache);

        /* Swith back to current memory context */
        MemoryContextSwitchTo(current_memctx);
    }
#endif

    if (HAS_INSTR(rustate, true)) {
        errno_t rc =
            memset_s(&((rustate->ps.instrument)->recursiveInfo), sizeof(RecursiveInfo), 0, sizeof(RecursiveInfo));
        securec_check(rc, "\0", "\0");
    }

    /* Init start with related variables */
    rustate->swstate = NULL;
    rustate->sw_tuple_idx = 1;
    rustate->convertContext = AllocSetContextCreate(CurrentMemoryContext,
        "RecursiveUnion Start With",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    return rustate;
}

/* ----------------------------------------------------------------
 *		ExecEndRecursiveUnionScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndRecursiveUnion(RecursiveUnionState* node)
{
    if (node->rucontroller != NULL) {
        /*
         * We delay to the free working_table and intermidiate_table to let TopConsumer
         * to invoke free StreamRuntimeContext to drop them.
         */
        node->rucontroller->controller.executor_stop = true;
    } else {
        /* Release tuplestores */
        tuplestore_end(node->working_table);
        tuplestore_end(node->intermediate_table);
    }

    /* free subsidiary stuff including hashtable */
    if (node->tempContext)
        MemoryContextDelete(node->tempContext);
    if (node->tableContext)
        MemoryContextDelete(node->tableContext);
    if (node->convertContext) {
        MemoryContextDelete(node->convertContext);
    }

    /*
     * clean out the upper tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecReScanRecursiveUnion
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanRecursiveUnion(RecursiveUnionState* node)
{
    PlanState* outer_plan = outerPlanState(node);
    PlanState* inner_plan = innerPlanState(node);
    RecursiveUnion* plan = (RecursiveUnion*)node->ps.plan;

    /*
     * Set recursive term's chgParam to tell it that we'll modify the working
     * table and therefore it has to rescan.
     */
    inner_plan->chgParam = bms_add_member(inner_plan->chgParam, plan->wtParam);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.	Because of above, we only have to do this to the
     * non-recursive term.
     */
    if (outer_plan->chgParam == NULL)
        ExecReScan(outer_plan);

    /* Release any hashtable storage */
    if (node->tableContext)
        MemoryContextResetAndDeleteChildren(node->tableContext);

    /* And rebuild empty hashtable if needed */
    if (plan->numCols > 0)
        build_hash_table(node);

    /* reset processing state */
    node->recursing = false;
    node->intermediate_empty = true;
    node->iteration = 0;
    tuplestore_clear(node->working_table);
    tuplestore_clear(node->intermediate_table);
}

/*
 * @Function: ExecSyncControllerCreate()
 *
 * @Brief: Create the SyncController object with given planstate, after the controller
 *         is created we register it into global controller list under StreamNodeGroup
 *         (shared among threads), for this reason we have to allcated the heap-pointer
 *         with persistance of stream-workflow level, so put it along with nodegroup's
 *         memory context StreamRunTimeContext
 *
 * @Input state: planstate pointer based on which the controller is created
 *
 * @Return: void
 */
void ExecSyncControllerCreate(Plan* node)
{
    Assert(IS_PGXC_DATANODE && node != NULL && EXEC_IN_RECURSIVE_MODE(node) && u_sess->stream_cxt.global_obj != NULL);

    SyncController* controller = NULL;

    /* SyncController is allocated in StreamRunTime Memory Context */
    MemoryContext stream_runtime_memctx = u_sess->stream_cxt.global_obj->m_streamRuntimeContext;
    Assert(stream_runtime_memctx != NULL);

    MemoryContext current_memctx = MemoryContextSwitchTo(stream_runtime_memctx);

    switch (nodeTag(node)) {
        case T_RecursiveUnion: {
            controller = create_recursiveunion_synccontroller((RecursiveUnion*)node);
        } break;
        case T_Stream:
        case T_VecStream: {
            controller = create_stream_synccontroller((Stream*)node);
        } break;
        default: {
            elog(ERROR,
                "Unsupported SyncController type typeid:%d typename%s",
                nodeTag(node),
                nodeTagToString(nodeTag(node)));
        }
    }

    /* Assert the controller is correctly created */
    Assert(controller != NULL);

    /*
     * Register the with recursive controller cor current RecursiveUnion operator
     * into StreamNodeGroup where a process-share and visible across the whole datanode
     * among different stream plans
     */
    u_sess->stream_cxt.global_obj->AddSyncController((SyncController*)controller);

    /* Swith back to current memory context */
    MemoryContextSwitchTo(current_memctx);

    return;
}

/*
 * @Function: ExecSyncControllerDelete()
 *
 * @Brief: free the given controller's pointer fileds. Note, the controller itself is not
 *         free-ed here instead the invoker StreamNodeGroupp::deInit() will do this
 *
 * @input param @controller: controller pointer that going to free
 *
 * @Return: void
 */
void ExecSyncControllerDelete(SyncController* controller)
{
    Assert(IS_PGXC_DATANODE && controller != NULL);

    NodeTag controller_type = controller->controller_type;

    /*
     * Free base-class part
     *
     * Note! planstate is created from Executor MemoryContext, so we just set NULL
     * pointer here without invoking pfree(), also when we get here, it the global
     * "StreamNodeGroup"'s deinit() process where each individual planstate is already
     * free-ed in ExecutorEnd()
     ***/
    controller->controller_planstate = NULL;

    /*
     * Free sub-class part
     */
    if (T_RecursiveUnion == controller_type) {
        RecursiveUnionController* ru_controller = (RecursiveUnionController*)controller;

        pfree_ext(ru_controller->none_recursive_tuples);
        pfree_ext(ru_controller->recursive_tuples);
    }

    /* The caller will free the controller pointer itself */
    return;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * Function: FindSyncUpStream()
 *
 * Brief: Traverse the RecursiveUnion's underlying plan tree to find stream node and
 *        build a stream node list to hold them
 *
 * input param @controller: the recursive-union controller object
 * input param @state: the input planstate for current plan-tree iteration
 */
static void FindSyncUpStream(RecursiveUnionController* controller, PlanState* state, List** initplans)
{
    if (!IS_PGXC_DATANODE) {
        /*
         * exit when executing on coordinator node, as we only setup recursive union
         * controller.
         */
        return;
    }

    if (state == NULL) {
        /* exit on the end of planstate-tree traversion */
        return;
    }

    if (controller->syncup_streamnode != NULL) {
        return;
    }

    ListCell* lc = NULL;
    Plan* node = state->plan;
    if (node->initPlan)
        *initplans = list_concat(*initplans, list_copy(node->initPlan));

    List* ps_list = getSpecialSubPlanStateNodes(state);
    if (ps_list != NULL) {
        foreach (lc, ps_list) {
            FindSyncUpStream(controller, (PlanState*)lfirst(lc), initplans);
        }
    }

    /*
     * StreamNodeGroup has to be setup at this point, also we need assert-ly to confirm
     * that stream_recursive mode is enabled.
     */
    Assert(u_sess->stream_cxt.global_obj != NULL);

    switch (nodeTag(state)) {
        case T_RecursiveUnionState: {
            /*
             * Search stream plan nodes from recursive part, none-recursive part
             * is executed regularly
             */
            FindSyncUpStream(controller, (PlanState*)innerPlanState(state), initplans);
        } break;
        case T_StreamState:
        case T_VecStreamState: {
            StreamState* stream_state = (StreamState*)state;

            /*
             * Mark the current node is the syncup stream-node
             *
             * mainly for multi-stream node case where the syncup stream node is
             * response for receive and send sync-up messages
             */
            if (stream_state->ss.ps.plan->is_sync_plannode) {
                if (controller->syncup_streamnode) {
                    elog(ERROR, "more than one sync-up stream node");
                }

                controller->syncup_streamnode = stream_state;
            }
        } break;
        default: {
            /* Search stream plan nodes from left tree */
            FindSyncUpStream(controller, (PlanState*)outerPlanState(state), initplans);

            /* Search stream plan nodes from right tree */
            FindSyncUpStream(controller, (PlanState*)innerPlanState(state), initplans);
        }

        break;
    }

    List* subplan_list = getSubPlan(node, state->state->es_subplanstates, *initplans);
    foreach (lc, subplan_list) {
        PlanState* substate = (PlanState*)lfirst(lc);
        FindSyncUpStream(controller, substate, initplans);
    }

    list_free_ext(subplan_list);
    return;
}
#endif

/*
 * Function: create_recursiveunion_controller()
 *
 * Brief: create the RecursiveUnionSyncController object and attach it to global controller
 *        list, so that the follow-up controllee exec steps can see it and do proper "Producer
 *        synchronization steps", only used in recursive-stream plan.
 *
 * input param @state: the input planstate for current RecursiveUnion operator
 */
static SyncController* create_recursiveunion_synccontroller(RecursiveUnion* ru_node)
{
    RecursiveUnionController* controller = (RecursiveUnionController*)palloc0(sizeof(RecursiveUnionController));

    /* Initialize Recursive-Union control objects */
    controller->controller.controller_type = nodeTag(ru_node);
    /* Depend on RecursiveUnionState is ready, so controller_planstate will be initialized later in ExecInitNode */
    controller->controller.controller_planstate = NULL;
    controller->controller.controller_plannodeid = ru_node->plan.plan_node_id;
    controller->controller.controlnode_xcnodeid = 0;
    controller->controller.executor_stop = false;
    controller->streamlist = NIL;
    controller->total_execution_datanodes = list_length(ru_node->plan.exec_nodes->nodeList);
    controller->none_recursive_finished = false;
    controller->none_recursive_tuples = (int*)palloc0(sizeof(int) * controller->total_execution_datanodes);
    controller->recursive_tuples = (int*)palloc0(sizeof(int) * controller->total_execution_datanodes);
    for (int i = 0; i < controller->total_execution_datanodes; i++) {
        controller->none_recursive_tuples[i] = -1;
        controller->recursive_tuples[i] = -1;
    }

    controller->iteration = 0;
    controller->recursive_finished = false;

    errno_t rc = memset_s(&controller->recursive_vfd, sizeof(RecursiveVfd), 0, sizeof(RecursiveVfd));
    securec_check(rc, "\0", "\0");

    /* Initialize the row counters */
    controller->total_step1_rows = 0;
    controller->total_step2_substep_rows = 0;
    controller->total_step2_rows = 0;
    controller->total_step_rows = 0;
    controller->ru_coordnodeid = 0;

    return (SyncController*)controller;
}

/*
 * Function: create_stream_controller()
 *
 * Brief: create the StreamSyncController object and attach it to global controller list,
 *        so that the follow-up controllee exec steps can see it and do proper "Producer
 *        synchronization steps", only used in recursive-stream plan.
 *
 * input param @state: the input planstate for current Stream operator
 */
static SyncController* create_stream_synccontroller(Stream* stream_node)
{
    /*
     * Caution! we are in StreamRunTime memory context
     */
    StreamController* controller = (StreamController*)palloc0(sizeof(StreamController));

    controller->controller.controller_type = nodeTag(stream_node);
    /* Depend on StreamState is ready, so controller_planstate will be initialized later in ExecInitNode */
    controller->controller.controller_planstate = NULL;
    controller->controller.controller_plannodeid = stream_node->scan.plan.plan_node_id;
    controller->controller.controlnode_xcnodeid = 0;
    controller->iteration = 0;
    controller->iteration_finished = false;

    /* Overall summary fields */
    controller->total_tuples = 0;
    controller->stream_finish = false;

    return (SyncController*)controller;
}

/*
 * Function: ExecSyncRecursiveUnionConsumer()
 *
 * Brief: syncup-mechanism inteface for consumer side
 *
 * input param @controller: the controller of recursive union
 * input param @step: current step we need to syncup
 */
void ExecSyncRecursiveUnionConsumer(RecursiveUnionController* controller, int step)
{
    Assert(controller != NULL && IsA(controller->controller.controller_planstate, RecursiveUnionState));

    StreamNodeGroup* stream_node_group = u_sess->stream_cxt.global_obj;
    RecursiveUnionState* rustate = (RecursiveUnionState*)controller->controller.controller_planstate;

    int tuple_produced = rustate->step_tuple_produced;

    /* Report step-syncronization dfx messages */
    if (step == WITH_RECURSIVE_SYNC_RQSTEP) {
        RECURSIVE_LOG(LOG,
            "MPP with-recursive step%d (C) arrive@ node-step%d.%d finish with (%d)rows in step%d.%d",
            step,
            step,
            controller->iteration,
            tuple_produced,
            step,
            controller->iteration);
    } else {
        RECURSIVE_LOG(LOG,
            "MPP with-recursive step%d (C) arrive@ node-step%d finish with (%d)rows in step1",
            step,
            step,
            tuple_produced);
    }

    StreamState* state = controller->syncup_streamnode;
    state->isReady = false;

    int consumer_number = controller->total_execution_datanodes;

    /* Consumer report finish curerrent step */
    switch (step) {
        case WITH_RECURSIVE_SYNC_NONERQ: {
            /* Mark none-recursive part is done on current datanode */
            controller->none_recursive_tuples[u_sess->pgxc_cxt.PGXCNodeId] = tuple_produced;
            int step_produced_tuples = 0;
            int loop_count = 0;

            while (true) {
                /* Try to receive 'R' for each none RU-Coordinator nodes */
                bool step_ready = true;
                stream_node_group->ConsumerGetSyncUpMessage(
                    controller, step, (StreamState*)state, RUSYNC_MSGTYPE_NODE_FINISH);
                LOOP_ELOG(
                    DEBUG1, "MPP with-recursive[DEBUG] consumer step:%d in-loop[%d] wait step-finish", 
                    step, 
                    loop_count);
                loop_count++;
                for (int i = 0; i < consumer_number; i++) {
                    if (controller->none_recursive_tuples[i] == -1) {
                        step_ready = false;
                        break;
                    } else {
                        step_produced_tuples += controller->none_recursive_tuples[i];
                    }
                }

                /* Confirm none-recursive step is finished */
                if (step_ready) {
                    /* Update statistics */
                    controller->total_step1_rows = step_produced_tuples;
                    RECURSIVE_LOG(LOG,
                        "MPP with-recursive step%d (C) confirm@ '%c' from all DN total (%d)rows, "
                        "continue to step2 on control-node %s",
                        step,
                        RUSYNC_MSGTYPE_NODE_FINISH,
                        controller->total_step1_rows,
                        g_instance.attr.attr_common.PGXCNodeName);

                    /* SyncUp finish point! exit the loop */
                    break;
                }

                (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_CONSUMER_NEXT_STEP);
                WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
            }
        } break;
        case WITH_RECURSIVE_SYNC_RQSTEP: {
            int loop_count = 0;

            /*
             * Record num of tuples produced is done on current datanode
             *
             * Caution! In worker node, when the value of recursive_tuples[x] is
             * assinged, the producer is woke up and send 'R' with the number of
             * tuples to control-node.
             */
            controller->recursive_tuples[u_sess->pgxc_cxt.PGXCNodeId] = tuple_produced;

            /*
             * At this point, the producer on current dn is woke-up to send 'R'
             * to other DNs that we finish the none-recursive part on current DN
             */
            int step_produced_tuples = 0;
            while (true) {
                /* Try to receive 'R' for each none RU-Coordinator nodes */
                bool step_ready = true;
                LOOP_ELOG(DEBUG1,
                    "MPP with-recursive[DEBUG] consumer step:%d in-loop[%d] wait all node-finish",
                    step,
                    loop_count);
                loop_count++;
                stream_node_group->ConsumerGetSyncUpMessage(
                    controller, step, (StreamState*)state, RUSYNC_MSGTYPE_NODE_FINISH);

                /* lookup each datanodes to see if all finish */
                for (int i = 0; i < consumer_number; i++) {
                    if (controller->recursive_tuples[i] == -1) {
                        step_ready = false;
                        break;
                    } else {
                        step_produced_tuples += controller->recursive_tuples[i];
                    }
                }

                if (step_ready) {
                    /* Mark control structure finished, for now the producer is blocked */
                    controller->total_step2_substep_rows = step_produced_tuples;
                    controller->total_step2_rows += controller->total_step2_substep_rows;

                    RECURSIVE_LOG(LOG,
                        "MPP with-recursive step2 (C) receive@ '%c' from all DN total (%d)rows, "
                        "continue to step2.%d/step3 on control-node %s",
                        RUSYNC_MSGTYPE_NODE_FINISH,
                        controller->total_step2_substep_rows,
                        controller->iteration + 1,
                        g_instance.attr.attr_common.PGXCNodeName);

                    {
                        /*
                         * If current step finished, we need further check if the whole
                         * RecursiveUnion operator is finished before mark recursive_finish
                         */
                        bool recursive_union_finish = true;
                        for (int i = 0; i < consumer_number; i++) {
                            if (controller->recursive_tuples[i] > 0) {
                                /* If any datanodes still return value, we have to restart */
                                recursive_union_finish = false;
                                break;
                            }
                        }

                        /* Mark the cluster's recursive union finished */
                        if (recursive_union_finish) {
                            controller->total_step_rows = controller->total_step1_rows + controller->total_step2_rows;

                            RECURSIVE_LOG(LOG,
                                "MPP with-recursive step3 (C) conclude@ the whole RU finish with (%d)rows "
                                "(%d)iterations",
                                controller->total_step_rows,
                                controller->iteration);

                            /* Mark the whole recursion finished */
                            controller->recursive_union_finish = true;
                            break;
                        }
                    }

                    break;
                }

                (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_CONSUMER_NEXT_STEP);
                WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
            }
        } break;
        case WITH_RECURSIVE_SYNC_DONE: {
            (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_CONSUMER_NEXT_STEP);
            WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
        } break;

        default:
            elog(ERROR, "un-recognize steps for consumer side when synchronizing recusive-union.");
    }

    state->isReady = false;
    (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);

    return;
}

/*
 * Function: ExecSyncRecursiveUnionProducer()
 *
 * Brief: syncup-mechanism inteface for producer side
 *
 * input param @controller: the controller of recursive union
 * input param @producer_plannodeid: the top plan node id of producer thread
 * input param @step: the step we need syncup at producer side
 * output param @need_rescan: flag to indicate if producer thread need go rescan
 * input param @target_iteration: the target iteration
 */
void ExecSyncRecursiveUnionProducer(RecursiveUnionController* controller, int producer_plannodeid, int step,
    int tuple_count, bool* need_rescan, int target_iteration)
{
    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;

    switch (step) {
        case WITH_RECURSIVE_SYNC_NONERQ: {
            Assert(controller != NULL && IsA(controller->controller.controller_planstate, RecursiveUnionState));

            int tuple_produced = -1;
            int loop_count = 0;
            /* wait on current node-step1 to finish */
            while (true) {
                LOOP_ELOG(
                    DEBUG1, "MPP with-recursive[DEBUG] producer step:%d in-loop[%d] wait node-finish", 
                    step, 
                    loop_count);
                loop_count++;
                tuple_produced = controller->controller.executor_stop ? 0 : 
                    controller->none_recursive_tuples[u_sess->pgxc_cxt.PGXCNodeId];
                if (tuple_produced != -1) {
                    /* send 'R' */
                    stream_nodegroup->ProducerSendSyncMessage(controller,
                        producer_plannodeid,
                        WITH_RECURSIVE_SYNC_NONERQ,
                        tuple_produced,
                        RUSYNC_MSGTYPE_NODE_FINISH);

                    if (is_syncup_producer) {
                        RECURSIVE_LOG(LOG,
                            "MPP with-recursive step%d (P) report@ node-step%d done with (%d)rows to control-node. %s",
                            step,
                            step,
                            tuple_produced,
                            producer_top_plannode_str);
                    }

                    break;
                }

                (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
                WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
            }

            /* ------------------------------------------------------------------
             * RecursiveUnion step1:
             *
             * In control-node(Producer), when "singaled" by Consumer node then
             * send 'F' to worker-node's consumer
             * ------------------------------------------------------------------
             */
            loop_count = 0;
            /* wait cluster-step1 go finish */
            while (true) {
                /* the corresponding consumer may encounter a "short-circuit" */
                if (controller->controller.executor_stop) {
                    u_sess->exec_cxt.executorStopFlag = true;
                    break;
                }

                LOOP_ELOG(
                    DEBUG1, "MPP with-recursive[DEBUG] producer step:%d in-loop[%d] wait step-finish", 
                    step, 
                    loop_count);
                loop_count++;
                if (controller->none_recursive_finished) {
                    /* Mark the curent step is forwarding to recursive-term (control-node) */
                    if (is_syncup_producer) {
                        RECURSIVE_LOG(LOG,
                            "MPP with-recursive step%d (P) notify@ cluster-step%d done with (%d) rows to all "
                            "worker-node. %s",
                            step,
                            step,
                            controller->total_step1_rows,
                            producer_top_plannode_str);
                    }

                    /* exit point */
                    break;
                }

                (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
                WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
            }
            /* Producer contine next step */
        } break;
        case WITH_RECURSIVE_SYNC_RQSTEP: {
            Assert(controller != NULL && IsA(controller->controller.controller_planstate, RecursiveUnionState));

            int loop_count = 0;
            int current_iteration = controller->iteration;

            /* reach the end of producer thread, send 'Z' */
            stream_nodegroup->ProducerFinishRUIteration(step);

            *need_rescan = false;

            int tuple_produced = -1;

            /*
             * step1, wait and send 'R' to control node if we found iteration step
             * is finish in current datanode.
             */
            while (controller->iteration <= target_iteration) {
                /*
                 * Only syncup producer need check recursive_tuples and send node finish 'R'
                 * other producer doesn't need check recursive_tuples, in case syncup producer
                 * initialize recursive_tuples to -1 before other product check it.
                 */
                if (!is_syncup_producer) {
                    break;
                }

                LOOP_ELOG(
                    DEBUG1, "MPP with-recursive[DEBUG] producer step:%d in-loop[%d] wait node-finish", 
                    step, 
                    loop_count);
                loop_count++;

                /* Fetch tuple count */
                if (controller->controller.executor_stop || u_sess->exec_cxt.executorStopFlag) {
                    /* in case of executor marked as stop, we are going to send 0 */
                    tuple_produced = 0;
                } else {
                    tuple_produced = controller->recursive_tuples[u_sess->pgxc_cxt.PGXCNodeId];
                }

                if (tuple_produced != -1) {
                    /* Send 'R' to control node */
                    stream_nodegroup->ProducerSendSyncMessage(controller,
                        producer_plannodeid,
                        WITH_RECURSIVE_SYNC_RQSTEP,
                        tuple_produced,
                        RUSYNC_MSGTYPE_NODE_FINISH);

                    /* Note, after send 'R' to control node, F comes later, producer wait on recursive_union */
                    RECURSIVE_LOG(LOG,
                        "MPP with-recursive step2 (P) report@ node-step2.%d done with (%d)rows",
                        controller->iteration,
                        tuple_produced);

                    break;
                }

                (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
                WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
            }

            /*
             * Step2, after send 'R' we are going to wait step finish
             */
            while (target_iteration <= current_iteration) {
                LOOP_ELOG(
                    DEBUG1, "MPP with-recursive[DEBUG] producer step:%d in-loop[%d] wait step-finish", 
                    step, 
                    loop_count);
                loop_count++;
                /* the corresponding consumer may encounter a "short-circuit" */
                if (controller->controller.executor_stop) {
                    u_sess->exec_cxt.executorStopFlag = true;
                    break;
                }
                /* Syncup producer */
                if (is_syncup_producer) {
                    /*
                     * Only syncup producer thread receive 'R', send finish 'F',
                     * controll by recursive state and change recursive state
                     */
                    if (controller->recursive_union_finish) {
                        RECURSIVE_LOG(LOG,
                            "MPP with-recursive step3 (P) notify@ RU done with (%d)rows (%d)iterations (all finish) %s",
                            controller->total_step_rows,
                            controller->iteration,
                            producer_top_plannode_str);

                        /* exit point for sync-up producer */
                        break;
                    }

                    /* current step is finished, send F to worker node */
                    if (controller->recursive_finished) {
                        controller->recursive_finished = false;

                        *need_rescan = true;

                        LOOP_ELOG(LOG,
                            "MPP with-recursive step2 (P)(%d) notify@ cluster-step2.%d done.",
                            producer_plannodeid,
                            controller->iteration);

                        break;
                    }
                } else {
                    /* Other producer */
                    if (controller->recursive_union_finish) {
                        /* exit point for none sync-up producer */
                        break;
                    }

                    /*
                     * Other producer thread just controll by iteration number,
                     * it won't go through until controller's iteration number
                     * changed by syncup producer thread
                     */
                    if (controller->iteration > current_iteration) {
                        *need_rescan = true;

                        LOOP_ELOG(LOG,
                            "MPP with-recursive step2 (P)(%d) notify@ cluster-step2.%d done.",
                            producer_plannodeid,
                            controller->iteration);

                        break;
                    }
                }

                (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
                WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
            }
        } break;
        case WITH_RECURSIVE_SYNC_DONE: {
            (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
            WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
        } break;

        default:
            elog(ERROR, "un-recognized steps for producer side when sychronizing recursive-union.");
    }

    (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
    return;
}

/*
 * Function: ExecSyncStreamConsumer()
 *
 * Brief: syncup-mechanism inteface for consumer side of Stream node
 *
 * input param @controller: the controller of stream node
 *
 * return: none
 */
void ExecSyncStreamConsumer(StreamController* controller)
{
    RECURSIVE_LOG(LOG,
        "MPP with-recursive stream-step (C) SyncStreamConsumer set iteration %d to finish %s",
        controller->iteration,
        producer_top_plannode_str);

    /* update iteration no */
    controller->iteration++;

    /* invoke producer to start */
    controller->iteration_finished = true;

    StreamState* state = (StreamState*)controller->controller.controller_planstate;
    state->isReady = false;

    return;
}

/*
 * Function: ExecSyncStreamProducer()
 *
 * Brief: syncup-mechanism inteface for producer side of Stream node
 *
 * input param @controller: the controller of stream node
 * input param @need_rescan: the output parameter indicate the current producer
 *             thread needs rescan
 *
 * return: none
 */
void ExecSyncStreamProducer(StreamController* controller, bool* need_rescan, int target_iteration)
{
    StreamNodeGroup* stream_node_group = u_sess->stream_cxt.global_obj;

    /*
     * 1st. Always by-pass the first iteration
     */
    if (need_rescan == NULL && global_iteration == 0) {
        /*
         * Fetch the belonging RecursiveUnion operator id and look up if we have done the
         * none-recursive part
         */
        Stream* stream_plan = (Stream*)controller->controller.controller_planstate->plan;
        int cte_plan_id = ((Plan*)stream_plan)->recursive_union_plan_nodeid;

        /*
         * Wait recursive union controller's is ready
         */
        RecursiveUnionController* ru_controller = NULL;
        while (true) {
            ru_controller = (RecursiveUnionController*)stream_node_group->GetSyncController(cte_plan_id);
            if (ru_controller != NULL && ru_controller->controller.controller_planstate != NULL) {
                break;
            }

            (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
            WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
        }

        /*
         * Report Error when the controller for its belonging RecursiveUnion node is
         * not found.
         */
        if (ru_controller == NULL) {
            elog(ERROR,
                "MPP with-recursive. Controller is not found in ExecSyncStreamProducer with stream[%d] top:%s",
                ((Plan*)stream_plan)->plan_node_id,
                producer_top_plannode_str);
        }

        int loop_count = 0;
        while (true) {
            LOOP_ELOG(LOG,
                "MPP with-recursive stream-step0 stream node [%d], loop:%d",
                controller->controller.controller_plannodeid,
                loop_count);
            loop_count++;

            if (ru_controller->none_recursive_finished) {
                break;
            }

            /* the corresponding consumer may encounter a "short-circuit" */
            if (ru_controller->controller.executor_stop) {
                u_sess->exec_cxt.executorStopFlag = true;
                break;
            }

            (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
            WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
        }

        return;
    }

    /* Send node finish message to consumner */
    stream_node_group->ProducerFinishRUIteration(0);

    int loop_count = 0;
    if (need_rescan == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("need_rescan should not be NULL")));
    }
    *need_rescan = false;

    /*
     * 2nd. Check if normal iteration step or the whole recursive union step is finished
     **/
    while (true) {
        LOOP_ELOG(LOG,
            "MPP with-recursive stream-step(P) stream node [%d], loop:%d",
            controller->controller.controller_plannodeid,
            loop_count);
        loop_count++;

        /* Check if whole step is finished */
        if (controller->stream_finish) {
            RECURSIVE_LOG(LOG,
                "MPP with-recursive stream-step(P) recursive union iteration[%d] finish with loop:%d %s",
                u_sess->exec_cxt.global_iteration,
                loop_count,
                producer_top_plannode_str);
            break;
        }

        /* Check if current iteration is finished */
        if (controller->iteration_finished) {
            /* Only SyncUpProducer can reset the itieration finish back to false */
            if (is_syncup_producer) {
                controller->iteration_finished = false;
            }

            *need_rescan = true;
            RECURSIVE_LOG(LOG,
                "MPP with-recursive stream-step(P) recursive step on iteration %d with loop:%d %s",
                u_sess->exec_cxt.global_iteration,
                loop_count,
                producer_top_plannode_str);
            break;
        }

        /* the corresponding consumer may encounter a "short-circuit" */
        if (controller->controller.executor_stop) {
            u_sess->exec_cxt.executorStopFlag = true;
            break;
        }

        (void)pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
        WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
    }

    (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
    return;
}

/*
 * ----------------- MPPDB with-recursive rescan support
 */
/*
 * @Function: getSpecialSubPlanStateNodes()
 *
 * @Brief: Return plan node's underlying plan nodes that is not create under
 *         left/right plan tree
 *
 * @Input node: the iteratin entry point "PlanState *" pointer
 *
 * @Return: return the list of plansate that under "special" planstate nodes
 */
static List* getSpecialSubPlanStateNodes(const PlanState* node)
{
    List* ps_list = NIL;

    if (node == NULL) {
        return NIL;
    }

    /* Find plan list in special plan nodes. */
    switch (nodeTag(node->plan)) {
        case T_Append:
        case T_VecAppend: {
            AppendState* append = (AppendState*)node;
            for (int i = 0; i < append->as_nplans; i++) {
                PlanState* plan = append->appendplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTableState* mt = (ModifyTableState*)node;
            for (int i = 0; i < mt->mt_nplans; i++) {
                PlanState* plan = mt->mt_plans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppendState* ma = (MergeAppendState*)node;
            for (int i = 0; i < ma->ms_nplans; i++) {
                PlanState* plan = ma->mergeplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAndState* ba = (BitmapAndState*)node;
            for (int i = 0; i < ba->nplans; i++) {
                PlanState* plan = ba->bitmapplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOrState* bo = (BitmapOrState*)node;
            for (int i = 0; i < bo->nplans; i++) {
                PlanState* plan = bo->bitmapplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScanState* ss = (SubqueryScanState*)node;
            PlanState* plan = ss->subplan;
            ps_list = lappend(ps_list, plan);
        } break;
        default: {
            ps_list = NIL;
        } break;
    }

    return ps_list;
}

/*
 * @Function: ExecSetStreamFinish()
 *
 * @Brief: Treverse the planstate tree to get the stream node to set the underlying
 *         stream-controlling node to tell the producer to finish iteration.
 *
 * For Example: For the execution tree below, the invoke ExecSetStreamFinish() on <A>,
 * then we iterate to get stream[4] & stream[3] and set "finish" to tell AGG[8] and
 * WorkTableScan[9] to exit without rescan
 *
 *           CteScan
 *              /
 *       RecursiveUnion
 *         /        \
 *    SeqScan      HashJoin
 *                 /     \
 *            Stream[1] Stream[2]
 *               /          \
 *            SeqScan     HashJoin   <A>     ----------- Call ExecSetStreamFinish()
 *                         /    \
 *               SubqueryScan   Stream[3]
 *                      /          \
 *                 Stream[4]     WorkTableScan
 *                    /
 *                  Agg[8]
 *                  /
 *               Scan
 *
 *
 * @Input state: the planstate tree entry pointer
 *
 * @Return void
 **/
void ExecSetStreamFinish(PlanState* state)
{
    if (state == NULL) {
        return;
    }

    if (u_sess->stream_cxt.global_obj == NULL) {
        return;
    }

    /*
     * First, process the special planstate tree node that is not traversed as
     * left/right tree.
     */
    ListCell* l = NULL;
    foreach (l, state->initPlan) {
        SubPlanState* sstate = (SubPlanState*)lfirst(l);
        PlanState* splan = sstate->planstate;

        ExecSetStreamFinish(splan);
    }

    foreach (l, state->subPlan) {
        SubPlanState* sstate = (SubPlanState*)lfirst(l);
        PlanState* splan = sstate->planstate;

        ExecSetStreamFinish(splan);
    }

    List* ps_list = getSpecialSubPlanStateNodes(state);
    if (ps_list != NIL) {
        ListCell* lc = NULL;
        foreach (lc, ps_list) {
            PlanState* ps = (PlanState*)lfirst(lc);
            ExecSetStreamFinish(ps);
        }
    }

    /*
     * Second, process the regular plansate tree node
     */
    switch (nodeTag(state)) {
        case T_StreamState:
        case T_VecStreamState: {
            int stream_plan_nodeid = GET_PLAN_NODEID(state->plan);
            StreamNodeGroup* stream_node_group = u_sess->stream_cxt.global_obj;
            StreamController* controller = (StreamController*)stream_node_group->GetSyncController(stream_plan_nodeid);

            /* Check if stream state is correct set global controller list */
            if (controller == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("MPP With-Recursive sync controller for Stream[%d] is not found", stream_plan_nodeid)));
            }

            Assert(controller->controller.controller_planstate == (PlanState*)state);

            /* Mark iteration finish */
            controller->iteration_finished = true;

            /*
             * Mark whole step finish.
             *
             * Note: after this the Producer thread that blocked at the end of
             * ExecutePlan is woked up and continue to finish.
             */
            controller->stream_finish = true;
        } break;
        default: {
            /* drill down into inner/outer plantree */
            PlanState* lstate = outerPlanState(state);
            PlanState* rstate = innerPlanState(state);
            ExecSetStreamFinish(lstate);
            ExecSetStreamFinish(rstate);
        }
    }
}

/*
 * ---------------------------------------------- For each operator
 */

/*
 * - brief: determine if current stream node is the first level of recursive union as example:
 *
 *            CteScan
 *              /
 *       RecursiveUnion
 *         /        \
 *    SeqScan      HashJoin
 *                 /     \
 *            Stream[1]  Stream[2]        ------ True multi-stream node on first level
 *               /          \
 *            SeqScan       Hash
 *                             \
 *                            Stream[3]   ------ False
 *                                \
 *                             WorkTableScan
 *
 * Return: true/false to indicate if current thread(top_plan node) is the sync-up thread
 */
bool IsFirstLevelStreamStateNode(StreamState* node)
{
    Assert(node != NULL && IsA(node, StreamState) && node->ss.ps.plan != NULL && IsA(node->ss.ps.plan, Stream));

    Plan* plan = (Plan*)node->ss.ps.plan;
    Stream* stream_plan = (Stream*)plan;

    if (plan->recursive_union_plan_nodeid == 0) {
        return false;
    }

    if (stream_plan->stream_level == 1) {
        return true;
    }

    return false;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * Function: StartNextRecursiveIteration()
 *
 * Brief: kick-off the whole cluster run into next recursive steps where
 * we do controller metadata update and reset the control-variables to let
 * producer thread continue to run.
 *
 * Also, in final step, there is no next step to start, so instead we do
 * some infomation check to verify if current CTE is correctly executed and
 * shutdown gracefully.
 *
 * input param @controller, the recursive-union sync-up controller
 */
static void StartNextRecursiveIteration(RecursiveUnionController* controller, int step)
{
    /* Assert Execution Context on DataNode */
    Assert(u_sess->stream_cxt.global_obj != NULL && IS_PGXC_DATANODE && controller != NULL);

    /* Assert planstates */
    RecursiveUnionState* rustate = (RecursiveUnionState*)controller->controller.controller_planstate;
    Assert(rustate != NULL && IsA(rustate, RecursiveUnionState));

    int cte_plan_nodeid = rustate->ps.plan->parent_node_id;

    /* reset tuple produced */
    rustate->step_tuple_produced = 0;

    switch (step) {
        case WITH_RECURSIVE_SYNC_NONERQ: {
            /*
             * After synchronize the none-recursive steps we check the
             * iteration context and reset control variables.
             */
            Assert(controller->iteration == 0);

            /* Caution! increase the global iteration indicator to next step */
            controller->iteration++;

            /* Caution! kick off the whole execution go next step */
            controller->none_recursive_finished = true;

            /* Mark operator run into recurisve-stage */
            controller->stepno = WITH_RECURSIVE_SYNC_DONE;

            /* Output DFX messages */
            RECURSIVE_LOG(LOG,
                "MPP with-recursive CTE(%d) finish none-recursive step with [%d] rows produced in \"%s\"",
                cte_plan_nodeid,
                controller->none_recursive_tuples[u_sess->pgxc_cxt.PGXCNodeId],
                g_instance.attr.attr_common.PGXCNodeName);

            if (StreamNodeGroup::IsRUCoordNode()) {
                /* Report All datanode summary in control node */
                RECURSIVE_LOG(LOG,
                    "MPP with-recursive CTE(%d) finish none-recursive step with [%d] rows produced in all-nodes",
                    cte_plan_nodeid,
                    controller->total_step1_rows);
            }
        } break;
        case WITH_RECURSIVE_SYNC_RQSTEP: {
            int save_step2_substep_rows = controller->total_step2_substep_rows;
            int save_step2_current_datanode_rows = controller->recursive_tuples[u_sess->pgxc_cxt.PGXCNodeId];

            /* Have to be modified event without any rows return by current datanode */
            Assert(save_step2_current_datanode_rows != -1);

            /* ReSet the DN arrays */
            for (int i = 0; i < controller->total_execution_datanodes; i++) {
                controller->recursive_tuples[i] = -1;
            }

            /* ReSet DFX variables */
            controller->total_step2_substep_rows = 0;

            /*
             * Caution! Incresing the iteration step number is critical to the whole
             * recursive cte execution. In current design, we only allow step incresing
             */
            controller->iteration++;

            /* Caution!, will trigger producer to continue, and producer will set it back to false */
            controller->recursive_finished = true;

            /* Mark operator run into recurisve-stage */
            controller->stepno = WITH_RECURSIVE_SYNC_DONE;

            if (controller->recursive_union_finish) {
                controller->stepno = WITH_RECURSIVE_SYNC_DONE;
            }

            /* Wait the producer thread set the flag to false */
            RecursiveUnionWaitCondNegtive(&controller->recursive_finished, &controller->controller.executor_stop);

            /* Output DFX messages */
            RECURSIVE_LOG(LOG,
                "MPP with-recursive CTE(%d) finish recursive step iteration[%d] with [%d] rows produced in \"%s\"",
                cte_plan_nodeid,
                controller->iteration - 1,
                save_step2_current_datanode_rows,
                g_instance.attr.attr_common.PGXCNodeName);

            if (StreamNodeGroup::IsRUCoordNode()) {
                /* Report All datanode summary in control node */
                RECURSIVE_LOG(LOG,
                    "MPP with-recursive CTE(%d) finish recursive step iteration[%d] with [%d] rows produced in "
                    "all-nodes",
                    cte_plan_nodeid,
                    controller->iteration - 1,
                    save_step2_substep_rows);
            }
        } break;
        case WITH_RECURSIVE_SYNC_DONE: {
            /*
             * Normally, we don't have critical stuffs to be handled here, but we need
             * do some sanity check if there is something wrong at current point
             ***/
            /* (1) none-recursive should be correctly set */
            if (!controller->none_recursive_finished) {
                elog(ERROR,
                    "MPP with-recursive datanode:%s CTE(%d) none_recursive_finished is not set to finish",
                    g_instance.attr.attr_common.PGXCNodeName,
                    cte_plan_nodeid);
            }

            /* (2) recursvie_union_finish should be correctly set */
            if (!controller->recursive_union_finish) {
                elog(ERROR,
                    "MPP with-recursive datanode:%s CTE(%d) recursive_union_finish is not set to finish",
                    g_instance.attr.attr_common.PGXCNodeName,
                    cte_plan_nodeid);
            }

            /*
             * (3) verify the stepno stored in recursive union's syncup-controller
             * is correct.
             ***/
            if (controller->stepno != WITH_RECURSIVE_SYNC_DONE) {
                elog(ERROR,
                    "MPP with-recursive step information in SyncUpController is not correct %d",
                    controller->stepno);
            }

            /* Additional checks could be added here */
            /* After check we output the summary information */
            if (StreamNodeGroup::IsRUCoordNode()) {
                RECURSIVE_LOG(LOG,
                    "MPP with-recursive CTE[%d] done. all-nodes: iteration:%d, rows:%d",
                    cte_plan_nodeid,
                    controller->iteration,
                    controller->total_step_rows);
            } else {
                RECURSIVE_LOG(LOG,
                    "MPP with-recursive CTE[%d] done. single-node[%s]: iteration:%d:, rows:%d",
                    cte_plan_nodeid,
                    g_instance.attr.attr_common.PGXCNodeName,
                    controller->iteration,
                    controller->total_step_rows);
            }
        } break;
        default: {
            elog(ERROR, "Invalid start next recursvie iteration when kick-off the whole cluster.");
        }
    }

    return;
}
#endif

/*
 * Record recursive information in Instrumentation structure.
 */
template <bool isNonRecursive>
static void recordRecursiveInfo(RecursiveUnionState* node, int controller_plannodeid)
{
    /* We don't have todo any-thing */
    if (!IS_PGXC_DATANODE) {
        return;
    }

    /* We don't have todo step syncup when there is no stream operator in execution plan */
    if (u_sess->stream_cxt.global_obj == NULL) {
        return;
    }

    StreamNodeGroup* stream_node_group = u_sess->stream_cxt.global_obj;
    SyncController* controller = stream_node_group->GetSyncController(controller_plannodeid);

    if (controller == NULL)
        return;

    if (controller->controller_type != T_RecursiveUnion)
        return;

    if (HAS_INSTR(node, true)) {
        int niters = INSTR->recursiveInfo.niters;
        if (niters < RECUSIVE_MAX_ITER_NUM) {
            if (isNonRecursive) {
                /* non-recursive part */
                INSTR->recursiveInfo.iter_ntuples[niters] = ((RecursiveUnionController*)controller)->total_step1_rows;
            } else {
                /* recursive part */
                INSTR->recursiveInfo.iter_ntuples[niters] =
                    ((RecursiveUnionController*)controller)->total_step2_substep_rows;
            }
            INSTR->recursiveInfo.niters++;
        } else {
            /*
             * reached RECUSIVE_MAX_ITER_NUM limit of explain performance
             * and has not yet calcaulated the result.
             */
            INSTR->recursiveInfo.has_reach_limit = true;
        }
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/* For initializing with recursive result tuple slots. */
static void ExecInitRecursiveResultTupleSlot(EState* estate, PlanState* planstate)
{
    TupleTableSlot* slot = makeNode(TupleTableSlot);

    slot->tts_isempty = true;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
    slot->tts_tuple = NULL;
    slot->tts_tupleDescriptor = NULL;
#ifdef PGXC
    slot->tts_shouldFreeRow = false;
    slot->tts_dataRow = NULL;
    slot->tts_dataLen = -1;
    slot->tts_attinmeta = NULL;
#endif
    slot->tts_mcxt = CurrentMemoryContext;
    slot->tts_buffer = InvalidBuffer;
    slot->tts_nvalid = 0;
    slot->tts_values = NULL;
    slot->tts_isnull = NULL;
    slot->tts_mintuple = NULL;
    slot->tts_per_tuple_mcxt = AllocSetContextCreate(slot->tts_mcxt,
        "RUSlotPerTupleSharedMcxt",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    planstate->ps_ResultTupleSlot = slot;
}
#endif

/* Reset Recursive Plan Tree */
void ExecReScanRecursivePlanTree(PlanState* ps)
{
    /* Set es_recursive_next_iteration */
    ps->state->es_recursive_next_iteration = true;

    /* reset the whole execution tree */
    ExecReSetRecursivePlanTree(ps);

    /* rescan the whole execution tree */
    ExecReScan(ps);
    ps->state->es_recursive_next_iteration = false;
}

/*
 * @Function: ExecutePlanSyncProducer()
 *
 * @Brief: syncup-mechanism inteface for producer side during execute plan
 *
 * input param @planstate: execute planstate
 * input param @top_plan: top_plan of the planstate
 * input param @step: iteration step
 * input param @current_tuple_count: current iteration tuple number
 *
 * @Return: TRUE current iteration step finish
 */
bool ExecutePlanSyncProducer(PlanState* planstate, int step, bool* recursive_early_stop, long* current_tuple_count)
{
    Plan* top_plan = planstate->plan;

    switch (step) {
        case WITH_RECURSIVE_SYNC_NONERQ: {
            /* Reset executor stop flag based on its original list */
            if (u_sess->stream_cxt.producer_obj->m_originProducerExecNodeList) {
                *recursive_early_stop = !list_member_int(
                    u_sess->stream_cxt.producer_obj->m_originProducerExecNodeList, u_sess->pgxc_cxt.PGXCNodeId);
            }

            elog(DEBUG1,
                "MPP with-recursive stream thread starts RecursiveUnion[%d] %s",
                GET_RECURSIVE_UNION_PLAN_NODEID(top_plan),
                producer_top_plannode_str);

            StreamNodeGroup::SyncProducerNextPlanStep(GET_CONTROL_PLAN_NODEID(top_plan),
                GET_PLAN_NODEID(top_plan),
                WITH_RECURSIVE_SYNC_NONERQ,
                *current_tuple_count,
                NULL,
                u_sess->exec_cxt.global_iteration);
        } break;

        case WITH_RECURSIVE_SYNC_RQSTEP: {
            bool need_rescan = false;

            RECURSIVE_LOG(LOG,
                "MPP with-recursive (P) producer thread to check need rescan iteration[%d] %s",
                u_sess->exec_cxt.global_iteration,
                producer_top_plannode_str);

            /* 1. Send current iteration finished 'R' */
            StreamNodeGroup::SyncProducerNextPlanStep(GET_CONTROL_PLAN_NODEID(top_plan),
                GET_PLAN_NODEID(top_plan),
                WITH_RECURSIVE_SYNC_RQSTEP,
                *current_tuple_count,
                &need_rescan,
                u_sess->exec_cxt.global_iteration);

            if (need_rescan) {
                /*
                 * If ReScan is required at the end of current iteration, we need
                 * clean-up the intermidiate variables and reset the execution tree.
                 */
                RECURSIVE_LOG(LOG,
                    "MPP with-recursive step%d (P) Start to ReScan from iteration[%d] on DN %s with (%ld) rows "
                    "produced in last iteration, thread-top:%s",
                    WITH_RECURSIVE_SYNC_RQSTEP,
                    u_sess->exec_cxt.global_iteration,
                    g_instance.attr.attr_common.PGXCNodeName,
                    *current_tuple_count,
                    producer_top_plannode_str);

                *current_tuple_count = 0;
                u_sess->exec_cxt.global_iteration++;

                /* Reset recursive plan tree */
                ExecReScanRecursivePlanTree(planstate);
                return false;
            } else {
                /* If no rescan required, we set the underlying stream node to finish */
                ExecSetStreamFinish(planstate);
                RECURSIVE_LOG(LOG,
                    "MPP with-recursive step%d (P) on DN %s finished, total (%d)times",
                    WITH_RECURSIVE_SYNC_DONE,
                    g_instance.attr.attr_common.PGXCNodeName,
                    ++u_sess->exec_cxt.global_iteration);
            }
        } break;
        default:
            break;
    }
    return true;
}

/*
 * @Description: Reset recursive plantree.
 *
 * @param[IN] node:  PlanState tree paralleling the Plan tree
 * @return: void
 */
void ExecReSetRecursivePlanTree(PlanState* node)
{
    ListCell* l = NULL;

    foreach (l, node->initPlan) {
        SubPlanState* sstate = (SubPlanState*)lfirst(l);
        PlanState* splan = sstate->planstate;

        ExecReSetRecursivePlanTree(splan);
    }

    foreach (l, node->subPlan) {
        SubPlanState* sstate = (SubPlanState*)lfirst(l);
        PlanState* splan = sstate->planstate;

        ExecReSetRecursivePlanTree(splan);
    }

    switch (nodeTag(node)) {
        /* Materialize operators, need for reset */
        case T_SortState:
            ExecReSetSort((SortState*)node);
            break;

        case T_MaterialState:
            ExecReSetMaterial((MaterialState*)node);
            break;

        case T_AggState:
            ExecReSetAgg((AggState*)node);
            break;

        case T_HashJoinState:
            ExecReSetHashJoin((HashJoinState*)node);
            break;

        case T_SetOpState:
            ExecReSetSetOp((SetOpState*)node);
            break;

        case T_StreamState:
            ExecReSetStream((StreamState*)node);
            break;

        /* No need for reset */
        case T_MergeAppendState: {
            MergeAppendState* append_state = (MergeAppendState*)node;
            node->earlyFreed = true;
            for (int planNo = 0; planNo < append_state->ms_nplans; planNo++) {
                ExecReSetRecursivePlanTree(append_state->mergeplans[planNo]);
            }
        } break;

        case T_VecAppendState:
        case T_AppendState: {
            AppendState* append_state = (AppendState*)node;
            node->earlyFreed = true;
            for (int planNo = 0; planNo < append_state->as_nplans; planNo++) {
                ExecReSetRecursivePlanTree(append_state->appendplans[planNo]);
            }
        } break;

        case T_VecModifyTableState:
        case T_ModifyTableState:
        case T_DistInsertSelectState: {
            ModifyTableState* mt = (ModifyTableState*)node;
            for (int planNo = 0; planNo < mt->mt_nplans; planNo++) {
                ExecReSetRecursivePlanTree(mt->mt_plans[planNo]);
            }
        } break;

        case T_VecSubqueryScanState:
        case T_SubqueryScanState: {
            SubqueryScanState* ss = (SubqueryScanState*)node;
            if (ss->subplan)
                ExecReSetRecursivePlanTree(ss->subplan);
        } break;

        case T_CStoreIndexAndState:
        case T_BitmapAndState: {
            BitmapAndState* and_state = (BitmapAndState*)node;
            for (int planNo = 0; planNo < and_state->nplans; planNo++) {
                ExecReSetRecursivePlanTree(and_state->bitmapplans[planNo]);
            }
        } break;

        case T_CStoreIndexOrState:
        case T_BitmapOrState: {
            BitmapOrState* or_state = (BitmapOrState*)node;
            for (int planNo = 0; planNo < or_state->nplans; planNo++) {
                ExecReSetRecursivePlanTree(or_state->bitmapplans[planNo]);
            }
        } break;

        default:
            if (outerPlanState(node)) {
                ExecReSetRecursivePlanTree(outerPlanState(node));
            }

            if (innerPlanState(node)) {
                ExecReSetRecursivePlanTree(innerPlanState(node));
            }
            break;
    }
}
