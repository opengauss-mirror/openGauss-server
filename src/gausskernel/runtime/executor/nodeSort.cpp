/* -------------------------------------------------------------------------
 *
 * nodeSort.cpp
 *	  Routines to handle sorting of relations.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeSort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodeSort.h"
#include "miscadmin.h"
#include "optimizer/streamplan.h"
#include "pgstat.h"
#include "instruments/instr_unique_sql.h"
#include "utils/tuplesort.h"
#include "workload/workload.h"

#include "optimizer/var.h"
#include "optimizer/tlist.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

static TupleTableSlot* ExecSort(PlanState* state);
/* ----------------------------------------------------------------
 *		ExecSort
 *
 *		Sorts tuples from the outer subtree of the node using tuplesort,
 *		which saves the results in a temporary file or memory. After the
 *		initial call, returns a tuple from the file with each call.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecSort(PlanState* state)
{
    SortState* node = castNode(SortState, state);
    TupleTableSlot* slot = NULL;

    CHECK_FOR_INTERRUPTS();

    /*
     * get state info from node
     */
    SO1_printf("ExecSort: %s\n", "entering routine");

    EState* estate = node->ss.ps.state;
    ScanDirection dir = estate->es_direction;
    Tuplesortstate* tuple_sortstate = (Tuplesortstate*)node->tuplesortstate;
    TimestampTz start_time = 0;

    /*
     * If first time through, read all tuples from outer plan and pass them to
     * tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
     */
    if (!node->sort_Done) {
        Sort* plan_node = (Sort*)node->ss.ps.plan;
        PlanState* outer_node = NULL;
        PlanState* plan_state = NULL;
        TupleDesc tup_desc = NULL;
        int64 sort_mem = SET_NODEMEM(plan_node->plan.operatorMemKB[0], plan_node->plan.dop);
        int64 max_mem =
            (plan_node->plan.operatorMaxMem > 0) ? SET_NODEMEM(plan_node->plan.operatorMaxMem, plan_node->plan.dop) : 0;
        /* init unique sort state at the first time */
        UpdateUniqueSQLSortStats(NULL, &start_time);


        SO1_printf("ExecSort: %s\n", "sorting subplan");

        /*
         * Want to scan subplan in the forward direction while creating the
         * sorted data.
         */
        estate->es_direction = ForwardScanDirection;

        /*
         * Initialize tuplesort module.
         */
        SO1_printf("ExecSort: %s\n", "calling tuplesort_begin");

        outer_node = outerPlanState(node);
        tup_desc = ExecGetResultType(outer_node);

        tuple_sortstate = tuplesort_begin_heap(tup_desc,
            plan_node->numCols,
            plan_node->sortColIdx,
            plan_node->sortOperators,
            plan_node->collations,
            plan_node->nullsFirst,
            sort_mem,
            node->randomAccess,
            max_mem,
            plan_node->plan.plan_node_id,
            SET_DOP(plan_node->plan.dop));

        /*
         * Here used for start with order siblings by
         * We need to set our customized sort function for siblings key
         */
        if (IsA(outer_node->plan, RecursiveUnion)) {
            RecursiveUnion *ru = (RecursiveUnion *)outer_node->plan;

            tuplesort_set_siblings(tuple_sortstate, plan_node->numCols, ru->internalEntryList);
        }

        if (node->bounded) {
            tuplesort_set_bound(tuple_sortstate, node->bound);
        }

        node->tuplesortstate = (void*)tuple_sortstate;
        WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_SORT_FETCH_TUPLE);

        /*
         * Scan the subplan and feed all the tuples to tuplesort.
         */
        for (;;) {
            slot = ExecProcNode(outer_node);
            if (TupIsNull(slot))
                break;
#ifdef PGXC
            if (plan_node->srt_start_merge)
                tuplesort_puttupleslotontape(tuple_sortstate, slot);
            else
#endif /* PGXC */
                tuplesort_puttupleslot(tuple_sortstate, slot);
        }
        
        pgstat_report_waitstatus(STATE_EXEC_SORT);

        sort_count(tuple_sortstate);

        /*
         * Cache peak memory info into SortState for display of explain analyze here
         * to ensure correct peak memory log for external sort cases.
         */
        if (node->ss.ps.instrument != NULL) {
            int64 peakMemorySize = (int64)tuplesort_get_peak_memory(tuple_sortstate);
            if (node->ss.ps.instrument->memoryinfo.peakOpMemory < peakMemorySize)
                node->ss.ps.instrument->memoryinfo.peakOpMemory = peakMemorySize;
        }

        /* Finish scanning the subplan, it's safe to early free the memory of lefttree */
        ExecEarlyFree(outerPlanState(node));

        EARLY_FREE_LOG(elog(LOG,
            "Early Free: Before completing the sort "
            "at node %d, memory used %d MB.",
            plan_node->plan.plan_node_id,
            getSessionMemoryUsageMB()));

        /*
         * Complete the sort.
         */
        tuplesort_performsort(tuple_sortstate);
        (void)pgstat_report_waitstatus(old_status);

        /*
         * restore to user specified direction
         */
        estate->es_direction = dir;

        /*
         * finally set the sorted flag to true
         */
        node->sort_Done = true;
        node->bounded_Done = node->bounded;
        node->bound_Done = node->bound;
        plan_state = &node->ss.ps;

        /* analyze the tuple_sortstate information for update unique sql sort info */
        UpdateUniqueSQLSortStats(tuple_sortstate, &start_time);

        /* Cache sort info into SortState for display of explain analyze */
        if (node->ss.ps.instrument != NULL) {
            tuplesort_get_stats(tuple_sortstate, &(node->sortMethodId), &(node->spaceTypeId), &(node->spaceUsed));
        }

        if (HAS_INSTR(&node->ss, true)) {
            plan_state->instrument->width = (int)tuplesort_get_avgwidth(tuple_sortstate);
            plan_state->instrument->sysBusy = tuplesort_get_busy_status(tuple_sortstate);
            plan_state->instrument->spreadNum = tuplesort_get_spread_num(tuple_sortstate);
            tuplesort_get_stats(tuple_sortstate,
                &(plan_state->instrument->sorthashinfo.sortMethodId),
                &(plan_state->instrument->sorthashinfo.spaceTypeId),
                &(plan_state->instrument->sorthashinfo.spaceUsed));
        }
        SO1_printf("ExecSort: %s\n", "sorting done");
    }

    SO1_printf("ExecSort: %s\n", "retrieving tuple from tuplesort");

    /*
     * Get the first or next tuple from tuplesort. Returns NULL if no more
     * tuples.
     */
    slot = node->ss.ps.ps_ResultTupleSlot;
    (void)tuplesort_gettupleslot(tuple_sortstate, ScanDirectionIsForward(dir), slot, NULL);
    return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitSort
 *
 *		Creates the run-time state information for the sort node
 *		produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
SortState* ExecInitSort(Sort* node, EState* estate, int eflags)
{
    SO1_printf("ExecInitSort: %s\n", "initializing sort node");

    /*
     * create state structure
     */
    SortState* sortstate = makeNode(SortState);
    sortstate->ss.ps.plan = (Plan*)node;
    sortstate->ss.ps.state = estate;
    sortstate->ss.ps.ExecProcNode = ExecSort;

    /*
     * We must have random access to the sort output to do backward scan or
     * mark/restore.  We also prefer to materialize the sort output if we
     * might be called on to rewind and replay it many times.
     */
    sortstate->randomAccess = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0;

    sortstate->bounded = false;
    sortstate->sort_Done = false;
    sortstate->tuplesortstate = NULL;

    /*
     * Miscellaneous initialization
     *
     * Sort nodes don't initialize their ExprContexts because they never call
     * ExecQual or ExecProject.
     */
    /*
     * tuple table initialization
     *
     * sort nodes only return scan tuples from their sorted relation.
     */
    ExecInitResultTupleSlot(estate, &sortstate->ss.ps);
    ExecInitScanTupleSlot(estate, &sortstate->ss);

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
    outerPlanState(sortstate) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * initialize tuple type.  no need to initialize projection info because
     * this node doesn't do projections.
     */
    ExecAssignScanTypeFromOuterPlan(&sortstate->ss);

    ExecAssignResultTypeFromTL(
            &sortstate->ss.ps,
            sortstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    sortstate->ss.ps.ps_ProjInfo = NULL;

    Assert(sortstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    SO1_printf("ExecInitSort: %s\n", "sort node initialized");

    return sortstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSort(node)
 * ----------------------------------------------------------------
 */
void ExecEndSort(SortState* node)
{
    SO1_printf("ExecEndSort: %s\n", "shutting down sort node");

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * must drop pointer to sort result tuple
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    /*
     * Release tuplesort resources
     */
    if (node->tuplesortstate != NULL)
        tuplesort_end((Tuplesortstate*)node->tuplesortstate);
    node->tuplesortstate = NULL;

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node));

    SO1_printf("ExecEndSort: %s\n", "sort node shutdown");
}

/* ----------------------------------------------------------------
 *		ExecSortMarkPos
 *
 *		Calls tuplesort to save the current position in the sorted file.
 * ----------------------------------------------------------------
 */
void ExecSortMarkPos(SortState* node)
{
    /*
     * if we haven't sorted yet, just return
     */
    if (!node->sort_Done)
        return;

    tuplesort_markpos((Tuplesortstate*)node->tuplesortstate);
}

/* ----------------------------------------------------------------
 *		ExecSortRestrPos
 *
 *		Calls tuplesort to restore the last saved sort file position.
 * ----------------------------------------------------------------
 */
void ExecSortRestrPos(SortState* node)
{
    /*
     * if we haven't sorted yet, just return.
     */
    if (!node->sort_Done)
        return;

    /*
     * restore the scan to the previously marked position
     */
    tuplesort_restorepos((Tuplesortstate*)node->tuplesortstate);
}

void ExecReScanSort(SortState* node)
{
    int param_no = 0;
    int part_itr = 0;
    ParamExecData* param = NULL;
    bool need_switch_partition = false;

    /* Already reset, just rescan lefttree */
    if (node->ss.ps.recursive_reset && node->ss.ps.state->es_recursive_next_iteration) {
        if (node->ss.ps.lefttree->chgParam == NULL)
            ExecReScan(node->ss.ps.lefttree);

        node->ss.ps.recursive_reset = false;
        return;
    }

    if (node->ss.ps.plan->ispwj) {
        param_no = node->ss.ps.plan->paramno;
        param = &(node->ss.ps.state->es_param_exec_vals[param_no]);
        part_itr = (int)param->value;

        if (part_itr != node->ss.currentSlot) {
            need_switch_partition = true;
        }

        node->ss.currentSlot = part_itr;
    }

    /*
     * If we haven't sorted yet, just return. If outerplan's chgParam is not
     * NULL then it will be re-scanned by ExecProcNode, else no reason to
     * re-scan it at all.
     */
    if (!node->sort_Done) {
        if (node->ss.ps.plan->ispwj) {
            ExecReScan(node->ss.ps.lefttree);
        }

        return;
    }

    /* must drop pointer to sort result tuple */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    /*
     * If subnode is to be rescanned then we forget previous sort results; we
     * have to re-read the subplan and re-sort.  Also must re-sort if the
     * bounded-sort parameters changed or we didn't select randomAccess.
     *
     * Otherwise we can just rewind and rescan the sorted output.
     */
    if (node->ss.ps.lefttree->chgParam != NULL || node->bounded != node->bounded_Done ||
        node->bound != node->bound_Done || !node->randomAccess) {
        node->sort_Done = false;
        if (node->tuplesortstate != NULL) {
            tuplesort_end((Tuplesortstate*)node->tuplesortstate);
            node->tuplesortstate = NULL;
        }

        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (node->ss.ps.lefttree->chgParam == NULL)
            ExecReScan(node->ss.ps.lefttree);
    } else {
        if (node->ss.ps.plan->ispwj && need_switch_partition) {
            node->sort_Done = false;
            if (node->tuplesortstate != NULL) {
                tuplesort_end((Tuplesortstate*)node->tuplesortstate);
                node->tuplesortstate = NULL;
            }

            /*
             * if chgParam of subnode is not null then plan will be re-scanned by
             * first ExecProcNode.
             */
            ExecReScan(node->ss.ps.lefttree);
        } else {
            tuplesort_rescan((Tuplesortstate*)node->tuplesortstate);
        }
    }
}

/*
 * @Description: Early free the memory for Sort.
 *
 * @param[IN] node:  executor state for Sort
 * @return: void
 */
void ExecEarlyFreeSort(SortState* node)
{
    PlanState* plan_state = &node->ss.ps;

    if (plan_state->earlyFreed)
        return;

    SO_printf("ExecEarlyFreeSort start\n");

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * must drop pointer to sort result tuple
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    /*
     * Release tuplesort resources
     */
    if (node->tuplesortstate != NULL)
        tuplesort_end((Tuplesortstate*)node->tuplesortstate);
    node->tuplesortstate = NULL;

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Sort "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));

    SO_printf("ExecEarlyFreeSort end\n");
}

/*
 * @Function: ExecReSetSort()
 *
 * @Brief: Reset the sort state structure in rescan case
 * 	under recursive-stream new iteration condition.
 *
 * @Input node: node sort planstate
 *
 * @Return: no return value
 */
void ExecReSetSort(SortState* node)
{
    Assert(IS_PGXC_DATANODE && node != NULL && (IsA(node, SortState)));

    /* Mark sort is not done */
    node->sort_Done = false;

    if (node->tuplesortstate != NULL) {
        (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
        tuplesort_end((Tuplesortstate*)node->tuplesortstate);
        node->tuplesortstate = NULL;
    }

    node->ss.ps.recursive_reset = true;
    ExecReSetRecursivePlanTree(outerPlanState(node));

    return;
}
