/* -------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * vecsort.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecsort.cpp
 *
 * -----------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/vecsortcodegen.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "instruments/instr_unique_sql.h"
#include "utils/batchsort.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecnodesort.h"
#include "vecexecutor/vecexecutor.h"
#include "miscadmin.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);
typedef int (*LLVM_CMC_func)(const MultiColumns* a, const MultiColumns* b, Batchsortstate* state);

/*
 * @Description	: Check if the parent node of current sort node is Limit node.
 * @in node		: Current sort node information.
 * @in estate	:
 */
bool MatchLimitNode(Sort* node, Plan* plan_tree);

/* ----------------------------------------------------------------
 *
 *              ExecVecSort
 *
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecSort(VecSortState* node)
{
    EState* estate = NULL;
    ScanDirection dir;
    Batchsortstate* batch_sort_stat = NULL;
    VectorBatch* batch = NULL;
    long total_size = 0;
    uint64 spill_count = 0;
    TimestampTz start_time = 0;


    /*
     * get state info from node
     */
    SO1_printf("ExecSort: %s\n", "entering routine");

    estate = node->ss.ps.state;
    dir = estate->es_direction;
    batch_sort_stat = (Batchsortstate*)node->tuplesortstate;

    /*
     * If first time through, read all tuples from outer plan and pass them to
     * tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
     */
    if (!node->sort_Done) {
        Sort* plan_node = (Sort*)node->ss.ps.plan;
        PlanState* outer_node = NULL;
        TupleDesc tup_desc;
        PlanState* plan_stat = NULL;
        int64 local_work_mem = SET_NODEMEM(plan_node->plan.operatorMemKB[0], plan_node->plan.dop);
        int64 max_mem =
            (plan_node->plan.operatorMaxMem > 0) ? SET_NODEMEM(plan_node->plan.operatorMaxMem, plan_node->plan.dop) : 0;

        /* init the unique sql vec sort state at the first time */
        UpdateUniqueSQLVecSortStats(NULL, 0, &start_time);

        SO1_printf("ExecVecSort: %s\n", "sorting subplan");
        WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_SORT);

        /*
         * Want to scan subplan in the forward direction while creating the
         * sorted data.
         */
        estate->es_direction = ForwardScanDirection;

        /*
         * Initialize tuplesort module.
         */
        SO1_printf("ExecVecSort: %s\n", "calling batchsort_begin");

        outer_node = outerPlanState(node);
        tup_desc = ExecGetResultType(outer_node);

        batch_sort_stat = batchsort_begin_heap(tup_desc,
            plan_node->numCols,
            plan_node->sortColIdx,
            plan_node->sortOperators,
            plan_node->collations,
            plan_node->nullsFirst,
            local_work_mem,
            node->randomAccess,
            max_mem,
            plan_node->plan.plan_node_id,
            SET_DOP(plan_node->plan.dop));

        if (node->jitted_CompareMultiColumn) {
            /* mark if comparemulticolumn has been codegened or not */
            if (HAS_INSTR(&node->ss, false))
                node->ss.ps.instrument->isLlvmOpt = true;

            /* Pass the jitted function to Batchsortstate from VecSortState */
            batch_sort_stat->jitted_CompareMultiColumn = node->jitted_CompareMultiColumn;
            batch_sort_stat->jitted_CompareMultiColumn_TOPN = node->jitted_CompareMultiColumn_TOPN;

            if (batch_sort_stat->sortKeys[0].abbrev_converter == NULL) {
                batch_sort_stat->compareMultiColumn = ((LLVM_CMC_func)(node->jitted_CompareMultiColumn));
            }
        } else {
            batch_sort_stat->jitted_CompareMultiColumn = NULL;
            batch_sort_stat->jitted_CompareMultiColumn_TOPN = NULL;
        }

        if (node->bounded) {
            batchsort_set_bound(batch_sort_stat, node->bound);
        }
        node->tuplesortstate = (void*)batch_sort_stat;

        if (!batch_sort_stat->m_colInfo) {
            batch_sort_stat->InitColInfo(node->m_pCurrentBatch);
        }

        /*
         * Scan the subplan and feed all the tuples to tuplesort.
         *
         * Since every time we get BatchMaxSize tuples, we may need
         * several times to do the batchsort_putbatch
         *
         * if(batch_sort_stat->sortKeys[0].abbrev_converter != NULL)
         * means that we use abbreviate compare function to speed up
         * sort, but we need to allocate size for abbreviate value
         * of first sort column.
         */
        for (;;) {
            batch = VectorEngine(outer_node);
            if (BatchIsNull(batch)) {
                break;
            }

            batch_sort_stat->sort_putbatch(batch_sort_stat, batch, 0, batch->m_rows);

            /* sql active feature */
            if (batch_sort_stat->m_tapeset) {
                long currentFileBlocks = LogicalTapeSetBlocks(batch_sort_stat->m_tapeset);
                int64 spill_size = (int64)(currentFileBlocks - batch_sort_stat->m_lastFileBlocks);
                total_size += spill_size * (BLCKSZ);
                spill_count += 1;
                pgstat_increase_session_spill_size(spill_size * (BLCKSZ));
                batch_sort_stat->m_lastFileBlocks = currentFileBlocks;
                if (node->ss.ps.instrument) {
                    node->ss.ps.instrument->sorthashinfo.spill_size += total_size;
                }
            }
        }

        /*
         * Cache peak memory info into SortState for display of explain analyze here
         * to ensure correct peak memory log for external sort cases.
         */
        if (node->ss.ps.instrument != NULL) {
            if (node->ss.ps.instrument->memoryinfo.peakOpMemory < batch_sort_stat->peakMemorySize) {
                node->ss.ps.instrument->memoryinfo.peakOpMemory = batch_sort_stat->peakMemorySize;
            }
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
        batchsort_performsort(batch_sort_stat);

        /* sql active feature */
        if (batch_sort_stat->m_tapeset) {
            long curr_file_blocks = LogicalTapeSetBlocks(batch_sort_stat->m_tapeset);
            int64 spill_size = (int64)(curr_file_blocks - batch_sort_stat->m_lastFileBlocks);
            total_size += spill_size * (BLCKSZ);
            spill_count += 1;
            pgstat_increase_session_spill_size(spill_size * (BLCKSZ));
            batch_sort_stat->m_lastFileBlocks = curr_file_blocks;
            if (node->ss.ps.instrument) {
                node->ss.ps.instrument->sorthashinfo.spill_size += total_size;
            }
        }

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
        plan_stat = &node->ss.ps;

        /* analyze the batch_sort_stat information to update unique sql */
        UpdateUniqueSQLVecSortStats(batch_sort_stat, spill_count, &start_time);

        /* Cache sort info into SortState for display of explain analyze */
        if (node->ss.ps.instrument != NULL) {
            batchsort_get_stats(batch_sort_stat, &(node->sortMethodId), &(node->spaceTypeId), &(node->spaceUsed));
        }

        if (HAS_INSTR(&node->ss, true)) {
            plan_stat->instrument->width = (int)batch_sort_stat->m_colWidth;
            plan_stat->instrument->sysBusy = batch_sort_stat->m_sysBusy;
            plan_stat->instrument->spreadNum = batch_sort_stat->m_spreadNum;
            batchsort_get_stats(batch_sort_stat,
                &(plan_stat->instrument->sorthashinfo.sortMethodId),
                &(plan_stat->instrument->sorthashinfo.spaceTypeId),
                &(plan_stat->instrument->sorthashinfo.spaceUsed));
            plan_stat->instrument->sorthashinfo.spill_size = total_size;
        }
        SO1_printf("ExecVecSort: %s\n", "sorting done");
        (void)pgstat_report_waitstatus(old_status);
    }

    SO1_printf("ExecVecSort: %s\n", "retrieving batch from batchsort");

    /*
     * Get the first or next tuple from tuplesort. Returns NULL if no more
     * tuples.
     */
    batch = node->m_pCurrentBatch;
    batchsort_getbatch(batch_sort_stat, ScanDirectionIsForward(dir), batch);

    return batch;
}

/* ----------------------------------------------------------------
 *
 *              ExecInitVecSort
 *
 * ----------------------------------------------------------------
 */
VecSortState* ExecInitVecSort(Sort* node, EState* estate, int eflags)
{
    VecSortState* sort_stat = NULL;

    SO1_printf("ExecInitVecSort: %s\n", "initializing sort node");

    /*
     * create state structure
     */
    sort_stat = makeNode(VecSortState);
    sort_stat->ss.ps.plan = (Plan*)node;
    sort_stat->ss.ps.state = estate;
    sort_stat->ss.ps.vectorized = true;

    /*
     * We must have random access to the sort output to do backward scan or
     * mark/restore.  We also prefer to materialize the sort output if we
     * might be called on to rewind and replay it many times.
     */
    sort_stat->randomAccess = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0;

    sort_stat->bounded = false;
    sort_stat->sort_Done = false;
    sort_stat->tuplesortstate = NULL;

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
    ExecInitResultTupleSlot(estate, &sort_stat->ss.ps);
    ExecInitScanTupleSlot(estate, &sort_stat->ss);

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    outerPlanState(sort_stat) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * initialize tuple type.  no need to initialize projection info because
     * this node doesn't do projections.
     */
    ExecAssignScanTypeFromOuterPlan(&sort_stat->ss);

    ExecAssignResultTypeFromTL(
            &sort_stat->ss.ps,
            sort_stat->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);

    sort_stat->ss.ps.ps_ProjInfo = NULL;

    /*
     * Vectorization specific initialization
     */
    TupleDesc res_desc = sort_stat->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
    MemoryContext context = CurrentMemoryContext;
    sort_stat->m_pCurrentBatch = New(context) VectorBatch(context, res_desc);

    SO1_printf("ExecInitVecSort: %s\n", "sort node initialized");

#ifdef ENABLE_LLVM_COMPILE
    /*
     * Consider codegeneration for sort node. In fact, CompareMultiColumn is the
     * hotest function in sort node.
     */
    bool use_prefetch = false;
    sort_stat->jitted_CompareMultiColumn = NULL;
    sort_stat->jitted_CompareMultiColumn_TOPN = NULL;
    llvm::Function* jitted_comparecol = NULL;
    llvm::Function* jitted_comparecol_topn = NULL;
    dorado::GsCodeGen* llvm_codegen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    bool consider_codegen = CodeGenThreadObjectReady() && CodeGenPassThreshold(((Plan*)outerPlan(node))->plan_rows,
                                                              estate->es_plannedstmt->num_nodes,
                                                              ((Plan*)outerPlan(node))->dop);
    if (consider_codegen) {
        jitted_comparecol = dorado::VecSortCodeGen::CompareMultiColumnCodeGen(sort_stat, use_prefetch);
        if (jitted_comparecol != NULL) {
            llvm_codegen->addFunctionToMCJit(
                jitted_comparecol, reinterpret_cast<void**>(&(sort_stat->jitted_CompareMultiColumn)));
        }

        /* If the current sort node has a 'Limit' parent node, codegen more */
        Plan* plan_tree = estate->es_plannedstmt->planTree;
        bool has_topn = MatchLimitNode(node, plan_tree);
        if (has_topn && (jitted_comparecol != NULL)) {
            jitted_comparecol_topn = dorado::VecSortCodeGen::CompareMultiColumnCodeGen_TOPN(sort_stat, use_prefetch);
            if (jitted_comparecol_topn != NULL) {
                llvm_codegen->addFunctionToMCJit(
                    jitted_comparecol_topn, reinterpret_cast<void**>(&(sort_stat->jitted_CompareMultiColumn_TOPN)));
            }
        }
    }
#endif

    return sort_stat;
}

/* ----------------------------------------------------------------
 *
 *              ExecEndVecSort(node)
 *
 * ----------------------------------------------------------------
 */
void ExecEndVecSort(VecSortState* node)
{
    SO1_printf("ExecEndVecSort: %s\n", "shutting down sort node");

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
    if (node->tuplesortstate != NULL) {
        batchsort_end((Batchsortstate*)node->tuplesortstate);
    }
    node->tuplesortstate = NULL;

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node));

    SO1_printf("ExecEndVecSort: %s\n", "sort node shutdown");
}

/* ----------------------------------------------------------------
 *
 *              ExecVecSortMarkPos
 *
 * ----------------------------------------------------------------
 */
void ExecVecSortMarkPos(VecSortState* node)
{
    /*
     * if we haven't sorted yet, just return
     */
    if (!node->sort_Done) {
        return;
    }

    batchsort_markpos((Batchsortstate*)node->tuplesortstate);
}

/* ----------------------------------------------------------------
 *
 *		ExecVecSortRestrPos
 *
 *		Calls tuplesort to restore the last saved sort file position.
 * ----------------------------------------------------------------
 */
void ExecVecSortRestrPos(VecSortState* node)
{
    /*
     * if we haven't sorted yet, just return.
     */
    if (!node->sort_Done) {
        return;
    }

    /*
     * restore the scan to the previously marked position
     */
    batchsort_restorepos((Batchsortstate*)node->tuplesortstate);
}

void ExecReScanVecSort(VecSortState* node)
{
    int paramno = 0;
    int part_itr = 0;
    ParamExecData* param = NULL;
    bool need_switch_part = false;

    if (node->ss.ps.plan->ispwj) {
        paramno = node->ss.ps.plan->paramno;
        param = &(node->ss.ps.state->es_param_exec_vals[paramno]);
        part_itr = (int)param->value;

        if (part_itr != node->ss.currentSlot) {
            need_switch_part = true;
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
            VecExecReScan(node->ss.ps.lefttree);
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
            batchsort_end((Batchsortstate*)node->tuplesortstate);
            node->tuplesortstate = NULL;
        }

        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (node->ss.ps.lefttree->chgParam == NULL) {
            VecExecReScan(node->ss.ps.lefttree);
        }
    } else {
        if (node->ss.ps.plan->ispwj && need_switch_part) {
            node->sort_Done = false;
            if (node->tuplesortstate != NULL) {
                batchsort_end((Batchsortstate*)node->tuplesortstate);
                node->tuplesortstate = NULL;
            }

            /*
             * if chgParam of subnode is not null then plan will be re-scanned by
             * first ExecProcNode.
             */
            VecExecReScan(node->ss.ps.lefttree);
        } else {
            batchsort_rescan((Batchsortstate*)node->tuplesortstate);
        }
    }
}

/*
 * @Description: Early free the memory for VecSort.
 *
 * @param[IN] node:  vector executor state for Sort
 * @return: void
 */
void ExecEarlyFreeVecSort(VecSortState* node)
{
    PlanState* plan_state = &node->ss.ps;

    if (plan_state->earlyFreed) {
        return;
    }

    SO_printf("ExecEarlyFreeVecSort start\n");

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
    if (node->tuplesortstate != NULL) {
        batchsort_end((Batchsortstate*)node->tuplesortstate);
    }
    node->tuplesortstate = NULL;

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Sort "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));

    SO_printf("ExecEarlyFreeVecSort end\n");
}

/*
 * @Description	: Check if there is a limit node on the up level of current sort node.
 * @in node		: Current sort node.
 * @in plantree	: The whole plan tree pushed down into the datanode.
 * @retun		: Return true if the parent node of current sort node is limit.
 */
bool MatchLimitNode(Sort* node, Plan* plan_tree)
{
    bool be_matched = false;

    /* return back if current node is NULL */
    if (plan_tree == NULL) {
        return false;
    }

    /* no need to match if current node's plan_node_id is bigger than the sort's
     * plan_node_id.
     */
    if (plan_tree->plan_node_id > node->plan.plan_node_id) {
        return false;
    }

    /* If current node's plan_node_id equals to the sort's parent node id and
     * current node is VecLimit, return true, else traversal left-substree and
     * right-subtree until match the limit node.
     */
    if (plan_tree->plan_node_id == node->plan.parent_node_id && nodeTag(plan_tree) == T_VecLimit) {
        return true;
    }

    /* visit left-subtree only when it is not null. */
    if (plan_tree->lefttree) {
        be_matched = MatchLimitNode(node, plan_tree->lefttree);
    }

    /* try to visit right-subtree when no match node is found. */
    if (!be_matched) {
        be_matched = MatchLimitNode(node, plan_tree->righttree);
    }

    return be_matched;
}
