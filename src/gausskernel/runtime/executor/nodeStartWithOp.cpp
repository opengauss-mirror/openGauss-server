/* -------------------------------------------------------------------------
 *
 * nodeStartWithOp.cpp
 *	  routines to handle StartWithOp operator a.w.k. START WITH ... CONNECT BY
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeStartWithOp.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodeCtescan.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/cash.h"
#include "miscadmin.h"

#define SWCB_DEFAULT_NULLSTR "null"

typedef enum StartWithOpExecStatus {
    SWOP_UNKNOWN = 0,
    SWOP_BUILD = 1,
    SWOP_EXECUTE,
    SWOP_FINISH,
    SWOP_ESCAPE
} StartWithOpExecStatus;

#define KEY_START_TAG "{"
#define KEY_SPLIT_TAG "/"
#define KEY_CLOSE_TAG "}"

#define ITRCYCLE_CHECK_LIMIT  50
#define DEPTH_CRITERION       100000
#define ITR_MAX_TUPLES        80000
#define MAX_SIBLINGS_TUPLE    1000000000

char* get_typename(Oid typid);

static void ProcessPseudoReturnColumns(StartWithOpState *state);
static AttrNumber FetchRUItrTargetEntryResno(StartWithOpState *state);

/* functions to check and set pseudo column value */
static void CheckIsCycle(StartWithOpState *state, bool *connect_by_iscycle);
static void CheckIsLeaf(StartWithOpState *state, bool *connect_by_isleaf);
static void UpdatePseudoReturnColumn(StartWithOpState *state,
                                       TupleTableSlot *slot,
                                       bool isleaf, bool iscycle);


/* functions to get/fetch array_key content */
static List *TupleSlotGetKeyArrayList(TupleTableSlot *slot);
static List *GetCurrentArrayColArray(const FunctionCallInfo fcinfo,
                const char *value, char **raw_array_str, bool *isConstArrayList = NULL);
static AttrNumber GetInternalArrayAttnum(TupleDesc tupDesc, AttrNumber origVarAttno);
static const char *GetPseudoArrayTargetValueString(TupleTableSlot *slot,
                AttrNumber attnum);
static List *CovertInternalArrayStringToList(char *raw_array_str);

/* Local functions used in tuple-conversion from RecursiveUnion -> StartWith */
static Datum ConvertRuScanArrayAttr(RecursiveUnionState *rustate,
                                       TargetEntry *te, TupleTableSlot *srcSlot, bool inrecursing);
static const char *GetCurrentValue(TupleTableSlot *slot, AttrNumber attnum);
static const char *GetCurrentWorkTableScanPath(TupleTableSlot *srcSlot,
                AttrNumber pseudo_attnum);
static bytea *GetCurrentSiblingsArrayPath(TupleTableSlot *srcSlot,
                AttrNumber pseudo_attnum);
static const char *GetKeyEntryArrayStr(RecursiveUnionState *state,
                TupleTableSlot *scanSlot);
static bool PRCCanSkip(StartWithOpState *state, int prcType);

/* PRC optimization */
static bytea *GetSiblingsKeyEntry(RecursiveUnionState *state, bytea *path);
static inline AttrNumber PseudoResnameGetOriginResno(const char *resname)
{
    /* resname is in attr_key/col_.. format */
    AttrNumber attno = atoi((char *)resname + strlen("array_col_"));

    return attno;
}

static inline void ResetResultSlotAttValueArray(StartWithOpState *state,
            Datum *values, bool *isnull)
{
    errno_t rc = EOK;

    /* confirm the same *values* and *isnull* pointer to avoid mis-use */
    Assert (state->sw_values != NULL && state->sw_values == values);
    Assert (state->sw_isnull != NULL && state->sw_isnull == isnull);

    int natts = list_length(state->ps.plan->targetlist);
    size_t valuesArraySize = natts * sizeof(Datum);
    size_t isnullArraySize = natts * sizeof(bool);

        rc = memset_s(state->sw_values, valuesArraySize, 0, valuesArraySize);
    securec_check(rc, "\0", "\0");

    rc = memset_s(state->sw_isnull, isnullArraySize, (bool)false, isnullArraySize);
    securec_check(rc, "\0", "\0");
}

extern char* pg_strrstr(char* string, const char* subStr);

/*
 * Compare function for any siblings key entry
 * Offset represent the start offset index in array_siblings pseudo column
 */
static int SibglingsKeyEntryCmp(bytea* arg1, bytea* arg2, int offset)
{
    int entryIdx = 4;
    int cmp = 0;
    char *c1 = VARDATA_ANY(arg1);
    char *c2 = VARDATA_ANY(arg2);

    while (entryIdx > 0) {
        cmp = c1[offset + entryIdx - 1] - c2[offset + entryIdx - 1];

        if (cmp != 0) {
            break;
        }

        entryIdx--;
    }

    return cmp;
}

/*
 * Dummy compare function for array_siblings pseudo column
 */
extern int SibglingsKeyCmpFast(Datum x, Datum y, SortSupport ssup)
{
    /*
     * Let SibglingsKey compare function looks like other sort type function.
     * Now just return 0 to fast array_siblings compare function, and we
     * do not have any ways to compare them fast because array_siblings type is customized.
     */
    return 0;
}

/*
 * Compare function for array_siblings pseudo column
 */
extern int SibglingsKeyCmp(Datum x, Datum y, SortSupport ssup)
{
    int i = 0;
    int cmp = 0;
    const int bucketSize = 4;

    bytea* arg1 = DatumGetByteaP(x);
    bytea* arg2 = DatumGetByteaP(y);

    if (arg1 == NULL || arg2 == NULL) {
        return cmp;
    }

    int len1 = VARSIZE_ANY_EXHDR(arg1);
    int len2 = VARSIZE_ANY_EXHDR(arg2);
    int round = Min(len1, len2);
    round /= bucketSize;

    while (i < round) {
        int entryIdx = i * bucketSize;

        cmp = SibglingsKeyEntryCmp(arg1, arg2, entryIdx);

        if (cmp != 0) {
            break;
        }

        i++;
    }

    if (cmp == 0) {
        cmp = (len1 < len2) ? -1 : 1;
    }

    /* We can't afford to leak memory here. */
    if (PointerGetDatum(arg1) != x)
        pfree_ext(arg1);
    if (PointerGetDatum(arg2) != y)
        pfree_ext(arg2);

    return cmp;
}

static bool unsupported_filter_walker(Node *node, Node *context_node)
{
    if (node == NULL) {
        return false;
    }

    if (!IsA(node, SubPlan)) {
        return expression_tree_walker(node, (bool (*)()) unsupported_filter_walker, node);
    }

    /*
     * Currently we do not support subqueries
     * pushed down to connect-by clauses.
     */
    ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Unsupported subquery found in connect by clause.")));

    return true;
}

/*
 * --------------------------------------------------------------------------------------
 * - EXPORT FUNCTIONS..
 *   (1). Standard SQL engine exeuctor functions
 *   (2). Helper fucntions that StartWith execution
 *   (3). builtin functions that support StartWith execution
 * --------------------------------------------------------------------------------------
 */

/*
 * Executor functions
 *   - ExecInitStartWithOp()
 *   - ExecStartWithOp()
 *   - ExecEndStartWithOp()
 *   - ExecReScanStartWithOp()
 */
StartWithOpState* ExecInitStartWithOp(StartWithOp* node, EState* estate, int eflags)
{
    /*
     * Error-out unsupported cases before they
     * actually cause any harm.
     */
    expression_tree_walker((Node*)node->plan.qual,
            (bool (*)())unsupported_filter_walker, NULL);

    /*
     * create state structure
     */
    StartWithOpState *state = makeNode(StartWithOpState);
    state->ps.plan = (Plan *)node;
    state->ps.state = estate;

    state->sw_context = AllocSetContextCreate(CurrentMemoryContext,
        "StartWith Inner",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &state->ps);

    state->ps.ps_TupFromTlist = false;

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &state->ps);

    /*
     * initialize child expressions
     */
    state->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist,
                (PlanState*)state);
    state->ps.qual = (List*)ExecInitExpr((Expr*)node->plan.qual, (PlanState*)state);

    /*
     * initialize child nodes
     */
    outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * we don't use inner plan
     */
    Assert(innerPlan(node) == NULL);

    /*
     * initialize tuple type and projection info
     * no relations are involved in nodeResult, set the default
     * tableAm type to HEAP
     */
    ExecAssignResultTypeFromTL(&state->ps, TAM_HEAP);

    ExecAssignProjectionInfo(&state->ps, NULL);

    /* Setup pseudo return column calculation related variables */
    /* 1. assisng psduco column targetEntry */
    List *targetlist = node->plan.targetlist;
    ListCell *lc = NULL;
    TargetEntry *tle = NULL;
    foreach (lc, targetlist) {
        tle = (TargetEntry *)lfirst(lc);
        if (!IsPseudoReturnTargetEntry(tle)) {
            continue;
        }

        StartWithOpColumnType type = GetPseudoColumnType(tle);
        state->sw_pseudoCols[type] = tle;
    }

    /*
     *  2. setup keyAttnum
     *
     *  Note: We expect keyEntryList to be non-NIL.
     */
    if (node->keyEntryList != NIL) {
        TargetEntry *keyTEntry = (TargetEntry *)list_nth(node->internalEntryList, 1);
        Assert (pg_strncasecmp(keyTEntry->resname, "array_key_", strlen("array_key_")) == 0);
        state->sw_keyAttnum = keyTEntry->resno;
    }

    /*
     * 3. setup tuplestore, tupleslot and status controlling variables,
     *
     * Note: tuple store of backupTable and resultTable is not setup here,
     *       instead we do their intialization in on demand
     */
    state->swop_status = SWOP_BUILD;
    state->sw_workingTable = tuplestore_begin_heap(false, false,
            u_sess->attr.attr_memory.work_mem);

    /* create a temp tuplestore to help isleaf/iscycle calculation */
    state->sw_backupTable = tuplestore_begin_heap(
                            false, false, u_sess->attr.attr_memory.work_mem);
    state->sw_resultTable = tuplestore_begin_heap(
                            false, false, u_sess->attr.attr_memory.work_mem);

    /* create the working TupleTableslot */
    state->sw_workingSlot = ExecAllocTableSlot(&estate->es_tupleTable, TAM_HEAP);
    ExecSetSlotDescriptor(state->sw_workingSlot, ExecTypeFromTL(targetlist, false));

    int natts = list_length(node->plan.targetlist);
    state->sw_values = (Datum *)palloc0(natts * sizeof(Datum));
    state->sw_isnull=  (bool *)palloc0(natts * sizeof(bool));
    ResetResultSlotAttValueArray(state, state->sw_values, state->sw_isnull);

    /* Setup other variables to NULL as they will be initialized later */
    state->sw_curKeyArrayStr = NULL;
    state->sw_cycle_rowmarks = NULL;

    /* Initialize nocycle controling flag */
    state->sw_nocycleStopOrderSiblings = false;

    /* set underlying RecursiveUnionState pointing to current curernt node */
    if (node->swoptions->siblings_orderby_clause != NULL) {
        SortState *sstate = (SortState *)outerPlanState(state);
        RecursiveUnionState *rustate = (RecursiveUnionState *)outerPlanState(sstate);
        rustate->swstate = state;
    } else {
        RecursiveUnionState *rustate = (RecursiveUnionState *)outerPlanState(state);
        rustate->swstate = state;
    }

    /* init other elements */
    state->sw_level = 0;
    state->sw_numtuples = 0;

    return state;
}

bool CheckCycleExeception(StartWithOpState *node, TupleTableSlot *slot)
{
    RecursiveUnionState *rustate = NULL;
    StartWithOp *swplan = (StartWithOp *)node->ps.plan;
    bool   incycle = false;

    if (swplan->swoptions->siblings_orderby_clause != NULL) {
        SortState *sstate = (SortState *)node->ps.lefttree;
        rustate = (RecursiveUnionState *)sstate->ss.ps.lefttree;
    } else {
        rustate = (RecursiveUnionState *)node->ps.lefttree;
    }

    Assert (IsA(rustate, RecursiveUnionState));

    if (IsConnectByLevelStartWithPlan(swplan) || node->sw_keyAttnum == 0) {
        /* for connect by level case, we do not do cycle check */
        return false;
    }

    /*
     * If current current iteration is greator than cycle check threshold, we are going
     * to do real check and error out cycle cases, we do not this check at begin is for
     * performance considerations
     */
    node->sw_curKeyArrayStr = GetPseudoArrayTargetValueString(slot, node->sw_keyAttnum);
    CheckIsCycle(node, &incycle);

    /* compatible with ORA behavior, if NOCYCLE is not set, we report error */
    if (!swplan->swoptions->nocycle && incycle) {
        ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                errmsg("START WITH .. CONNECT BY statement runs into cycle exception")));
    }

    /*
     * Free per-tuple keyArrayStr in nocycle case, normally we need use sw_curKeyArrayStr
     * to output the cycle information at caller function and let executor stop to free it
     * eventually
     */
    if (!incycle) {
        pfree_ext(node->sw_curKeyArrayStr);
    }

    return incycle;
}


/*
 * Peeking a tuple's connectable descendants for exactly one level.
 * It seems to be incurring no significant time / memory overhead compared to
 * BFS CONNECT BY so far.
 *
 * This peek is still done by Recursive Union. But its scale is much smaller
 * for at each iteration we pushes only one tuple into the outer node
 * so that RU only returns one tuple's connectables.
 *
 * Does not support order siblings by yet.
 */
static List* peekNextLevel(TupleTableSlot* startSlot, PlanState* outerNode, int level)
{
    List* queue = NULL;
    RecursiveUnionState* rus = (RecursiveUnionState*) outerNode;
    StartWithOpState *swnode = rus->swstate;
    /* clean up RU's old working table */
    ExecReScan(outerNode);
    /* pushing the depth-first tuple into RU's working table */
    rus->recursing = true;
    tuplestore_puttupleslot(rus->working_table, startSlot);

    /* fetch the depth-first tuple's children exactly one level below */
    rus->iteration = level;
    int begin_iteration = rus->iteration;
    int begin_rowCount = swnode->sw_rownum;
    TupleTableSlot* srcSlot = NULL;
    for (;;) {
        swnode->sw_rownum = begin_rowCount;
        srcSlot = ExecProcNode(outerNode);
        if (TupIsNull(srcSlot) || (rus->iteration != begin_iteration)) break;
        TupleTableSlot* newSlot = MakeSingleTupleTableSlot(srcSlot->tts_tupleDescriptor);
        newSlot = ExecCopySlot(newSlot, srcSlot);
        queue = lappend(queue, newSlot);
    }

    return queue;
}

/*
 * Construct CONNECT BY result set by depth-first order.
 *
 * Does not support order siblings by yet.
 */
static bool depth_first_connect(int currentLevel, StartWithOpState *node, List* queue, int* dfsRowCount)
{
    CHECK_FOR_INTERRUPTS();

    bool isCycle = false;
    if (queue == NULL) {
        return isCycle;
    }
    Tuplestorestate* outputStore = node->sw_workingTable;
    PlanState* outerNode = outerPlanState(node);

    /* loop until all siblings' DFS are done */
    for (;;) {
        if (queue->head == NULL || node->swop_status == SWOP_ESCAPE) {
            return isCycle;
        }
        TupleTableSlot* leader = (TupleTableSlot*) lfirst(queue->head);
        if (leader == NULL || TupIsNull(leader)) {
            return isCycle;
        }

        /* DFS: output the depth-first tuple to result table */
        TupleTableSlot* dstSlot = leader;

        queue->head = queue->head->next;

        if (CheckCycleExeception(node, dstSlot)) {
            // cycled slots are not kept
            isCycle = true;
            continue;
        }

        tuplestore_puttupleslot(outputStore, dstSlot);
        (*dfsRowCount)++;
        int rowCountBefore = *dfsRowCount;

        /* Go into the depth NOW: sibling tuples won't get processed
         *  until all children are done */
        node->sw_rownum = rowCountBefore;
        bool expectCycle = depth_first_connect(currentLevel + 1, node,
                                               peekNextLevel(leader, outerNode, currentLevel),
                                               dfsRowCount);
        if (expectCycle) {
            node->sw_cycle_rowmarks = lappend_int(node->sw_cycle_rowmarks, rowCountBefore);
        }
    }
    return isCycle;
}

static List* makeStartTuples(StartWithOpState *node)
{
    PlanState* outerNode = outerPlanState(node);
    List* startWithQueue = NULL;
    RecursiveUnionState* rus = (RecursiveUnionState*) outerNode;
    TupleTableSlot* dstSlot = ExecProcNode(outerNode);
    int begin_iteration = rus->iteration;
    for (;;) {
        if (TupIsNull(dstSlot) || rus->iteration != begin_iteration) {
            break;
        }
        TupleTableSlot* newSlot = MakeSingleTupleTableSlot(dstSlot->tts_tupleDescriptor);
        newSlot = ExecCopySlot(newSlot, dstSlot);
        startWithQueue = lappend(startWithQueue, newSlot);
        dstSlot = ExecProcNode(outerNode);
    }
    return startWithQueue;
}

void markSWLevelBegin(StartWithOpState *node)
{
    if (node->ps.instrument == NULL) {
        return;
    }
    gettimeofday(&(node->iterStats.currentStartTime), NULL);
}

void markSWLevelEnd(StartWithOpState *node, int64 rowCount)
{
    if (node->ps.instrument == NULL) {
        return;
    }
    node->iterStats.totalIters = node->iterStats.totalIters + 1;
    int bufIndex = node->iterStats.totalIters - 1;

    if (bufIndex >= SW_LOG_ROWS_FULL) {
        bufIndex = SW_LOG_ROWS_HALF + (bufIndex % SW_LOG_ROWS_HALF);
    }

    node->iterStats.levelBuf[bufIndex] = node->iterStats.totalIters;
    node->iterStats.rowCountBuf[bufIndex] = rowCount;
    node->iterStats.startTimeBuf[bufIndex] = node->iterStats.currentStartTime;
    gettimeofday(&(node->iterStats.endTimeBuf[bufIndex]), NULL);
}

/*
 * @Function: ExecStartWithRowLevelQual()
 *
 * @Brief:
 *         Check if LEVEL/ROWNUM conditions still hold for the recursion to
 *         continue. Level and rownum should have been made available
 *         in dstSlot by ConvertStartWithOpOutputSlot() already.
 *
 * @Input node: The current recursive union node.
 * @Input slot: The next slot to be scanned into start with operator.
 *
 * @Return: Boolean flag to tell if the iteration should continue.
 */
bool ExecStartWithRowLevelQual(RecursiveUnionState* node, TupleTableSlot* dstSlot)
{
    StartWithOp* swplan = (StartWithOp*)node->swstate->ps.plan;
    if (!IsConnectByLevelStartWithPlan(swplan)) {
        return true;
    }

    ExprContext* expr = node->swstate->ps.ps_ExprContext;

    /*
     * Level and rownum pseudo attributes are extracted from StartWithOpPlan
     * node so we set filtering tuple as ecxt_scantuple
     */
    expr->ecxt_scantuple = dstSlot;
    if (!ExecQual(node->swstate->ps.qual, expr, false)) {
        return false;
    }
    return true;
}

static bool isStoppedByRowNum(RecursiveUnionState* node, TupleTableSlot* slot)
{
    TupleTableSlot* dstSlot = node->swstate->ps.ps_ResultTupleSlot;
    bool ret = false;

    /* 1. roll back to the converted result row */
    node->swstate->sw_rownum--;
    /* 2. roll back to one row before execution to check rownum stop condition */
    node->swstate->sw_rownum--;
    dstSlot = ConvertStartWithOpOutputSlot(node->swstate, slot, dstSlot);
    if (ExecStartWithRowLevelQual(node, dstSlot)) {
        ret = true;
    }
    /* undo the rollback after conversion (yes, just one line) */
    node->swstate->sw_rownum++;
    return ret;
}

TupleTableSlot* GetStartWithSlot(RecursiveUnionState* node, TupleTableSlot* slot)
{
    TupleTableSlot* dstSlot = node->swstate->ps.ps_ResultTupleSlot;
    dstSlot = ConvertStartWithOpOutputSlot(node->swstate, slot, dstSlot);

    if (!ExecStartWithRowLevelQual(node, dstSlot)) {
        StartWithOpState *swnode = node->swstate;
        StartWithOp *swplan = (StartWithOp *)swnode->ps.plan;
        PlanState      *outerNode = outerPlanState(swnode);
        bool isDfsEnabled = swplan->swoptions->nocycle && !IsA(outerNode, SortState);
        /*
         * ROWNUM/LEVEL limit reached:
         * Tell ExecRecursiveUnion to terminate the recursion by returning NULL
         *
         * Specifically for ROWNUM limit reached:
         * Tell DFS routine to stop immediately by setting SW status to ESCAPE.
         */
        node->swstate->swop_status = isDfsEnabled && isStoppedByRowNum(node, slot) ?
                                     SWOP_ESCAPE :
                                     node->swstate->swop_status;
        return NULL;
    }

    return dstSlot;
}

TupleTableSlot* ExecStartWithOp(StartWithOpState *node)
{
    TupleTableSlot *dstSlot = node->ps.ps_ResultTupleSlot;
    PlanState      *outerNode = outerPlanState(node);
    StartWithOp    *swplan = (StartWithOp *)node->ps.plan;
    /* initialize row num count */
    node->sw_rownum = 0;

    Assert (node->sw_workingTable != NULL);
    /* start processing */
    switch (node->swop_status) {
        case SWOP_BUILD: {
            Assert (node->sw_workingTable != NULL);
            markSWLevelBegin(node);
            bool isDfsEnabled = swplan->swoptions->nocycle && !IsA(outerNode, SortState);
            if (isDfsEnabled) {
                /* For nocycle and non-order-siblings cases we use
                 * depth-first connect by to achieve result consistency.
                 */

                /* Kick off dfs with StartWith tuples */
                int dfsRowCount = 0;
                depth_first_connect(1, node, makeStartTuples(node), &dfsRowCount);
            }

            /* Otherwise, use breadth-first style connect-by routine */
            /* Materialize all content to make internal part ready */
            for (; !isDfsEnabled;) {
                /*
                 * We check for interrupts here because infinite loop might happen.
                 */
                CHECK_FOR_INTERRUPTS();

                /*
                 * The actual executions and conversions are done
                 * in the underlying recursve union node.
                 */
                dstSlot = ExecProcNode(outerNode);

                if (TupIsNull(dstSlot)) {
                    break;
                }

                tuplestore_puttupleslot(node->sw_workingTable, dstSlot);

                /*
                 * check we need stop infinit recursive iteration if NOCYCLE is specified in
                 * ConnectByExpr, then return. Also, in case of cycle-report-error the ereport
                 * is processed inside of CheckCycleException()
                 */
                if (CheckCycleExeception(node, dstSlot)) {
                    break;
                }
            }

            /* report we have done material step for current StartWithOp node */
            ereport(DEBUG1,
                    (errmodule(MOD_EXECUTOR),
                    errmsg("[SWCB DEBUG] StartWithOp[%d] finish material-step and forward to PRC-step with level:%d rownum_total:%lu",
                    node->ps.plan->plan_node_id,
                    node->sw_level,
                    node->sw_rownum)));

            /* calculate pseudo output column */
            ProcessPseudoReturnColumns(node);

            /*
             * After all tuples is stored, we rewind the tuplestore and mark current
             * StartWithOp node status into EXECUTE
             */
            tuplestore_rescan(node->sw_workingTable);
            node->swop_status = SWOP_EXECUTE;
        }

        /* fall-thru for first time */
        case SWOP_EXECUTE: {
            dstSlot = node->ps.ps_ResultTupleSlot;
            (void)tuplestore_gettupleslot(node->sw_resultTable, true, false, dstSlot);

            if (TupIsNull(dstSlot)) {
                node->swop_status = SWOP_FINISH;
            }

            break;
        }

        default: {
            elog(ERROR, "known status status in %s(), with context:%s",
                __FUNCTION__,
                nodeToString(node->ps.plan));
        }
    }

    return (TupleTableSlot *)dstSlot;
}

void ExecEndStartWithOp(StartWithOpState *node)
{
    tuplestore_clear(node->sw_workingTable);
    tuplestore_clear(node->sw_backupTable);
    tuplestore_clear(node->sw_resultTable);
    node->swop_status = SWOP_UNKNOWN;

    pfree_ext(node->sw_values);
    pfree_ext(node->sw_isnull);

    /*
     * clean out the upper tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    if (node->sw_context) {
        MemoryContextDelete(node->sw_context);
    }

    ExecEndNode(outerPlanState(node));

    return;
}

void ExecReScanStartWithOp(StartWithOpState *state)
{
    PlanState* outer_plan = outerPlanState(state);

    (void)ExecClearTuple(state->ps.ps_ResultTupleSlot);

    if (outer_plan->chgParam == NULL) {
        ExecReScan(outer_plan);
    }

    state->swop_status = SWOP_BUILD;
    ResetResultSlotAttValueArray(state, state->sw_values, state->sw_isnull);

    tuplestore_clear(state->sw_workingTable);
    tuplestore_clear(state->sw_backupTable);
    tuplestore_clear(state->sw_resultTable);

    state->sw_nocycleStopOrderSiblings = false;

    /* init other elements */
    state->sw_level = 0;
    state->sw_numtuples = 0;

    return;
}

/*
 * Helper functions for StartWith Operation
 */
StartWithOpColumnType GetPseudoColumnType(const char *resname)
{
    StartWithOpColumnType result = SWCOL_UNKNOWN;

    /*
     * some plan node's resname is not assign explicit value, so just return and
     * consider them as regular column
     */
    if (resname == NULL) {
        return SWCOL_REGULAR;
    }

    if (pg_strcasecmp(resname, "level") == 0) {
        result = SWCOL_LEVEL;
    } else if (pg_strcasecmp(resname, "connect_by_isleaf") == 0) {
        result = SWCOL_ISLEAF;
    } else if (pg_strcasecmp(resname, "connect_by_iscycle") == 0) {
        result = SWCOL_ISCYCLE;
    } else if (pg_strcasecmp(resname, "ruitr") == 0) {
        result = SWCOL_RUITR;
    } else if (pg_strcasecmp(resname, "array_siblings") == 0) {
        result = SWCOL_ARRAY_SIBLINGS;
    } else if (pg_strncasecmp(resname, "array_key_", strlen("array_key_")) == 0) {
        result = SWCOL_ARRAY_KEY;
    } else if (pg_strncasecmp(resname, "array_col_", strlen("array_col_")) == 0) {
        result = SWCOL_ARRAY_COL;
    } else if (pg_strcasecmp(resname, "rownum") == 0) {
        result = SWCOL_ROWNUM;
    } else {
        result = SWCOL_REGULAR;
    }

    return result;
}


StartWithOpColumnType GetPseudoColumnType(const TargetEntry *entry)
{
    StartWithOpColumnType result = GetPseudoColumnType(entry->resname);

    return result;
}

bool IsPseudoReturnColumn(const char *colname)
{
    Assert (colname != NULL);
    StartWithOpColumnType type = GetPseudoColumnType(colname);

    return (type == SWCOL_LEVEL || type == SWCOL_ISLEAF ||
             type == SWCOL_ISCYCLE || type == SWCOL_ROWNUM);
}

/*
 *  - srcSlot: the slot returned from RecursiveUnion (table column + internal TE)
 *  - dstSlot: the slot will return to CteScan (table column + pseudo return colum + internal TE)
 */
TupleTableSlot *ConvertStartWithOpOutputSlot(StartWithOpState *node,
                                              TupleTableSlot *srcSlot,
                                              TupleTableSlot *dstSlot)
{
    /*
     * Process start-with pseodu return columns from pseodu tagetentry
     * CteScan: col[1......n], level, is_leaf, is_cycle, RUITR, array_name[1,2,...... m]
     *                         | pseudo return column| | <<- pseudo internal column ->>|
     */
    HeapTuple tup = ExecMaterializeSlot(srcSlot);
    HeapTuple tup_new = NULL;
    TupleDesc srcTupDesc = srcSlot->tts_tupleDescriptor;
    TupleDesc dstTupDesc = dstSlot->tts_tupleDescriptor;

    /* Reset internal value/isnull array */
    ResetResultSlotAttValueArray(node, node->sw_values, node->sw_isnull);
    Datum *values = node->sw_values;
    bool  *isnull = node->sw_isnull;

    /* Reset dstslot and memorycontext */
    (void)MemoryContextReset(node->sw_context);
    MemoryContext oldcxt = MemoryContextSwitchTo(node->sw_context);

    /*
     * Step 1, fill StartWithOp's pseudo internal colum
     *
     * Note: As the conversion share the same values/isnull array, we do tail->front
     * element-copy to avoid overlaping
     */
    heap_deform_tuple(tup, srcTupDesc, values, isnull);
    StartWithOp *swplan = (StartWithOp *)node->ps.plan;
    RecursiveUnion *ruplan = (RecursiveUnion *)swplan->ruplan;
    int plist_len = list_length(swplan->internalEntryList);
    TargetEntry *srcEntry = NULL;
    TargetEntry *dstEntry = NULL;

    Assert (list_length(swplan->internalEntryList) ==
            list_length(ruplan->internalEntryList));

    for (int i = 0; i < plist_len; i++) {
        srcEntry = (TargetEntry *)list_nth(ruplan->internalEntryList, plist_len - i - 1);
        dstEntry = (TargetEntry *)list_nth(swplan->internalEntryList, plist_len - i - 1);

        values[dstEntry->resno - 1] = values[srcEntry->resno - 1];
        isnull[dstEntry->resno - 1] = isnull[srcEntry->resno - 1];
    }

    /* Step2, fill StartWithOp's pseudo return columns */
    ListCell *lc = NULL;
    foreach (lc, swplan->plan.targetlist) {
        TargetEntry *te = (TargetEntry *)lfirst(lc);
        if (!IsPseudoReturnTargetEntry(te)) {
            continue;
        }

        if (pg_strcasecmp(te->resname, "level") == 0) {
            AttrNumber att = FetchRUItrTargetEntryResno(node);
            int level = DatumGetInt32(values[att - 1]);

            /* Set level/ruitr value */
            if (node->sw_level != level) {
                node->sw_level = level;
                node->sw_numtuples = 0;
            }
            values[te->resno - 1] = values[att - 1] + 1;
            isnull[te->resno - 1] = false;
        } else if (pg_strcasecmp(te->resname, "connect_by_isleaf") == 0) {
            values[te->resno - 1] = 0;
            isnull[te->resno - 1] = true;
        } else if (pg_strcasecmp(te->resname, "connect_by_iscycle") == 0) {
            values[te->resno - 1] = 0;
            isnull[te->resno - 1] = true;
        } else if (pg_strcasecmp(te->resname, "rownum") == 0) {
            values[te->resno - 1] = node->sw_rownum + 1;
            isnull[te->resno - 1] = false;
        }
    }

    tup_new = heap_form_tuple(dstTupDesc, values, isnull);
    dstSlot = ExecStoreTuple(tup_new, dstSlot, InvalidBuffer, false);

    MemoryContextSwitchTo(oldcxt);

    /* update row num count before returning */
    node->sw_rownum++;
    node->sw_numtuples++;

    return dstSlot;
}

/*
 * --------------------------------------------------------------------------------------
 * -Internal functinos
 * --------------------------------------------------------------------------------------
 */
/*
 * Build in functions that support start with
 */
Datum sys_connect_by_path(PG_FUNCTION_ARGS)
{
    Datum resultDatum = (Datum)0;
    const char *value = NULL;
    const char *split = NULL;
    bool constArrayList = false;

    /* check invalid spliter */
    if (PG_GETARG_DATUM(1) == (Datum)0) {
        elog(ERROR, "illegal parameter in SYS_CONNECT_BY_PATH function");
    }

    if (PG_GETARG_DATUM(0) == (Datum)0) {
        value = SWCB_DEFAULT_NULLSTR;
    } else {
        value = TextDatumGetCString(PG_GETARG_TEXT_PP(0));
    }
    split = TextDatumGetCString(PG_GETARG_TEXT_PP(1));


    /*
     * step[1]:
     *   Fetch current processed ScanTuple and its internal array_col, then convert
     *   it into token_list and do proper calculation for sys_connect_by_path()
     */
    char *raw_array_str = NULL;
    List *token_list = GetCurrentArrayColArray(fcinfo, value, &raw_array_str, &constArrayList);

    ListCell *token = NULL;

    if (!constArrayList) {
        bool valid = false;
        foreach (token, token_list) {
            const char *curValue = (const char *)lfirst(token);
            if (strcmp(TrimStr(value), TrimStr(curValue)) == 0) {
                valid = true;
                break;
            }
        }

        if (!valid) {
            elog(ERROR, "node value is not in path (value:%s path:%s)", value, raw_array_str);
        }
    }

    /*
     * step[2]:
     *   start to build result array string
     */
    StringInfoData si;
    initStringInfo(&si);

    foreach (token, token_list) {
        const char *node = (const char *)lfirst(token);
        appendStringInfo(&si, "%s%s", split, node);
    }

    resultDatum = CStringGetTextDatum(si.data);
    pfree_ext(si.data);

    PG_RETURN_DATUM(resultDatum);
}

/*
 *  ledger_hist_check -- check whether user table hash and history table hash are equal
 *
 *  parameter1: user table name [type: text]
 *  parameter2: namespace of user table [type: text]
 **/
Datum connect_by_root(PG_FUNCTION_ARGS)
{
    Datum datum = (Datum)0;
    const char *value = TextDatumGetCString(PG_GETARG_TEXT_PP(0));
    bool constArrayList = false;

    /*
     * step[1]:
     *   Fetch current processed ScanTuple and its internal array_col, then convert
     *   it into token_list and do proper calculation for connect_by_root()
     */
    char *raw_array_str = NULL;
    List *token_list = GetCurrentArrayColArray(fcinfo, value, &raw_array_str, &constArrayList);
    ListCell *lc = NULL;
    bool valid = false;
    foreach (lc, token_list) {
        const char *curValue = (const char *)lfirst(lc);
        if (strcmp(TrimStr(value), TrimStr(curValue)) == 0) {
            valid = true;
            break;
        }
    }

    if (!valid) {
        elog(ERROR, "node value is not in path (value:%s path:%s)", value, raw_array_str);
    }

    /*
     * step[2]:
     *   start to build result string value
     */
    datum = CStringGetTextDatum((char *)linitial(token_list));

    PG_RETURN_DATUM(datum);
}

/*
 * -brief: array_key cotent in List<tokenString> format
 */
static List *TupleSlotGetKeyArrayList(TupleTableSlot *slot)
{
    Assert (slot != NULL);

    /* 1. find key attnum */
    AttrNumber arrayKeyAttno = InvalidAttrNumber;
    Datum arrayKeyDatum = (Datum)0;
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    HeapTuple tup = ExecFetchSlotTuple(slot);
    bool isnull = true;

    for (int n = 0; n < tupDesc->natts; n++) {
        Form_pg_attribute attr = TupleDescAttr(tupDesc, n);
        if (pg_strncasecmp(attr->attname.data, "array_key", strlen("arra_key")) == 0) {
            arrayKeyAttno = n + 1;
            break;
        }
    }

    if (arrayKeyAttno == InvalidAttrNumber) {
        elog(ERROR, "Internal array_key is not found for sys_connect_by_path()");
    }

    /* 2. fetch key str */
    arrayKeyDatum = heap_getattr(tup, arrayKeyAttno, tupDesc, &isnull);
    Assert (!isnull && arrayKeyDatum != 0);

    char *rawKeyArrayStr = TextDatumGetCString(arrayKeyDatum);
    List *tokenList = CovertInternalArrayStringToList(rawKeyArrayStr);

    return tokenList;;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Get current array_col array from current function invoke context "fcinfo",
 *         value returned in List<const char*>
 *
 * @Note: ScanTuple curernt in processing is held under fcinfo pointer, so we don't pass
 *        the attnum/varattno of which array_col_no to fetch, sounds a little bit
 *        "implicit" but deserving so for more convinience
 *
 * isConstArrayList is an output parameter, indicate current we are handling a CONST
 * expression set in sys_connect_by_path()'s first parameter
 * --------------------------------------------------------------------------------------
 */
static List *GetCurrentArrayColArray(const FunctionCallInfo fcinfo,
            const char *value, char **raw_array_str, bool *isConstArrayList)
{
    List *token_list = NIL;
    TupleTableSlot *slot = NULL;
    Var *variable = NULL;
    *isConstArrayList = false;

    ExprContext *econtext = (ExprContext *)fcinfo->swinfo.sw_econtext;
    ExprState   *exprstate = (ExprState *)fcinfo->swinfo.sw_exprstate;

    /* context check */
    Assert (econtext != NULL && exprstate != NULL);

    /*
     * Oracle "START WITH ... CONNECT BY" only allow single plaint column to be
     * specified, so the eval-context's argument only have one argument with *Var*
     * node ported
     */
    List *vars = pull_var_clause((Node*)exprstate->expr,
                PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

    /* handle case where */
    if (vars == NIL) {
        TupleTableSlot *slot = econtext->ecxt_scantuple;
        List *keyTokenList = NIL;

        if (fcinfo->flinfo->fn_oid == CONNECT_BY_ROOT_FUNCOID && TupIsNull(slot)) {
            /*
             * By semantic, connect_by_root() aiming to return the root value of current
             * recursive tree, if there is a const value set as parameter, its hierachy
             * is considered as /{v}/{v}/{v}..., so simplely return const value itself
             *
             * Note: we can do so only for connect_by_root(), sys_connect_by_path is still
             * processed as /{v}/{v}/{v}
             */
            token_list = list_make1(pstrdup(value));
        } else {
            keyTokenList = TupleSlotGetKeyArrayList(slot);
            for (int n = 0; n < list_length(keyTokenList); n++) {
                token_list = lappend(token_list, pstrdup(value));
            }
        }

        /* mark current list is const array */
        *isConstArrayList = true;

        return token_list;
    }

    /* regular case 1st parm is column name */
    variable = (Var *)linitial(vars);
    if (!IsA(variable, Var)) {
        elog(ERROR, "connect_by_root pass int invalid argument type:%d",
             nodeTag(exprstate->expr));
    }

    /*
     * Get the input slot and attribute number we want
     */
    switch (variable->varno) {
        case INNER_VAR: /* get the tuple from the inner node */
            slot = econtext->ecxt_innertuple;
            break;

        case OUTER_VAR: /* get the tuple from the outer node */
            slot = econtext->ecxt_outertuple;
            break;

            /* INDEX_VAR is handled by default case */
        default: /* get the tuple from the relation being scanned */
            slot = econtext->ecxt_scantuple;
            break;
    }

    Assert(slot != NULL);

    /* error out invalid case */
    if (variable->varattno <= 0) {
        ereport(ERROR,
               (errcode(ERRCODE_INVALID_ATTRIBUTE),
                errmodule(MOD_EXECUTOR),
                errmsg("attribute number %d exceeds number of columns %d", variable->varattno,
                        slot->tts_tupleDescriptor->natts)));
    }

    /* Start do tuple content fetching work */
    HeapTuple tup = ExecFetchSlotTuple(slot);
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    bool isnull = true;

    /* context check */
    Assert (econtext != NULL && exprstate != NULL && !TupIsNull(slot));


    /* step 1. find proper internal array to support */
    AttrNumber origAttnum = variable->varattno;
    AttrNumber arrayColAttnum = GetInternalArrayAttnum(tupDesc , origAttnum);
    Datum arrayColDatum = heap_getattr(tup, arrayColAttnum,
                        slot->tts_tupleDescriptor, &isnull);
    Assert (!isnull && origAttnum != InvalidAttrNumber &&
            arrayColAttnum != InvalidAttrNumber);
    Assert (arrayColAttnum > origAttnum);

    /* step 2. process the internal array and get result */
    *raw_array_str = TextDatumGetCString(arrayColDatum);
    token_list = CovertInternalArrayStringToList(pstrdup(*raw_array_str));

    return token_list;
}

static List *CovertInternalArrayStringToList(char *raw_array_str)
{
    List *token_list = NIL;
    char *token_tmp = NULL;
    char *token = strtok_s(raw_array_str, "/", &token_tmp);
    while (token != NULL) {
        token_list = lappend(token_list, token);
        token = strtok_s(NULL, "/", &token_tmp);
    }

    return token_list;
}

static AttrNumber GetInternalArrayAttnum(TupleDesc tupDesc, AttrNumber origVarAttno)
{
    NameData arrayAttName;
    AttrNumber  arrayAttNum = InvalidAttrNumber;
    errno_t rc = 0;

    rc = memset_s(arrayAttName.data, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    rc = sprintf_s(arrayAttName.data, NAMEDATALEN, "array_col_%d", origVarAttno);
    securec_check_ss(rc, "\0", "\0");

    /* find proper internal array to support */
    for (int n = 0; n < tupDesc->natts; n++) {
        Form_pg_attribute attr = TupleDescAttr(tupDesc, n);
        if (pg_strcasecmp(attr->attname.data, arrayAttName.data) == 0) {
            arrayAttNum = n + 1;
            break;
        }
    }

    if (arrayAttNum == InvalidAttrNumber) {
        elog(ERROR, "Internal array_col for origVarAttno:%d related internal array is not found",
                origVarAttno);
    }

    return arrayAttNum;
}

/*
 * fill start with return columns is_leaf, is_cycle
 */
static const char *GetPseudoArrayTargetValueString(TupleTableSlot *slot, AttrNumber attnum)
{
    const char *raw_array_string = NULL;
    HeapTuple tup = NULL;
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    Datum datum = 0;
    bool  isnull = true;

    /* fetch physical tuple */
    tup = ExecFetchSlotTuple(slot);
    datum = heap_getattr(tup, attnum, tupDesc, &isnull);
    raw_array_string = TextDatumGetCString(datum);

    Assert (!isnull && raw_array_string != NULL);

    return raw_array_string;
}

/*
 * - brief: A helper function to update tupleSlot with given isleaf/iscycle value
 */
static void UpdatePseudoReturnColumn(StartWithOpState *state,
            TupleTableSlot *slot, bool isleaf, bool iscycle)
{
    HeapTuple tup = NULL;
    TupleDesc tupDesc = slot->tts_tupleDescriptor;

    ResetResultSlotAttValueArray(state, state->sw_values, state->sw_isnull);
    Datum *values = state->sw_values;
    bool  *isnull = state->sw_isnull;

    /* fetch physical tuple */
    tup = ExecFetchSlotTuple(slot);
    heap_deform_tuple(tup, tupDesc, values, isnull);

    /* check the unset value isleaf, iscycle is not set yet */
    AttrNumber attnum2 = state->sw_pseudoCols[SWCOL_ISLEAF]->resno;
    AttrNumber attnum3 = state->sw_pseudoCols[SWCOL_ISCYCLE]->resno;
    Assert (values[attnum2 - 1] == 0 && isnull[attnum2 - 1]);
    Assert (values[attnum3 - 1] == 0 && isnull[attnum3 - 1]);

    /* set proper value and mark isnull to false */
    values[attnum2 - 1] = BoolGetDatum(isleaf);
    isnull[attnum2 - 1] = false;
    values[attnum3 - 1] = BoolGetDatum(iscycle);
    isnull[attnum3 - 1] = false;

    /* create a local copy tuple and store it to tuplestore, mark shouldFree as 'true ' */
    tup = heap_form_tuple(tupDesc, values, isnull);
    slot = ExecStoreTuple(tup, slot, InvalidBuffer, true);
}

/*
 * - brief: check if curernt processing tupleSlot is in cycle
 *
 * - return: none, iscycle flag is returned by param "bool *connect_by_iscycle"
 */
static void CheckIsCycle(StartWithOpState *state, bool *connect_by_iscycle)
{
    List     *entry_list = NIL;
    ListCell *entry = NULL;
    char     *keyArrayStrCopy = NULL;

    Assert (state != NULL && state->sw_curKeyArrayStr != NULL &&
            connect_by_iscycle != NULL);

    /* The underlying strok_r() will modify the splited string itself so make a copy */
    keyArrayStrCopy = pstrdup(state->sw_curKeyArrayStr);
    entry_list = CovertInternalArrayStringToList(keyArrayStrCopy);
    foreach (entry, entry_list) {
        const char *node1 = (const char *)lfirst(entry);
        ListCell *rest = lnext(entry);
        while (rest != NULL) {
            const char *node2 = (const char *)lfirst(rest);
            ListCell *next = lnext(rest);
            if (pg_strcasecmp(node1, node2) == 0) {
                *connect_by_iscycle = true;
                return;
            }
            rest = next;
        }
    }

    /* Free the tokenized list as we pass int the copy of sw_curKeyArrayStr */
    list_free_ext(entry_list);
    pfree_ext(keyArrayStrCopy);

    *connect_by_iscycle = false;
}

/*
 * - brief: check if curernt processing tupleSlot is leaf
 *
 * - return: none, iscycle flag is returned by param "bool *connect_by_isleaf"
 */
static void CheckIsLeaf(StartWithOpState *state, bool *connect_by_isleaf)
{
    /* PRC optimization is applied */
    if (PRCCanSkip(state, SWCOL_ISLEAF)) {
        return;
    }

    Assert (state != NULL && connect_by_isleaf != NULL);

    const char *curRow = state->sw_curKeyArrayStr;
    int curRowLen = strlen(curRow);
    TupleTableSlot *slot = state->sw_workingSlot;
    const char *scanRow = NULL;

    tuplestore_rescan(state->sw_backupTable);
    for (;;) {
        (void)tuplestore_gettupleslot(state->sw_backupTable, true, false, slot);
        if (TupIsNull(slot)) {
            /*
             * Finish scan all tuples, but we are not found "curRow is not a prefix"
             * plus "a prefix and string is longer "
             */
            *connect_by_isleaf = true;
            break;
        }

        /* Free the scanRow memory allocate in last loop */
        if (scanRow != NULL) {
            pfree_ext(scanRow);
        }

        scanRow = GetPseudoArrayTargetValueString(slot, state->sw_keyAttnum);
        int scanRowLen = strlen(scanRow);

        if (strcmp(curRow, scanRow) == 0) {
            /* continue if curernt row */
            continue;
        }

        /*
         * if curRow is the forward prefix of scanRow then set flag connect_by_isleaf false
         * -- forward prefix
         *    abc is forward prefix of abcde
         * -- backward predix
         *    cde is backward prefix of abcde
         *
         * Attention, we only accept forward prefix in this case.
         * */
        char *pos = pg_strrstr((char *)scanRow, curRow);
        if (pos != NULL && pos == scanRow && curRowLen < scanRowLen) {
            *connect_by_isleaf = false;
            break;
        }
    }

    /* Free text_to_cstring() palloc()-ed string */
    if (scanRow != NULL) {
        pfree_ext(scanRow);
    }

    return;
}

static void CheckIsCycleByRowmarks(StartWithOpState *state, bool* connect_by_iscycle, int row)
{
    if (state->sw_cycle_rowmarks == NULL) {
        return;
    }
    ListCell* mark = NULL;
    foreach (mark, state->sw_cycle_rowmarks) {
        int markIndex = lfirst_int(mark);
        if (row == markIndex) {
            *connect_by_iscycle = true;
        }
    }
}

/*
 * - brief: process the recursive-union returned tuples with isleaf/iscycle
 *          pseudo value filled
 *
 * - return: none
 */
static void ProcessPseudoReturnColumns(StartWithOpState *state)
{
    TupleTableSlot *dstSlot = state->ps.ps_ResultTupleSlot;
    StartWithOp    *swplan = (StartWithOp *)state->ps.plan;

    /* step1. put all tuples itno tmp table */
    tuplestore_rescan(state->sw_workingTable);
    for (;;) {
        (void)tuplestore_gettupleslot(state->sw_workingTable, true, false, dstSlot);
        if (TupIsNull(dstSlot)) {
            break;
        }

        tuplestore_puttupleslot(state->sw_backupTable, dstSlot);
    }

    /* seek tuplestore offset to begining */
    tuplestore_rescan(state->sw_workingTable);
    tuplestore_rescan(state->sw_backupTable);

    int rowCount = 0;
    /* step2. start processing */
    for (;;) {
        /* fetch one tuple from workingTable */
        (void)tuplestore_gettupleslot(state->sw_workingTable, true, false, dstSlot);
        if (TupIsNull(dstSlot)) {
            state->swop_status = SWOP_FINISH;
            break;
        }

        rowCount++;
        bool connect_by_isleaf = true;
        bool connect_by_iscycle = true;

        if (swplan->keyEntryList != NIL) {
            /* fetch current key array string for later isleaf/iscycle determination */
            state->sw_curKeyArrayStr =
                    GetPseudoArrayTargetValueString(dstSlot, state->sw_keyAttnum);

            /* Calculate connect_by_iscycle */
            CheckIsCycle(state, &connect_by_iscycle);
            CheckIsCycleByRowmarks(state, &connect_by_iscycle, rowCount);
            /* Calculate connect_by_isleaf */
            CheckIsLeaf(state, &connect_by_isleaf);
            /* Free per-tuple keyArrayStr */
            pfree_ext(state->sw_curKeyArrayStr);
        } else {
            /*
             * !!Note:  It is a rare case when gets here because keyEntryList is not
             * created for current START WITH clause, simplely it is an abnormal ConnectByExpr
             * specified in "CONNECT BY" clause e.g. "CONNECT BY col1 = abs(x)", we set
             * isleaf/iscycle both as FALSE value for more failure-tolerance and reasonble
             */
            connect_by_isleaf = false;
            connect_by_iscycle = false;
        }

        /* update result, fill the result tuple */
        UpdatePseudoReturnColumn(state, dstSlot, connect_by_isleaf, connect_by_iscycle);
        tuplestore_puttupleslot(state->sw_resultTable, dstSlot, false);
        ExecClearTuple(dstSlot);
    }

    /* rewind resultTable to let later EXECUTE stage can be processed */
    tuplestore_rescan(state->sw_resultTable);
}

static AttrNumber FetchRUItrTargetEntryResno(StartWithOpState *state)
{
    AttrNumber attnum = InvalidAttrNumber;
    StartWithOp *plan = (StartWithOp *)state->ps.plan;
    List *plist = plan->internalEntryList;
    ListCell *lc = NULL;

    foreach (lc, plist) {
        TargetEntry *te = (TargetEntry *)lfirst(lc);
        if (pg_strcasecmp(te->resname, "RUITR") == 0) {
            attnum = te->resno;
            break;
        }
    }

    Assert (attnum != InvalidAttrNumber && attnum <= list_length(plan->plan.targetlist));

    return attnum;
}

/*
 * functinos to process in RuScan
 *
 * Note: these function are mainly invoked in recursive union operator, but we put them in
 * startwith.cpp as they are special logic for start-with converted RecursiveUnion
 */
static const char *GetCurrentWorkTableScanPath(TupleTableSlot *srcSlot, AttrNumber pseudo_attnum)
{
    TupleDesc tupDesc = srcSlot->tts_tupleDescriptor;
    HeapTuple tup = ExecFetchSlotTuple(srcSlot);
    bool isnull = true;

    /* if srcSlot is from non-recursive(no pseudo column), then just return */
    if (pseudo_attnum > tupDesc->natts) {
        return NULL;
    }

    if (tup != NULL) {
        return TextDatumGetCString(heap_getattr(tup, pseudo_attnum, tupDesc, &isnull));
    }

    return NULL;
}

static bytea *GetCurrentSiblingsArrayPath(TupleTableSlot *srcSlot, AttrNumber pseudo_attnum)
{
    TupleDesc tupDesc = srcSlot->tts_tupleDescriptor;
    HeapTuple tup = ExecFetchSlotTuple(srcSlot);
    bool isnull = true;

    /* if srcSlot is from non-recursive(no pseudo column), then just return */
    if (pseudo_attnum > tupDesc->natts) {
        return NULL;
    }

    if (tup != NULL) {
        return DatumGetByteaP(heap_getattr(tup, pseudo_attnum, tupDesc, &isnull));
    }

    return NULL;
}

static Datum ConvertRuScanArrayAttr(RecursiveUnionState *rustate,
                TargetEntry *te, TupleTableSlot *srcSlot, bool inrecursing)
{
    /*
     * !!Reminding:
     *    In the future, we need improve he array_key and array_col into native array
     *    format rather then use bare literal type
     */
    Datum datum  = (Datum)0;
    const char *spliter = "/";
    const char *node_path = NULL;
    const char *value = NULL;

    if (!IsA(te->expr, Var)) {
        elog(ERROR, "Invalid TargetEntry expr type %s", nodeToString(te->expr));
    }

    node_path = GetCurrentWorkTableScanPath(srcSlot, te->resno);
    StartWithOpColumnType type = GetPseudoColumnType(te);
    switch (type) {
        case SWCOL_ARRAY_KEY: {
            value = GetKeyEntryArrayStr(rustate, srcSlot);
            break;
        }
        case SWCOL_ARRAY_SIBLINGS: {
            bytea *path = GetCurrentSiblingsArrayPath(srcSlot, te->resno);
            bytea *res = GetSiblingsKeyEntry(rustate, path);
            return PointerGetDatum(res);
            break;
        }
        case SWCOL_ARRAY_COL: {
            TupleDesc tupDesc = srcSlot->tts_tupleDescriptor;
            AttrNumber attno = PseudoResnameGetOriginResno(te->resname);
            if (!inrecursing && tupDesc->natts < attno) {
                /*
                 * None-recursive branch
                 *
                 * Handle a case where we need generate arry_col for PRC, we need handle
                 * it properly as in none-recursing stage, PRC is not attached in inner
                 * plan tree's targetlist, thus there is no way to invoke heap_getattr(),
                 * instead we need handle it manually,
                 *
                 * e.g. select sys_connect_by_path(level, 'xx')
                 *
                 * PRC: abbrev for "pseudo return column" level/isleaf/iscycle/rownum
                 */
                Datum valueDatum = rustate->swstate->sw_values[attno];
                value = pstrdup(DatumGetCString(DirectFunctionCall1(int8out, valueDatum)));
            } else {
                /*
                 * Recursive Branch
                 *
                 * for recursive case, PRC it attached to RecursiveUnion's inner plan
                 * targetlist, this we can hanle normally
                 */
                value = GetCurrentValue(srcSlot, attno);
            }
            break;
        }
        default: {
            /* fall-thru */
        }
    }


    StringInfo si = makeStringInfo();

    if (GetPseudoColumnType(te) == SWCOL_ARRAY_SIBLINGS) {
        if (node_path == NULL) {
            appendStringInfo(si, "%s", value);
        } else {
            appendStringInfo(si, "%s%s", node_path, value);
        }
    } else {
        if (node_path == NULL) {
            appendStringInfo(si, "%s%s", spliter, value);
        } else {
            appendStringInfo(si, "%s%s%s", node_path, spliter, value);
        }
    }

    /* Drop memory allocation created in current function, free as early as possible */
    datum = CStringGetTextDatum(si->data);
    pfree_ext(si->data);
    pfree_ext(node_path);
    pfree_ext(value);

    return datum;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Invoked at return point of RecursiveUnion, add the key/array information  store
 *         in worktable, so that next round of recursive iteration can append new node
 *         value to previous nodepath
 * --------------------------------------------------------------------------------------
 */
TupleTableSlot *ConvertRuScanOutputSlot(RecursiveUnionState *rustate,
                                           TupleTableSlot *scanSlot,
                                           bool inrecursing)
{
    if (TupIsNull(scanSlot)) {
        return scanSlot;
    }

    /* Origin RuScan output tuple  */
    HeapTuple scanTup = ExecFetchSlotTuple(scanSlot);
    TupleDesc scanTupDesc = scanSlot->tts_tupleDescriptor;

    /*
     * New RuScan output tuple will containd RUITR, array_key/col_nn, ....
     *
     * RecursiveUnion plan node's "result slot" tupDesc contains RUITR, array_key/col_nn
     * as they were generated from RecursiveUnion's targetlist
     */
    HeapTuple outTup = NULL;
    TupleDesc outTupDesc = rustate->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleTableSlot *outSlot = rustate->ps.ps_ResultTupleSlot;

    StartWithOpState *swstate = rustate->swstate;
    ResetResultSlotAttValueArray(swstate, swstate->sw_values, swstate->sw_isnull);
    Datum *values = swstate->sw_values;
    bool *isnull = swstate->sw_isnull;

    /* Reset scanslot and memorycontext */
    (void)MemoryContextReset(rustate->convertContext);
    MemoryContext oldcxt = MemoryContextSwitchTo(rustate->convertContext);

    /*
     * scanTuple does not contain RUITR, array_name, deformed by old desc and use new
     * values/isbull to accept deformed content
     */
    heap_deform_tuple(scanTup, scanTupDesc, values, isnull);

    ListCell *lc = NULL;
    RecursiveUnion *ruplan = (RecursiveUnion *)rustate->ps.plan;
    foreach (lc, ruplan->internalEntryList) {
        TargetEntry *te = (TargetEntry *)lfirst(lc);
        AttrNumber attnum = te->resno;

        if (pg_strcasecmp(te->resname, "RUITR") == 0) {
            values[attnum - 1] = rustate->iteration;
        }  else if (pg_strncasecmp(te->resname, "array_key_", strlen("array_key_")) == 0) {
            values[attnum - 1] = ConvertRuScanArrayAttr(rustate, te, scanSlot, inrecursing);
        } else {
            values[attnum - 1] = ConvertRuScanArrayAttr(rustate, te, scanSlot, inrecursing);
        }

        /* for converted slot is always filled with valid value(none-empty) */
        isnull[attnum - 1] = false;
    }

    /* build converted tuple */
    outTup = heap_form_tuple(outTupDesc, values, isnull);
    outSlot = ExecStoreTuple(outTup, outSlot, InvalidBuffer, false);

    MemoryContextSwitchTo(oldcxt);

    /*
     * Start With dfx
     *
     *  - print key information for each iteration in RuSacn
     **/
    if (u_sess->attr.attr_sql.enable_startwith_debug) {
        bool _isnull = true;
        Datum d = heap_getattr(outTup, rustate->swstate->sw_keyAttnum, outTupDesc, &_isnull);
        elog(LOG, "StartWithDebug: LEVEL:%d array_key_1:%s",
                        rustate->iteration + 1, TextDatumGetCString(d));
    }

    /* sw_tuple_idx++ after one tuple finished. */
    rustate->sw_tuple_idx++;

    return outSlot;
}

static bytea* bytea_catenate(bytea* t1, bytea* t2)
{
    bytea* result = NULL;
    int len1, len2, len;
    char* ptr = NULL;
    int rc = 0;

    len1 = VARSIZE_ANY_EXHDR(t1);
    len2 = VARSIZE_ANY_EXHDR(t2);

    if (len1 < 0) {
        len1 = 0;
    }
    if (len2 < 0) {
        len2 = 0;
    }

    len = len1 + len2 + VARHDRSZ;
    result = (bytea*)palloc(len);

    /* Set size of result string... */
    SET_VARSIZE(result, len);

    /* Fill data field of result string... */
    ptr = VARDATA(result);
    if (len1 > 0) {
        rc = memcpy_s(ptr, len1, VARDATA_ANY(t1), len1);
        securec_check(rc, "\0", "\0");
    }
    if (len2 > 0) {
        rc = memcpy_s(ptr + len1, len2, VARDATA_ANY(t2), len2);
        securec_check(rc, "\0", "\0");
    }
    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: return order pseudo order siblings key for further order
 *      pseudo column array like this:
 *          [00001][00001][00001][00002] -->  every buckets use byea type
 *          [00001] -> tuple ordered position in current level, array index represent level
 * --------------------------------------------------------------------------------------
 */
static bytea *GetSiblingsKeyEntry(RecursiveUnionState *rustate, bytea *old_path)
{
    bytea *result = NULL;
    bytea *newKey = NULL;
    const int bucketSize = 4;
    const int maxEntrySize = 127;
    const uint64 maxSize = pow(maxEntrySize, 4);

    /*
     * We use four bytea size for one bucket sibings key
     * so we could support max 127^4 tuple to order
     */
    newKey = (bytea *)palloc0(bucketSize + VARHDRSZ);
    SET_VARSIZE(newKey, bucketSize + VARHDRSZ);
    char *keyVar = VARDATA_ANY(newKey);

    if (rustate->sw_tuple_idx >= maxSize) {
        ereport(ERROR,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                errmsg("Exceed max numbers of siblings tuples")));
    }

    /* Fill correct order position into buckets */
    uint64 tupleIdx = rustate->sw_tuple_idx;

    const uint64 lastMod = (const uint64)pow(maxEntrySize, 3);
    keyVar[3] = tupleIdx / lastMod;
    tupleIdx = tupleIdx % lastMod;

    const uint64 thirdMod = (const uint64)pow(maxEntrySize, 2);
    keyVar[2] = tupleIdx / thirdMod;
    tupleIdx = tupleIdx % thirdMod;

    keyVar[1] = tupleIdx / maxEntrySize;
    tupleIdx = tupleIdx % maxEntrySize;

    keyVar[0] = tupleIdx;

    /* Reach level 0 if path is NULL then return */
    if (old_path == NULL) {
        elog(LOG, "siblings key is %s current level is %d tuple position is %lu.",
            DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(newKey))),
            rustate->iteration + 1,
            rustate->sw_tuple_idx);
        return newKey;
    }

    /* Otherwise we catenate new siblings key to old key path */
    result = bytea_catenate(old_path, newKey);

    if (u_sess->attr.attr_sql.enable_startwith_debug) {
        elog(LOG, "siblings key is %s current level is %d tuple position is %lu",
            DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(result))),
            rustate->iteration + 1,
            rustate->sw_tuple_idx);
    }

    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: return a key/keys in string format, e.g {1,2}, {1}, the next step is
 *         to construct path like /{1,A}/{2,B}/{3,3}
 * --------------------------------------------------------------------------------------
 */
static const char *GetKeyEntryArrayStr(RecursiveUnionState *state, TupleTableSlot *scanSlot)
{
    const char *result = NULL;

    List *keyEntryList = ((StartWithOp *)state->swstate->ps.plan)->keyEntryList;

    /* build key string {key,key,key} */
    StringInfoData si;
    initStringInfo(&si);
    appendStringInfo(&si, "%s", KEY_START_TAG);
    {
        for (int i = 0; i < list_length(keyEntryList); i++) {
            TargetEntry *te = (TargetEntry *)list_nth(keyEntryList, i);
            const char *curKeyValue = GetCurrentValue(scanSlot, te->resno);
            if (curKeyValue == NULL) {
                /*
                 * ArrayKeyValue could return NULL, for example, base table's column
                 * contains NULL and startWithExpr is set condition "TRUE"
                 */
                elog(WARNING, "The internal key column[%d]:%s with NULL value.",
                            te->resno,
                            scanSlot->tts_tupleDescriptor->attrs[i]->attname.data);
            }

            if (i == 0) {
                appendStringInfo(&si, "%s", curKeyValue);
            } else {
                appendStringInfo(&si, ",%s", curKeyValue);
            }
        }

    }
    appendStringInfo(&si, "%s", KEY_CLOSE_TAG);
    result = pstrdup(si.data);
    pfree_ext(si.data);

    return result;
}

/* 
 * --------------------------------------------------------------------------------------
 * @Brief: Helper function to output column value in string format, caller need to free
 *         the return string
 * --------------------------------------------------------------------------------------
 */
static const char *GetCurrentValue(TupleTableSlot *slot, AttrNumber attnum)
{
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    HeapTuple tup = ExecFetchSlotTuple(slot);
    bool isnull = true;
    Oid atttypid = tupDesc->attrs[attnum - 1]->atttypid;
    Datum d = heap_getattr(tup, attnum, tupDesc, &isnull);
    char *value_str = NULL;

    /*
     * Return string SWCB_DEFAULT_NULLSTR to represent NULL in internal array_key/col's element,
     *
     * Note, slot value int array_key/array_col list is to represent iteration level's rather
     * than its real value
     */
    if (isnull) {
        return pstrdup(SWCB_DEFAULT_NULLSTR);
    }

    switch (atttypid) {
        case INT8OID:
            value_str =  DatumGetCString(DirectFunctionCall1(int8out, d));
            break;
        case INT1OID:
            value_str =  DatumGetCString(DirectFunctionCall1(int1out, d));
            break;
        case INT2OID:
            value_str =  DatumGetCString(DirectFunctionCall1(int2out, d));
            break;
        case OIDOID:
            value_str =  DatumGetCString(DirectFunctionCall1(oidout, d));
            break;
        case INT4OID:
            value_str =  DatumGetCString(DirectFunctionCall1(int4out, d));
            break;
        case BOOLOID:
            value_str =  DatumGetCString(DirectFunctionCall1(boolout, d));
            break;
        case CHAROID:
            value_str =  DatumGetCString(DirectFunctionCall1(charout, d));
            break;
        case NAMEOID:
            value_str =  DatumGetCString(DirectFunctionCall1(nameout, d));
            break;
        case FLOAT4OID:
            value_str =  DatumGetCString(DirectFunctionCall1(float4out, d));
            break;
        case FLOAT8OID:
            value_str =  DatumGetCString(DirectFunctionCall1(float8out, d));
            break;
        case ABSTIMEOID:
            value_str =  DatumGetCString(DirectFunctionCall1(abstimeout, d));
            break;
        case RELTIMEOID:
            value_str =  DatumGetCString(DirectFunctionCall1(reltimeout, d));
            break;
        case DATEOID:
            value_str =  DatumGetCString(DirectFunctionCall1(date_out, d));
            break;
        case CASHOID:
            value_str =  DatumGetCString(DirectFunctionCall1(cash_out, d));
            break;
        case TIMEOID:
            value_str =  DatumGetCString(DirectFunctionCall1(time_out, d));
            break;
        case TIMESTAMPOID:
            value_str =  DatumGetCString(DirectFunctionCall1(timestamp_out, d));
            break;
        case TIMESTAMPTZOID:
            value_str =  DatumGetCString(DirectFunctionCall1(timestamptz_out, d));
            break;
        case SMALLDATETIMEOID:
            value_str =  DatumGetCString(DirectFunctionCall1(smalldatetime_out, d));
            break;
        case UUIDOID:
            value_str =  DatumGetCString(DirectFunctionCall1(uuid_out, d));
            break;
        case INTERVALOID:
            value_str =  DatumGetCString(DirectFunctionCall1(interval_out, d));
            break;
        case TIMETZOID:
            value_str =  DatumGetCString(DirectFunctionCall1(timetz_out, d));
            break;
        case INT2VECTOROID:
            value_str =  DatumGetCString(DirectFunctionCall1(int2vectorout, d));
            break;
        case CLOBOID:
            value_str =  DatumGetCString(DirectFunctionCall1(textout, d));
            break;
        case NVARCHAR2OID:
            value_str =  DatumGetCString(DirectFunctionCall1(nvarchar2out, d));
            break;
        case VARCHAROID:
            value_str =  DatumGetCString(DirectFunctionCall1(varcharout, d));
            break;
        case TEXTOID:
            value_str =  DatumGetCString(DirectFunctionCall1(textout, d));
            break;
        case OIDVECTOROID:
            value_str =  DatumGetCString(DirectFunctionCall1(oidvectorout, d));
            break;
        case BPCHAROID:
            value_str =  DatumGetCString(DirectFunctionCall1(bpcharout, d));
            break;
        case RAWOID:
            value_str =  DatumGetCString(DirectFunctionCall1(rawout, d));
            break;
        case BYTEAOID:
            value_str =  DatumGetCString(DirectFunctionCall1(byteaout, d));
            break;
        case NUMERICOID:
            value_str =  DatumGetCString(DirectFunctionCall1(numeric_out, d));
            break;
        default: {
            elog(ERROR, "unspported type for attname:%s (typid:%u typname:%s)",
                tupDesc->attrs[attnum - 1]->attname.data,
                atttypid, get_typename(atttypid));
        }
    }

    return pstrdup(value_str);
}

static bool PRCCanSkip(StartWithOpState *state, int prcType)
{
    StartWithOp *swplan = (StartWithOp *)state->ps.plan;
    Assert (IsA(swplan, StartWithOp));
    bool result = false;

    switch (prcType) {
        case SWCOL_ISCYCLE: {
            result = (IsSkipIsCycle(swplan->swExecOptions) != 0);
            break;
        }
        case SWCOL_ISLEAF: {
            result = (IsSkipIsLeaf(swplan->swExecOptions) != 0);
            break;
        }
        case SWCOL_LEVEL:
        case SWCOL_ROWNUM:
            /*
             * fall-thru level/rownum is light weight, we do not apply PRC-skip
             * optimization on them
             */
        default: {
            /* other PRC is always not-skipable */
            result = false;
        }
    }

    return result;
}
