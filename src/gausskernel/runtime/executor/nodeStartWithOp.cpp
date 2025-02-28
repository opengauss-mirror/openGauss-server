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
    SWOP_FINISH
} StartWithOpExecStatus;

#define KEY_START_TAG "{"
#define KEY_SPLIT_TAG "/"
#define KEY_CLOSE_TAG "}"

#define ITRCYCLE_CHECK_LIMIT  50
#define DEPTH_CRITERION       100000
#define ITR_MAX_TUPLES        80000
#define MAX_SIBLINGS_TUPLE    1000000000

char* get_typename(Oid typid);
static List *CovertInternalArrayStringToList(char *raw_array_str);
static int TupleSlotGetCurrentLevel(TupleTableSlot *slot);
static List *GetCurrentArrayColArray(const FunctionCallInfo fcinfo,
                                     const char *value, char **raw_array_str,
                                     bool is_path, bool *isConstArrayList = NULL);
static AttrNumber GetInternalArrayAttnum(TupleDesc tupDesc, AttrNumber origVarAttno, bool isPath);
static const char *GetCurrentValue(TupleTableSlot *slot, AttrNumber attnum);
static TupleTableSlot* ExecStartWithOp(PlanState* state);
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
        return expression_tree_walker(node, (bool (*)()) unsupported_filter_walker, context_node);
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
 *   - ResetRecursiveInner()
 */
StartWithOpState* ExecInitStartWithOp(StartWithOp* node, EState* estate, int eflags)
{
    /*
     * Error-out unsupported cases before they
     * actually cause any harm.
     */
    expression_tree_walker((Node*)node->connect_by_qual,
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

    state->ps.ExecProcNode = ExecStartWithOp;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &state->ps);

    state->ps.ps_vec_TupFromTlist = false;
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &state->ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        state->ps.qual = (List*)ExecInitQualByFlatten(node->plan.qual, (PlanState*)state);
    } else {
        state->ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->plan.targetlist,
                                                    (PlanState*)state);
        state->ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->plan.qual, (PlanState*)state);
    }

    state->connect_by_qual = (List*)ExecInitExprByRecursion((Expr*)node->connect_by_qual, (PlanState*)state);
    state->start_with_qual = (List*)ExecInitExprByRecursion((Expr*)node->start_with_qual, (PlanState*)state);

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
    ExecAssignResultTypeFromTL(&state->ps);

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
     *  2. setup tuplestore, tupleslot and status controlling variables,
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
    state->sw_workingSlot = ExecAllocTableSlot(&estate->es_tupleTable, TableAmHeap);
    ExecSetSlotDescriptor(state->sw_workingSlot, ExecTypeFromTL(targetlist, false));

    int natts = list_length(node->plan.targetlist);
    state->sw_values = (Datum *)palloc0(natts * sizeof(Datum));
    state->sw_isnull=  (bool *)palloc0(natts * sizeof(bool));
    ResetResultSlotAttValueArray(state, state->sw_values, state->sw_isnull);

    /* Setup other variables to NULL as they will be initialized later */
    state->sw_curKeyArrayStr = NULL;
    state->sw_cycle_rowmarks = NULL;

    /* set underlying RecursiveUnionState pointing to current curernt node */
    RecursiveUnionState *rustate = (RecursiveUnionState *)outerPlanState(state);
    rustate->swstate = state;

    /* init other elements */
    state->sw_level = 0;
    state->sw_numtuples = 0;

    return state;
}

bool CheckCycleExeception(StartWithOpState *node, TupleTableSlot *slot)
{
    RecursiveUnionState *rustate = NULL;
    StartWithOp *swplan = (StartWithOp *)node->ps.plan;
    SWDfsOpState *dfs_state = node->dfs_state;
    bool nocycle = swplan->swoptions->nocycle;
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    List* keyEntryList = swplan->keyEntryList;

    if (!keyEntryList) {
        return false;
    }

    rustate = (RecursiveUnionState *)node->ps.lefttree;
    Assert (IsA(rustate, RecursiveUnionState));

    if (!nocycle && IsConnectByLevelStartWithPlan(swplan)) {
        /* for connect by level/rownum case, we do not do cycle check */
        return false;
    }

    for (int i = 0; i < dfs_state->cur_level; i++) {
        List* ancestorKeys = dfs_state->prior_key_stack[i];
        Assert(list_length(ancestorKeys) == list_length(keyEntryList));
        bool hascycle = true;
        ListCell* lc1 = NULL;
        ListCell* lc2 = NULL;
        forboth(lc1, keyEntryList, lc2, ancestorKeys) {
            bool isnull = false;
            TargetEntry* te = (TargetEntry*)lfirst(lc1);
            Datum newkey = heap_slot_getattr(slot, te->resno, &isnull);
            Datum oldkey = (Datum)lfirst(lc2);
            if (slot->tts_isnull[te->resno - 1] || isnull) {
                hascycle = false;
                break;
            } else {
                FormData_pg_attribute attr = tupDesc->attrs[te->resno - 1];
                if (!datumIsEqual(newkey, oldkey, attr.attbyval, attr.attlen)) {
                    hascycle = false;
                    break;
                }
            }
        }

        if (!hascycle) {
            continue;
        }

        if (nocycle) {
            return true;
        } else {
            /* compatible with ORA behavior, if NOCYCLE is not set, we report error */
            ereport(ERROR,
                    (errmodule(MOD_EXECUTOR),
                    errmsg("START WITH .. CONNECT BY statement runs into cycle exception")));
        }
    }
    return false;
}

/*
 * This function is called during executing recursive part(inner plan) of recursive union,
 * so it would be enough to rescan(reset) only inner_plan of RecursiveUnionState to refresh
 * last working state including working table.
 */
void ResetRecursiveInner(RecursiveUnionState *node)
{
    PlanState *outerPlanState = outerPlanState(node);
    PlanState *innerPlanState = innerPlanState(node);
    RecursiveUnion *ruPlan = (RecursiveUnion*)node->ps.plan;

    /*
     * Set recursive term's chgParam to tell it that we'll modify the working
     * table and therefore it has to rescan.
     */
    innerPlanState->chgParam = bms_add_member(innerPlanState->chgParam, ruPlan->wtParam);
    if (outerPlanState->chgParam == NULL) {
        ExecReScan(innerPlanState);
    }

    node->intermediate_empty = true;
    tuplestore_clear(node->working_table);
    tuplestore_clear(node->intermediate_table);
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
static List* peekNextLevel(TupleTableSlot* startSlot, PlanState* outerNode, int level, bool* iscycle)
{
    List* queue = NULL;
    RecursiveUnionState* rus = (RecursiveUnionState*) outerNode;
    StartWithOpState *swnode = rus->swstate;
    /* ReSet RU's inner plan, inlcuding re-scan inner and free its working table */
    ResetRecursiveInner(rus);
    /* pushing the depth-first tuple into RU's working table */
    rus->recursing = true;
    tuplestore_puttupleslot(rus->working_table, startSlot);

    /* fetch the depth-first tuple's children exactly one level below */
    rus->iteration = level;
    TupleTableSlot* srcSlot = NULL;
    *iscycle = false;
    markSWLevelBegin(swnode);
    for (;;) {
        srcSlot = ExecProcNode(outerNode);
        if (TupIsNull(srcSlot)) {
            break;
        }
        if (CheckCycleExeception(swnode, srcSlot)) {
            *iscycle = true;
            continue;
        }
        TupleTableSlot* newSlot = MakeSingleTupleTableSlot(srcSlot->tts_tupleDescriptor);
        newSlot = ExecCopySlot(newSlot, srcSlot);
        queue = lappend(queue, newSlot);
    }
    markSWLevelEnd(swnode, list_length(queue));
    return queue;
}

void DfsExpandStack(SWDfsOpState* state)
{
    int oldsize = state->stack_size;
    int newsize = oldsize * 2;
    errno_t rc = 0;
    state->prior_key_stack = (List **)repalloc(state->prior_key_stack, sizeof(List *) * newsize);
    state->tuples_stack = (List **)repalloc(state->tuples_stack, sizeof(List *) * newsize);
    rc = memset_s((void*)(state->prior_key_stack + oldsize), oldsize * sizeof(List *), 0, oldsize * sizeof(List *));
    securec_check(rc, "\0", "\0");
    rc = memset_s((void*)(state->tuples_stack + oldsize), oldsize * sizeof(List *), 0, oldsize * sizeof(List *));
    securec_check(rc, "\0", "\0");
    state->stack_size = newsize;
}

static void DfsPushTuples(SWDfsOpState* state, List* queue)
{
    if (state->cur_level >= state->stack_size) {
        DfsExpandStack(state);
    }
    state->tuples_stack[state->cur_level] = queue;
}

static void DfsPushPriorKey(TupleTableSlot* slot, SWDfsOpState* state, List* keyEntryList)
{
    ListCell* lc = NULL;
    List* keys = NIL;
    foreach(lc, keyEntryList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        TupleDesc tupDesc = slot->tts_tupleDescriptor;
        FormData_pg_attribute attr = tupDesc->attrs[te->resno - 1];
        Datum key = (Datum)0;
        if (slot->tts_isnull[te->resno - 1]) {
            key = CStringGetTextDatum(pstrdup(SWCB_DEFAULT_NULLSTR));
            keys = lappend(keys, (void*)key);
        } else {
            key = slot->tts_values[te->resno - 1];
            /* check attr type */
            keys = lappend(keys, (void*)datumCopy(key, attr.attbyval, attr.attlen));
        }
    }
    int pos = state->cur_level - 1;
    if (pos >= state->stack_size) {
        DfsExpandStack(state);
    }
    state->prior_key_stack[pos] = keys;
}

static void DfsClearPriorKey(SWDfsOpState* state)
{
    int pos = state->cur_level - 1;
    List* keys = state->prior_key_stack[pos];
    list_free_ext(keys);
    state->prior_key_stack[pos] = NULL;
}

static void DfsPopStack(SWDfsOpState* state)
{
    if (state->cur_level < 1) {
        return;
    }
    int pos = state->cur_level - 1;
    Assert(list_length(state->tuples_stack[pos]) == 0);
    state->tuples_stack[pos] = NULL;
    DfsClearPriorKey(state);
    state->cur_level--;
}

static void DfsResetState(SWDfsOpState* state)
{
    if (state->last_ru_slot) {
        ExecDropSingleTupleTableSlot(state->last_ru_slot);
        state->last_ru_slot = NULL;
    }
    while (state->cur_level) {
        int pos = state->cur_level - 1;
        List* tuples = state->tuples_stack[pos];
        while (list_length(tuples)) {
            ExecDropSingleTupleTableSlot((TupleTableSlot*)linitial(tuples));
            tuples = list_delete_first(tuples);
        }
        state->tuples_stack[pos] = NULL;
        DfsClearPriorKey(state);
        state->cur_level--;
    }
    state->cur_rownum = 0;
    state->cur_level = 0;
}

TupleTableSlot* DeformRUSlot(TupleTableSlot* ru_slot, StartWithOpState *node)
{
    TupleDesc tupDesc = ru_slot->tts_tupleDescriptor;
    TupleTableSlot* dst_slot = node->sw_workingSlot;
    ExecClearTuple(dst_slot);
    for (int i = 0; i < dst_slot->tts_tupleDescriptor->natts; i++) {
        dst_slot->tts_isnull[i] = true;
    }
    for (int i = 0; i < tupDesc->natts; i++) {
        bool isnull = true;
        dst_slot->tts_values[i] = heap_slot_getattr(ru_slot, i + 1, &isnull);
        dst_slot->tts_isnull[i] = isnull;
    }
    ExecStoreVirtualTuple(dst_slot);
    return dst_slot;
}

static void UpdateVirtualSWCBTuple(TupleTableSlot* slot, StartWithOpState *node,
                                   StartWithOpColumnType type, Datum value)
{
    AttrNumber attnum = node->sw_pseudoCols[type]->resno;
    /* set proper value and mark isnull to false */
    slot->tts_values[attnum - 1] = value;
    slot->tts_isnull[attnum - 1] = false;
}

static void UpdateCurrentSlotRootValues(TupleTableSlot* slot, List* internal_root_entry_list, List* root_entry_list)
{
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    forboth(lc1, root_entry_list, lc2, internal_root_entry_list) {
        TargetEntry* te1 = (TargetEntry*)lfirst(lc1);
        const char* curStr = GetCurrentValue(slot, te1->resno);
        Datum value = CStringGetTextDatum(curStr);
        TargetEntry* te2 = (TargetEntry*)lfirst(lc2);
        slot->tts_values[te2->resno - 1] = value;
        slot->tts_isnull[te2->resno - 1] = false;
        pfree_ext(curStr);
    }
}

static void UpdateCurrentSlotPathValues(TupleTableSlot* slot, List* internal_path_entry_list, List* path_entry_list)
{
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    forboth(lc1, path_entry_list, lc2, internal_path_entry_list) {
        char* nodepath = NULL;
        char* spliter = "/";
        StringInfoData si;
        initStringInfo(&si);
        TargetEntry* te1 = (TargetEntry*)lfirst(lc1);
        const char* curStr = GetCurrentValue(slot, te1->resno);
        TargetEntry* te2 = (TargetEntry*)lfirst(lc2);
        Datum value = slot->tts_values[te2->resno - 1];
        nodepath = (slot->tts_isnull[te2->resno - 1]) ? NULL : TextDatumGetCString(value);
        if (nodepath) {
            appendStringInfo(&si, "%s%s%s", nodepath, spliter, curStr);
        } else {
            appendStringInfo(&si, "%s%s", spliter, curStr);
        }
        slot->tts_values[te2->resno - 1] = CStringGetTextDatum(si.data);
        slot->tts_isnull[te2->resno - 1] = false;
        pfree_ext(si.data);
        pfree_ext(nodepath);
        pfree_ext(curStr);
    }
}

/*
 * Construct CONNECT BY result set by depth-first order.
 *
 * Does not support order siblings by yet.
 */
static TupleTableSlot* depth_first_connect(StartWithOpState *node)
{
    TupleTableSlot* result = NULL;
    SWDfsOpState* dfs_state = node->dfs_state;
    while (dfs_state->cur_level > 0) {
        List* tuple_list = dfs_state->tuples_stack[dfs_state->cur_level - 1];
        if (dfs_state->last_ru_slot != NULL) {
            ExecDropSingleTupleTableSlot(dfs_state->last_ru_slot);
            dfs_state->last_ru_slot = NULL;
        }

        if (list_length(tuple_list) == 0) {
            DfsPopStack(dfs_state);
            continue;
        }

        TupleTableSlot* ru_slot = (TupleTableSlot*)linitial(tuple_list);
        TupleTableSlot* cur_slot = DeformRUSlot(ru_slot, node);
        dfs_state->last_ru_slot = ru_slot;

        List* new_tuple_list = list_delete_first(tuple_list);
        dfs_state->tuples_stack[dfs_state->cur_level - 1] = new_tuple_list;

        UpdateVirtualSWCBTuple(cur_slot, node, SWCOL_LEVEL, dfs_state->cur_level);
        UpdateVirtualSWCBTuple(cur_slot, node, SWCOL_ROWNUM, dfs_state->cur_rownum);

        StartWithOp* swplan = (StartWithOp*)node->ps.plan;
        RecursiveUnionState* runode = NULL;
        runode = castNode(RecursiveUnionState, outerPlanState(node));
        if (ExecStartWithRowLevelQual(runode, cur_slot)) {
            DfsPushPriorKey(cur_slot, dfs_state, swplan->keyEntryList);
            UpdateCurrentSlotPathValues(cur_slot, swplan->internal_path_entry_list, swplan->path_entry_list);
            if (dfs_state->cur_level == 1) {
                UpdateCurrentSlotRootValues(cur_slot, swplan->internal_root_entry_list, swplan->root_entry_list);
            }

            bool iscycle = false;
            List* queue = peekNextLevel(cur_slot, (PlanState*)runode, dfs_state->cur_level, &iscycle);
            dfs_state->cur_rownum++;

            bool isleaf = (list_length(queue) == 0);
            UpdateVirtualSWCBTuple(cur_slot, node, SWCOL_ISCYCLE, BoolGetDatum(iscycle));
            UpdateVirtualSWCBTuple(cur_slot, node, SWCOL_ISLEAF, BoolGetDatum(isleaf));

            if (isleaf) {
                DfsClearPriorKey(dfs_state);
            } else {
                DfsPushTuples(dfs_state, queue);
                dfs_state->cur_level++;
            }
            result = cur_slot;
        }
        if (result) {
            return result;
        }
    }
    return NULL;
}

static List* makeStartTuples(StartWithOpState *node)
{
    PlanState* outerNode = outerPlanState(node);
    List* startWithQueue = NULL;
    RecursiveUnionState* rus = (RecursiveUnionState*) outerNode;
    TupleTableSlot* dstSlot = ExecProcNode(outerNode);
    for (;;) {
        if (TupIsNull(dstSlot)) {
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
 *         Check if SWCB's conditions still hold for the recursion to
 *         continue.
 *
 * @Input node: The current recursive union node.
 * @Input slot: The next slot to be scanned into start with operator.
 *
 * @Return: Boolean flag to tell if the iteration should continue.
 */
bool ExecStartWithRowLevelQual(RecursiveUnionState* node, TupleTableSlot* dstSlot)
{
    StartWithOp* swplan = (StartWithOp*)node->swstate->ps.plan;
    ExprContext* expr = node->swstate->ps.ps_ExprContext;

    List* qual = node->swstate->dfs_state->cur_level == 1 ?
                 node->swstate->start_with_qual : node->swstate->connect_by_qual;

    /*
     * Level and rownum pseudo attributes are extracted from StartWithOpPlan
     * node so we set filtering tuple as ecxt_scantuple
     */
    expr->ecxt_scantuple = dstSlot;
    if (qual && !ExecQual(qual, expr, false)) {
        return false;
    }
    return true;
}

static void DiscardSWLastTuple(StartWithOpState *node)
{
    node->sw_rownum--;
    node->sw_numtuples--;
}

static SWDfsOpState* InitSWDfsOpState()
{
    SWDfsOpState* state = (SWDfsOpState*)palloc0(sizeof(SWDfsOpState));
    const int defaultStackSize = 32;
    state->prior_key_stack = (List **)palloc0(sizeof(List *) * defaultStackSize);
    state->tuples_stack = (List **)palloc0(sizeof(List *) * defaultStackSize);
    state->stack_size = defaultStackSize;
    state->cur_level = 0;
    state->cur_rownum = 0;
    state->last_ru_slot = NULL;
    return state;
}

static void ClearSWDfsOpState(SWDfsOpState* state)
{
    pfree_ext(state->prior_key_stack);
    pfree_ext(state->tuples_stack);
}

static TupleTableSlot* ExecStartWithOp(PlanState* state)
{
    StartWithOpState *node = castNode(StartWithOpState, state);
    TupleTableSlot *dstSlot = node->ps.ps_ResultTupleSlot;

    /* start processing */
    switch (node->swop_status) {
        case SWOP_BUILD: {
            markSWLevelBegin(node);
            List* queue = makeStartTuples(node);
            markSWLevelEnd(node, list_length(queue));
            if (queue == NULL) {
                node->swop_status = SWOP_FINISH;
                return NULL;
            } else {
                if (!node->dfs_state) {
                    node->dfs_state = InitSWDfsOpState();
                }
                DfsPushTuples(node->dfs_state, queue);
                node->dfs_state->cur_rownum = 1;
                node->dfs_state->cur_level = 1;
                node->swop_status = SWOP_EXECUTE;
            }
        } // @suppress("No break at end of case")

        /* fall-thru for first time */
        case SWOP_EXECUTE: {
            return depth_first_connect(node);
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

    if (node->dfs_state) {
        DfsResetState(node->dfs_state);
        ClearSWDfsOpState(node->dfs_state);
        pfree_ext(node->dfs_state);
    }

    ExecEndNode(outerPlanState(node));

    return;
}

void ExecReScanStartWithOp(StartWithOpState *state)
{
    if (state->dfs_state) {
        DfsResetState(state->dfs_state);
    }
    
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
    } else if (pg_strcasecmp(resname, "array_siblings") == 0) {
        result = SWCOL_ARRAY_SIBLINGS;
    } else if (pg_strncasecmp(resname, "array_key_", strlen("array_key_")) == 0) {
        result = SWCOL_ARRAY_KEY;
    } else if (pg_strncasecmp(resname, "array_path_", strlen("array_path_")) == 0) {
        result = SWCOL_ARRAY_PATH;
    }  else if (pg_strncasecmp(resname, "array_root_", strlen("array_root_")) == 0) {
        result = SWCOL_ARRAY_ROOT;
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
     *   Fetch current processed ScanTuple and its internal array_path, then convert
     *   it into token_list and do proper calculation for sys_connect_by_path()
     */
    char *raw_array_str = NULL;
    List *token_list = GetCurrentArrayColArray(fcinfo, value, &raw_array_str, true, &constArrayList);

    ListCell *token = NULL;
    char *trimValue = TrimStr(value);
    if (trimValue != NULL && !constArrayList) {
        bool valid = false;
        foreach (token, token_list) {
            char *curValue = TrimStr((const char *)lfirst(token));
            if (curValue == NULL) {
                continue;
            }
            if (strcmp(trimValue, curValue) == 0) {
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
     *   Fetch current processed ScanTuple and its internal array_root_x, then convert
     *   it into token_list and do proper calculation for connect_by_root()
     */
    char *raw_array_str = NULL;
    List *token_list = GetCurrentArrayColArray(fcinfo, value, &raw_array_str, false, &constArrayList);

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
static int TupleSlotGetCurrentLevel(TupleTableSlot *slot)
{
    Assert (slot != NULL);

    /* 1. find key attnum */
    AttrNumber attno = InvalidAttrNumber;
    Datum levelDatum = (Datum)0;
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    HeapTuple tup = ExecFetchSlotTuple(slot);
    bool isnull = true;

    for (int n = 0; n < tupDesc->natts; n++) {
        Form_pg_attribute attr = TupleDescAttr(tupDesc, n);
        if (pg_strncasecmp(attr->attname.data, "level", strlen("level")) == 0) {
            attno = n + 1;
            break;
        }
    }

    /* 2. fetch key str */
    levelDatum = heap_getattr(tup, attno, tupDesc, &isnull);
    Assert (!isnull && levelDatum != 0);

    return DatumGetInt32(levelDatum);
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
                                     const char *value, char **raw_array_str, bool is_path, bool *isConstArrayList)
{
    List *token_list = NIL;
    TupleTableSlot *slot = NULL;
    Var *variable = NULL;
    *isConstArrayList = false;
    List *vars = NIL;

    ExprContext *econtext = (ExprContext *)fcinfo->swinfo.sw_econtext;
    ExprState   *exprstate = (ExprState *)fcinfo->swinfo.sw_exprstate;

    /* context check */
    Assert (econtext != NULL && exprstate != NULL);

    /*
     * Oracle "START WITH ... CONNECT BY" only allow single plaint column to be
     * specified, so the eval-context's argument only have one argument with *Var*
     * node ported
     */
    if (fcinfo->swinfo.sw_is_flt_frame) {
        vars = pull_var_clause((Node*)fcinfo->swinfo.sw_exprstate,
                                       PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
    } else {
        vars = pull_var_clause((Node*)exprstate->expr,
                                     PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
    }


    /* handle case where */
    if (vars == NIL) {
        TupleTableSlot *slot = econtext->ecxt_scantuple;
        int level = -1;

        if (fcinfo->flinfo->fn_oid == CONNECT_BY_ROOT_FUNCOID) {
            token_list = list_make1(pstrdup(value));
        } else if (!TupIsNull(slot)) {
            level = TupleSlotGetCurrentLevel(slot);
            for (int n = 0; n < level; n++) {
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
    AttrNumber arrayColAttnum = GetInternalArrayAttnum(tupDesc, origAttnum, is_path);
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

static AttrNumber GetInternalArrayAttnum(TupleDesc tupDesc, AttrNumber origVarAttno, bool isPath)
{
    NameData arrayAttName;
    AttrNumber  arrayAttNum = InvalidAttrNumber;
    errno_t rc = 0;

    rc = memset_s(arrayAttName.data, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    if (isPath) {
        rc = sprintf_s(arrayAttName.data, NAMEDATALEN, "array_path_%d", origVarAttno);
    } else {
        rc = sprintf_s(arrayAttName.data, NAMEDATALEN, "array_root_%d", origVarAttno);
    }
    securec_check_ss(rc, "\0", "\0");

    /* find proper internal array to support */
    for (int n = tupDesc->natts - 1; n >= 0; n--) {
        Form_pg_attribute attr = TupleDescAttr(tupDesc, n);
        if (pg_strcasecmp(attr->attname.data, arrayAttName.data) == 0) {
            arrayAttNum = n + 1;
            break;
        }
    }

    if (arrayAttNum == InvalidAttrNumber) {
        elog(ERROR, "Internal array_path/array_root for origVarAttno:%d related internal array is not found",
                origVarAttno);
    }

    return arrayAttNum;
}

/* 
 * --------------------------------------------------------------------------------------
 * @Brief: Helper function to output column value in string format, caller need to free
 *         the return string
 * --------------------------------------------------------------------------------------
 */
static const char *GetCurrentValue(TupleTableSlot *srcslot, AttrNumber attnum)
{
    TupleTableSlot* slot = MakeSingleTupleTableSlot(srcslot->tts_tupleDescriptor);
    slot = ExecCopySlot(slot, srcslot);
    
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    HeapTuple tup = ExecFetchSlotTuple(slot);
    bool isnull = true;
    Oid atttypid = tupDesc->attrs[attnum - 1].atttypid;
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
                tupDesc->attrs[attnum - 1].attname.data,
                atttypid, get_typename(atttypid));
        }
    }

    ExecDropSingleTupleTableSlot(slot);
    return pstrdup(value_str);
}