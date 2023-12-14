/* -------------------------------------------------------------------------
 *
 * executor.h
 *	  support for the openGauss executor module
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/executor/executor.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "executor/exec/execdesc.h"
#include "parser/parse_coerce.h"
#include "nodes/parsenodes.h"
#include "nodes/params.h"
#include "pgxc/pgxc.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/partitionmap_gs.h"
#include "utils/plpgsql.h"
#include "utils/guc.h"

/*
 * The "eflags" argument to ExecutorStart and the various ExecInitNode
 * routines is a bitwise OR of the following flag bits, which tell the
 * called plan node what to expect.  Note that the flags will get modified
 * as they are passed down the plan tree, since an upper node may require
 * functionality in its subnode not demanded of the plan as a whole
 * (example: MergeJoin requires mark/restore capability in its inner input),
 * or an upper node may shield its input from some functionality requirement
 * (example: Materialize shields its input from needing to do backward scan).
 *
 * EXPLAIN_ONLY indicates that the plan tree is being initialized just so
 * EXPLAIN can print it out; it will not be run.  Hence, no side-effects
 * of startup should occur.  However, error checks (such as permission checks)
 * should be performed.
 *
 * REWIND indicates that the plan node should try to efficiently support
 * rescans without parameter changes.  (Nodes must support ExecReScan calls
 * in any case, but if this flag was not given, they are at liberty to do it
 * through complete recalculation.	Note that a parameter change forces a
 * full recalculation in any case.)
 *
 * BACKWARD indicates that the plan node must respect the es_direction flag.
 * When this is not passed, the plan node will only be run forwards.
 *
 * MARK indicates that the plan node must support Mark/Restore calls.
 * When this is not passed, no Mark/Restore will occur.
 *
 * SKIP_TRIGGERS tells ExecutorStart/ExecutorFinish to skip calling
 * AfterTriggerBeginQuery/AfterTriggerEndQuery.  This does not necessarily
 * mean that the plan can't queue any AFTER triggers; just that the caller
 * is responsible for there being a trigger context for them to be queued in.
 *
 * WITH/WITHOUT_OIDS tell the executor to emit tuples with or without space
 * for OIDs, respectively.	These are currently used only for CREATE TABLE AS.
 * If neither is set, the plan may or may not produce tuples including OIDs.
 */
#define EXEC_FLAG_EXPLAIN_ONLY 0x0001  /* EXPLAIN, no ANALYZE */
#define EXEC_FLAG_REWIND 0x0002        /* need efficient rescan */
#define EXEC_FLAG_BACKWARD 0x0004      /* need backward scan */
#define EXEC_FLAG_MARK 0x0008          /* need mark/restore */
#define EXEC_FLAG_SKIP_TRIGGERS 0x0010 /* skip AfterTrigger calls */
#define EXEC_FLAG_WITH_OIDS 0x0020     /* force OIDs in returned tuples */
#define EXEC_FLAG_WITHOUT_OIDS 0x0040  /* force no OIDs in returned tuples */
#define EXEC_FLAG_WITH_NO_DATA	0x0080	/* rel scannability doesn't matter */

extern inline bool is_errmodule_enable(int elevel, ModuleId mod_id);

#define STREAM_LOG(level, format, ...)                                                                      \
    do {                                                                                                    \
        if (is_errmodule_enable(level, MOD_STREAM)) {                                                       \
            ereport(level, (errmodule(MOD_STREAM), errmsg(format, ##__VA_ARGS__), ignore_interrupt(true))); \
        }                                                                                                   \
    } while (0)

#define MEMCTL_LOG(level, format, ...)                                                                   \
    do {                                                                                                 \
        if (is_errmodule_enable(level, MOD_MEM)) {                                                       \
            ereport(level, (errmodule(MOD_MEM), errmsg(format, ##__VA_ARGS__), ignore_interrupt(true))); \
        }                                                                                                \
    } while (0)

#ifdef USE_ASSERT_CHECKING
#define EARLY_FREE_LOG(A)
#else
#define EARLY_FREE_LOG(A)
#endif

#ifdef USE_ASSERT_CHECKING
#define RECURSIVE_LOG(level, format, ...)                \
    do {                                                 \
        ereport(level, (errmsg(format, ##__VA_ARGS__))); \
    } while (0)
#else
#define RECURSIVE_LOG(level, format, ...)                 \
    do {                                                  \
        ereport(DEBUG1, (errmsg(format, ##__VA_ARGS__))); \
    } while (0)
#endif

#define SET_DOP(dop) (dop > 1 ? dop : 1)
/*
 * Calculate the memory restriction in each thread.
 * And the memeory in each thread must be larger than 64K.
 */
#define SET_NODEMEM(opmem, dop)                                \
    ((opmem > 0 ? opmem : u_sess->attr.attr_memory.work_mem) / \
        ((opmem > 0 ? opmem : u_sess->attr.attr_memory.work_mem) > (SET_DOP(dop) * 64) ? SET_DOP(dop) : 1))

#define SET_NUMGROUPS(node) \
    (node->numGroups / SET_DOP(node->plan.dop) > 0 ? node->numGroups / SET_DOP(node->plan.dop) : 1)

#define HAS_INSTR(node, dnonly) \
    (!dnonly || IS_PGXC_DATANODE) && u_sess->instr_cxt.global_instr != NULL && (node)->ps.instrument != NULL

/*
 * recursive union macro
 */
#define INVALID_RU_PLANNODE_ID 0
#define EXEC_IN_RECURSIVE_MODE(x) (((Plan*)x)->recursive_union_plan_nodeid != INVALID_RU_PLANNODE_ID)

/* Hook for plugins to get control in ExecutorStart() */
typedef void (*ExecutorStart_hook_type)(QueryDesc* queryDesc, int eflags);
extern THR_LOCAL PGDLLIMPORT ExecutorStart_hook_type ExecutorStart_hook;

/* Hook for plugins to get control in ExecutorRun() */
typedef void (*ExecutorRun_hook_type)(QueryDesc* queryDesc, ScanDirection direction, long count);
extern THR_LOCAL PGDLLIMPORT ExecutorRun_hook_type ExecutorRun_hook;

/* Hook for plugins to get control in ExecutorFinish() */
typedef void (*ExecutorFinish_hook_type)(QueryDesc* queryDesc);
extern THR_LOCAL PGDLLIMPORT ExecutorFinish_hook_type ExecutorFinish_hook;

/* Hook for plugins to get control in ExecutorEnd() */
typedef void (*ExecutorEnd_hook_type)(QueryDesc* queryDesc);
extern THR_LOCAL PGDLLIMPORT ExecutorEnd_hook_type ExecutorEnd_hook;

/* Hook for plugins to get control in ExecCheckRTPerms() */
typedef bool (*ExecutorCheckPerms_hook_type)(List*, bool);
extern THR_LOCAL PGDLLIMPORT ExecutorCheckPerms_hook_type ExecutorCheckPerms_hook;

typedef ExprState* (*execInitExprFunc)(Expr* node, PlanState* parent);
/*
 * prototypes from functions in execAmi.c
 */
extern void ExecReScan(PlanState* node);
extern void ExecMarkPos(PlanState* node);
extern void ExecRestrPos(PlanState* node);
extern bool ExecSupportsMarkRestore(Path *pathnode);
extern bool ExecSupportsBackwardScan(Plan* node);
extern bool ExecMaterializesOutput(NodeTag plantype);

/*
 * prototypes from functions in execCurrent.c
 */
extern bool execCurrentOf(CurrentOfExpr* cexpr, ExprContext* econtext, Relation relation, ItemPointer current_tid,
    RelationPtr partitionOfCursor_tid);
#ifdef PGXC
ScanState* search_plan_tree(PlanState* node, Oid table_oid);
#endif

/*
 * prototypes from functions in execGrouping.c
 */
extern bool execTuplesMatch(TupleTableSlot* slot1, TupleTableSlot* slot2, int numCols, AttrNumber* matchColIdx,
    FmgrInfo* eqfunctions, MemoryContext evalContext, Oid *collations);
extern bool execTuplesUnequal(TupleTableSlot* slot1, TupleTableSlot* slot2, int numCols, AttrNumber* matchColIdx,
    FmgrInfo* eqfunctions, MemoryContext evalContext, Oid *collations);
extern FmgrInfo* execTuplesMatchPrepare(int numCols, Oid* eqOperators);
extern void execTuplesHashPrepare(int numCols, Oid* eqOperators, FmgrInfo** eqFunctions, FmgrInfo** hashFunctions);
extern TupleHashTable BuildTupleHashTable(int numCols, AttrNumber* keyColIdx, FmgrInfo* eqfunctions,
    FmgrInfo* hashfunctions, long nbuckets, Size entrysize, MemoryContext tablecxt, MemoryContext tempcxt, int workMem,
    Oid *collations = NULL);
extern TupleHashEntry LookupTupleHashEntry(
    TupleHashTable hashtable, TupleTableSlot* slot, bool* isnew, bool isinserthashtbl = true);
extern TupleHashEntry FindTupleHashEntry(
    TupleHashTable hashtable, TupleTableSlot* slot, FmgrInfo* eqfunctions, FmgrInfo* hashfunctions);

/*
 * prototypes from functions in execJunk.c
 */
extern JunkFilter *ExecInitJunkFilter(List *targetList, bool hasoid, TupleTableSlot *slot,
                                      const TableAmRoutine *tam_ops = TableAmHeap);
extern void ExecInitJunkAttr(EState *estate, CmdType operation, List *targetlist, ResultRelInfo *result_rel_info);
extern JunkFilter *ExecInitJunkFilterConversion(List *targetList, TupleDesc cleanTupType, TupleTableSlot *slot);
extern AttrNumber ExecFindJunkAttribute(JunkFilter *junkfilter, const char *attrName);
extern AttrNumber ExecFindJunkAttributeInTlist(List *targetlist, const char *attrName);
extern Datum ExecGetJunkAttribute(TupleTableSlot *slot, AttrNumber attno, bool *isNull);
extern TupleTableSlot *ExecFilterJunk(JunkFilter *junkfilter, TupleTableSlot *slot);
extern void ExecSetjunkFilteDescriptor(JunkFilter *junkfilter, TupleDesc tupdesc);

#ifdef PGXC
extern List* ExecFindJunkPrimaryKeys(List* targetlist);
#endif
extern VectorBatch* BatchExecFilterJunk(JunkFilter* junkfilter, VectorBatch* batch);
extern void BatchCheckNodeIdentifier(JunkFilter* junkfilter, VectorBatch* batch);
extern int ExecGetPlanNodeid(void);

/*
 * prototypes from functions in execMain.c
 */
extern void ExecutorStart(QueryDesc* queryDesc, int eflags);
extern void standard_ExecutorStart(QueryDesc* queryDesc, int eflags);
extern void ExecutorRun(QueryDesc* queryDesc, ScanDirection direction, long count);
extern void standard_ExecutorRun(QueryDesc* queryDesc, ScanDirection direction, long count);
extern void ExecutorFinish(QueryDesc* queryDesc);
extern void standard_ExecutorFinish(QueryDesc* queryDesc);
extern void ExecutorEnd(QueryDesc* queryDesc);
extern void standard_ExecutorEnd(QueryDesc* queryDesc);
extern void ExecutorRewind(QueryDesc* queryDesc);
extern bool ExecCheckRTPerms(List* rangeTable, bool ereport_on_violation);
extern void CheckValidResultRel(Relation resultRel, CmdType operation);
extern void InitResultRelInfo(
    ResultRelInfo* resultRelInfo, Relation resultRelationDesc, Index resultRelationIndex, int instrument_options);
extern ResultRelInfo* ExecGetTriggerResultRel(EState* estate, Oid relid);
extern bool ExecContextForcesOids(PlanState* planstate, bool* hasoids);
extern bool ExecConstraints(ResultRelInfo* resultRelInfo, TupleTableSlot* slot, EState* estate, bool skipAutoInc = false);
extern void ExecWithCheckOptions(ResultRelInfo *resultRelInfo, TupleTableSlot *slot, EState *estate);
extern ExecRowMark* ExecFindRowMark(EState* estate, Index rti);
extern ExecAuxRowMark* ExecBuildAuxRowMark(ExecRowMark* erm, List* targetlist);
extern TupleTableSlot* EvalPlanQual(EState* estate, EPQState* epqstate, Relation relation, Index rti,
    int lockmode, ItemPointer tid, TransactionId priorXmax, bool partRowMoveUpdate);
extern HeapTuple heap_lock_updated(
    CommandId cid, Relation relation, int lockmode, ItemPointer tid, TransactionId priorXmax);
extern TupleTableSlot* EvalPlanQualUHeap(EState* estate, EPQState* epqstate, Relation relation, Index rti, ItemPointer tid, TransactionId priorXmax);
extern TupleTableSlot *EvalPlanQualUSlot(EPQState *epqstate, Relation relation, Index rti);
extern HeapTuple EvalPlanQualFetch(
    EState* estate, Relation relation, int lockmode, ItemPointer tid, TransactionId priorXmax);
extern void EvalPlanQualInit(EPQState* epqstate, EState* estate, Plan* subplan, List* auxrowmarks, int epqParam,
    ProjectionInfo** projInfos = NULL);
extern void EvalPlanQualSetPlan(EPQState* epqstate, Plan* subplan, List* auxrowmarks);
extern void EvalPlanQualSetTuple(EPQState* epqstate, Index rti, Tuple tuple);
extern Tuple EvalPlanQualGetTuple(EPQState* epqstate, Index rti);

#define EvalPlanQualSetSlot(epqstate, slot) ((epqstate)->origslot = (slot))
extern void EvalPlanQualFetchRowMarks(EPQState* epqstate);
extern void EvalPlanQualFetchRowMarksUHeap(EPQState* epqstate);
extern TupleTableSlot* EvalPlanQualNext(EPQState* epqstate);
extern void EvalPlanQualBegin(EPQState* epqstate, EState* parentestate, bool isUHeap = false);
extern void EvalPlanQualEnd(EPQState* epqstate);
extern bool ExecSetArgIsByValue(FunctionCallInfoData* fcinfo);

/*
 * functions in execProcnode.c
 */
extern PlanState* ExecInitNode(Plan* node, EState* estate, int eflags);
extern Node* MultiExecProcNode(PlanState* node);
extern void ExecEndNode(PlanState* node);
extern bool NeedStubExecution(Plan* plan);
extern TupleTableSlot* FetchPlanSlot(PlanState* subPlanState, ProjectionInfo** projInfos, bool isinherit);

extern long ExecGetPlanMemCost(Plan* node);

/* ----------------------------------------------------------------
 *		ExecProcNode
 *
 *		Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */
#ifndef FRONTEND
#ifndef ENABLE_MULTIPLE_NODES

static inline TupleTableSlot *ExecProcNode(PlanState *node)
{
    TupleTableSlot* result;
    Assert(node->ExecProcNode);
    if (unlikely(node->nodeContext)) {
        MemoryContext old_context = MemoryContextSwitchTo(node->nodeContext); /* Switch to Node Level Memory Context */
        if (node->chgParam != NULL) /* something changed? */
            ExecReScan(node);       /* let ReScan handle this */
        result = node->ExecProcNode(node);
        MemoryContextSwitchTo(old_context);
    } else {
        if (node->chgParam != NULL) /* something changed? */
            ExecReScan(node);
        result = node->ExecProcNode(node);
    }
    node->ps_rownum++;
    return result;
}
#else  /* ENABLE_MULTIPLE_NODES */

static inline TupleTableSlot *ExecProcNode(PlanState *node)
{
    return NULL;
}

#endif /* ENABLE_MULTIPLE_NODES */

#endif /* FRONTEND */


/*
 * prototypes from functions in execExpr.c
 */
extern bool ExecQual(List* qual, ExprContext* econtext, bool resultForNull = false);
extern TupleTableSlot* ExecProject(ProjectionInfo* projInfo, ExprDoneCond* isDone = NULL);
extern ExprState* ExecInitExpr(Expr* node, PlanState* parent);
extern List * ExecInitExprListByRecursion(List *nodes, PlanState* parent);
extern List * ExecInitExprListByFlatten(List *nodes, PlanState* parent);
extern ExprState *ExecInitQual(List *qual, PlanState *parent);
extern ExprState *ExecInitCheck(List *qual, PlanState *parent);
extern List *ExecInitExprList(List *nodes, PlanState *parent);
extern ExprState* ExecBuildAggTrans(AggState* aggstate, struct AggStatePerPhaseData *phase, bool doSort, bool doHash);
extern ProjectionInfo* ExecBuildProjectionInfo(List *targetList,
    ExprContext *econtext, TupleTableSlot *slot, PlanState *parent, TupleDesc inputDesc);
extern ExprState* ExecPrepareExpr(Expr* node, EState* estate);
extern ExprState *ExecPrepareCheck(List *qual, EState *estate);
extern List *ExecPrepareExprList(List *nodes, EState *estate);
extern bool ExecCheck(ExprState *state, ExprContext *context);

/**
 * new expr
 */
template <bool vectorized>
extern void init_fcache(
   Oid foid, Oid input_collation, FuncExprState* fcache, MemoryContext fcacheCxt, bool allowSRF, bool needDescForSRF);
extern ExprState* ExecInitExprByRecursion(Expr* node, PlanState* parent);
extern bool ExecQualByRecursion(List* qual, ExprContext* econtext, bool resultForNull);
extern ExprState* ExecInitExprByFlatten(Expr* node, PlanState* parent);
extern ExprState *ExecInitQualByFlatten(List *qual, PlanState *parent);
extern ExprState *ExecInitQualByRecursion(Expr *qual, PlanState *parent, bool resultForNull = false);
extern ExprState *ExecInitCheckByFlatten(List *qual, PlanState *parent);
extern bool ExecCheckByFlatten(ExprState *state, ExprContext *context);
extern ProjectionInfo* ExecBuildProjectionInfoByFlatten(List *targetList,
                                               ExprContext *econtext, TupleTableSlot *slot, PlanState *parent, TupleDesc inputDesc);
extern ProjectionInfo* ExecBuildProjectionInfoByRecursion(List *targetList, ExprContext *econtext, TupleTableSlot *slot, TupleDesc inputDesc);
extern ExprState *ExecPrepareQualByFlatten(List *qual, EState *estate);
extern TupleTableSlot* ExecProjectByRecursion(ProjectionInfo* projInfo, ExprDoneCond* isDone);

/**
 * prototypes from functions in execSRF.cpp
 */
extern FuncExprState *ExecInitTableFunctionResult(Expr *expr, ExprContext *econtext, PlanState *parent);
extern FuncExprState *ExecInitFunctionResultSet(Expr *expr, ExprContext *econtext, PlanState *parent);
extern Datum ExecMakeFunctionResultSet(FuncExprState *fcache,
                                       ExprContext *econtext,
                                       MemoryContext argContext,
                                       bool *isNull,
                                       ExprDoneCond *isDone);

/*
 * ExecEvalExpr
 *
 * Evaluate expression identified by "state" in the execution context
 * given by "econtext".  *isNull is set to the is-null flag for the result,
 * and the Datum value is the function result.
 *
 * The caller should already have switched into the temporary memory
 * context econtext->ecxt_per_tuple_memory.  The convenience entry point
 * ExecEvalExprSwitchContext() is provided for callers who don't prefer to
 * do the switch in an outer loop.
 */
#ifndef FRONTEND
static inline Datum
ExecEvalExpr(ExprState *state, ExprContext *econtext, bool *isNull, ExprDoneCond *isDone= NULL)
{
	return state->evalfunc(state, econtext, isNull, isDone);
}
#endif

/*
 * ExecEvalExprSwitchContext
 *
 * Same as ExecEvalExpr, but get into the right allocation context explicitly.
 */
#ifndef FRONTEND
Datum
static inline ExecEvalExprSwitchContext(ExprState *state,
						  ExprContext *econtext,
						  bool *isNull,
						  ExprDoneCond *isDone = NULL)
{
	Datum		retDatum;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	retDatum = state->evalfunc(state, econtext, isNull, isDone);
	MemoryContextSwitchTo(oldContext);
	return retDatum;
}
#endif

/*
 * ExecProject
 *
 * Projects a tuple based on projection info and stores it in the slot passed
 * to ExecBuildProjectInfo().
 *
 * Note: the result is always a virtual tuple; therefore it may reference
 * the contents of the exprContext's scan tuples and/or temporary results
 * constructed in the exprContext.  If the caller wishes the result to be
 * valid longer than that data will be valid, he must call ExecMaterializeSlot
 * on the result slot.
 */
#ifndef FRONTEND
static inline TupleTableSlot *
ExecProjectByFlatten(ProjectionInfo *projInfo, ExprDoneCond* isDone = NULL)
{
	ExprContext *econtext = projInfo->pi_exprContext;
	ExprState  *state = &projInfo->pi_state;
	TupleTableSlot *slot = state->resultslot;
	bool		isnull;
    ListCell *lc;
    char* resname = NULL;

    if (isDone) {
        *isDone = ExprSingleResult;
    }

	/*
	 * Clear any former contents of the result slot.  This makes it safe for
	 * us to use the slot's Datum/isnull arrays as workspace.
	 */
	ExecClearTuple(slot);

    if (state->expr) {
        lc = list_head((List*)(state->expr));
        TargetEntry *te = (TargetEntry*)lfirst(lc);
        state->current_targetentry = lc;
        resname = te->resname;
    }

    ELOG_FIELD_NAME_START(resname);

    /* Run the expression, discarding scalar result from the last column. */
	(void) ExecEvalExprSwitchContext(state, econtext, &isnull);

    ELOG_FIELD_NAME_END;

	/*
	 * Successfully formed a result row.  Mark the result slot as containing a
	 * valid virtual tuple (inlined version of ExecStoreVirtualTuple()).
	 */
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

	return slot;
}
#endif

/*
 * ExecQual - evaluate a qual prepared with ExecInitQual (possibly via
 * ExecPrepareQualByFlatten).  Returns true if qual is satisfied, else false.
 *
 * Note: ExecQual used to have a third argument "resultForNull".  The
 * behavior of this function now corresponds to resultForNull == false.
 * If you want the resultForNull == true behavior, see ExecCheck.
 */
#ifndef FRONTEND
static inline bool
ExecQualByFlatten(ExprState *state, ExprContext *econtext)
{
    Datum ret;
    bool isnull;
    ExprDoneCond isDone = ExprSingleResult;

    /* short-circuit (here and in ExecInitQual) for empty restriction list */
    if (state == NULL)
        return true;

    /* verify that expression was compiled using ExecInitQual */
    Assert(state->flags & EEO_FLAG_IS_QUAL);

    ret = ExecEvalExprSwitchContext(state, econtext, &isnull, &isDone);

    /* EEOP_QUAL should never return NULL */
    Assert(!isnull);

    return DatumGetBool(ret);
}
#endif

/*
 * prototypes from functions in execSRF.c
 */
Tuplestorestate* ExecMakeTableFunctionResult(
    ExprState* setexpr, ExprContext* econtext, TupleDesc expectedDesc, bool randomAccess, FunctionScanState* node);

/*
 * prototypes from functions in execScan.c
 */
extern TupleTableSlot* ExecScan(ScanState* node, ExecScanAccessMtd accessMtd, ExecScanRecheckMtd recheckMtd);
extern void ExecAssignScanProjectionInfo(ScanState* node);
extern void ExecScanReScan(ScanState* node);
extern void initExecTableOfIndexInfo(ExecTableOfIndexInfo* execTableOfIndexInfo, ExprContext* econtext);
extern bool ExecEvalParamExternTableOfIndexById(ExecTableOfIndexInfo* execTableOfIndexInfo);
extern void ExecEvalParamExternTableOfIndex(Node* node, ExecTableOfIndexInfo* execTableOfIndexInfo);

/*
 * prototypes from functions in execTuples.c
 */
extern void ExecInitResultTupleSlot(EState *estate, PlanState *planstate, const TableAmRoutine *tam_ops = TableAmHeap);
extern void ExecInitScanTupleSlot(EState *estate, ScanState *scanstate, const TableAmRoutine *tam_ops = TableAmHeap);
extern TupleTableSlot *ExecInitExtraTupleSlot(EState *estate, const TableAmRoutine *tam_ops = TableAmHeap);
extern TupleTableSlot *ExecInitNullTupleSlot(EState *estate, TupleDesc tupType);
extern TupleDesc ExecTypeFromTL(List *targetList, bool hasoid, bool markdropped = false,
                                const TableAmRoutine *tam_ops = TableAmHeap);
extern TupleDesc ExecCleanTypeFromTL(List *targetList, bool hasoid, const TableAmRoutine *tam_ops = TableAmHeap);
extern TupleDesc ExecTypeFromExprList(List *exprList, List *namesList, const TableAmRoutine *tam_ops = TableAmHeap);
extern void UpdateChangedParamSet(PlanState *node, Bitmapset *newchg);
extern void InitOutputValues(RightRefState* refState, Datum* values, bool* isnull, bool* hasExecs);
extern void SortTargetListAsArray(RightRefState *refState, List *targetList, GenericExprState *targetArr[]);

typedef struct TupOutputState {
    TupleTableSlot* slot;
    DestReceiver* dest;
} TupOutputState;

typedef struct ExecIndexTuplesState {
    EState* estate;
    Relation targetPartRel;
    Partition p;
    bool* conflict;
    bool rollbackIndex;
} ExecIndexTuplesState;

typedef struct ConflictInfoData {
    TransactionId     conflictXid;
    ItemPointerData   conflictTid;
} ConflictInfoData;

extern TupOutputState* begin_tup_output_tupdesc(DestReceiver* dest, TupleDesc tupdesc);
extern void do_tup_output(
    TupOutputState* tstate, Datum* values, size_t values_len, const bool* isnull, size_t isnull_len);
extern int do_text_output_multiline(TupOutputState* tstate, char* text);
extern void end_tup_output(TupOutputState* tstate);

/*
 * Write a single line of text given as a C string.
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
#define do_text_output_oneline(tstate, str_to_emit)                 \
    do {                                                            \
        Datum values_[1];                                           \
        bool isnull_[1];                                            \
        values_[0] = PointerGetDatum(cstring_to_text(str_to_emit)); \
        isnull_[0] = false;                                         \
        do_tup_output(tstate, values_, 1, isnull_, 1);              \
        pfree(DatumGetPointer(values_[0]));                         \
    } while (0)

/*
 * prototypes from functions in execUtils.c
 */
extern EState* CreateExecutorState();
extern void FreeExecutorState(EState* estate);
extern ExprContext* CreateExprContext(EState* estate);
extern ExprContext* CreateStandaloneExprContext(void);
extern void FreeExprContext(ExprContext* econtext, bool isCommit);
extern void ReScanExprContext(ExprContext* econtext);
extern void ShutdownExprContext(ExprContext* econtext, bool isCommit);
extern Datum GetAttributeByNum(HeapTupleHeader tuple, AttrNumber attrno, bool* isNull);
extern Datum GetAttributeByName(HeapTupleHeader tuple, const char* attname, bool* isNull);
extern int ExecTargetListLength(List* targetlist);
extern int ExecCleanTargetListLength(List* targetlist);
extern bool is_external_clob(Oid type_oid, bool is_null, Datum value);
extern bool is_huge_clob(Oid type_oid, bool is_null, Datum value);
extern bool func_has_refcursor_args(Oid Funcid, FunctionCallInfoData* fcinfo);
extern void set_result_for_plpgsql_language_function_with_outparam(FuncExprState *fcache, Datum *result, bool *isNull);
extern void set_result_for_plpgsql_language_function_with_outparam_by_flatten(Datum *result, bool *isNull);
extern void ShutdownFuncExpr(Datum arg);
extern void ExecPrepareTuplestoreResult(FuncExprState* fcache, ExprContext* econtext, Tuplestorestate* resultStore, TupleDesc resultDesc);
extern bool expr_func_has_refcursor_args(Oid Funcid);
extern Datum fetch_lob_value_from_tuple(varatt_lob_pointer *lob_pointer, Oid update_oid, bool *is_null);

#define ResetExprContext(econtext) MemoryContextReset((econtext)->ecxt_per_tuple_memory)

extern ExprContext* MakePerTupleExprContext(EState* estate);

#define RuntimeBinding(func, strategy) (this->*func[strategy])
#define InvokeFp(func) (this->*func)

/* Get an EState's per-output-tuple exprcontext, making it if first use */
#define GetPerTupleExprContext(estate) \
    ((estate)->es_per_tuple_exprcontext ? (estate)->es_per_tuple_exprcontext : MakePerTupleExprContext(estate))

#define GetPerTupleMemoryContext(estate) (GetPerTupleExprContext(estate)->ecxt_per_tuple_memory)

/* Reset an EState's per-output-tuple exprcontext, if one's been created */
#define ResetPerTupleExprContext(estate)                          \
    do {                                                          \
        if ((estate)->es_per_tuple_exprcontext)                   \
            ResetExprContext((estate)->es_per_tuple_exprcontext); \
    } while (0)

/* Reset an Slot's per-output-tuple context, if one's been created */
#define ResetSlotPerTupleContext(slot)                      \
    do {                                                    \
        if ((slot) && (slot)->tts_per_tuple_mcxt)           \
            MemoryContextReset((slot)->tts_per_tuple_mcxt); \
    } while (0)

extern void ExecAssignExprContext(EState* estate, PlanState* planstate);
extern void ExecAssignResultType(PlanState* planstate, TupleDesc tupDesc);
extern void ExecAssignResultTypeFromTL(PlanState* planstate, const TableAmRoutine* tam_ops = TableAmHeap);
extern TupleDesc ExecGetResultType(PlanState* planstate);
extern void ExecAssignVectorForExprEval(ExprContext* econtext);

extern void ExecAssignProjectionInfo(PlanState* planstate, TupleDesc inputDesc);
extern void ExecAssignScanProjectionInfoWithVarno(ScanState* node, Index varno);
extern void ExecFreeExprContext(PlanState* planstate);
extern TupleDesc ExecGetScanType(ScanState* scanstate);
extern void ExecAssignScanType(ScanState* scanstate, TupleDesc tupDesc);
extern void ExecAssignScanTypeFromOuterPlan(ScanState* scanstate);

extern bool ExecRelationIsTargetRelation(EState* estate, Index scanrelid);

extern Relation ExecOpenScanRelation(EState* estate, Index scanrelid);
extern void ExecCloseScanRelation(Relation scanrel);

static inline RangeTblEntry *exec_rt_fetch(Index rti, EState *estate)
{
    return (RangeTblEntry *)list_nth(estate->es_range_table, rti - 1);
}

static inline int128 datum2autoinc(ConstrAutoInc *cons_autoinc, Datum datum)
{
    if (cons_autoinc->datum2autoinc_func != NULL) {
        return DatumGetInt128(DirectFunctionCall1((PGFunction)(uintptr_t)cons_autoinc->datum2autoinc_func, datum));
    }
    return DatumGetInt128(datum);
}

static inline Datum autoinc2datum(ConstrAutoInc *cons_autoinc, int128 autoinc)
{
    if (cons_autoinc->autoinc2datum_func != NULL) {
        return DirectFunctionCall1((PGFunction)(uintptr_t)cons_autoinc->autoinc2datum_func, Int128GetDatum(autoinc));
    }
    return Int128GetDatum(autoinc);
}

extern Partition ExecOpenScanParitition(
    EState* estate, Relation parent, PartitionIdentifier* partID, LOCKMODE lockmode);

extern void ExecOpenIndices(ResultRelInfo* resultRelInfo, bool speculative);
extern void ExecCloseIndices(ResultRelInfo* resultRelInfo);
extern List* ExecInsertIndexTuples(
    TupleTableSlot* slot, ItemPointer tupleid, EState* estate, Relation targetPartRel,
    Partition p, int2 bucketId, bool* conflict, Bitmapset *modifiedIdxAttrs, bool inplaceUpdated = false);
extern bool ExecCheckIndexConstraints(TupleTableSlot *slot, EState *estate, Relation targetRel, Partition p,
                                      bool *isgpi, int2 bucketId, ConflictInfoData *conflictInfo,
                                      Oid *conflictPartOid = NULL, int2 *conflictBucketId = NULL);

extern void ExecDeleteIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
    Relation targetPartRel, Partition p, const Bitmapset *modifiedIdxAttrs,
    const bool inplaceUpdated, const bool isRollbackIndex);

extern void ExecUHeapDeleteIndexTuplesGuts(
    TupleTableSlot* oldslot, Relation rel, ModifyTableState* node, ItemPointer tupleid,
    ExecIndexTuplesState exec_index_tuples_state, Bitmapset *modifiedIdxAttrs, bool inplaceUpdated);

extern bool check_exclusion_constraint(Relation heap, Relation index, IndexInfo* indexInfo, ItemPointer tupleid,
    Datum* values, const bool* isnull, EState* estate, bool newIndex, bool errorOK);
extern void RegisterExprContextCallback(ExprContext* econtext, ExprContextCallbackFunction function, Datum arg);
extern void UnregisterExprContextCallback(ExprContext* econtext, ExprContextCallbackFunction function, Datum arg);
extern List* GetAccessedVarnoList(List* targetList, List* qual);
extern ProjectionInfo* ExecBuildVecProjectionInfo(
    List* targetList, List* nt_qual, ExprContext* econtext, TupleTableSlot* slot, TupleDesc inputDesc);
extern bool tlist_matches_tupdesc(PlanState* ps, List* tlist, Index varno, TupleDesc tupdesc);

extern int PthreadMutexLock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace = true);
extern int PthreadMutexTryLock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace = true);
extern int PthreadMutexUnlock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace = true);

extern int PthreadRWlockTryRdlock(ResourceOwner owner, pthread_rwlock_t* rwlock);
extern void PthreadRWlockRdlock(ResourceOwner owner, pthread_rwlock_t* rwlock);
extern int PthreadRWlockTryWrlock(ResourceOwner owner, pthread_rwlock_t* rwlock);
extern void PthreadRWlockWrlock(ResourceOwner owner, pthread_rwlock_t* rwlock);
extern void PthreadRWlockUnlock(ResourceOwner owner, pthread_rwlock_t* rwlock);
extern void PthreadRwLockInit(pthread_rwlock_t* rwlock, pthread_rwlockattr_t *attr);

extern bool executorEarlyStop();
extern void ExecEarlyFree(PlanState* node);
extern void ExecEarlyFreeBody(PlanState* node);
extern void ExecReSetRecursivePlanTree(PlanState* node);

extern void ExplainNodePending(PlanState* result_plan);
extern void ExplainNodeFinish(PlanState* result_plan, PlannedStmt* pstmt, TimestampTz current_time, bool is_pending);

extern void ExecCopyDataFromDatum(PLpgSQL_datum** datums, int dno, Cursor_Data* target_cursor);
extern void ExecCopyDataToDatum(PLpgSQL_datum** datums, int dno, Cursor_Data* target_cursor);

extern Tuple ExecAutoIncrement(Relation rel, EState* estate, TupleTableSlot* slot, Tuple tuple);
extern void RestoreAutoIncrement(Relation rel, EState* estate, Tuple tuple);

/*
 * prototypes from functions in execReplication.cpp
 */
/* Record the fake relation for heap and index */
typedef struct FakeRelationPartition {
    Relation partRel;
    Partition part;
    List *partList;
    Oid partOid;
    bool needRleaseDummyRel;
} FakeRelationPartition;

extern bool RelationFindReplTuple(EState *estate, Relation rel, Oid idxoid, LockTupleMode lockmode, TupleTableSlot *searchslot,
    TupleTableSlot *outslot, FakeRelationPartition *fakeRelInfo);

extern void ExecSimpleRelationInsert(EState *estate, TupleTableSlot *slot, FakeRelationPartition *relAndPart);
extern void ExecSimpleRelationUpdate(EState *estate, EPQState *epqstate, TupleTableSlot *searchslot,
    TupleTableSlot *slot, FakeRelationPartition *relAndPart);
extern void ExecSimpleRelationDelete(EState *estate, EPQState *epqstate, TupleTableSlot *searchslot,
    FakeRelationPartition *relAndPart);
extern void CheckCmdReplicaIdentity(Relation rel, CmdType cmd);
extern void GetFakeRelAndPart(EState *estate, Relation rel, TupleTableSlot *slot, FakeRelationPartition *relAndPart);
extern Datum GetTypeZeroValue(Form_pg_attribute att_tup);
extern Tuple ReplaceTupleNullCol(TupleDesc tupleDesc, TupleTableSlot* slot);

extern Datum ExecEvalArrayRef(ArrayRefExprState* astate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
extern int ResourceOwnerForgetIfExistPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex, bool trace);

// AutoMutexLock
//		Auto object for non-recursive pthread_mutex_t lock
//
class AutoMutexLock {
public:
    AutoMutexLock(pthread_mutex_t* mutex, bool trace = true)
        : m_mutex(mutex), m_fLocked(false), m_trace(trace), m_owner(t_thrd.utils_cxt.CurrentResourceOwner)
    {
        if (mutex == &file_list_lock) {
            m_owner = t_thrd.lsc_cxt.local_sysdb_resowner;
        }
    }

    ~AutoMutexLock()
    {
        unLock();
    }

    inline void lock()
    {
        // Guard against recursive lock
        //
        if (!m_fLocked) {
            int ret = 0;

            ret = PthreadMutexLock(m_owner, m_mutex, m_trace);
            m_fLocked = (ret == 0 ? true : false);
            if (!m_fLocked) {
                /* this should never happen, system may be completely in a mess */
                if (ErrorContext == NULL) {
                    write_stderr(_("ERROR: pthread mutex lock failed, query id %lu: %s\n"),
                        u_sess->debug_query_id,
                        strerror(ret));
                    abort();
                } else
                    ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("pthread mutex lock failed: %s", strerror(ret))));
            }
        }
    }

    inline bool TryLock()
    {
        // Guard against recursive lock
        //
        if (!m_fLocked) {
            m_fLocked = (PthreadMutexTryLock(m_owner, m_mutex, m_trace) == 0 ? true : false);
            return m_fLocked;
        }

        return false;
    }

    inline void unLock()
    {
        if (m_fLocked) {
            int ret = 0;

            if (m_mutex == &file_list_lock) {
                ret = ResourceOwnerForgetIfExistPthreadMutex(m_owner, m_mutex, m_trace);
            } else {
                ret = PthreadMutexUnlock(m_owner, m_mutex, m_trace);
            }
            m_fLocked = (ret == 0 ? false : true);
            if (m_fLocked) {
                /* this should never happen, system may be completely in a mess */
                if (ErrorContext == NULL) {
                    write_stderr(_("ERROR: pthread mutex unlock failed, query id %lu: %s\n"),
                        u_sess->debug_query_id,
                        strerror(ret));
                    abort();
                } else
                    ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("pthread mutex unlock failed: %s", strerror(ret))));
            }
        }
    }

private:
    pthread_mutex_t* m_mutex;
    bool m_fLocked;
    bool m_trace;
    ResourceOwner m_owner;
};

class AutoDopControl {
public:
    AutoDopControl()
    {
        if (likely(u_sess != NULL)) {
            m_smpEnabled = u_sess->opt_cxt.smp_enabled;
        } else {
            m_smpEnabled = true;
        }
    }

    ~AutoDopControl()
    {
        if (u_sess != NULL) {
            u_sess->opt_cxt.smp_enabled = m_smpEnabled;
        }
    }

    void CloseSmp()
    {
        if (likely(u_sess != NULL)) {
            u_sess->opt_cxt.smp_enabled = false;
        }
    }

    void ResetSmp()
    {
        if (u_sess != NULL) {
            u_sess->opt_cxt.smp_enabled = m_smpEnabled;
        }
    }

private:
    bool m_smpEnabled;
};

#ifdef USE_SPQ
extern bool IsJoinExprNull(List *joinExpr, ExprContext *econtext);
#endif
#endif /* EXECUTOR_H  */
