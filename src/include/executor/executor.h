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
#include "nodes/parsenodes.h"
#include "nodes/params.h"
#include "pgxc/pgxc.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/partitionmap_gs.h"
#include "utils/plpgsql.h"

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
 * ExecEvalExpr was formerly a function containing a switch statement;
 * now it's just a macro invoking the function pointed to by an ExprState
 * node.  Beware of double evaluation of the ExprState argument!
 */
#define ExecEvalExpr(expr, econtext, isNull, isDone) ((*(expr)->evalfunc)(expr, econtext, isNull, isDone))

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
    FmgrInfo* eqfunctions, MemoryContext evalContext);
extern bool execTuplesUnequal(TupleTableSlot* slot1, TupleTableSlot* slot2, int numCols, AttrNumber* matchColIdx,
    FmgrInfo* eqfunctions, MemoryContext evalContext);
extern FmgrInfo* execTuplesMatchPrepare(int numCols, Oid* eqOperators);
extern void execTuplesHashPrepare(int numCols, Oid* eqOperators, FmgrInfo** eqFunctions, FmgrInfo** hashFunctions);
extern TupleHashTable BuildTupleHashTable(int numCols, AttrNumber* keyColIdx, FmgrInfo* eqfunctions,
    FmgrInfo* hashfunctions, long nbuckets, Size entrysize, MemoryContext tablecxt, MemoryContext tempcxt, int workMem);
extern TupleHashEntry LookupTupleHashEntry(
    TupleHashTable hashtable, TupleTableSlot* slot, bool* isnew, bool isinserthashtbl = true);
extern TupleHashEntry FindTupleHashEntry(
    TupleHashTable hashtable, TupleTableSlot* slot, FmgrInfo* eqfunctions, FmgrInfo* hashfunctions);

/*
 * prototypes from functions in execJunk.c
 */
extern JunkFilter* ExecInitJunkFilter(List* targetList, bool hasoid, TupleTableSlot* slot, TableAmType tam = TAM_HEAP);
extern JunkFilter* ExecInitJunkFilterConversion(List* targetList, TupleDesc cleanTupType, TupleTableSlot* slot);
extern AttrNumber ExecFindJunkAttribute(JunkFilter* junkfilter, const char* attrName);
extern AttrNumber ExecFindJunkAttributeInTlist(List* targetlist, const char* attrName);
extern Datum ExecGetJunkAttribute(TupleTableSlot* slot, AttrNumber attno, bool* isNull);
extern TupleTableSlot* ExecFilterJunk(JunkFilter* junkfilter, TupleTableSlot* slot);
extern void ExecSetjunkFilteDescriptor(JunkFilter* junkfilter, TupleDesc tupdesc);

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
extern void ExecConstraints(ResultRelInfo* resultRelInfo, TupleTableSlot* slot, EState* estate);
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
extern void EvalPlanQualInit(EPQState* epqstate, EState* estate, Plan* subplan, List* auxrowmarks, int epqParam);
extern void EvalPlanQualSetPlan(EPQState* epqstate, Plan* subplan, List* auxrowmarks);
extern void EvalPlanQualSetTuple(EPQState* epqstate, Index rti, Tuple tuple);
extern Tuple EvalPlanQualGetTuple(EPQState* epqstate, Index rti);

#define EvalPlanQualSetSlot(epqstate, slot) ((epqstate)->origslot = (slot))
extern void EvalPlanQualFetchRowMarks(EPQState* epqstate);
extern void EvalPlanQualFetchRowMarksUHeap(EPQState* epqstate);
extern TupleTableSlot* EvalPlanQualNext(EPQState* epqstate);
extern void EvalPlanQualBegin(EPQState* epqstate, EState* parentestate, bool isUHeap = false);
extern void EvalPlanQualEnd(EPQState* epqstate);

/*
 * prototypes from functions in execProcnode.c
 */
extern PlanState* ExecInitNode(Plan* node, EState* estate, int eflags);
extern TupleTableSlot* ExecProcNode(PlanState* node);
extern Node* MultiExecProcNode(PlanState* node);
extern void ExecEndNode(PlanState* node);
extern bool NeedStubExecution(Plan* plan);

extern long ExecGetPlanMemCost(Plan* node);

/*
 * prototypes from functions in execQual.c
 */
extern Datum GetAttributeByNum(HeapTupleHeader tuple, AttrNumber attrno, bool* isNull);
extern Datum GetAttributeByName(HeapTupleHeader tuple, const char* attname, bool* isNull);
extern Tuplestorestate* ExecMakeTableFunctionResult(
    ExprState* funcexpr, ExprContext* econtext, TupleDesc expectedDesc, bool randomAccess, FunctionScanState* node);
extern Datum ExecEvalExprSwitchContext(
    ExprState* expression, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
extern ExprState* ExecInitExpr(Expr* node, PlanState* parent);
extern ExprState* ExecPrepareExpr(Expr* node, EState* estate);
extern bool ExecQual(List* qual, ExprContext* econtext, bool resultForNull);
extern int ExecTargetListLength(List* targetlist);
extern int ExecCleanTargetListLength(List* targetlist);
extern TupleTableSlot* ExecProject(ProjectionInfo* projInfo, ExprDoneCond* isDone);

extern TupleTableSlot* ExecScan(ScanState* node, ExecScanAccessMtd accessMtd, ExecScanRecheckMtd recheckMtd);
extern void ExecAssignScanProjectionInfo(ScanState* node);
extern void ExecScanReScan(ScanState* node);
extern void initExecTableOfIndexInfo(ExecTableOfIndexInfo* execTableOfIndexInfo, ExprContext* econtext);
extern bool ExecEvalParamExternTableOfIndexById(ExecTableOfIndexInfo* execTableOfIndexInfo);
extern void ExecEvalParamExternTableOfIndex(Node* node, ExecTableOfIndexInfo* execTableOfIndexInfo);
extern bool is_external_clob(Oid type_oid, bool is_null, Datum value);
extern bool is_huge_clob(Oid type_oid, bool is_null, Datum value);

/*
 * prototypes from functions in execTuples.c
 */
extern void ExecInitResultTupleSlot(EState* estate, PlanState* planstate, TableAmType tam = TAM_HEAP);
extern void ExecInitScanTupleSlot(EState* estate, ScanState* scanstate, TableAmType tam = TAM_HEAP);
extern TupleTableSlot* ExecInitExtraTupleSlot(EState* estate, TableAmType tam = TAM_HEAP);
extern TupleTableSlot* ExecInitNullTupleSlot(EState* estate, TupleDesc tupType);
extern TupleDesc ExecTypeFromTL(List* targetList, bool hasoid, bool markdropped = false, TableAmType tam = TAM_HEAP);
extern TupleDesc ExecCleanTypeFromTL(List* targetList, bool hasoid, TableAmType tam = TAM_HEAP);
extern TupleDesc ExecTypeFromExprList(List* exprList, List* namesList, TableAmType tam = TAM_HEAP);
extern void UpdateChangedParamSet(PlanState* node, Bitmapset* newchg);

typedef struct TupOutputState {
    TupleTableSlot* slot;
    DestReceiver* dest;
} TupOutputState;

typedef struct ExecIndexTuplesState {
    EState* estate;
    Relation targetPartRel;
    Partition p;
    bool* conflict;
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
extern EState* CreateExecutorState(MemoryContext saveCxt = NULL);
extern void FreeExecutorState(EState* estate);
extern ExprContext* CreateExprContext(EState* estate);
extern ExprContext* CreateStandaloneExprContext(void);
extern void FreeExprContext(ExprContext* econtext, bool isCommit);
extern void ReScanExprContext(ExprContext* econtext);

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
extern void ExecAssignResultTypeFromTL(PlanState* planstate, TableAmType tam = TAM_HEAP);
extern TupleDesc ExecGetResultType(PlanState* planstate);
extern void ExecAssignVectorForExprEval(ExprContext* econtext);
extern ProjectionInfo* ExecBuildProjectionInfo(
    List* targetList, ExprContext* econtext, TupleTableSlot* slot, TupleDesc inputDesc);
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
    Relation targetPartRel, Partition p, const Bitmapset *modifiedIdxAttrs, const bool inplaceUpdated);

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

// AutoMutexLock
//		Auto object for non-recursive pthread_mutex_t lock
//
class AutoMutexLock {
public:
    AutoMutexLock(pthread_mutex_t* mutex, bool trace = true)
        : m_mutex(mutex), m_fLocked(false), m_trace(trace), m_owner(t_thrd.utils_cxt.CurrentResourceOwner)
    {}

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

            if (t_thrd.utils_cxt.CurrentResourceOwner == NULL)
                m_owner = NULL;

            ret = PthreadMutexUnlock(m_owner, m_mutex, m_trace);
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

#endif /* EXECUTOR_H  */
