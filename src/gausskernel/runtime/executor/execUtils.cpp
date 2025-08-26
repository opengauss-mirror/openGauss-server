/* -------------------------------------------------------------------------
 *
 * execUtils.cpp
 *	  miscellaneous executor utility routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execUtils.cpp
 *
 * -------------------------------------------------------------------------
 * INTERFACE ROUTINES
 *		CreateExecutorState		Create/delete executor working state
 *		FreeExecutorState
 *		CreateExprContext
 *		CreateStandaloneExprContext
 *		FreeExprContext
 *		ReScanExprContext
 *
 *		ExecAssignExprContext	Common code for plan node init routines.
 *		ExecAssignResultType
 *		etc
 *
 *		ExecOpenScanRelation	Common code for scan node init routines.
 *		ExecCloseScanRelation
 *
 *		ExecOpenIndices			\
 *		ExecCloseIndices		 | referenced by InitPlan, EndPlan,
 *		ExecInsertIndexTuples	/  ExecInsert, ExecUpdate
 *
 *		RegisterExprContextCallback    Register function shutdown callback
 *		UnregisterExprContextCallback  Deregister function shutdown callback
 *
 *	 NOTES
 *		This file has traditionally been the place to stick misc.
 *		executor support stuff that doesn't really go anyplace else.
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc_ext.h"
#include "executor/exec/execdebug.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_expr.h"
#include "storage/lmgr.h"
#include "storage/tcap.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/typcache.h"
#include "optimizer/var.h"
#include "utils/resowner.h"
#include "miscadmin.h"
#include "utils/date.h"
#include "utils/nabstime.h"
#include "utils/geo_decls.h"
#include "utils/varbit.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/xml.h"
#include "utils/rangetypes.h"
#include "commands/sequence.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_proc_fn.h"
#include "funcapi.h"

/* Function Cached Result */

#define FNCACHE_NUM_ARGS			(0x00FF)
#define FNCACHE_NUMARGS(fflags)		((short)((fflags) & FNCACHE_NUM_ARGS))

#define FNCACHE_REF_COUNT			(0xFF00)
#define FNCACHE_REFCOUNT(fflags)	((fflags) & FNCACHE_REF_COUNT)
#define FNCACHE_REF_MAX				(0xFF00)
#define FNCACHE_REF_ONE				(0x0100)

#define FNCACHE_CHECKED			    (1 << 24)   /* function is checked */
#define FNCACHE_ENABLE_CACHE		(1 << 25)   /* function can use cache */
#define FNCACHE_ARG_UNIQUE		    (1 << 26)   /* function has unique arg */

#define FCR_REFCOUNT_ONE 1
#define FCR_REFCOUNT_MASK ((1U << 4) - 1)

#define FCR_USAGECOUNT_MASK 0x003C0000U
#define FCR_USAGECOUNT_ONE (1U << 18)
#define FCR_USAGECOUNT_SHIFT 18
#define FCR_FLAG_MASK 0xFFC00000U

#define FCR_STATE_GET_REFCOUNT(state) ((state) & FCR_REFCOUNT_MASK)
#define FCR_STATE_GET_USAGECOUNT(state) (((state) & FCR_USAGECOUNT_MASK) >> FCR_USAGECOUNT_SHIFT)

/* Function Result Flags */
#define FR_VALID				(1U << 24)	/* cache is valid */
#define FR_RETNULL				(1U << 25)	/* result is null */
#define FR_HITTED				(1U << 26)	/* cache has hitted */

#define FR_STATE_IS_VALID(state) (((state) & FR_VALID) == FR_VALID)
#define FR_STATE_RET_NULL(state) (((state) & FR_RETNULL) == FR_RETNULL)
#define FR_STATE_HITTED(state) (((state) & FR_HITTED) == FR_HITTED)

#define FR_MAX_REF_COUNT	FCR_REFCOUNT_MASK
#define FR_MAX_USAGE_COUNT	5

#define FR_CACHE_NUM_BUCKETS (32)
#define FR_CACHE_SIZE_SECTION (4)
#define FR_CACHE_NUM_SECTIONS (4)
#define FR_CACHE_SIZE_BUCKET (FR_CACHE_SIZE_SECTION * FR_CACHE_NUM_SECTIONS)
#define FR_CACHE_MAX_SIZE ((FR_CACHE_NUM_BUCKETS) * (FR_CACHE_SIZE_BUCKET))

#define FR_CACHE_SEG_RANGE ((UINT32_MAX + 1) / FR_CACHE_NUM_BUCKETS)

#if FUNC_MAX_ARGS > 16
#define FCR_MAX_ARGS (16)
#else
#define FCR_MAX_ARGS (FUNC_MAX_ARGS)
#endif

#if FNCACHE_NUM_ARGS < FCR_MAX_ARGS
#error FCR_MAX_ARGS too large
#endif

typedef struct FuncRetCache
{
    uint32 state;
    uint32 argisnull;
    Datum* args;
    Datum retval;
} FuncRetCache;

typedef struct FuncRetBucket
{
    short nextvict;
    pg_crc32c* argcrcs;
    FuncRetCache** retcache;
} FuncRetBucket;

typedef struct FuncCacheData
{
    Oid fnoid;
    Oid fncoll;
    uint32 fcflags;
    bool security;
    short prevhit;
    Oid *argtypes;
    Oid rettype;
    int usagecount;
    int hitcount;
    union
    {
        FuncRetBucket** retbuckets;
        FuncRetCache* retcache;
    } cacheptr;
} FuncCacheData;

static FuncCache EStateFuncGetCache(EState *es, Oid fid, Oid fncoll);
static FuncCache EStateFuncPutCache(EState *es, Oid fid, Oid fncoll);

static bool get_last_attnums(Node* node, ProjectionInfo* projInfo);
static bool index_recheck_constraint(
    Relation index, Oid* constr_procs, Datum* existing_values, const bool* existing_isnull, Datum* new_values);
static bool check_violation(Relation heap, Relation index, IndexInfo *indexInfo, ItemPointer tupleid, Datum *values,
                            const bool *isnull, EState *estate, bool newIndex, bool errorOK, CheckWaitMode waitMode,
                            ConflictInfoData *conflictInfo, Oid partoid = InvalidOid, int2 bucketid = InvalidBktId,
                            Oid *conflictPartOid = NULL, int2 *conflictBucketid = NULL);
extern struct varlena *heap_tuple_fetch_and_copy(Relation rel, struct varlena *attr, bool needcheck);

/* ----------------------------------------------------------------
 *				 Executor state and memory management functions
 * ----------------------------------------------------------------
 */
/* ----------------
 *		CreateExecutorState
 *
 *		Create and initialize an EState node, which is the root of
 *		working storage for an entire Executor invocation.
 *
 * Principally, this creates the per-query memory context that will be
 * used to hold all working data that lives till the end of the query.
 * Note that the per-query context will become a child of the caller's
 * CurrentMemoryContext.
 * ----------------
 */
EState* CreateExecutorState()
{
    EState* estate = NULL;
    MemoryContext qcontext;
    MemoryContext oldcontext;

    /*
     * Create the per-query context for this Executor run.
     */
    qcontext = AllocSetContextCreate(CurrentMemoryContext,
        "ExecutorState",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Make the EState node within the per-query context.  This way, we don't
     * need a separate pfree_ext() operation for it at shutdown.
     */
    oldcontext = MemoryContextSwitchTo(qcontext);

    /*
     * estate->first_autoinc is int128, when compiled with CLAGS=-O2 on some
     * platforms, using the Movaps directive requires 16-byte alignment.
     */
#ifdef __aarch64__
    estate = makeNode(EState);
#else
    estate = (EState *) palloc0(sizeof(EState) + 16);
    estate = (EState *) TYPEALIGN(16, estate);
#endif  /* __aarch64__ */

    /*
     * Initialize all fields of the Executor State structure
     */
    estate->es_direction = ForwardScanDirection;
    estate->es_snapshot = SnapshotNow;
    estate->es_crosscheck_snapshot = InvalidSnapshot; /* no crosscheck */
    estate->es_range_table = NIL;
    estate->es_plannedstmt = NULL;

    estate->es_junkFilter = NULL;

    estate->es_output_cid = (CommandId)0;

    estate->es_result_relations = NULL;
    estate->es_num_result_relations = 0;
    estate->es_result_relation_info = NULL;
#ifdef PGXC
    estate->es_result_remoterel = NULL;
#endif
    estate->esCurrentPartition = NULL;
    estate->esfRelations = NULL;
    estate->es_trig_target_relations = NIL;
    estate->es_trig_tuple_slot = NULL;
    estate->es_trig_oldtup_slot = NULL;
    estate->es_trig_newtup_slot = NULL;

    estate->es_param_list_info = NULL;
    estate->es_param_exec_vals = NULL;

    estate->es_query_cxt = qcontext;
    estate->es_const_query_cxt = qcontext; /* context query context, it will not be changed */

    estate->es_tupleTable = NIL;
    estate->es_epqTupleSlot = NULL;

    estate->es_rowMarks = NIL;

    estate->es_modifiedRowHash = NIL;
    estate->es_processed = 0;
    estate->es_last_processed = 0;
    estate->es_lastoid = InvalidOid;

    estate->es_top_eflags = 0;
    estate->es_instrument = INSTRUMENT_NONE;
    estate->es_finished = false;

    estate->es_exprcontexts = NIL;

    estate->es_subplanstates = NIL;

    estate->es_auxmodifytables = NIL;
    estate->es_remotequerystates = NIL;

    estate->es_per_tuple_exprcontext = NULL;

    estate->es_epqTuple = NULL;
    estate->es_epqTupleSet = NULL;
    estate->es_epqScanDone = NULL;

    estate->es_subplan_ids = NIL;
    estate->es_skip_early_free = false;
    estate->es_skip_early_deinit_consumer = false;
    estate->es_under_subplan = false;
    estate->es_material_of_subplan = NIL;
    estate->es_recursive_next_iteration = false;

    estate->pruningResult = NULL;
    estate->first_autoinc = 0;
    estate->cur_insert_autoinc = 0;
    estate->next_autoinc = 0;
    estate->es_is_flt_frame = (u_sess->attr.attr_common.enable_expr_fusion && u_sess->attr.attr_sql.query_dop_tmp == 1);
    estate->compileCodegen = false;
    /*
     * Return the executor state structure
     */
    MemoryContextSwitchTo(oldcontext);

    return estate;
}

/* ----------------
 *		FreeExecutorState
 *
 *		Release an EState along with all remaining working storage.
 *
 * Note: this is not responsible for releasing non-memory resources,
 * such as open relations or buffer pins.  But it will shut down any
 * still-active ExprContexts within the EState.  That is sufficient
 * cleanup for situations where the EState has only been used for expression
 * evaluation, and not to run a complete Plan.
 *
 * This can be called in any memory context ... so long as it's not one
 * of the ones to be freed.
 * ----------------
 */
void FreeExecutorState(EState* estate)
{
    /*
     * Shut down and free any remaining ExprContexts.  We do this explicitly
     * to ensure that any remaining shutdown callbacks get called (since they
     * might need to release resources that aren't simply memory within the
     * per-query memory context).
     */
    while (estate->es_exprcontexts) {
        /*
         * XXX: seems there ought to be a faster way to implement this than
         * repeated list_delete(), no?
         */
        FreeExprContext((ExprContext*)linitial(estate->es_exprcontexts), true);
        /* FreeExprContext removed the list link for us */
    }

    /*
     * Free the per-query memory context, thereby releasing all working
     * memory, including the EState node itself.
     */
    MemoryContextDelete(estate->es_query_cxt);
}

/* ----------------
 *		CreateExprContext
 *
 *		Create a context for expression evaluation within an EState.
 *
 * An executor run may require multiple ExprContexts (we usually make one
 * for each Plan node, and a separate one for per-output-tuple processing
 * such as constraint checking).  Each ExprContext has its own "per-tuple"
 * memory context.
 *
 * Note we make no assumption about the caller's memory context.
 * ----------------
 */
ExprContext* CreateExprContext(EState* estate)
{
    ExprContext* econtext = NULL;
    MemoryContext oldcontext;

    /* Create the ExprContext node within the per-query memory context */
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    econtext = makeNode(ExprContext);

    /* Initialize fields of ExprContext */
    econtext->ecxt_scantuple = NULL;
    econtext->ecxt_innertuple = NULL;
    econtext->ecxt_outertuple = NULL;

    econtext->ecxt_per_query_memory = estate->es_query_cxt;

    /*
     * Create working memory for expression evaluation in this context.
     */
    econtext->ecxt_per_tuple_memory = AllocSetContextCreate(estate->es_query_cxt,
        "ExprContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    econtext->ecxt_param_exec_vals = estate->es_param_exec_vals;
    econtext->ecxt_param_list_info = estate->es_param_list_info;

    econtext->ecxt_aggvalues = NULL;
    econtext->ecxt_aggnulls = NULL;

    econtext->caseValue_datum = (Datum)0;
    econtext->caseValue_isNull = true;

    econtext->domainValue_datum = (Datum)0;
    econtext->domainValue_isNull = true;

    econtext->ecxt_estate = estate;

    econtext->ecxt_callbacks = NULL;
    econtext->plpgsql_estate = NULL;
    econtext->hasSetResultStore = false;
#ifdef USE_SPQ
    memset(econtext->cached_root_offsets, 0, sizeof(econtext->cached_root_offsets));
    econtext->cached_blkno = InvalidBlockNumber;
#endif
    /*
     * Link the ExprContext into the EState to ensure it is shut down when the
     * EState is freed.  Because we use lcons(), shutdowns will occur in
     * reverse order of creation, which may not be essential but can't hurt.
     */
    estate->es_exprcontexts = lcons(econtext, estate->es_exprcontexts);

    MemoryContextSwitchTo(oldcontext);

    return econtext;
}

/* ----------------
 *		CreateStandaloneExprContext
 *
 *		Create a context for standalone expression evaluation.
 *
 * An ExprContext made this way can be used for evaluation of expressions
 * that contain no Params, subplans, or Var references (it might work to
 * put tuple references into the scantuple field, but it seems unwise).
 *
 * The ExprContext struct is allocated in the caller's current memory
 * context, which also becomes its "per query" context.
 *
 * It is caller's responsibility to free the ExprContext when done,
 * or at least ensure that any shutdown callbacks have been called
 * (ReScanExprContext() is suitable).  Otherwise, non-memory resources
 * might be leaked.
 * ----------------
 */
ExprContext* CreateStandaloneExprContext(void)
{
    ExprContext* econtext = NULL;

    /* Create the ExprContext node within the caller's memory context */
    econtext = makeNode(ExprContext);

    /* Initialize fields of ExprContext */
    econtext->ecxt_scantuple = NULL;
    econtext->ecxt_innertuple = NULL;
    econtext->ecxt_outertuple = NULL;

    econtext->ecxt_per_query_memory = CurrentMemoryContext;

    /*
     * Create working memory for expression evaluation in this context.
     */
    econtext->ecxt_per_tuple_memory = AllocSetContextCreate(CurrentMemoryContext,
        "ExprContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    econtext->ecxt_param_exec_vals = NULL;
    econtext->ecxt_param_list_info = NULL;

    econtext->ecxt_aggvalues = NULL;
    econtext->ecxt_aggnulls = NULL;

    econtext->caseValue_datum = (Datum)0;
    econtext->caseValue_isNull = true;

    econtext->domainValue_datum = (Datum)0;
    econtext->domainValue_isNull = true;

    econtext->ecxt_estate = NULL;

    econtext->ecxt_callbacks = NULL;

    return econtext;
}

/* ----------------
 *		FreeExprContext
 *
 *		Free an expression context, including calling any remaining
 *		shutdown callbacks.
 *
 * Since we free the temporary context used for expression evaluation,
 * any previously computed pass-by-reference expression result will go away!
 *
 * If isCommit is false, we are being called in error cleanup, and should
 * not call callbacks but only release memory.	(It might be better to call
 * the callbacks and pass the isCommit flag to them, but that would require
 * more invasive code changes than currently seems justified.)
 *
 * Note we make no assumption about the caller's memory context.
 * ----------------
 */
void FreeExprContext(ExprContext* econtext, bool isCommit)
{
    EState* estate = NULL;

    /* Call any registered callbacks */
    ShutdownExprContext(econtext, isCommit);
    /* And clean up the memory used */
    MemoryContextDelete(econtext->ecxt_per_tuple_memory);
    /* Unlink self from owning EState, if any */
    estate = econtext->ecxt_estate;
    if (estate != NULL)
        estate->es_exprcontexts = list_delete_ptr(estate->es_exprcontexts, econtext);
    /* And delete the ExprContext node */
    pfree_ext(econtext);
}

/*
 * ReScanExprContext
 *
 *		Reset an expression context in preparation for a rescan of its
 *		plan node.	This requires calling any registered shutdown callbacks,
 *		since any partially complete set-returning-functions must be canceled.
 *
 * Note we make no assumption about the caller's memory context.
 */
void ReScanExprContext(ExprContext* econtext)
{
    /* Call any registered callbacks */
    ShutdownExprContext(econtext, true);
    /* And clean up the memory used */
    MemoryContextReset(econtext->ecxt_per_tuple_memory);
}

/*
 * Build a per-output-tuple ExprContext for an EState.
 *
 * This is normally invoked via GetPerTupleExprContext() macro,
 * not directly.
 */
ExprContext* MakePerTupleExprContext(EState* estate)
{
    if (estate->es_per_tuple_exprcontext == NULL)
        estate->es_per_tuple_exprcontext = CreateExprContext(estate);

    return estate->es_per_tuple_exprcontext;
}

/* ----------------------------------------------------------------
 *				 miscellaneous node-init support functions
 *
 * Note: all of these are expected to be called with CurrentMemoryContext
 * equal to the per-query memory context.
 * ----------------------------------------------------------------
 */
/* ----------------
 *		ExecAssignExprContext
 *
 *		This initializes the ps_ExprContext field.	It is only necessary
 *		to do this for nodes which use ExecQual or ExecProject
 *		because those routines require an econtext. Other nodes that
 *		don't have to evaluate expressions don't need to do this.
 * ----------------
 */
void ExecAssignExprContext(EState* estate, PlanState* planstate)
{
    planstate->ps_ExprContext = CreateExprContext(estate);
}

/* ----------------
 *		ExecAssignResultType
 * ----------------
 */
void ExecAssignResultType(PlanState* planstate, TupleDesc tupDesc)
{
    TupleTableSlot* slot = planstate->ps_ResultTupleSlot;

    ExecSetSlotDescriptor(slot, tupDesc);
}

/* ----------------
 *		ExecAssignResultTypeFromTL
 * ----------------
 */
void ExecAssignResultTypeFromTL(PlanState* planstate, const TableAmRoutine* tam_ops)
{
    bool hasoid = false;
    TupleDesc tupDesc;

    if (ExecContextForcesOids(planstate, &hasoid)) {
        /* context forces OID choice; hasoid is now set correctly */
    } else {
        /* given free choice, don't leave space for OIDs in result tuples */
        hasoid = false;
    }

    /*
     * ExecTypeFromTL needs the parse-time representation of the tlist, not a
     * list of ExprStates.	This is good because some plan nodes don't bother
     * to set up planstate->targetlist ...
     */
    tupDesc = ExecTypeFromTL(planstate->plan->targetlist, hasoid, false, tam_ops);
    ExecAssignResultType(planstate, tupDesc);
}

/* ----------------
 *		ExecGetResultType
 * ----------------
 */
TupleDesc ExecGetResultType(PlanState* planstate)
{
    TupleTableSlot* slot = NULL;
    /* if the child node is  PartIteratorState, overhead to it's child node */
    if (IsA(planstate, PartIteratorState) || IsA(planstate, VecPartIteratorState)) {
        planstate = outerPlanState(planstate);
    }
    slot = planstate->ps_ResultTupleSlot;

    return slot->tts_tupleDescriptor;
}

void ExecAssignVectorForExprEval(ExprContext* econtext)
{
    Assert(econtext != NULL);

    ScalarDesc unknownDesc;
    ScalarDesc boolDesc;

    boolDesc.typeId = BOOLOID;
    boolDesc.encoded = false;

    econtext->qual_results = New(CurrentMemoryContext) ScalarVector();
    econtext->qual_results->init(CurrentMemoryContext, boolDesc);

    econtext->boolVector = New(CurrentMemoryContext) ScalarVector();
    econtext->boolVector->init(CurrentMemoryContext, boolDesc);

    econtext->caseValue_vector = New(CurrentMemoryContext) ScalarVector();
    econtext->caseValue_vector->init(CurrentMemoryContext, unknownDesc);
}

/* Support info for column store.*/
/* targetList is given from ExprState tree, qual is given from Expr node tree.*/
static void GetAccessedVarNumbers(ProjectionInfo* projInfo, List* targetList, List* qual)
{
    List* vars = NIL;
    List* varattno_list = NIL;
    List* lateAccessVarNoList = NIL;
    List* projectVarNumbers = NIL;
    List* sysVarList = NIL;
    List* qualVarNoList = NIL;
    bool isConst = false;
    ListCell* l = NULL;
    List* PackLateAccessList = NIL;

    foreach (l, targetList) {
        ListCell* vl = NULL;
        GenericExprState* gstate = (GenericExprState*)lfirst(l);
        TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;

        /* Pull vars from  the targetlist .*/
        vars = pull_var_clause((Node*)tle, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

        foreach (vl, vars) {
            Var* var = (Var*)lfirst(vl);
            int varattno = (int)var->varattno;
            if (!list_member_int(varattno_list, varattno)) {
                if (varattno >= 0) {
                    varattno_list = lappend_int(varattno_list, varattno);
                } else {
                    sysVarList = lappend_int(sysVarList, varattno);
                }
            }
        }
    }

    /*
     * Used for PackT optimization: PackTCopyVarsList records those columns what we need to move.
     */
    List* PackTCopyVarsList = list_copy(varattno_list);
    projectVarNumbers = list_copy(varattno_list);

    /* Now consider the  quals */
    vars = pull_var_clause((Node*)qual, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    foreach (l, vars) {
        Var* var = (Var*)lfirst(l);
        int varattno = (int)var->varattno;

        if (var->varattno >= 0) {
            if (!list_member_int(varattno_list, varattno))
                varattno_list = lappend_int(varattno_list, varattno);
            qualVarNoList = lappend_int(qualVarNoList, varattno);
        } else
            sysVarList = lappend_int(sysVarList, varattno);
    }

    if ((list_length(varattno_list) == 0) && (list_length(sysVarList) == 0)) {
        isConst = true;
    }

    // Now we need get which var can be late accessed.
    // In other words, these columns can be load after filter
    // We can read these columns as late as possible
    //
    if (qualVarNoList != NIL) {
        lateAccessVarNoList = list_difference_int(varattno_list, qualVarNoList);
        list_free_ext(qualVarNoList);
    }

    if (PackTCopyVarsList != NIL) {
        PackLateAccessList = list_difference_int(PackTCopyVarsList, lateAccessVarNoList);
    }

    /*
     * Here projInfo->pi_PackTCopyVars records the specific column data what we want.
     */
    projInfo->pi_PackTCopyVars = PackTCopyVarsList;
    projInfo->pi_acessedVarNumbers = varattno_list;
    projInfo->pi_lateAceessVarNumbers = lateAccessVarNoList;
    projInfo->pi_projectVarNumbers = projectVarNumbers;
    projInfo->pi_sysAttrList = sysVarList;
    projInfo->pi_const = isConst;
    projInfo->pi_PackLateAccessVarNumbers = PackLateAccessList;
}

List* GetAccessedVarnoList(List* targetList, List* qual)
{
    ProjectionInfo tmp_pi;

    /* get accessed attno of this query statement */
    GetAccessedVarNumbers(&tmp_pi, targetList, qual);
    if (tmp_pi.pi_PackTCopyVars) {
        list_free_ext(tmp_pi.pi_PackTCopyVars);
    }
    return list_concat(tmp_pi.pi_acessedVarNumbers, tmp_pi.pi_lateAceessVarNumbers);
}

ProjectionInfo* ExecBuildVecProjectionInfo(
    List* targetList, List* nt_qual, ExprContext* econtext, TupleTableSlot* slot, TupleDesc inputDesc)
{
    ProjectionInfo* projInfo = makeNode(ProjectionInfo);
    int len = ExecTargetListLength(targetList);
    int* workspace = NULL;
    int* varSlotOffsets = NULL;
    int* varNumbers = NULL;
    int* varOutputCols = NULL;
    List* exprlist = NIL;
    int numSimpleVars;
    bool directMap = false;
    ListCell* tl = NULL;

    // Guard for zero length projection
    //
    if (len == 0)
        return NULL;

    projInfo->pi_exprContext = econtext;
    projInfo->pi_slot = slot;
    /* since these are all int arrays, we need do just one palloc */
    workspace = (int*)palloc(len * 3 * sizeof(int));
    projInfo->pi_varSlotOffsets = varSlotOffsets = workspace;
    projInfo->pi_varNumbers = varNumbers = workspace + len;
    projInfo->pi_varOutputCols = varOutputCols = workspace + len * 2;
    projInfo->pi_lastInnerVar = 0;
    projInfo->pi_lastOuterVar = 0;
    projInfo->pi_lastScanVar = 0;
    /* Support info for column store.*/
    GetAccessedVarNumbers(projInfo, targetList, nt_qual);

    // Allocate batch for current project.
    //
    projInfo->pi_batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, slot->tts_tupleDescriptor);

    /*
     * We separate the target list elements into simple Var references and
     * expressions which require the full ExecTargetList machinery.  To be a
     * simple Var, a Var has to be a user attribute and not mismatch the
     * inputDesc.  (Note: if there is a type mismatch then ExecEvalVar will
     * probably throw an error at runtime, but we leave that to it.)
     */
    exprlist = NIL;
    numSimpleVars = 0;
    directMap = true;
    foreach (tl, targetList) {
        GenericExprState* gstate = (GenericExprState*)lfirst(tl);
        Var* variable = (Var*)gstate->arg->expr;
        bool isSimpleVar = false;

        if (variable != NULL && IsA(variable, Var) && variable->varattno > 0) {
            if (!inputDesc)
                isSimpleVar = true; /* can't check type, assume OK */
            else if (variable->varattno <= inputDesc->natts) {
                Form_pg_attribute attr;

                attr = &inputDesc->attrs[variable->varattno - 1];
                if (!attr->attisdropped && variable->vartype == attr->atttypid)
                    isSimpleVar = true;
            }
        }

        if (isSimpleVar) {
            TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
            AttrNumber attnum = variable->varattno;

            varNumbers[numSimpleVars] = attnum;
            varOutputCols[numSimpleVars] = tle->resno;
            if (tle->resno != numSimpleVars + 1)
                directMap = false;

            switch (variable->varno) {
                case INNER_VAR:
                    varSlotOffsets[numSimpleVars] = offsetof(ExprContext, ecxt_innerbatch);
                    if (projInfo->pi_lastInnerVar < attnum)
                        projInfo->pi_lastInnerVar = attnum;
                    break;

                case OUTER_VAR:
                    varSlotOffsets[numSimpleVars] = offsetof(ExprContext, ecxt_outerbatch);
                    if (projInfo->pi_lastOuterVar < attnum)
                        projInfo->pi_lastOuterVar = attnum;
                    break;

                default:
                    varSlotOffsets[numSimpleVars] = offsetof(ExprContext, ecxt_scanbatch);
                    if (projInfo->pi_lastScanVar < attnum)
                        projInfo->pi_lastScanVar = attnum;
                    break;
            }
            numSimpleVars++;
        } else {
            /* Not a simple variable, add it to generic targetlist */
            exprlist = lappend(exprlist, gstate);
            /* Examine expr to include contained Vars in lastXXXVar counts */
            get_last_attnums((Node*)variable, projInfo);
        }
    }
    projInfo->pi_targetlist = exprlist;
    projInfo->pi_numSimpleVars = numSimpleVars;
    projInfo->pi_directMap = directMap;

    if (projInfo->pi_exprContext != NULL) {
        projInfo->pi_exprContext->vec_fun_sel = NULL;
        projInfo->pi_exprContext->current_row = 0;
    }

    if (exprlist != NIL) {
        if (projInfo->pi_exprContext != NULL && projInfo->pi_exprContext->have_vec_set_fun) {
            projInfo->pi_vec_itemIsDone = (ExprDoneCond*)palloc0(len * sizeof(ExprDoneCond));
            projInfo->pi_setFuncBatch =
                New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, slot->tts_tupleDescriptor);
            projInfo->pi_exprContext->vec_fun_sel = (bool*)palloc0(BatchMaxSize * sizeof(bool));
            for (int i = 0; i < BatchMaxSize; i++) {
                projInfo->pi_exprContext->vec_fun_sel[i] = true;
            }
        }
    }

    return projInfo;
}

ProjectionInfo* ExecBuildRightRefProjectionInfo(PlanState* planState, TupleDesc inputDesc)
{
    List* targetList = planState->targetlist;
    ExprContext* econtext = planState->ps_ExprContext;
    TupleTableSlot* slot = planState->ps_ResultTupleSlot;

    ProjectionInfo* projInfo = makeNode(ProjectionInfo);
    int len = ExecTargetListLength(targetList);

    econtext->rightRefState = planState->plan->rightRefState;
    projInfo->pi_exprContext = econtext;
    projInfo->pi_slot = slot;
    projInfo->pi_lastInnerVar = 0;
    projInfo->pi_lastOuterVar = 0;
    projInfo->pi_lastScanVar = 0;

    projInfo->pi_targetlist = targetList;
    projInfo->pi_numSimpleVars = 0;
    projInfo->pi_directMap = false;
    projInfo->pi_itemIsDone = (ExprDoneCond*)palloc(len * sizeof(ExprDoneCond));

    return projInfo;
}

/*
 * get_last_attnums: expression walker for ExecBuildProjectionInfo
 *
 *	Update the lastXXXVar counts to be at least as large as the largest
 *	attribute numbers found in the expression
 */
static bool get_last_attnums(Node* node, ProjectionInfo* projInfo)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* variable = (Var*)node;
        AttrNumber attnum = variable->varattno;

        switch (variable->varno) {
            case INNER_VAR:
                if (projInfo->pi_lastInnerVar < attnum)
                    projInfo->pi_lastInnerVar = attnum;
                break;

            case OUTER_VAR:
                if (projInfo->pi_lastOuterVar < attnum)
                    projInfo->pi_lastOuterVar = attnum;
                break;

                /* INDEX_VAR is handled by default case */
            default:
                if (projInfo->pi_lastScanVar < attnum)
                    projInfo->pi_lastScanVar = attnum;
                break;
        }
        return false;
    }

    /*
     * Don't examine the arguments of Aggrefs or WindowFuncs, because those do
     * not represent expressions to be evaluated within the overall
     * overall targetlist's econtext.  GroupingFunc arguments are never
     * evaluated at all.
     */
    if (IsA(node, Aggref) || IsA(node, GroupingFunc))
        return false;
    if (IsA(node, WindowFunc))
        return false;
    return expression_tree_walker(node, (bool (*)())get_last_attnums, (void*)projInfo);
}

ProjectionInfo* ExecBuildProjectionInfo(
    List* targetList, ExprContext* econtext, TupleTableSlot* slot, PlanState *parent, TupleDesc inputDesc)
{
    bool is_flt_frame = (parent != NULL) ? parent->state->es_is_flt_frame : false;

    if (is_flt_frame) {
        return ExecBuildProjectionInfoByFlatten(targetList, econtext, slot, parent, inputDesc);
    } else {
        List* targetListState = NULL;
        targetListState = (List*)ExecInitExprByRecursion((Expr*)targetList, parent);
        return ExecBuildProjectionInfoByRecursion(targetListState, econtext, slot, inputDesc);
    }

}

/* ----------------
 *		ExecBuildProjectionInfoByRecursion
 *
 * Build a ProjectionInfo node for evaluating the given tlist in the given
 * econtext, and storing the result into the tuple slot.  (Caller must have
 * ensured that tuple slot has a descriptor matching the tlist!)  Note that
 * the given tlist should be a list of ExprState nodes, not Expr nodes.
 *
 * inputDesc can be NULL, but if it is not, we check to see whether simple
 * Vars in the tlist match the descriptor.	It is important to provide
 * inputDesc for relation-scan plan nodes, as a cross check that the relation
 * hasn't been changed since the plan was made.  At higher levels of a plan,
 * there is no need to recheck.
 * ----------------
 */
ProjectionInfo* ExecBuildProjectionInfoByRecursion(
    List* targetList, ExprContext* econtext, TupleTableSlot* slot, TupleDesc inputDesc)
{
    ProjectionInfo* projInfo = makeNode(ProjectionInfo);
    int len = ExecTargetListLength(targetList);
    int* workspace = NULL;
    int* varSlotOffsets = NULL;
    int* varNumbers = NULL;
    int* varOutputCols = NULL;
    List* exprlist = NULL;
    int numSimpleVars;
    bool directMap = false;
    ListCell* tl = NULL;

    projInfo->pi_exprContext = econtext;
    projInfo->pi_slot = slot;
    /* since these are all int arrays, we need do just one palloc */
    workspace = (int*)palloc(len * 3 * sizeof(int));
    projInfo->pi_varSlotOffsets = varSlotOffsets = workspace;
    projInfo->pi_varNumbers = varNumbers = workspace + len;
    projInfo->pi_varOutputCols = varOutputCols = workspace + len * 2;
    projInfo->pi_lastInnerVar = 0;
    projInfo->pi_lastOuterVar = 0;
    projInfo->pi_lastScanVar = 0;
    projInfo->isUpsertHasRightRef = false;
    projInfo->pi_state.is_flt_frame = false;

    /*
     * We separate the target list elements into simple Var references and
     * expressions which require the full ExecTargetList machinery.  To be a
     * simple Var, a Var has to be a user attribute and not mismatch the
     * inputDesc.  (Note: if there is a type mismatch then ExecEvalScalarVar
     * will probably throw an error at runtime, but we leave that to it.)
     */
    exprlist = NIL;
    numSimpleVars = 0;
    directMap = true;
    foreach (tl, targetList) {
        GenericExprState* gstate = (GenericExprState*)lfirst(tl);
        Var* variable = (Var*)gstate->arg->expr;
        bool isSimpleVar = false;

        if (variable != NULL && IsA(variable, Var) && variable->varattno > 0) {
            if (!inputDesc)
                isSimpleVar = true; /* can't check type, assume OK */
            else if (variable->varattno <= inputDesc->natts) {
                Form_pg_attribute attr;

                attr = &inputDesc->attrs[variable->varattno - 1];
                if (!attr->attisdropped && variable->vartype == attr->atttypid)
                    isSimpleVar = true;
            }
        }

        if (isSimpleVar) {
            TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
            AttrNumber attnum = variable->varattno;

            varNumbers[numSimpleVars] = attnum;
            varOutputCols[numSimpleVars] = tle->resno;
            if (tle->resno != numSimpleVars + 1)
                directMap = false;

            switch (variable->varno) {
                case INNER_VAR:
                    varSlotOffsets[numSimpleVars] = offsetof(ExprContext, ecxt_innertuple);
                    if (projInfo->pi_lastInnerVar < attnum)
                        projInfo->pi_lastInnerVar = attnum;
                    break;

                case OUTER_VAR:
                    varSlotOffsets[numSimpleVars] = offsetof(ExprContext, ecxt_outertuple);
                    if (projInfo->pi_lastOuterVar < attnum)
                        projInfo->pi_lastOuterVar = attnum;
                    break;

                    /* INDEX_VAR is handled by default case */
                default:
                    varSlotOffsets[numSimpleVars] = offsetof(ExprContext, ecxt_scantuple);
                    if (projInfo->pi_lastScanVar < attnum)
                        projInfo->pi_lastScanVar = attnum;
                    break;
            }
            numSimpleVars++;

            if (econtext && IS_ENABLE_RIGHT_REF(econtext->rightRefState)) {
                exprlist = lappend(exprlist, gstate);
                get_last_attnums((Node*)variable, projInfo);
            }
        } else {
            /* Not a simple variable, add it to generic targetlist */
            exprlist = lappend(exprlist, gstate);
            /* Examine expr to include contained Vars in lastXXXVar counts */
            get_last_attnums((Node*)variable, projInfo);
        }
    }
    projInfo->pi_targetlist = exprlist;
    projInfo->pi_numSimpleVars = numSimpleVars;
    projInfo->pi_directMap = directMap;

    if (exprlist == NIL)
        projInfo->pi_itemIsDone = NULL; /* not needed */
    else
        projInfo->pi_itemIsDone = (ExprDoneCond*)palloc(len * sizeof(ExprDoneCond));

    return projInfo;
}

/* ----------------
 *		ExecAssignProjectionInfo
 *
 * forms the projection information from the node's targetlist
 *
 * Notes for inputDesc are same as for ExecBuildProjectionInfo: supply it
 * for a relation-scan node, can pass NULL for upper-level nodes
 * ----------------
 */
void ExecAssignProjectionInfo(PlanState* planstate, TupleDesc inputDesc)
{
    if (planstate->plan && IS_ENABLE_RIGHT_REF(planstate->plan->rightRefState) && !planstate->state->es_is_flt_frame) {
        planstate->ps_ProjInfo = ExecBuildRightRefProjectionInfo(planstate, inputDesc);
    } else {
        if (planstate->state->es_is_flt_frame) {
            planstate->ps_ProjInfo = ExecBuildProjectionInfoByFlatten(planstate->plan->targetlist, planstate->ps_ExprContext,
                                                                      planstate->ps_ResultTupleSlot, planstate, inputDesc);
        } else {
            planstate->ps_ProjInfo = ExecBuildProjectionInfoByRecursion(planstate->targetlist, planstate->ps_ExprContext,
                                                                        planstate->ps_ResultTupleSlot, inputDesc);
        }
    }
}

/* ----------------
 *		ExecFreeExprContext
 *
 * A plan node's ExprContext should be freed explicitly during executor
 * shutdown because there may be shutdown callbacks to call.  (Other resources
 * made by the above routines, such as projection info, don't need to be freed
 * explicitly because they're just memory in the per-query memory context.)
 *
 * However ... there is no particular need to do it during ExecEndNode,
 * because FreeExecutorState will free any remaining ExprContexts within
 * the EState.	Letting FreeExecutorState do it allows the ExprContexts to
 * be freed in reverse order of creation, rather than order of creation as
 * will happen if we delete them here, which saves O(N^2) work in the list
 * cleanup inside FreeExprContext.
 * ----------------
 */
void ExecFreeExprContext(PlanState* planstate)
{
    /*
     * Per above discussion, don't actually delete the ExprContext. We do
     * unlink it from the plan node, though.
     */
    planstate->ps_ExprContext = NULL;
}

/* ----------------------------------------------------------------
 *		the following scan type support functions are for
 *		those nodes which are stubborn and return tuples in
 *		their Scan tuple slot instead of their Result tuple
 *		slot..	luck fur us, these nodes do not do projections
 *		so we don't have to worry about getting the ProjectionInfo
 *		right for them...  -cim 6/3/91
 * ----------------------------------------------------------------
 */
/* ----------------
 *		ExecGetScanType
 * ----------------
 */
TupleDesc ExecGetScanType(ScanState* scanstate)
{
    TupleTableSlot* slot = scanstate->ss_ScanTupleSlot;

    return slot->tts_tupleDescriptor;
}

/* ----------------
 *		ExecAssignScanType
 * ----------------
 */
void ExecAssignScanType(ScanState* scanstate, TupleDesc tupDesc)
{
    TupleTableSlot* slot = scanstate->ss_ScanTupleSlot;

    ExecSetSlotDescriptor(slot, tupDesc);
}

/* ----------------
 *		ExecAssignScanTypeFromOuterPlan
 * ----------------
 */
void ExecAssignScanTypeFromOuterPlan(ScanState* scanstate)
{
    PlanState* outerPlan = NULL;
    TupleDesc tupDesc;

    outerPlan = outerPlanState(scanstate);
    tupDesc = ExecGetResultType(outerPlan);

    ExecAssignScanType(scanstate, tupDesc);
}

/* ----------------------------------------------------------------
 *				  Scan node support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		ExecRelationIsTargetRelation
 *
 *		Detect whether a relation (identified by rangetable index)
 *		is one of the target relations of the query.
 * ----------------------------------------------------------------
 */
bool ExecRelationIsTargetRelation(EState* estate, Index scanrelid)
{
    ResultRelInfo* resultRelInfos = NULL;
    int i;

    resultRelInfos = estate->es_result_relations;
    for (i = 0; i < estate->es_num_result_relations; i++) {
        if (resultRelInfos[i].ri_RangeTableIndex == scanrelid)
            return true;
    }
    return false;
}

/* ----------------------------------------------------------------
 *		ExecOpenScanRelation
 *
 *		Open the heap relation to be scanned by a base-level scan plan node.
 *		This should be called during the node's ExecInit routine.
 *
 * By default, this acquires AccessShareLock on the relation.  However,
 * if the relation was already locked by InitPlan, we don't need to acquire
 * any additional lock.  This saves trips to the shared lock manager.
 * ----------------------------------------------------------------
 */
Relation ExecOpenScanRelation(EState* estate, Index scanrelid)
{
    Oid reloid;
    LOCKMODE lockmode;
    Relation rel;

    /*
     * Determine the lock type we need.  First, scan to see if target relation
     * is a result relation.  If not, check if it's a FOR UPDATE/FOR SHARE
     * relation.  In either of those cases, we got the lock already.
     */
    lockmode = AccessShareLock;
    if (ExecRelationIsTargetRelation(estate, scanrelid))
        lockmode = NoLock;
    else {
        ListCell* l = NULL;

        foreach (l, estate->es_rowMarks) {
            ExecRowMark* erm = (ExecRowMark*)lfirst(l);

            /* Keep this check in sync with InitPlan! */
            if (erm->rti == scanrelid && erm->relation != NULL) {
                lockmode = NoLock;
                break;
            }
        }
    }

    /* Open the relation and acquire lock as needed */
    reloid = getrelid(scanrelid, estate->es_range_table);
    rel = heap_open(reloid, lockmode);

    if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
    } else
        validateTempRelation(rel);

    return rel;
}

/* ----------------------------------------------------------------
 *		ExecCloseScanRelation
 *
 *		Close the heap relation scanned by a base-level scan plan node.
 *		This should be called during the node's ExecEnd routine.
 *
 * Currently, we do not release the lock acquired by ExecOpenScanRelation.
 * This lock should be held till end of transaction.  (There is a faction
 * that considers this too much locking, however.)
 *
 * If we did want to release the lock, we'd have to repeat the logic in
 * ExecOpenScanRelation in order to figure out what to release.
 * ----------------------------------------------------------------
 */
void ExecCloseScanRelation(Relation scanrel)
{
    heap_close(scanrel, NoLock);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Open the heap partition to be scanned by a base-level scan plan
 *			: node. This should be called during the node's ExecInit routine.
 * Description	:
 * Notes		: By default, this acquires AccessShareLock on the partitioned relation.
 *			: However, if the relation was already locked by InitPlan, we don't need
 *			: to acquire any additional lock.  This saves trips to the shared lock manager.
 */
Partition ExecOpenScanParitition(EState* estate, Relation parent, PartitionIdentifier* partID, LOCKMODE lockmode)
{
    Oid partoid = InvalidOid;

    Assert(PointerIsValid(estate));
    Assert(PointerIsValid(parent));
    Assert(PointerIsValid(partID));

    /* OK, open the relation and acquire lock as needed */
    partoid = partIDGetPartOid(parent, partID);

    return partitionOpen(parent, partoid, lockmode);
}

void ExecOpenUnusedIndices(ResultRelInfo* resultRelInfo, bool speculative)
{
    Relation resultRelation = resultRelInfo->ri_RelationDesc;
    List* indexoidlist = NIL;
    ListCell* l = NULL;
    int len, i;
    RelationPtr relationDescs;
    IndexInfo** indexInfoArray;

    /* fast path if no indexes */
    if (!RelationGetForm(resultRelation)->relhasindex)
        return;

    /*
     * Get cached list of index OIDs
     */
    indexoidlist = RelationGetIndexList(resultRelation, true);
    len = list_length(indexoidlist);
    if (len == 0) {
        return;
    }

    /*
     * allocate space for result arrays
     */
    relationDescs = (RelationPtr)palloc(len * sizeof(Relation));
    indexInfoArray = (IndexInfo**)palloc(len * sizeof(IndexInfo*));

    resultRelInfo->ri_UnusableIndexRelationDescs = relationDescs;
    resultRelInfo->ri_UnusableIndexRelationInfo = indexInfoArray;

    /*
     * For each index, open the index relation and save pg_index info. We
     * acquire RowExclusiveLock, signifying we will update the index.
     *
     * Note: we do this even if the index is not IndexIsReady; it's not worth
     * the trouble to optimize for the case where it isn't.
     */
    i = 0;
    foreach (l, indexoidlist) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc;
        IndexInfo* ii = NULL;

        indexDesc = index_open(indexOid, RowExclusiveLock);

        if (IndexIsUsable(indexDesc->rd_index) || !IndexIsUnique(indexDesc->rd_index)) {
            index_close(indexDesc, RowExclusiveLock);
            continue;
        }

        /* extract index key information from the index's pg_index info */
        ii = BuildIndexInfo(indexDesc);

        /*
         * If the indexes are to be used for speculative insertion, add extra
         * information required by unique index entries.
         */
        if (speculative) {
            BuildSpeculativeIndexInfo(indexDesc, ii);
        }
        relationDescs[i] = indexDesc;
        indexInfoArray[i] = ii;
        i++;
    }
    // remember to set the number of unusable indexes
    resultRelInfo->ri_NumUnusableIndices = i;

    if (resultRelInfo->ri_NumUnusableIndices == 0) {
        ExecCloseUnsedIndices(resultRelInfo);
    }

    list_free_ext(indexoidlist);
}


/* ----------------------------------------------------------------
 *				  ExecInsertIndexTuples support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		ExecOpenIndices
 *
 *		Find the indices associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
void ExecOpenIndices(ResultRelInfo* resultRelInfo, bool speculative, bool checkDisableIndex)
{
    Relation resultRelation = resultRelInfo->ri_RelationDesc;
    List* indexoidlist = NIL;
    ListCell* l = NULL;
    int len, i;
    RelationPtr relationDescs;
    IndexInfo** indexInfoArray;

    resultRelInfo->ri_NumIndices = 0;
    resultRelInfo->ri_ContainGPI = false;
    
    resultRelInfo->ri_NumUnusableIndices = 0;
    resultRelInfo->ri_UnusableIndexRelationDescs = NULL;
    resultRelInfo->ri_UnusableIndexRelationInfo = NULL;

    /* fast path if no indexes */
    if (!RelationGetForm(resultRelation)->relhasindex)
        return;

    /*
     * Get cached list of index OIDs
     */
    indexoidlist = RelationGetIndexList(resultRelation);
    len = list_length(indexoidlist);
    if (len == 0) {
        return;
    }

    /*
     * allocate space for result arrays
     */
    relationDescs = (RelationPtr)palloc(len * sizeof(Relation));
    indexInfoArray = (IndexInfo**)palloc(len * sizeof(IndexInfo*));

    resultRelInfo->ri_IndexRelationDescs = relationDescs;
    resultRelInfo->ri_IndexRelationInfo = indexInfoArray;

    /*
     * For each index, open the index relation and save pg_index info. We
     * acquire RowExclusiveLock, signifying we will update the index.
     *
     * Note: we do this even if the index is not IndexIsReady; it's not worth
     * the trouble to optimize for the case where it isn't.
     */
    i = 0;
    bool immediateDelete = RelationImmediateDelete(resultRelation);
    foreach (l, indexoidlist) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc;
        IndexInfo* ii = NULL;

        indexDesc = index_open(indexOid, RowExclusiveLock);

        /* don't support IUD if there is an diable index */
        if (checkDisableIndex && indexOid >= FirstNormalObjectId &&
            !GetIndexEnableStateByTuple(indexDesc->rd_indextuple)) {
            ereport(ERROR, (errcode(ERRCODE_OPERATOR_INTERVENTION),
                errmsg("The relation(%s) has no permit to write because it has index(%s) in disable state",
                RelationGetRelationName(resultRelation), RelationGetRelationName(indexDesc))));
        }

        // ignore INSERT/UPDATE/DELETE on unusable index
        if (!IndexIsUsable(indexDesc->rd_index)) {
            index_close(indexDesc, RowExclusiveLock);
            continue;
        }

        /* Check index whether is global parition index, and save */
        if (RelationIsGlobalIndex(indexDesc)) {
            resultRelInfo->ri_ContainGPI = true;
        }

        if (unlikely(immediateDelete && !resultRelInfo->ri_hasDiskannIndex &&
                     indexDesc->rd_rel->relam == DISKANN_AM_OID)) {
            resultRelInfo->ri_hasDiskannIndex = true;
        }

        /* extract index key information from the index's pg_index info */
        ii = BuildIndexInfo(indexDesc);

        /*
         * If the indexes are to be used for speculative insertion, add extra
         * information required by unique index entries.
         */
        if (speculative && ii->ii_Unique) {
            BuildSpeculativeIndexInfo(indexDesc, ii);
        }
        relationDescs[i] = indexDesc;
        indexInfoArray[i] = ii;
        i++;
    }
    // remember to set the number of usable indexes
    resultRelInfo->ri_NumIndices = i;

    if (UPDATE_UNUSABLE_UNIQUE_INDEX_ON_IUD) {
        ExecOpenUnusedIndices(resultRelInfo, speculative);
    }

    list_free_ext(indexoidlist);
}

void ExecCloseUnsedIndices(ResultRelInfo* resultRelInfo)
{
    int i;
    int numIndices = resultRelInfo->ri_NumUnusableIndices;
    RelationPtr indexDescs = resultRelInfo->ri_UnusableIndexRelationDescs;
    for (i = 0; i < numIndices; i++) {
        if (indexDescs[i] == NULL)
            continue; /* shouldn't happen? */
        
         /* Drop lock acquired by ExecOpenIndices */
         index_close(indexDescs[i], RowExclusiveLock);
    }
    pfree_ext(resultRelInfo->ri_UnusableIndexRelationDescs);

    if (resultRelInfo->ri_UnusableIndexRelationInfo) {
        for (i = 0; i < numIndices; i++) {
            pfree_ext(resultRelInfo->ri_UnusableIndexRelationInfo[i]);
        }
        pfree_ext(resultRelInfo->ri_UnusableIndexRelationInfo);
    }
    resultRelInfo->ri_UnusableIndexRelationDescs = NULL;
    resultRelInfo->ri_UnusableIndexRelationInfo = NULL;
}


/* ----------------------------------------------------------------
 *		ExecCloseIndices
 *
 *		Close the index relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
void ExecCloseIndices(ResultRelInfo* resultRelInfo)
{
    int i;
    int numIndices;
    RelationPtr indexDescs;

    numIndices = resultRelInfo->ri_NumIndices;
    indexDescs = resultRelInfo->ri_IndexRelationDescs;

    for (i = 0; i < numIndices; i++) {
        if (indexDescs[i] == NULL)
            continue; /* shouldn't happen? */

        /* Drop lock acquired by ExecOpenIndices */
        index_close(indexDescs[i], RowExclusiveLock);
    }
    pfree_ext(resultRelInfo->ri_IndexRelationDescs);

    if (resultRelInfo->ri_IndexRelationInfo) {
        for (i = 0; i < numIndices; i++) {
            pfree_ext(resultRelInfo->ri_IndexRelationInfo[i]);
        }
        pfree_ext(resultRelInfo->ri_IndexRelationInfo);
    }

    if (UPDATE_UNUSABLE_UNIQUE_INDEX_ON_IUD && resultRelInfo->ri_NumUnusableIndices > 0) {
        ExecCloseUnsedIndices(resultRelInfo);
    }

}

/*
 * Copied from ExecInsertIndexTuples
 */
void ExecDeleteIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
    Relation targetPartRel, Partition p, const Bitmapset *modifiedIdxAttrs,
    const bool inplaceUpdated, const bool isRollbackIndex)
{
    ResultRelInfo* resultRelInfo = NULL;
    int numIndices;
    RelationPtr relationDescs;
    Relation heapRelation;
    IndexInfo** indexInfoArray;
    ExprContext* econtext = NULL;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    Relation actualheap;
    bool ispartitionedtable = false;
    List* partitionIndexOidList = NIL;

    resultRelInfo = estate->es_result_relation_info;

    numIndices = resultRelInfo->ri_NumIndices;
    if (numIndices == 0) {
        return;
    }

    if (slot->tts_nvalid == 0) {
        tableam_tslot_getallattrs(slot);
    }

    if (slot->tts_nvalid == 0) {
        elog(ERROR, "no values in slot when trying to delete index tuple");
    }

    /*
     * Get information from the result relation info structure.
     */
    relationDescs = resultRelInfo->ri_IndexRelationDescs;
    indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
    heapRelation = resultRelInfo->ri_RelationDesc;

    /*
     * We will use the EState's per-tuple context for evaluating predicates
     * and index expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    if (RELATION_IS_PARTITIONED(heapRelation)) {
        Assert(PointerIsValid(targetPartRel));

        ispartitionedtable = true;

        actualheap = targetPartRel;

        if (p == NULL || p->pd_part == NULL) {
            return;
        }
        if (!p->pd_part->indisusable) {
            numIndices = 0;
        }
    } else {
        actualheap = heapRelation;
    }

    if (!RelationIsUstoreFormat(heapRelation))
        return;

    AcceptInvalidationMessages();

    /*
     * for each index, form and insert the index tuple
     */
    for (int i = 0; i < numIndices; i++) {
        Relation indexRelation = relationDescs[i];
        IndexInfo* indexInfo = NULL;
        Oid partitionedindexid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Relation actualindex = NULL;
        Partition indexpartition = NULL;

        if (indexRelation == NULL) {
            continue;
        }

        indexInfo = indexInfoArray[i];

        /* If the index is marked as read-only, ignore it */
        /* XXXX: ???? */
        if (!indexInfo->ii_ReadyForInserts) {
            continue;
        }

        if (!IndexIsUsable(indexRelation->rd_index)) {
            continue;
        }

        /* modifiedIdxAttrs != NULL means updating, not every index are affected */
        if (inplaceUpdated && modifiedIdxAttrs != NULL) {
            /* Collect attribute Bitmapset of this index, and compare with modifiedIdxAttrs */
            Bitmapset *indexattrs = IndexGetAttrBitmap(indexRelation, indexInfo);
            bool overlap = bms_overlap(indexattrs, modifiedIdxAttrs);

            bms_free(indexattrs);
            if (!overlap) {
                continue; /* related columns are not modified */
            }
        }

        /* The GPI index insertion is the same as that of a common table */
        if (ispartitionedtable && !RelationIsGlobalIndex(indexRelation)) {
            partitionedindexid = RelationGetRelid(indexRelation);
            if (!PointerIsValid(partitionIndexOidList)) {
                partitionIndexOidList = PartitionGetPartIndexList(p);
                // no local indexes available
                if (!PointerIsValid(partitionIndexOidList)) {
                    return;
                }
            }

            indexpartitionid = searchPartitionIndexOid(partitionedindexid, partitionIndexOidList);

            searchFakeReationForPartitionOid(estate->esfRelations,
                                             estate->es_query_cxt,
                                             indexRelation,
                                             indexpartitionid,
                                             INVALID_PARTITION_NO,
                                             actualindex,
                                             indexpartition,
                                             RowExclusiveLock);
            // skip unusable index
            if (indexpartition != NULL && indexpartition->pd_part != NULL && !indexpartition->pd_part->indisusable) {
                continue;
            }
        } else {
            actualindex = indexRelation;
        }
        /* please adapt hash bucket for ustore here. Ref ExecInsertIndexTuples() */

        /* Check for partial index */
        if (indexInfo->ii_Predicate != NIL) {
            List* predicate = NIL;

            /*
             * If predicate state not set up yet, create it (in the estate's
             * per-query context)
             */
            predicate = indexInfo->ii_PredicateState;
            if (predicate == NIL) {
                if (estate->es_is_flt_frame) {
                    predicate = (List*)ExecPrepareQualByFlatten(indexInfo->ii_Predicate, estate);
                } else {
                    predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
                }
                indexInfo->ii_PredicateState = predicate;
            }

            /* Skip this index-update if the predicate isn't satisfied */
            if (!ExecQual(predicate, econtext)) {
                continue;
            }
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        index_delete(actualindex, values, isnull, tupleid, isRollbackIndex);
    }

    list_free_ext(partitionIndexOidList);
}

void ExecUHeapDeleteIndexTuplesGuts(
    TupleTableSlot* oldslot, Relation rel, ModifyTableState* node, ItemPointer tupleid,
    ExecIndexTuplesState exec_index_tuples_state, Bitmapset *modifiedIdxAttrs, bool inplaceUpdated)
{
    Assert(oldslot);
    if (node != NULL && node->mt_upsert->us_action == UPSERT_UPDATE) {
        ExecDeleteIndexTuples(node->mt_upsert->us_existing,
                              tupleid,
                              exec_index_tuples_state.estate, exec_index_tuples_state.targetPartRel,
                              exec_index_tuples_state.p,
                              modifiedIdxAttrs,
                              inplaceUpdated,
                              exec_index_tuples_state.rollbackIndex);
    } else {
        UHeapTuple tmpUtup = ExecGetUHeapTupleFromSlot(oldslot); // materialize the tuple
        tmpUtup->table_oid = RelationGetRelid(rel);
        ExecDeleteIndexTuples(oldslot,
                              tupleid,
                              exec_index_tuples_state.estate, exec_index_tuples_state.targetPartRel,
                              exec_index_tuples_state.p,
                              modifiedIdxAttrs,
                              inplaceUpdated,
                              exec_index_tuples_state.rollbackIndex);
    }
}

static Tuple autoinc_modify_tuple(TupleDesc desc, EState* estate, TupleTableSlot* slot, Tuple tuple, int128 autoinc)
{
    uint32 natts = (uint32)desc->natts;
    AttrNumber attnum = desc->constr->cons_autoinc->attnum;
    MemoryContext oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    Datum* values = (Datum *)palloc(sizeof(Datum) * natts);
    bool* nulls = (bool *)palloc(sizeof(bool) * natts);
    bool* replaces = (bool *)palloc0(sizeof(bool) * natts);
    errno_t rc = memset_s(replaces, sizeof(bool) * natts, 0, sizeof(bool) * natts);
    securec_check(rc, "\0", "\0");

    values[attnum - 1] = autoinc2datum(desc->constr->cons_autoinc, autoinc);
    nulls[attnum - 1] = false;
    replaces[attnum - 1] = true;
    tuple = tableam_tops_modify_tuple(tuple, desc, values, nulls, replaces);

    (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    (void)MemoryContextSwitchTo(oldContext);
    return tuple;
}

Tuple ExecAutoIncrement(Relation rel, EState* estate, TupleTableSlot* slot, Tuple tuple)
{
    if (rel->rd_att->constr->cons_autoinc == NULL) {
        return tuple;
    }

    ConstrAutoInc* cons_autoinc = rel->rd_att->constr->cons_autoinc;
    int128 autoinc;
    AttrNumber attnum = cons_autoinc->attnum;
    bool is_null = false;
    bool modify_tuple = false;
    Datum datum = tableam_tops_tuple_getattr(tuple, attnum, rel->rd_att, &is_null);

    if (is_null) {
        autoinc = 0;
        modify_tuple = rel->rd_att->attrs[attnum - 1].attnotnull;
    } else {
        autoinc = datum2autoinc(cons_autoinc, datum);
        modify_tuple = (autoinc == 0);
    }

    /* By default, when datum is NULL/0, auto increase;
     * If NO_AUTO_VALUE_ON_ZERO is set, auto increase only when datum is NULL.
     */
    if (is_null || (autoinc == 0 && !CheckPluginNoAutoValueOnZero())) {
        if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
            autoinc = tmptable_autoinc_nextval(rel->rd_rel->relfilenode, cons_autoinc->next);
        } else {
            if (estate->next_autoinc > 0) {
                autoinc = estate->next_autoinc;
                estate->next_autoinc = 0;
            } else {
                autoinc = nextval_internal(cons_autoinc->seqoid);
            }
        }

        estate->cur_insert_autoinc = autoinc;
        if (estate->first_autoinc == 0) {
            estate->first_autoinc = autoinc;
        }
        if (modify_tuple) {
            tuple = autoinc_modify_tuple(rel->rd_att, estate, slot, tuple, autoinc);
        }
    }
    return tuple;
}

static void UpdateAutoIncrement(Relation rel, Tuple tuple, EState* estate)
{
    if (!RelHasAutoInc(rel)) {
        return;
    }

    bool isnull = false;
    ConstrAutoInc* cons_autoinc = rel->rd_att->constr->cons_autoinc;
    Datum datum = tableam_tops_tuple_getattr(tuple, cons_autoinc->attnum, rel->rd_att, &isnull);

    if (!isnull) {
        int128 autoinc = datum2autoinc(cons_autoinc, datum);
        if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
            tmptable_autoinc_setval(rel->rd_rel->relfilenode, cons_autoinc->next, autoinc, true);
        } else {
            autoinc_setval(cons_autoinc->seqoid, autoinc, true);
        }
    }

    if (estate->first_autoinc != 0 && u_sess->cmd_cxt.last_insert_id != estate->first_autoinc) {
        u_sess->cmd_cxt.last_insert_id = estate->first_autoinc;
    }
}

void RestoreAutoIncrement(Relation rel, EState* estate, Tuple tuple)
{
    bool isnull = false;
    ConstrAutoInc* cons_autoinc = rel->rd_att->constr->cons_autoinc;
    Datum datum = tableam_tops_tuple_fast_getattr(tuple, cons_autoinc->attnum, rel->rd_att, &isnull);

    if (!isnull) {
        int128 autoinc = datum2autoinc(cons_autoinc, datum);
        if (autoinc >= estate->cur_insert_autoinc) {
            return;
        }
    }

    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
        *cons_autoinc->next = estate->cur_insert_autoinc;
    } else {
        estate->next_autoinc = estate->cur_insert_autoinc;
    }
}

/* purely for reducing cyclomatic complexity */
static inline bool GetPartiionIndexOidList(List **oidlist_ptr, Partition part)
{
    Assert(oidlist_ptr != NULL);

    if (!PointerIsValid(*oidlist_ptr)) {
        *oidlist_ptr = PartitionGetPartIndexList(part);
        if (!PointerIsValid(*oidlist_ptr)) {
            return false;
        }
    }

    return true;
}

static inline bool CheckForPartialIndex(IndexInfo* indexInfo, EState* estate, ExprContext* econtext)
{
    List* predicate = indexInfo->ii_PredicateState;

    if (indexInfo->ii_Predicate != NIL) {
        /*
         * If predicate state not set up yet, create it (in the estate's
         * per-query context)
         */
        if (predicate == NIL) {
            if (estate->es_is_flt_frame) {
                predicate = (List*)ExecPrepareQualByFlatten(indexInfo->ii_Predicate, estate);
            } else {
                predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
            }
            indexInfo->ii_PredicateState = predicate;
        }

        /* Skip this index-update if the predicate isn't satisfied */
        if (!ExecQual(predicate, econtext)) {
            return false;
        }
    }

    /*
     * If indexInfo->ii_Predicate == NIL, just return true to caller to proceed.
     */
    return true;
}

static inline void SetInfoForUpsertGPI(bool isgpi, Relation *actualHeap, Relation *parentRel, bool *isgpiResult,
                                       Oid *partoid, int2 *bktid)
{
    if (isgpi) {
        *actualHeap = *parentRel;
        *isgpiResult = true;
        *partoid = InvalidOid;
        *bktid = InvalidBktId;
    } else {
        *isgpiResult = false;
    }
}

/* ----------------------------------------------------------------
 *     ExecCheckIndexConstraints
 *
 *     This routine checks if a tuple violates any unique or
 *     exclusion constraints.  Returns true if there is no no conflict.
 *     Otherwise returns false, and the TID of the conflicting
 *     tuple is returned in *conflictTid.
 *
 *     Note that this doesn't lock the values in any way, so it's
 *     possible that a conflicting tuple is inserted immediately
 *     after this returns.  But this can be used for a pre-check
 *     before insertion.
 * ----------------------------------------------------------------
 */
bool ExecCheckIndexConstraints(TupleTableSlot *slot, EState *estate, Relation targetRel, Partition p, bool *isgpiResult,
                               int2 bucketId, ConflictInfoData *conflictInfo, Oid *conflictPartOid,
                               int2 *conflictBucketid)
{
    ResultRelInfo* resultRelInfo = NULL;
    RelationPtr relationDescs = NULL;
    int i = 0;
    int 	numIndices = 0;
    IndexInfo** indexInfoArray = NULL;
    Relation heapRelationDesc = NULL;
    Relation actualHeap = NULL;
    ExprContext* econtext = NULL;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    ItemPointerData invalidItemPtr;
    bool isPartitioned = false;
    bool containGPI;
    List* partitionIndexOidList = NIL;
    Oid partoid;
    int2 bktid;
    errno_t rc;

    ItemPointerSetInvalid(&conflictInfo->conflictTid);
    ItemPointerSetInvalid(&invalidItemPtr);

    /*
     * Get information from the result relation info structure.
     */
    resultRelInfo = estate->es_result_relation_info;
    numIndices = resultRelInfo->ri_NumIndices;
    relationDescs = resultRelInfo->ri_IndexRelationDescs;
    indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
    heapRelationDesc = resultRelInfo->ri_RelationDesc;
    containGPI = resultRelInfo->ri_ContainGPI;
    actualHeap = targetRel;

    rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
    securec_check(rc, "", "");

    if (RELATION_IS_PARTITIONED(heapRelationDesc)) {
        Assert(p != NULL && p->pd_part != NULL);
        isPartitioned = true;

        if (!p->pd_part->indisusable && !containGPI) {
            return true;
        }
    }

    /*
     * use the EState's per-tuple context for evaluating predicates
     * and index expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /*
     * For each index, form index tuple and check if it satisfies the
     * constraint.
     */
    for (i = 0; i < numIndices; i++) {
        actualHeap = targetRel;
        Relation indexRelation = relationDescs[i];
        IndexInfo* indexInfo = NULL;
        bool satisfiesConstraint = false;
        Relation actualIndex = NULL;
        Oid partitionedindexid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition indexpartition = NULL;

        if (indexRelation == NULL)
            continue;

        bool isgpi = RelationIsGlobalIndex(indexRelation);
        bool iscbi = RelationIsCrossBucketIndex(indexRelation);

        indexInfo = indexInfoArray[i];

        if (!indexInfo->ii_Unique && !indexInfo->ii_ExclusionOps)
            continue;

        /* If the index is marked as read-only, ignore it */
        if (!indexInfo->ii_ReadyForInserts)
            continue;

        if (!indexRelation->rd_index->indimmediate)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("INSERT ON DUPLICATE KEY UPDATE does not support deferrable"
                                    " unique constraints/exclusion constraints.")));
        /*
         * We consider a partitioned table with a global index as a normal table,
         * because conflicts can be between multiple partitions.
         */
        if (isPartitioned && !isgpi) {
            partitionedindexid = RelationGetRelid(indexRelation);

            if (!GetPartiionIndexOidList(&partitionIndexOidList, p)) {
                /* no local indexes available */
                return true;
            }

            indexpartitionid = searchPartitionIndexOid(partitionedindexid, partitionIndexOidList);

            searchFakeReationForPartitionOid(estate->esfRelations,
                estate->es_query_cxt,
                indexRelation,
                indexpartitionid,
                INVALID_PARTITION_NO,
                actualIndex,
                indexpartition,
                RowExclusiveLock);
            /* skip unusable index */
            if (indexpartition->pd_part->indisusable == false) {
                continue;
            }
        } else {
            actualIndex = indexRelation;
        }

        if (bucketId != InvalidBktId && !iscbi) {
            searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, actualIndex, bucketId, actualIndex);
        }

        /* Check for partial index */
        if (!CheckForPartialIndex(indexInfo, estate, econtext)) {
            continue;
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        partoid = (isgpi ? p->pd_id : InvalidOid);
        bktid = (iscbi ? bucketId : InvalidBktId);

        SetInfoForUpsertGPI(isgpi, &actualHeap, &heapRelationDesc, isgpiResult, &partoid, &bktid);

        satisfiesConstraint =
            check_violation(actualHeap, actualIndex, indexInfo, &invalidItemPtr, values, isnull, estate, false, true,
                            CHECK_WAIT, conflictInfo, partoid, bktid, conflictPartOid, conflictBucketid);
        if (!satisfiesConstraint) {
            return false;
        }
    }

    return true;
}

/* ----------------------------------------------------------------
 *		ExecInsertIndexTuples
 *
 *		This routine takes care of inserting index tuples
 *		into all the relations indexing the result relation
 *		when a heap tuple is inserted into the result relation.
 *		Much of this code should be moved into the genam
 *		stuff as it only exists here because the genam stuff
 *		doesn't provide the functionality needed by the
 *		executor.. -cim 9/27/89
 *
 *		This returns a list of index OIDs for any unique or exclusion
 *		constraints that are deferred and that had
 *		potential (unconfirmed) conflicts.
 *
 *		CAUTION: this must not be called for a HOT update.
 *		We can't defend against that here for lack of info.
 *		Should we change the API to make it safer?
 * ----------------------------------------------------------------
 */
List* ExecInsertIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
    Relation targetPartRel, Partition p, int2 bucketId, bool* conflict,
    Bitmapset *modifiedIdxAttrs, bool inplaceUpdated)
{
    List* result = NIL;
    ResultRelInfo* resultRelInfo = NULL;
    int i;
    int numIndices;
    int numUnusedIndices;
    RelationPtr relationDescs;
    RelationPtr unusedRelationDescs;
    Relation heapRelation;
    IndexInfo** indexInfoArray;
    IndexInfo** unusedindexInfoArray;
    ExprContext* econtext = NULL;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    Relation actualheap;
    bool ispartitionedtable = false;
    bool containGPI;
    List* partitionIndexOidList = NIL;
    int totalIndices;

    /*
     * Get information from the result relation info structure.
     */
    resultRelInfo = estate->es_result_relation_info;
    numIndices = resultRelInfo->ri_NumIndices;
    relationDescs = resultRelInfo->ri_IndexRelationDescs;
    indexInfoArray = resultRelInfo->ri_IndexRelationInfo;

    numUnusedIndices = resultRelInfo->ri_NumUnusableIndices;
    unusedRelationDescs = resultRelInfo->ri_UnusableIndexRelationDescs;
    unusedindexInfoArray = resultRelInfo->ri_UnusableIndexRelationInfo;

    heapRelation = resultRelInfo->ri_RelationDesc;
    containGPI = resultRelInfo->ri_ContainGPI;

    /*
     * We will use the EState's per-tuple context for evaluating predicates
     * and index expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    if (RELATION_IS_PARTITIONED(heapRelation)) {
        Assert(PointerIsValid(targetPartRel));

        ispartitionedtable = true;

        actualheap = targetPartRel;

        if (p == NULL || p->pd_part == NULL) {
            return NIL;
        }
        /* If the global partition index is included, the index insertion process needs to continue */
        if (!p->pd_part->indisusable && !containGPI && !UPDATE_UNUSABLE_UNIQUE_INDEX_ON_IUD) {
            numIndices = 0;
        }
    } else {
        actualheap = heapRelation;
    }

    totalIndices = numIndices + numUnusedIndices;

    if (bucketId != InvalidBktId) {
        searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, actualheap, bucketId, actualheap);
    }

    /* Partition create in current transaction, set partition and rel reloption wait_clean_gpi */
    if (RelationCreateInCurrXact(actualheap) && containGPI && !PartitionEnableWaitCleanGpi(p)) {
        /* partition create not set wait_clean_gpi, must use update, and we ensure no concurrency */
        PartitionSetWaitCleanGpi(RelationGetRelid(actualheap), true, false);
        /* Partitioned create set wait_clean_gpi=n, and we want save it, so just use inplace */
        PartitionedSetWaitCleanGpi(RelationGetRelationName(heapRelation), RelationGetRelid(heapRelation), true, true);
    }

    /*
     * for each index, form and insert the index tuple
     */
    for (i = 0; i < totalIndices; i++) {
        Relation indexRelation = i < numIndices ? relationDescs[i] : unusedRelationDescs[i - numIndices];
        IndexInfo* indexInfo = NULL;
        IndexUniqueCheck checkUnique;
        bool satisfiesConstraint = false;
        Oid partitionedindexid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Relation actualindex = NULL;
        Partition indexpartition = NULL;

        if (indexRelation == NULL) {
            continue;
        }

        indexInfo = i < numIndices ?  indexInfoArray[i] : unusedindexInfoArray[i - numIndices];

        /* If the index is marked as read-only, ignore it */
        if (!indexInfo->ii_ReadyForInserts) {
            continue;
        }

        /* modifiedIdxAttrs != NULL means updating, not every index are affected */
        if (inplaceUpdated && modifiedIdxAttrs != NULL) {
            /* Collect attribute Bitmapset of this index, and compare with modifiedIdxAttrs */
            Bitmapset *indexattrs = IndexGetAttrBitmap(indexRelation, indexInfo);
            bool overlap = bms_overlap(indexattrs, modifiedIdxAttrs);

            bms_free(indexattrs);
            if (!overlap) {
                continue; /* related columns are not modified */
            }
        }

        /* The GPI index insertion is the same as that of a common table */
        if (ispartitionedtable && !RelationIsGlobalIndex(indexRelation)) {
            partitionedindexid = RelationGetRelid(indexRelation);
            if (!PointerIsValid(partitionIndexOidList)) {
                partitionIndexOidList = PartitionGetPartIndexList(p);
                // no local indexes available
                if (!PointerIsValid(partitionIndexOidList)) {
                    return NIL;
                }
            }

            indexpartitionid = searchPartitionIndexOid(partitionedindexid, partitionIndexOidList);

            searchFakeReationForPartitionOid(estate->esfRelations,
                estate->es_query_cxt,
                indexRelation,
                indexpartitionid,
                INVALID_PARTITION_NO,
                actualindex,
                indexpartition,
                RowExclusiveLock);
            // skip unusable index except UPDATE_UNUSABLE_UNIQUE_INDEX_ON_IUD is set and index is unique
            if (!indexpartition->pd_part->indisusable && !(UPDATE_UNUSABLE_UNIQUE_INDEX_ON_IUD &&
                IndexIsUnique(indexRelation->rd_index))) {
                continue;
            }
        } else {
            actualindex = indexRelation;
        }
        if (bucketId != InvalidBktId && !RelationIsCrossBucketIndex(indexRelation)) {
            searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, actualindex, bucketId, actualindex);
        }

        /* Check for partial index */
        if (indexInfo->ii_Predicate != NIL) {
            List* predicate = NIL;

            /*
             * If predicate state not set up yet, create it (in the estate's
             * per-query context)
             */
            predicate = indexInfo->ii_PredicateState;
            if (predicate == NIL) {
                if (estate->es_is_flt_frame) {
                    predicate = (List*)ExecPrepareQualByFlatten(indexInfo->ii_Predicate, estate);
                } else {
                    predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
                }
                indexInfo->ii_PredicateState = predicate;
            }

            /* Skip this index-update if the predicate isn't satisfied */
            if (!ExecQual(predicate, econtext)) {
                continue;
            }
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        /*
         * The index AM does the actual insertion, plus uniqueness checking.
         *
         * For an immediate-mode unique index, we just tell the index AM to
         * throw error if not unique.
         *
         * For a deferrable unique index, we tell the index AM to just detect
         * possible non-uniqueness, and we add the index OID to the result
         * list if further checking is needed.
         */
        if (!u_sess->attr.attr_common.unique_checks || !indexRelation->rd_index->indisunique) {
            checkUnique = UNIQUE_CHECK_NO;
        } else if (conflict != NULL) {
            checkUnique = UNIQUE_CHECK_PARTIAL;
        } else if (indexRelation->rd_index->indimmediate) {
            checkUnique = UNIQUE_CHECK_YES;
        } else {
            checkUnique = UNIQUE_CHECK_PARTIAL;
        }
        satisfiesConstraint = index_insert(actualindex, /* index relation */
            values,                                     /* array of index Datums */
            isnull,                                     /* null flags */
            tupleid,                                    /* tid of heap tuple */
            actualheap,                                 /* heap relation */
            checkUnique);                               /* type of uniqueness check to do */

        /*
         * If the index has an associated exclusion constraint, check that.
         * This is simpler than the process for uniqueness checks since we
         * always insert first and then check.	If the constraint is deferred,
         * we check now anyway, but don't throw error on violation; instead
         * we'll queue a recheck event.
         *
         * An index for an exclusion constraint can't also be UNIQUE (not an
         * essential property, we just don't allow it in the grammar), so no
         * need to preserve the prior state of satisfiesConstraint.
         */
        if (indexInfo->ii_ExclusionOps != NULL) {
            bool errorOK = !actualindex->rd_index->indimmediate;

            satisfiesConstraint = check_exclusion_constraint(
                actualheap, actualindex, indexInfo, tupleid, values, isnull, estate, false, errorOK);
        }

        if ((checkUnique == UNIQUE_CHECK_PARTIAL || indexInfo->ii_ExclusionOps != NULL) && !satisfiesConstraint) {
            /*
             * The tuple potentially violates the uniqueness or exclusion
             * constraint, so make a note of the index so that we can re-check
             * it later. Speculative inserters are told if there was a
             * speculative conflict, since that always requires a restart.
             */
            result = lappend_oid(result, RelationGetRelid(indexRelation));
            if (conflict != NULL) {
                *conflict = true;
            }
        }
    }

    if (result == NULL) {
        UpdateAutoIncrement(heapRelation, slot->tts_tuple, estate);
    }

    list_free_ext(partitionIndexOidList);
    return result;
}

/*
 * Check for violation of an exclusion constraint
 *
 * heap: the table containing the new tuple
 * index: the index supporting the exclusion constraint
 * indexInfo: info about the index, including the exclusion properties
 * tupleid: heap TID of the new tuple we have just inserted
 * values, isnull: the *index* column values computed for the new tuple
 * estate: an EState we can do evaluation in
 * newIndex: if true, we are trying to build a new index (this affects
 *		only the wording of error messages)
 * errorOK: if true, don't throw error for violation
 *
 * Returns true if OK, false if actual or potential violation
 *
 * When errorOK is true, we report violation without waiting to see if any
 * concurrent transaction has committed or not; so the violation is only
 * potential, and the caller must recheck sometime later.  This behavior
 * is convenient for deferred exclusion checks; we need not bother queuing
 * a deferred event if there is definitely no conflict at insertion time.
 *
 * When errorOK is false, we'll throw error on violation, so a false result
 * is impossible.
 */
bool check_exclusion_constraint(Relation heap, Relation index, IndexInfo* indexInfo, ItemPointer tupleid, Datum* values,
    const bool* isnull, EState* estate, bool newIndex, bool errorOK)
{
    return check_violation(heap, index, indexInfo, tupleid, values, isnull,
        estate, newIndex, errorOK, errorOK ? CHECK_NOWAIT : CHECK_WAIT, NULL);
}

static inline IndexScanDesc scan_handler_idx_beginscan_wrapper(Relation parentheap, Relation heap, Relation index,
    Snapshot snapshot, int nkeys, int norderbys, ScanState* scan_state)
{
    IndexScanDesc index_scan;
    if (RelationIsCrossBucketIndex(index) && RELATION_OWN_BUCKET(parentheap)) {
        /* for cross-bucket index, pass parent relation to construct HBktIdxScanDesc */
        index_scan = scan_handler_idx_beginscan(parentheap, index, snapshot, nkeys, norderbys, scan_state);
        HBktIdxScanDesc hpscan = (HBktIdxScanDesc)index_scan;
        /* then set scan scope to target heap */
        hpscan->currBktHeapRel = hpscan->currBktIdxScan->heapRelation = heap;
        /* also make sure the target heap won't be released at the end of the scan */
        hpscan->rs_rd = heap;
    } else {
        index_scan = scan_handler_idx_beginscan(heap, index, snapshot, nkeys, norderbys, scan_state);
    }

    return index_scan;
}

static inline bool index_scan_need_recheck(IndexScanDesc scan)
{
    if (RELATION_OWN_BUCKET(scan->indexRelation)) {
        return ((HBktIdxScanDesc)scan)->currBktIdxScan->xs_recheck;
    }

    return scan->xs_recheck;
}

bool check_violation(Relation heap, Relation index, IndexInfo *indexInfo, ItemPointer tupleid, Datum *values,
                     const bool *isnull, EState *estate, bool newIndex, bool errorOK, CheckWaitMode waitMode,
                     ConflictInfoData *conflictInfo, Oid partoid, int2 bucketid, Oid *conflictPartOid,
                     int2 *conflictBucketid)
{
    Oid* constr_procs = indexInfo->ii_ExclusionProcs;
    uint16* constr_strats = indexInfo->ii_ExclusionStrats;
    Oid* index_collations = index->rd_indcollation;
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    IndexScanDesc index_scan;
    Tuple tup;
    ScanKeyData scankeys[INDEX_MAX_KEYS];
    SnapshotData DirtySnapshot;
    int i;
    bool conflict = false;
    bool found_self = false;
    ExprContext* econtext = NULL;
    TupleTableSlot* existing_slot = NULL;
    TupleTableSlot* save_scantuple = NULL;
    Relation parentheap;

    /*
     * If any of the input values are NULL, the constraint check is assumed to
     * pass (i.e., we assume the operators are strict).
     */
    for (i = 0; i < indnkeyatts; i++) {
        if (isnull[i]) {
            return true;
        }
    }

    if (indexInfo->ii_ExclusionOps) {
        constr_procs = indexInfo->ii_ExclusionProcs;
        constr_strats = indexInfo->ii_ExclusionStrats;
    } else {
        constr_procs = indexInfo->ii_UniqueProcs;
        constr_strats = indexInfo->ii_UniqueStrats;
    }
    /*
     * Search the tuples that are in the index for any violations, including
     * tuples that aren't visible yet.
     */
    InitDirtySnapshot(DirtySnapshot);

    for (i = 0; i < indnkeyatts; i++) {
        ScanKeyEntryInitialize(
            &scankeys[i], 0, i + 1, constr_strats[i], InvalidOid, index_collations[i], constr_procs[i], values[i]);
    }

    /*
     * Need a TupleTableSlot to put existing tuples in.
     *
     * To use FormIndexDatum, we have to make the econtext's scantuple point
     * to this slot.  Be sure to save and restore caller's value for
     * scantuple.
     */
    existing_slot = MakeSingleTupleTableSlot(RelationGetDescr(heap), false, heap->rd_tam_ops);
    econtext = GetPerTupleExprContext(estate);
    save_scantuple = econtext->ecxt_scantuple;
    econtext->ecxt_scantuple = existing_slot;

    /*
     * May have to restart scan from this point if a potential conflict is
     * found.
     */
retry:
    conflict = false;
    found_self = false;

    /* purely for reducing cyclomatic complexity */
    parentheap = estate->es_result_relation_info->ri_RelationDesc;
    index_scan = scan_handler_idx_beginscan_wrapper(parentheap, heap, index, &DirtySnapshot, indnkeyatts, 0, NULL);
    scan_handler_idx_rescan_local(index_scan, scankeys, indnkeyatts, NULL, 0);
    index_scan->isUpsert = true;

    while ((tup = scan_handler_idx_getnext(index_scan, ForwardScanDirection, partoid, bucketid)) != NULL) {
        TransactionId xwait;
        Datum existing_values[INDEX_MAX_KEYS];
        bool existing_isnull[INDEX_MAX_KEYS];
        char* error_new = NULL;
        char* error_existing = NULL;

        /*
         * Ignore the entry for the tuple we're trying to check.
         */
        ItemPointer item = TUPLE_IS_UHEAP_TUPLE(tup) ? &((UHeapTuple)tup)->ctid : &((HeapTuple)tup)->t_self;
        if (ItemPointerIsValid(tupleid) && ItemPointerEquals(tupleid, item)) {
            if (found_self) /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_FETCH_DATA_FAILED),
                        errmsg("found self tuple multiple times in index \"%s\"", RelationGetRelationName(index))));
            found_self = true;
            continue;
        }

        /*
         * Extract the index column values and isnull flags from the existing
         * tuple.
         */
        (void)ExecStoreTuple(tup, existing_slot, InvalidBuffer, false);
        FormIndexDatum(indexInfo, existing_slot, estate, existing_values, existing_isnull);

        bool is_scan = index_scan_need_recheck(index_scan) &&
            !index_recheck_constraint(index, constr_procs, existing_values, existing_isnull, values);
        /* If lossy indexscan, must recheck the condition */
        if (is_scan) {
            /* tuple doesn't actually match, so no conflict */
            continue;
        }

        /*
         * At this point we have either a conflict or a potential conflict.
         * If an in-progress transaction is affecting the visibility of this
         * tuple, we need to wait for it to complete and then recheck (unless
         * the caller requested not to).  For simplicity we do rechecking by
         * just restarting the whole scan --- this case probably doesn't
         * happen often enough to be worth trying harder, and anyway we don't
         * want to hold any index internal locks while waiting.
         */
        xwait = TransactionIdIsValid(DirtySnapshot.xmin) ? DirtySnapshot.xmin : DirtySnapshot.xmax;

        if (TransactionIdIsValid(xwait) && waitMode == CHECK_WAIT) {
            scan_handler_idx_endscan(index_scan);

            /* for speculative insertion (INSERT ON DUPLICATE KEY UPDATE),
             * we only need to wait the speculative token lock to be release,
             * which happens when the tuple is speculative inserted by other
             * running transction, and has done it's insertion (eithter
             * finished or aborted).
             */
            XactLockTableWait(xwait);
            goto retry;
        }

        /* Determine whether the index column of the scanned tuple is the same
         * as that of the tuple to be inserted. If not, the tuple pointed to by
         * the item has been modified by other transactions. Check again for any conflicts.
         */
        for (int i=0; i < indnkeyatts; i++) {
            if (existing_isnull[i] != isnull[i]) {
                conflict = false;
                scan_handler_idx_endscan(index_scan);
                goto retry;
            }
            if (!existing_isnull[i] &&
                !DatumGetBool(FunctionCall2Coll(&scankeys[i].sk_func, scankeys[i].sk_collation,
                                existing_values[i], values[i]))) {
                conflict = false;
                scan_handler_idx_endscan(index_scan);
                goto retry;
            }
        }

        /*
         * We have a definite conflict (or a potential one, but the caller
         * didn't want to wait). If we're not supposed to raise error, just
         * return to the caller.
         */
        if (errorOK) {
            conflict = true;
            if (conflictInfo != NULL) {
                conflictInfo->conflictTid = *item;
                conflictInfo->conflictXid = tableam_tops_get_conflictXid(heap, tup);
            }
            *conflictPartOid = TUPLE_IS_UHEAP_TUPLE(tup) ? ((UHeapTuple)tup)->table_oid : ((HeapTuple)tup)->t_tableOid;
            *conflictBucketid = TUPLE_IS_UHEAP_TUPLE(tup) ? ((UHeapTuple)tup)->t_bucketId : ((HeapTuple)tup)->t_bucketId;
            break;
        }

        /*
         * We have a definite conflict (or a potential one, but the caller
         * didn't want to wait).  Report it.
         */
        error_new = BuildIndexValueDescription(index, values, isnull);
        error_existing = BuildIndexValueDescription(index, existing_values, existing_isnull);
        newIndex ?
            ereport(ERROR,
                (errcode(ERRCODE_EXCLUSION_VIOLATION),
                    errmsg("could not create exclusion constraint \"%s\" when trying to build a new index",
                        RelationGetRelationName(index)),
                    (error_new && error_existing) ? errdetail("Key %s conflicts with key %s.", error_new, error_existing)
                                                : errdetail("Key conflicts exist."))) :
            ereport(ERROR,
                (errcode(ERRCODE_EXCLUSION_VIOLATION),
                    errmsg(
                        "conflicting key value violates exclusion constraint \"%s\"", RelationGetRelationName(index)),
                    (error_new && error_existing)
                        ? errdetail("Key %s conflicts with existing key %s.", error_new, error_existing)
                        : errdetail("Key conflicts with existing key.")));
    }

    scan_handler_idx_endscan(index_scan);

    /*
     * Ordinarily, at this point the search should have found the originally
     * inserted tuple (if any), unless we exited the loop early because of conflict.
     * However, it is possible to define exclusion constraints for which that
     * wouldn't be true --- for instance, if the operator is <>. So we no
     * longer complain if found_self is still false.
     */
    econtext->ecxt_scantuple = save_scantuple;

    ExecDropSingleTupleTableSlot(existing_slot);

    return !conflict;
}

/*
 * Check existing tuple's index values to see if it really matches the
 * exclusion condition against the new_values.	Returns true if conflict.
 */
static bool index_recheck_constraint(
    Relation index, Oid* constr_procs, Datum* existing_values, const bool* existing_isnull, Datum* new_values)
{
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    int i;

    for (i = 0; i < indnkeyatts; i++) {
        /* Assume the exclusion operators are strict */
        if (existing_isnull[i]) {
            return false;
        }

        if (!DatumGetBool(
                OidFunctionCall2Coll(constr_procs[i], index->rd_indcollation[i], existing_values[i], new_values[i]))) {
            return false;
        }
    }

    return true;
}

/*
 * UpdateChangedParamSet
 *		Add changed parameters to a plan node's chgParam set
 */
void UpdateChangedParamSet(PlanState* node, Bitmapset* newchg)
{
    Bitmapset* parmset = NULL;

    /*
     * The plan node only depends on params listed in its allParam set. Don't
     * include anything else into its chgParam set.
     */
    parmset = bms_intersect(node->plan->allParam, newchg);

    /*
     * Keep node->chgParam == NULL if there's not actually any members; this
     * allows the simplest possible tests in executor node files.
     */
    if (!bms_is_empty(parmset))
        node->chgParam = bms_join(node->chgParam, parmset);
    else
        bms_free_ext(parmset);
}

/*
 * Register a shutdown callback in an ExprContext.
 *
 * Shutdown callbacks will be called (in reverse order of registration)
 * when the ExprContext is deleted or rescanned.  This provides a hook
 * for functions called in the context to do any cleanup needed --- it's
 * particularly useful for functions returning sets.  Note that the
 * callback will *not* be called in the event that execution is aborted
 * by an error.
 */
void RegisterExprContextCallback(ExprContext* econtext, ExprContextCallbackFunction function, Datum arg)
{
    ExprContext_CB* ecxt_callback = NULL;

    /* Save the info in appropriate memory context */
    ecxt_callback = (ExprContext_CB*)MemoryContextAlloc(econtext->ecxt_per_query_memory, sizeof(ExprContext_CB));

    ecxt_callback->function = function;
    ecxt_callback->arg = arg;
    ecxt_callback->resowner = t_thrd.utils_cxt.CurrentResourceOwner;

    /* link to front of list for appropriate execution order */
    ecxt_callback->next = econtext->ecxt_callbacks;
    econtext->ecxt_callbacks = ecxt_callback;
}

/*
 * Deregister a shutdown callback in an ExprContext.
 *
 * Any list entries matching the function and arg will be removed.
 * This can be used if it's no longer necessary to call the callback.
 */
void UnregisterExprContextCallback(ExprContext* econtext, ExprContextCallbackFunction function, Datum arg)
{
    ExprContext_CB** prev_callback = NULL;
    ExprContext_CB* ecxt_callback = NULL;

    prev_callback = &econtext->ecxt_callbacks;

    while ((ecxt_callback = *prev_callback) != NULL) {
        if (ecxt_callback->function == function && ecxt_callback->arg == arg) {
            *prev_callback = ecxt_callback->next;
            pfree_ext(ecxt_callback);
        } else
            prev_callback = &ecxt_callback->next;
    }
}

/*
 * Call all the shutdown callbacks registered in an ExprContext.
 *
 * The callback list is emptied (important in case this is only a rescan
 * reset, and not deletion of the ExprContext).
 *
 * If isCommit is false, just clean the callback list but don't call 'em.
 * (See comment for FreeExprContext.)
 */
void ShutdownExprContext(ExprContext* econtext, bool isCommit)
{
    ExprContext_CB* ecxt_callback = NULL;
    MemoryContext oldcontext;

    /* Fast path in normal case where there's nothing to do. */
    if (econtext->ecxt_callbacks == NULL)
        return;

    /*
     * Call the callbacks in econtext's per-tuple context.  This ensures that
     * any memory they might leak will get cleaned up.
     */
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    /*
     * Call each callback function in reverse registration order.
     */
    ResourceOwner oldOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        while ((ecxt_callback = econtext->ecxt_callbacks) != NULL) {
            econtext->ecxt_callbacks = ecxt_callback->next;
            if (isCommit) {
                t_thrd.utils_cxt.CurrentResourceOwner = ecxt_callback->resowner;
                (*ecxt_callback->function)(ecxt_callback->arg);
            }
            pfree_ext(ecxt_callback);
        }
    }
    PG_CATCH();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;
        PG_RE_THROW();
    }
    PG_END_TRY();
    t_thrd.utils_cxt.CurrentResourceOwner = oldOwner;

    MemoryContextSwitchTo(oldcontext);
}

/* ----------------------------------------------------------------
 *		ExecEvalOper / ExecEvalFunc support routines
 * ----------------------------------------------------------------
 */

int PthreadMutexLock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    HOLD_INTERRUPTS();
    if (owner)
        ResourceOwnerEnlargePthreadMutex(owner);

    int ret = pthread_mutex_lock(mutex);
    if (unlikely(ret != 0)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("aquire mutex lock failed.")));
    }
    if (trace && owner) {
        ResourceOwnerRememberPthreadMutex(owner, mutex);
    } else {
        START_CRIT_SECTION();
    }
    RESUME_INTERRUPTS();
    return ret;
}

int PthreadMutexTryLock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    HOLD_INTERRUPTS();
    if (owner)
        ResourceOwnerEnlargePthreadMutex(owner);

    int ret = pthread_mutex_trylock(mutex);
    if (likely(ret == 0)) {
        if (trace && owner) {
            ResourceOwnerRememberPthreadMutex(owner, mutex);
        } else {
            START_CRIT_SECTION();
        }
    }
    RESUME_INTERRUPTS();
    return ret;
}

int PthreadMutexUnlock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    HOLD_INTERRUPTS();
    int ret = pthread_mutex_unlock(mutex);
    if (unlikely(ret != 0)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("release mutex lock failed.")));
    }
    if (trace && owner) {
        ResourceOwnerForgetPthreadMutex(owner, mutex);
    } else {
        END_CRIT_SECTION();
    }
    RESUME_INTERRUPTS();

    return ret;
}

int PthreadRWlockTryRdlock(ResourceOwner owner, pthread_rwlock_t* rwlock)
{
    if (owner) {
        ResourceOwnerEnlargePthreadRWlock(owner);
    }
    bool ret;
    HOLD_INTERRUPTS();
    ret = pthread_rwlock_tryrdlock(rwlock);
    if (ret == 0) {
        if (owner) {
            ResourceOwnerRememberPthreadRWlock(owner, rwlock);
        } else {
            START_CRIT_SECTION();
        }
    }
    RESUME_INTERRUPTS();
    return ret;
}

void PthreadRWlockRdlock(ResourceOwner owner, pthread_rwlock_t* rwlock)
{
    if (owner) {
        ResourceOwnerEnlargePthreadRWlock(owner);
    }
    HOLD_INTERRUPTS();
    int ret = pthread_rwlock_rdlock(rwlock);
    Assert(ret == 0);
    if (ret != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("aquire rdlock failed")));
    }
    if (owner) {
        ResourceOwnerRememberPthreadRWlock(owner, rwlock);
    } else {
        START_CRIT_SECTION();
    }
    RESUME_INTERRUPTS();
}

int PthreadRWlockTryWrlock(ResourceOwner owner, pthread_rwlock_t* rwlock)
{
    if (owner) {
        ResourceOwnerEnlargePthreadRWlock(owner);
    }
    HOLD_INTERRUPTS();
    int ret = pthread_rwlock_trywrlock(rwlock);
    if (ret == 0) {
        if (owner) {
            ResourceOwnerRememberPthreadRWlock(owner, rwlock);
        } else {
            START_CRIT_SECTION();
        }
    }
    RESUME_INTERRUPTS();
    return ret;
}

void PthreadRWlockWrlock(ResourceOwner owner, pthread_rwlock_t* rwlock)
{
    if (owner) {
        ResourceOwnerEnlargePthreadRWlock(owner);
    }
    HOLD_INTERRUPTS();
    int ret = pthread_rwlock_wrlock(rwlock);
    Assert(ret == 0);
    if (ret != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("aquire wrlock failed")));
    }
    if (owner) {
        ResourceOwnerRememberPthreadRWlock(owner, rwlock);
    } else {
        START_CRIT_SECTION();
    }
    RESUME_INTERRUPTS();
}
void PthreadRWlockUnlock(ResourceOwner owner, pthread_rwlock_t* rwlock)
{
    HOLD_INTERRUPTS();
    int ret = pthread_rwlock_unlock(rwlock);
    Assert(ret == 0);
    if (ret != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("release rwlock failed")));
    }
    if (owner) {
        ResourceOwnerForgetPthreadRWlock(owner, rwlock);
    } else {
        END_CRIT_SECTION();
    }
    RESUME_INTERRUPTS();
}

void PthreadRwLockInit(pthread_rwlock_t* rwlock, pthread_rwlockattr_t *attr)
{
    int ret = pthread_rwlock_init(rwlock, attr);
    Assert(ret == 0);
    if (ret != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INITIALIZE_FAILED), errmsg("init rwlock failed")));
    }
}

/*
 * Get defaulted value of specific type
 */
Datum GetTypeZeroValue(Form_pg_attribute att_tup, bool can_ignore)
{
    if (u_sess->hook_cxt.getTypeZeroValueHook != NULL) {
        return ((getTypeZeroValueFunc)(u_sess->hook_cxt.getTypeZeroValueHook))(att_tup, can_ignore);
    }
    Datum result;
    switch (att_tup->atttypid) {
        case TIMESTAMPOID: {
            result = (Datum)DirectFunctionCall3(timestamp_in, CStringGetDatum("now"), ObjectIdGetDatum(InvalidOid),
                                                Int32GetDatum(-1));
            break;
        }
        case TIMESTAMPTZOID: {
            result = clock_timestamp(NULL);
            break;
        }
        case TIMETZOID: {
            result = (Datum)DirectFunctionCall3(
                timetz_in, CStringGetDatum("00:00:00"), ObjectIdGetDatum(0), Int32GetDatum(-1));
            break;
        }
        case INTERVALOID: {
            result = (Datum)DirectFunctionCall3(
                interval_in, CStringGetDatum("00:00:00"), ObjectIdGetDatum(0), Int32GetDatum(-1));
            break;
        }
        case TINTERVALOID: {
            Datum epoch = (Datum)DirectFunctionCall1(timestamp_abstime, (TimestampGetDatum(SetEpochTimestamp())));
            result = (Datum)DirectFunctionCall2(mktinterval, epoch, epoch);
            break;
        }
        case SMALLDATETIMEOID: {
            result = (Datum)DirectFunctionCall3(
                smalldatetime_in, CStringGetDatum("1970-01-01 00:00:00"), ObjectIdGetDatum(0), Int32GetDatum(-1));
            break;
        }
        case DATEOID: {
            result = timestamp2date(SetEpochTimestamp());
            break;
        }
        case UUIDOID: {
            result = (Datum)DirectFunctionCall3(uuid_in, CStringGetDatum("00000000-0000-0000-0000-000000000000"),
                                                ObjectIdGetDatum(0), Int32GetDatum(-1));
            break;
        }
        case NAMEOID: {
            result = (Datum)DirectFunctionCall1(namein, CStringGetDatum(""));
            break;
        }
        case POINTOID: {
            result = (Datum)DirectFunctionCall1(point_in, CStringGetDatum("(0,0)"));
            break;
        }
        case PATHOID: {
            result = (Datum)DirectFunctionCall1(path_in, CStringGetDatum("0,0"));
            break;
        }
        case POLYGONOID: {
            result = (Datum)DirectFunctionCall1(poly_in, CStringGetDatum("(0,0)"));
            break;
        }
        case CIRCLEOID: {
            result = (Datum)DirectFunctionCall1(circle_in, CStringGetDatum("0,0,0"));
            break;
        }
        case LSEGOID:
        case BOXOID: {
            result = (Datum)DirectFunctionCall1(box_in, CStringGetDatum("0,0,0,0"));
            break;
        }
        case JSONOID: {
            result = (Datum)DirectFunctionCall1(json_in, CStringGetDatum("null"));
            break;
        }
        case JSONBOID: {
            result = (Datum)DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
            break;
        }
        case XMLOID: {
            result = (Datum)DirectFunctionCall1(xml_in, CStringGetDatum("null"));
            break;
        }
        case BITOID: {
            result = (Datum)DirectFunctionCall3(bit_in, CStringGetDatum(""), ObjectIdGetDatum(0), Int32GetDatum(-1));
            break;
        }
        case VARBITOID: {
            result = (Datum)DirectFunctionCall3(varbit_in, CStringGetDatum(""), ObjectIdGetDatum(0), Int32GetDatum(-1));
            break;
        }
        case NUMERICOID: {
            result =
                (Datum)DirectFunctionCall3(numeric_in, CStringGetDatum("0"), ObjectIdGetDatum(0), Int32GetDatum(0));
            break;
        }
        case CIDROID: {
            result = DirectFunctionCall1(cidr_in, CStringGetDatum("0.0.0.0"));
            break;
        }
        case INETOID: {
            result = DirectFunctionCall1(inet_in, CStringGetDatum("0.0.0.0"));
            break;
        }
        case MACADDROID: {
            result = (Datum)DirectFunctionCall1(macaddr_in, CStringGetDatum("00:00:00:00:00:00"));
            break;
        }
        case NUMRANGEOID:
        case INT8RANGEOID:
        case INT4RANGEOID: {
            Type targetType = typeidType(att_tup->atttypid);
            result = stringTypeDatum(targetType, "(0,0)", NULL, NULL, att_tup->atttypmod, true);
            ReleaseSysCache(targetType);
            break;
        }
        case TSRANGEOID:
        case TSTZRANGEOID: {
            Type targetType = typeidType(att_tup->atttypid);
            result = stringTypeDatum(targetType, "(1970-01-01 00:00:00,1970-01-01 00:00:00)", NULL, NULL,
                att_tup->atttypmod, true);
            ReleaseSysCache(targetType);
            break;
        }
        case DATERANGEOID: {
            Type targetType = typeidType(att_tup->atttypid);
            result = stringTypeDatum(targetType, "(1970-01-01,1970-01-01)", NULL, NULL, att_tup->atttypmod, true);
            ReleaseSysCache(targetType);
            break;
        }
        case HASH16OID: {
            Type targetType = typeidType(att_tup->atttypid);
            result = stringTypeDatum(targetType, "0", NULL, NULL, att_tup->atttypmod, true);
            ReleaseSysCache(targetType);
            break;
        }
        case HASH32OID: {
            Type targetType = typeidType(att_tup->atttypid);
            result = stringTypeDatum(targetType, "00000000000000000000000000000000", NULL, NULL,
                att_tup->atttypmod, true);
            ReleaseSysCache(targetType);
            break;
        }
        case TSVECTOROID: {
            Type targetType = typeidType(att_tup->atttypid);
            result = stringTypeDatum(targetType, "", NULL, NULL, att_tup->atttypmod, true);
            ReleaseSysCache(targetType);
            break;
        }
        default: {
            bool typeIsVarlena = (!att_tup->attbyval) && (att_tup->attlen == -1);
            if (typeIsVarlena) {
                result = CStringGetTextDatum("");
            } else {
                result = (Datum)0;
            }
            break;
        }
    }
    return result;
}

/*
 * Replace tuple from the slot with a new one. The new tuple will replace null column with defaulted values according to
 * its type.
 */
Tuple ReplaceTupleNullCol(TupleDesc tupleDesc, TupleTableSlot *slot, bool canIgnore)
{
    /* find out all null column first */
    int natts = tupleDesc->natts;
    Datum values[natts];
    bool replaces[natts];
    bool nulls[natts];
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    int attrChk;
    for (attrChk = 1; attrChk <= natts; attrChk++) {
        if (tupleDesc->attrs[attrChk - 1].attnotnull && tableam_tslot_attisnull(slot, attrChk)) {
            values[attrChk - 1] = GetTypeZeroValue(&tupleDesc->attrs[attrChk - 1], canIgnore);
            replaces[attrChk - 1] = true;
        }
    }
    Tuple oldTup = slot->tts_tuple;
    Tuple newTup = tableam_tops_modify_tuple(oldTup, tupleDesc, values, nulls, replaces);

    /* revise members of slot */
    slot->tts_tuple = newTup;
    for (attrChk = 1; attrChk <= natts; attrChk++) {
        if (!replaces[attrChk - 1]) {
            continue;
        }
        slot->tts_isnull[attrChk - 1] = false;
        slot->tts_values[attrChk - 1] = values[attrChk - 1];
    }

    tableam_tops_free_tuple(oldTup);
    return newTup;
}


void InitOutputValues(RightRefState* refState, Datum* values, bool* isnull, bool* hasExecs)
{
    if (!IS_ENABLE_RIGHT_REF(refState)) {
        return;
    }

    refState->values = values;
    refState->isNulls = isnull;
    refState->hasExecs = hasExecs;
    const int colCnt = refState->colCnt;
    for (int i = 0; i < colCnt; ++i) {
        hasExecs[i] = false;
    }

    if (IS_ENABLE_INSERT_RIGHT_REF(refState)) {
        for (int i = 0; i < colCnt; ++i) {
            Const* con = refState->constValues[i];
            if (con) {
                values[i] = con->constvalue;
                isnull[i] = con->constisnull;
            } else {
                values[i] = (Datum)0;
                isnull[i] = true;
            }
        }
    }
}

void SortTargetListAsArray(RightRefState* refState, List* targetList, GenericExprState* targetArr[])
{
    ListCell* lc = NULL;
    if (IS_ENABLE_INSERT_RIGHT_REF(refState)) {
        const int len = list_length(targetList);
        GenericExprState* tempArr[len];
        int tempIndex = 0;
        foreach(lc, targetList) {
            tempArr[tempIndex++] = (GenericExprState*)lfirst(lc);
        }

        for (int i = 0; i < refState->explicitAttrLen; ++i) {
            int explIndex = refState->explicitAttrNos[i] - 1;
            targetArr[i] = tempArr[explIndex];
            tempArr[explIndex] = nullptr;
        }

        int defaultNodeOffset = refState->explicitAttrLen;
        for (int i = 0; i < len; ++i) {
            if (tempArr[i]) {
                targetArr[defaultNodeOffset++] = tempArr[i];
            }
        }

        if (defaultNodeOffset != len) {
            /* this should never happen, the system must come in mess */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("the number of elements put up does not match the length of targetlist, array:%d, list:%d",
                        defaultNodeOffset, len)));
        }
    } else if (IS_ENABLE_UPSERT_RIGHT_REF(refState)) {
        const int len = list_length(targetList);
        GenericExprState* tempArr[len];
        int tempIndex = 0;
        foreach(lc, targetList) {
            tempArr[tempIndex++] = (GenericExprState*)lfirst(lc);
        }

        for (int i = 0; i < refState->usExplicitAttrLen; ++i) {
            int explIndex = refState->usExplicitAttrNos[i] - 1;
            targetArr[i] = tempArr[explIndex];
            tempArr[explIndex] = nullptr;
        }

        int defaultNodeOffset = refState->usExplicitAttrLen;
        for (int i = 0; i < len; ++i) {
            if (tempArr[i]) {
                targetArr[defaultNodeOffset++] = tempArr[i];
            }
        }

        if (defaultNodeOffset != len) {
            /* this should never happen, the system must come in mess */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("the number of elements put up does not match the length of targetlist, array:%d, list:%d",
                        defaultNodeOffset, len)));
        }
    } else {
        int index = 0;
        foreach(lc, targetList) {
            targetArr[index++] = (GenericExprState*)lfirst(lc);
        }
    }
}

bool expr_func_has_refcursor_args(Oid Funcid)
{
    HeapTuple proctup = NULL;
    Form_pg_proc procStruct;
    int allarg;
    Oid* p_argtypes = NULL;
    char** p_argnames = NULL;
    char* p_argmodes = NULL;
    bool use_cursor = false;

    if (IsSystemObjOid(Funcid) && Funcid != CURSORTOXMLOID && Funcid != CURSORTOXMLSCHEMAOID) {
        return false;
    }
    proctup = SearchSysCache(PROCOID, ObjectIdGetDatum(Funcid), 0, 0, 0);

    /*
     * function may be deleted after clist be searched.
     */
    if (!HeapTupleIsValid(proctup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("function doesn't exist ")));
    }

    /* get the all args informations, only "in" parameters if p_argmodes is null */
    allarg = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
    procStruct = (Form_pg_proc)GETSTRUCT(proctup);

    if (procStruct->prorettype == REFCURSOROID) {
        use_cursor = true;
    }
    else {
        for (int i = 0; i < allarg; i++) {
            if (!(p_argmodes != NULL && (p_argmodes[i] == 'o' || p_argmodes[i] == 'b'))) {
                if (p_argtypes[i] == REFCURSOROID) {
                    use_cursor = true;
                    break;
                }
            }
        }
    }

    ReleaseSysCache(proctup);
    return use_cursor;
}

void check_huge_clob_paramter(FunctionCallInfoData* fcinfo, bool is_have_huge_clob)
{
    if (!is_have_huge_clob || IsSystemObjOid(fcinfo->flinfo->fn_oid)) {
        return;
    }
    Oid schema_oid = get_func_namespace(fcinfo->flinfo->fn_oid);
    if (IsPackageSchemaOid(schema_oid)) {
        return;
    }
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("huge clob do not support as function in parameter")));
}

HeapTuple get_tuple(Relation relation, ItemPointer tid)
{
    Buffer user_buf = InvalidBuffer;
    HeapTuple tuple = NULL;
    HeapTuple new_tuple = NULL;

    /* alloc mem for old tuple and set tuple id */
    tuple = (HeapTupleData *)heaptup_alloc(BLCKSZ);
    tuple->t_data = (HeapTupleHeader)((char *)tuple + HEAPTUPLESIZE);
    Assert(tid != NULL);
    tuple->t_self = *tid;

    if (heap_fetch(relation, SnapshotAny, tuple, &user_buf, false, NULL)) {
        new_tuple = heapCopyTuple((HeapTuple)tuple, relation->rd_att, NULL);
        ReleaseBuffer(user_buf);
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("The tuple is not found"),
            errdetail("Another user is getting tuple or the datum is NULL")));
    }

    heap_freetuple(tuple);
    return new_tuple;
}


void set_result_for_plpgsql_language_function_with_outparam_by_flatten(Datum *result, bool *isNull)
{
    HeapTupleHeader td = DatumGetHeapTupleHeader(*result);
    TupleDesc tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
    HeapTupleData tup;
    tup.t_len = HeapTupleHeaderGetDatumLength(td);
    tup.t_data = td;
    Datum *values = (Datum *)palloc(sizeof(Datum) * tupdesc->natts);
    bool *nulls = (bool *)palloc(sizeof(bool) * tupdesc->natts);
    heap_deform_tuple(&tup, tupdesc, values, nulls);
    *result = values[0];
    *isNull = nulls[0];
    pfree(values);
    pfree(nulls);
}

static bool func_cache_support_type(Oid typ)
{
    if (typ >= FirstBootstrapObjectId) {
        Oid basetyp = getBaseType(typ);
        if (!OidIsValid(basetyp)) {
            return false;
        }
        if (basetyp >= FirstBootstrapObjectId) {
            return false;
        }
        return func_cache_support_type(basetyp);
    }

    switch(typ) {
        case BOOLOID:
        case CHAROID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case TIMEOID:
        case DATEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case NUMERICOID:
            return true;
        default:
            return false;
    }
}

static bool func_cache_support_proc(FuncExpr *fexpr, HeapTuple proctup)
{
    short i;
    short nargs;
    bool isNull;
    Datum tmp;

    Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);

    Assert(!procform->proretset);

    if (!IsPlpgsqlLanguageOid(procform->prolang) ||
            procform->provolatile == PROVOLATILE_VOLATILE ||
            procform->proisagg || procform->proiswindow ||
            list_length(fexpr->args) != procform->pronargs) {
        return false;
    }

    tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prokind, &isNull);
    if (isNull) {
        return false;
    }

    if (DatumGetChar(tmp) != PROKIND_FUNCTION) {
        return false;
    }

    (void)SysCacheGetAttr(PROCNAMEARGSNSP, proctup,
                          Anum_pg_proc_proallargtypes,
                          &isNull);
    /* Containing none-IN args */
    if (!isNull) {
        return false;
    }

    nargs = procform->pronargs;

    /* Too many args */
    if (nargs > FCR_MAX_ARGS) {
        return false;
    }

    for (i = 0; i < nargs; i++) {
        if (!func_cache_support_type(procform->proargtypes.values[i])) {
            return false;
        }
    }

    fexpr->funcflags |= nargs;

    return true;
}

bool check_func_need_rescache(FuncExpr *fexpr)
{
    HeapTuple proctup;
    Relation relation;
    Datum datum;
    bool isNull = false;
    bool hasResultCacheFlag = false;

    /*
     * builtin functions not support.
     */
    if (fexpr->funcid < FirstNormalObjectId) {
        return false;
    }

    if (fexpr->funcretset || fexpr->funcvariadic ||
            !func_cache_support_type(fexpr->funcresulttype)) {
        return false;
    }

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
    if (!HeapTupleIsValid(proctup)) {
        return false;
    }

    /* Function's RESULT_CACHE property is false, not support */
    hasResultCacheFlag = GetResultCacheByOid(fexpr->funcid);
    if (!hasResultCacheFlag) {
        ReleaseSysCache(proctup);
        return false;
    }

    if (!func_cache_support_proc(fexpr, proctup)) {
        ReleaseSysCache(proctup);
        return false;
    }

    ReleaseSysCache(proctup);

    return true;
}

void set_func_checked(FuncExpr *fexpr)
{
    fexpr->funcflags = FNCACHE_CHECKED;
}

void estimate_func_retcache(FuncExpr *fexpr, PlannerInfo *root)
{
    ListCell *lc;
    Node *arg;
    VariableStatData rdata;

    Assert(IsA(fexpr, FuncExpr));
    Assert((fexpr->funcflags & FNCACHE_CHECKED) == 0);

    fexpr->funcflags = FNCACHE_CHECKED;

    if (!check_func_need_rescache(fexpr)) {
        return;
    }

    fexpr->funcflags |= FNCACHE_ENABLE_CACHE;

    if (root == NULL) {
        return;
    }

    foreach(lc, fexpr->args) {
        arg = (Node *)lfirst(lc);
        if (IsA(arg, Var)) {
            examine_variable(root, arg, 0, &rdata);
            if (rdata.isunique)
                fexpr->funcflags |= FNCACHE_ARG_UNIQUE;
            ReleaseVariableStats(rdata);
        }
    }
}

void EStateFuncAssignCache(ExprState *state, ExprContext *ectx, FuncExpr *fe, FunctionCallInfo fcinfo)
{
    Oid fnid;
    Oid fncoll;
    EState *es_top;
    FmgrInfo *flinfo;
    FuncCache fncache;

    Assert(!(state && ectx));
    Assert(state != NULL || ectx != NULL);

    if ((state == NULL && ectx == NULL) || fe == NULL || fcinfo == NULL) {
        return;
    }

    if (!ActivePortal || ActivePortal->func_retcache_cxt == NULL) {
        return;
    }

    if (!IsA(fe, FuncExpr)) {
        return;
    }

    if (fe->funcid < FirstNormalObjectId) {
        return;
    }

    /* smp not support */
    if (StreamThreadAmI() || StreamTopConsumerAmI()) {
        return;
    }

    if (state) {
        /* From ExecInitFunc */
        return;
    } else if (ectx && ActivePortal->top_estate) {
        /* From ExecEvalFunc */
        es_top = ActivePortal->top_estate;
    } else {
        /* EState not found */
        return;
    }

    if (es_top->es_topstate) {
        es_top = es_top->es_topstate;
    }

    if (es_top->es_topstate) {
        return;
    }

    flinfo = fcinfo->flinfo;

    Assert(flinfo->fn_oid >= FirstNormalObjectId);
    Assert(!flinfo->fn_retset);

    if (flinfo->fn_nargs > FCR_MAX_ARGS) {
        return;
    }

    fnid = flinfo->fn_oid;
    fncoll = fcinfo->fncollation;

    /*
     * Save top EState to function.
     */
    fcinfo->top_estate = es_top;
    Assert(fcinfo->fncache == NULL);
    fncache = EStateFuncGetCache(es_top, fnid, fncoll);

    if (fncache) {
        /* Just in case. Variable arguments and default arguments. */
        if (FNCACHE_NUMARGS(fncache->fcflags) != fcinfo->nargs) {
            return;
        }

        if ((fncache->fcflags & FNCACHE_ENABLE_CACHE) == 0) {
            /* Function has checked, not support result cache. */
            return;
        }

        if (FNCACHE_REFCOUNT(fncache->fcflags) >= FNCACHE_REF_MAX) {
            /* * Cache can only be used for the first 255 calls to the same function in same plan. */
            return;
        }

        if ((fe->funcflags & FNCACHE_ARG_UNIQUE) &&
                (fncache->fcflags & FNCACHE_ARG_UNIQUE) == 0) {
            fncache->fcflags |= FNCACHE_ARG_UNIQUE;
        }

        fncache->fcflags += FNCACHE_REF_ONE;
        fcinfo->fncache = fncache;

        return;
    }

    /* Function has not yet created a cache. */

    /*
     * Function has not yet checked? Check if support function result cache.
     */
    if ((fe->funcflags & FNCACHE_CHECKED) == 0) {
        estimate_func_retcache(fe, NULL);
    }

    fncache = EStateFuncPutCache(es_top, fnid, fncoll);
    if (fncache == NULL) {
        return;
    }

    Assert(fncache);

    /* Function result can not cached, return. */
    if ((fe->funcflags & FNCACHE_ENABLE_CACHE) == 0) {
        fncache->fcflags = FNCACHE_CHECKED;
        return;
    }

    fncache->fcflags = fe->funcflags;
    Assert(FNCACHE_REFCOUNT(fncache->fcflags) == 0);

    fncache->prevhit = -1;

    fncache->fcflags += FNCACHE_REF_ONE;

    Assert(fncache->argtypes == NULL);
    Assert(!OidIsValid(fncache->rettype));

    fcinfo->fncache = fncache;

    /*
     * Save function args and return type, found their base types.
     */
    if (flinfo->fn_nargs > 0) {
        HeapTuple proctup;
        Form_pg_proc procform;
        short i;
        short nargs;
        Oid* argtypes;

        proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fnid));
        Assert(HeapTupleIsValid(proctup));

        procform = (Form_pg_proc) GETSTRUCT(proctup);
        Assert(procform->pronargs == fcinfo->flinfo->fn_nargs);

        nargs = procform->pronargs;
        argtypes = procform->proargtypes.values;

        fncache->security = procform->prosecdef;
        fncache->argtypes = (Oid *)MemoryContextAllocZero(ActivePortal->func_retcache_cxt,
                                                          sizeof(Oid) * nargs);

        /* transform to base types */
        for (i = 0; i < nargs; i++) {
            if (argtypes[i] < FirstBootstrapObjectId) {
                fncache->argtypes[i] = argtypes[i];
                continue;
            }

            fncache->argtypes[i] = getBaseType(argtypes[i]);
            Assert(OidIsValid(fncache->argtypes[i]));
            Assert(fncache->argtypes[i] < FirstBootstrapObjectId);
        }

        ReleaseSysCache(proctup);
    } else if (flinfo->fn_nargs == 0) {
        HeapTuple proctup;
        Form_pg_proc procform;

        proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fnid));
        Assert(HeapTupleIsValid(proctup));

        procform = (Form_pg_proc) GETSTRUCT(proctup);
        fncache->security = procform->prosecdef;
        ReleaseSysCache(proctup);
    }

    fncache->rettype = fe->funcresulttype;

    /* transform to base types */
    if (fncache->rettype >= FirstBootstrapObjectId) {
        fncache->rettype = getBaseType(fncache->rettype);
        Assert(OidIsValid(fncache->rettype));
        Assert(fncache->rettype < FirstBootstrapObjectId);
    }
}

#define FNCACHE_ARRAY_SIZE (8)
#define MAX_FNCACHE_SLOT (16)   /* max slot: 14 */

/*
 * Before all functions in the plan are initialized, we cannot predict how
 * many functions need to be processed, including other functions called in
 * PL/pgSQL functions.
 *
 * Each function's own result cache is associated with a pointer after allocation,
 * so the location cannot be changed, and space needs to be saved as much as possible.
 * This cache only needs to be associated during the executor initialization phase,
 * and there is no need to worry about performance.
 *
 * When allocating function cache, an array of 8 pointers is used for management:
 * 1. When more space is needed, an array of pointers of 8 elements is allocated;
 * 2. The last element of the array points to the next array;
 * 3. Any function expression is initialized only once, so sequential traversal is tolerable.
 *
 * If a plan involves a lot of function calls, this may need to be reconsidered.
 * Currently limited to MAX_FNCACHE_SLOT.
 *
 * Use function oid and collation as cache key.
 */
static FuncCache EStateFuncGetCache(EState *es, Oid fnid, Oid fncoll)
{
    short i;
    FuncCache *fncaches;

    Assert(es);
    Assert(OidIsValid(fnid));
    Assert(fnid >= FirstNormalObjectId);
    Assert(es->es_topstate == NULL);

    if (es->es_topstate)
        return NULL;

    if (es->es_funcache.fncaches == NULL)
        return NULL;

    fncaches = es->es_funcache.fncaches;

    /* search FuncCache using by fnid,fncoll */
    for (i = 0;;) {
        if (i < (FNCACHE_ARRAY_SIZE - 1)) {
            /*
             * If fnoid is invalid, it indicates that the subsequent slots have not been used.
             * In this case, simply return.
             */
            if (!OidIsValid(fncaches[i]->fnoid)) {
                break;
            }

            /*
             * Check whether the slot is consistent with the current fnid and fncoll.
             * If it is, use this slot directly; otherwise, continue to iterate
             */
            if (fncaches[i]->fnoid != fnid || fncaches[i]->fncoll != fncoll) {
                i++;
                continue;
            }

            return fncaches[i];
        }

        /*
         * Check if the next 8 arrays are empty. If they are empty,
         * it means they haven't been initialized, so return directly.
         */
        if (fncaches[FNCACHE_ARRAY_SIZE - 1] == NULL) {
            break;
        }

        i = 0;
        /* Iterate the next 8 arrays */
        fncaches = (FuncCache *)fncaches[FNCACHE_ARRAY_SIZE - 1];
    }

    return NULL;
}

/*
 * Create function result cache.
 */
static FuncCache EStateFuncPutCache(EState *es, Oid fnid, Oid fncoll)
{
    short i;
    FuncCache *fncaches;
    FuncCache fncache;

    Assert(es);
    Assert(OidIsValid(fnid));
    Assert(fnid >= FirstNormalObjectId);
    Assert(es->es_topstate == NULL);

    if (es->es_topstate) {
        return NULL;
    }

    Assert(EStateFuncGetCache(es, fnid, fncoll) == NULL);

    if (es->es_funcache.fncaches == NULL) {
        fncaches = (FuncCache *)MemoryContextAllocZero(ActivePortal->func_retcache_cxt,
                                                       sizeof(FuncCache) * FNCACHE_ARRAY_SIZE);

        fncache = (FuncCache)MemoryContextAllocZero(ActivePortal->func_retcache_cxt,
                                                    sizeof(FuncCacheData) * (FNCACHE_ARRAY_SIZE - 1));

        for (i = 0; i < FNCACHE_ARRAY_SIZE - 1; i++) {
            fncaches[i] = fncache + i;
        }

        es->es_funcache.fncaches = fncaches;
        ActivePortal->funcRetcacheSlotCount += FNCACHE_ARRAY_SIZE;
    } else {
        i = 0;
        fncaches = es->es_funcache.fncaches;

        for (;;) {
            if (i < (FNCACHE_ARRAY_SIZE - 1)) {
                if (OidIsValid(fncaches[i]->fnoid)) {
                    i++;
                    continue;
                }

                fncache = fncaches[i];
                break;
            }

            if (fncaches[FNCACHE_ARRAY_SIZE - 1]) {
                i = 0;
                fncaches = (FuncCache *)fncaches[FNCACHE_ARRAY_SIZE - 1];
                continue;
            }

            if (ActivePortal->funcRetcacheSlotCount < MAX_FNCACHE_SLOT) {
                fncaches[FNCACHE_ARRAY_SIZE - 1] = (FuncCache)MemoryContextAllocZero(
                    ActivePortal->func_retcache_cxt,
                    sizeof(FuncCache) * FNCACHE_ARRAY_SIZE);

                fncache = (FuncCache)MemoryContextAllocZero(
                    ActivePortal->func_retcache_cxt,
                    sizeof(FuncCacheData) * (FNCACHE_ARRAY_SIZE - 1));

                fncaches = (FuncCache *)fncaches[FNCACHE_ARRAY_SIZE - 1];
                for (i = 0; i < FNCACHE_ARRAY_SIZE - 1; i++) {
                    fncaches[i] = fncache + i;
                }

                fncache = fncaches[0];
                ActivePortal->funcRetcacheSlotCount += FNCACHE_ARRAY_SIZE;
            } else {
                return NULL;
            }
            break;
        }
    }

    Assert(fncache);

    fncache->fnoid = fnid;
    fncache->fncoll = fncoll;

    return fncache;
}

/*
 * Recycling cache is mainly used for:
 * 1. The plan is called only once and uses unique parameters. The parameters
 *    are different each time and it is impossible to hit.
 * 2. There is no hit after several calls during the execution process.
 *
 * Although the cost of the result cache itself is much lower than the cost of
 * UDF calls, it is still not negligible, so it is best to stop using it at the
 * right time.
 */
static void EStateFuncReclaimCache(FuncCache fncache)
{
    Oid dtmtype;
    short i;
    short j;
    short k;
    short nargs;
    FuncRetBucket *retbucket;
    FuncRetCache *retcache;

    if (FNCACHE_REFCOUNT(fncache->fcflags) > FNCACHE_REF_ONE) {
        fncache->fcflags -= FNCACHE_REF_ONE;
        return;
    }

    if (fncache->cacheptr.retbuckets == NULL) {
        return;
    }

    nargs = FNCACHE_NUMARGS(fncache->fcflags);

    Assert(fncache->cacheptr.retbuckets);
    for (i = 0; i < FR_CACHE_NUM_BUCKETS; i++) {
        retbucket = fncache->cacheptr.retbuckets[i];

        if (retbucket == NULL) {
            continue;
        }

        for (j = FR_CACHE_SIZE_BUCKET - 1; j >= 0; j--) {
            if (retbucket->retcache[j] == NULL) {
                continue;
            }

            retcache = retbucket->retcache[j];
            if (!FR_STATE_IS_VALID(retcache->state)) {
                continue;
            }

            for (k = 0; k < nargs; k++) {
                dtmtype = fncache->argtypes[k];

                if (dtmtype == TEXTOID || dtmtype == VARCHAROID ||
                        dtmtype == NVARCHAR2OID || dtmtype == BPCHAROID ||
                        dtmtype == NUMERICOID) {
                    pfree(DatumGetPointer(retcache->args[k]));
                }
            }

            dtmtype = fncache->rettype;
            if (dtmtype == TEXTOID || dtmtype == VARCHAROID ||
                    dtmtype == NVARCHAR2OID || dtmtype == BPCHAROID ||
                    dtmtype == NUMERICOID) {
                pfree(DatumGetPointer(retcache->retval));
            }

            if ((j % FR_CACHE_SIZE_SECTION) == 0) {
                pfree(retbucket->retcache[j]->args);
                pfree(retbucket->retcache[j]);
            }
        }

        pfree(retbucket->retcache);
    }

    pfree(fncache->cacheptr.retbuckets);
}

/* Strings that are too long are difficult to match and are not cached. */
#define FCR_DATUM_LENGTH_THRESHOLD (127)

/*
 * Check Datum if not support cache.
 */
static inline bool FunctionDatumToolong(Datum d, Oid dtype)
{
    if (dtype == TEXTOID || dtype == VARCHAROID ||
        dtype == NVARCHAR2OID || dtype == BPCHAROID) {
        if (VARSIZE_ANY(DatumGetPointer(d)) > FCR_DATUM_LENGTH_THRESHOLD) {
            return true;
        }
    }

    return false;
}

/*
 * Check whether the function args used in this call cannot be cached.
 */
static bool FunctionArgCannotCache(FuncCache fncache, FunctionCallInfo fcinfo)
{
    short i;
    short nargs = fcinfo->nargs;
    Datum *fargs = fcinfo->arg;
    bool *argnull = fcinfo->argnull;

    if (nargs != fcinfo->flinfo->fn_nargs) {
        return true;
    }

    for (i = 0; i < nargs; i++) {
        if (argnull[i]) {
            continue;
        }

        if (FunctionDatumToolong(fargs[i], fncache->argtypes[i])) {
            return true;
        }
    }

    return false;
}

/*
 * Copy Datum to the current context according to type.
 *
 * Datum has some judgments before getting the cache, see FunctionDatumToolong,
 * and blocks the parts that are unclear and worry about problems. Here you can
 * directly use datumcopy.
 *
 * In the future, when adding new types or making adjustments, you should also
 * pay attention to the differences in operations of different types.
 */
static inline Datum FunctionDatumCopy(Datum d, Oid dtype)
{
    if (dtype == NUMERICOID) {
        return NumericGetDatum(DatumGetNumericCopy(d));
    } else if (dtype == TEXTOID || dtype == VARCHAROID ||
               dtype == NVARCHAR2OID || dtype == BPCHAROID) {
        return datumCopy(d, false, -1);
    } else {
        return d;
    }
}

#define INVALID_ARG_CRC32C (0u)
#define FISRT_ARG_CRC32C (1u)
#define ARG_CRC32C_IS_VALID(c) (!EQ_CRC32C(c, INVALID_ARG_CRC32C))

#if ((UINT32_MAX + 1) % FR_CACHE_MAX_SIZE != 0)
#error Wrong FR_CACHE_MAX_SIZE, must be divisible by UINT32_MAX + 1.
#endif

/*
 * Because not too many results are cached, the instruction set CRC operation
 * is faster and the hash range is sufficient.
 *
 * FunctionArgCannotCache first filters the parameters, so there is no need to
 * worry about data validity issues here.
 *
 * Pay attention to this when adjusting or adding supported types in the future.
 */
static inline pg_crc32c EStateFuncHashArgs(Oid *argtypes, Datum *fargs, bool *isnull, short nargs)
{
    struct varlena *vl;
    pg_crc32c tmpcrc;
    pg_crc32c argcrc;
    short i;
    Oid argtype;

    INIT_CRC32C(argcrc);

    for (i = 0; i < nargs; i++) {
        if (isnull[i]) {
            continue;
        }

        argtype = argtypes[i];

        if (argtype == NUMERICOID) {
            Size len;

            vl = (struct varlena*)DatumGetPointer(fargs[i]);

            Assert(!VARATT_IS_EXTENDED(vl) || !VARATT_IS_HUGE_TOAST_POINTER(vl));

            len = VARATT_IS_HUGE_TOAST_POINTER(vl) ? VARSIZE_EXTERNAL(vl) : VARSIZE(vl);

            INIT_CRC32C(tmpcrc);
            COMP_CRC32C(tmpcrc, vl, VARSIZE_ANY(vl));
            FIN_CRC32C(tmpcrc);

            COMP_CRC32C(argcrc, &tmpcrc, sizeof(pg_crc32c));
        } else if (argtype == TEXTOID || argtype == VARCHAROID ||
                   argtype == NVARCHAR2OID || argtype == BPCHAROID) {
            INIT_CRC32C(tmpcrc);
            vl = (struct varlena *) DatumGetPointer(fargs[i]);
            COMP_CRC32C(tmpcrc, vl, VARSIZE_ANY(vl));
            FIN_CRC32C(tmpcrc);

            COMP_CRC32C(argcrc, &tmpcrc, sizeof(pg_crc32c));
        } else {
            COMP_CRC32C(argcrc, fargs + i, sizeof(Datum));
        }
    }

    FIN_CRC32C(argcrc);

    if EQ_CRC32C(argcrc, INVALID_ARG_CRC32C) {
        argcrc = FISRT_ARG_CRC32C;
    }

    return argcrc;
}

/*
 * Compare the parameters Dautm one by one according to type for equality.
 */
static inline bool EStateFuncCompareArgs(Oid *argtypes, FuncRetCache *retcache, Datum *fargs, bool *isnull, short nargs)
{
    short i;
    Oid argtype;
    Datum *args = retcache->args;
    uint32 argisnull = retcache->argisnull;

    for (i = 0; i < nargs; i++) {
        /* Function arg input is NULL */
        if (isnull[i]) {
            if (argisnull & (1 << i)) {
                continue;
            } else {
                break;
            }
        }
        /* Function arg input is not NULL but cached arg is NULL */
        if (argisnull & (1 << i)) {
            break;
        }

        argtype = argtypes[i];

        if (argtype == NUMERICOID) {
            if (!DatumGetBool(DirectFunctionCall2(numeric_eq, fargs[i], args[i]))) {
                break;
            }
        } else if (argtype == TEXTOID || argtype == VARCHAROID ||
                   argtype == NVARCHAR2OID || argtype == BPCHAROID) {
            if (!datumIsEqual(fargs[i], args[i], false, -1)) {
                break;
            }
        } else if (fargs[i] != args[i]) {
            break;
        }
    }

    if (i == nargs) {
        return true;
    }

    return false;
}

/*
 * Get function's result cache.
 */
bool EStateFuncGetRetCache(FunctionCallInfo fcinfo, Datum *retvalue, bool *retnull)
{
    short i;
    short nargs;
    short prevhit;
    short buck;
    short offs;
    Datum *fargs;
    bool *argnull;
    FuncCache fncache;
    FuncRetBucket *retbucket;
    FuncRetCache *retcache;
    FuncRetCache **retcachea;
    pg_crc32c argcrc;
    pg_crc32c *argcrcs;

    Assert(fcinfo->fncache);
    Assert(fcinfo->top_estate);

    fncache = fcinfo->fncache;

    /*
     * Contains only one parameter and is called only once, disabling caching.
     * It should be determined when deciding whether to use caching (before create_plan),
     * but the current framework is difficult to code.
     * It is necessary to observe the number of function calls and whether the function
     * parameters are consistent, which cannot be done with one recursion.
     *
     * When it contains only one parameter, it is determined and closed in the first call.
     * Only the allocated internal elements are recycled, and the structures themselves are
     * retained because their pointers are allocated to each call. If recycled, it will
     * affect all other functions.
     */
    if (unlikely((fncache->fcflags & FNCACHE_ARG_UNIQUE) &&
            (fncache->fcflags & FNCACHE_REF_COUNT) == 1)) {
        Assert(fncache->cacheptr.retbuckets == NULL);

        if (fncache->argtypes) {
            pfree(fncache->argtypes);
        }

        fcinfo->fncache = NULL;

        return false;
    }

    /*
     * The number of times used reaches the cache number, and the hit rate is less than 15%.
     */
    if (unlikely(fncache->usagecount >= FR_MAX_USAGE_COUNT * FR_CACHE_MAX_SIZE &&
            ((fncache->hitcount / 0.15) < fncache->usagecount))) {
        /* A function without parameters will hit all of them after being called once. */
        Assert(FNCACHE_NUMARGS(fncache->fcflags) > 0);

        EStateFuncReclaimCache(fncache);

        fcinfo->fncache = NULL;

        return false;
    }

    if (fncache->usagecount < INT32_MAX) {
        fncache->usagecount++;
    }

    nargs = fcinfo->nargs;

    if (nargs == 0) {
        /* Never match when function first call. */
        if (unlikely(fncache->cacheptr.retcache == NULL)) {
            return false;
        }

        retcache = fncache->cacheptr.retcache;

        Assert(FR_STATE_IS_VALID(retcache->state));

        if (fncache->usagecount < INT32_MAX) {
            fncache->hitcount++;
        }

        /* Found cached results, no need to call function to return directly. */
        *retvalue = retcache->retval;
        *retnull = FR_STATE_RET_NULL(retcache->state);

        return true;
    }

    /*
     * Calculate CRC of function parameters for bucket matching.
     */
    fcinfo->arghash = INVALID_ARG_CRC32C;

    /* Check whether the function args used in this call cannot be cached. */
    if (FunctionArgCannotCache(fncache, fcinfo)) {
        return false;
    }

    fargs = fcinfo->arg;
    argnull = fcinfo->argnull;

    argcrc = EStateFuncHashArgs(fncache->argtypes, fargs, argnull, nargs);
    Assert(ARG_CRC32C_IS_VALID(argcrc));

    fcinfo->arghash = argcrc;

    /* No cache yet */
    if (fncache->cacheptr.retbuckets == NULL) {
        return false;
    }

    /*
     * When called multiple times, the next call may use the same parameters,
     * and the cache that hit the last time will be tried first.
     */
    if (fncache->prevhit >= 0) {
        prevhit = fncache->prevhit;

        buck = prevhit / FR_CACHE_SIZE_BUCKET;
        offs = prevhit % FR_CACHE_SIZE_BUCKET;

        Assert(fncache->cacheptr.retbuckets);
        retbucket = fncache->cacheptr.retbuckets[buck];

        Assert(retbucket);
        Assert(retbucket->retcache);

        Assert(ARG_CRC32C_IS_VALID(retbucket->argcrcs[offs]));

        retcache = retbucket->retcache[offs];
        Assert(retcache);
        Assert(FR_STATE_IS_VALID(retcache->state));

        if (EQ_CRC32C(argcrc, retbucket->argcrcs[offs]) &&
                EStateFuncCompareArgs(fncache->argtypes, retcache, fargs, argnull, nargs)) {
            if (fncache->usagecount < INT32_MAX)
                fncache->hitcount++;

            /* Matches the previous call and returns directly. */
            *retvalue = retcache->retval;
            *retnull = FR_STATE_RET_NULL(retcache->state);

            return true;
        } else {
            fncache->prevhit = -1;
        }
    }

    /* Locate the bucket where the cache for this call is located. */
    buck = argcrc % FR_CACHE_NUM_BUCKETS;
    retbucket = fncache->cacheptr.retbuckets[buck];
    if (retbucket == NULL) {
        return false;
    }

    retcachea = retbucket->retcache;
    argcrcs = retbucket->argcrcs;

    for (i = 0; i < FR_CACHE_SIZE_BUCKET; i++) {
        /* The last valid cache in the bucket has been reached. */
        if (!ARG_CRC32C_IS_VALID(argcrcs[i])) {
            break;
        }

        /* If the CRC is different, the parameters will also be different. Continue to compare the next. */
        if (!EQ_CRC32C(argcrc, argcrcs[i])) {
            continue;
        }

        /* CRC is the same, check if the parameters are the same. */
        if (!EStateFuncCompareArgs(fncache->argtypes, retcachea[i], fargs, argnull, nargs)) {
            continue;
        }

        if (fncache->usagecount < INT32_MAX) {
            fncache->hitcount++;
        }

        if (FCR_STATE_GET_USAGECOUNT(retcachea[i]->state) < FR_MAX_USAGE_COUNT) {
            retcachea[i]->state += FCR_USAGECOUNT_ONE;
        }

        if ((retcachea[i]->state & FR_HITTED) == 0) {
            retcachea[i]->state |= FR_HITTED;
        }

        /*
         * The function appears multiple times in the plan, and the next call may use the
         * same parameters, for example:
         *   SELECT f1(col1), f1(col1) ... FROM t1;
         * Record the hit position this time, and expect to hit it directly next time.
         */
        if ((fncache->fcflags & FNCACHE_REF_COUNT) > 1) {
            /*
             * Note that the way the last hit is saved here is different from the bucket
             * positioning method.
             */
            fncache->prevhit = buck * FR_CACHE_SIZE_BUCKET + i;
        }

        /* Found cached results, no need to call the function to return directly. */
        *retvalue = retcachea[i]->retval;
        *retnull = FR_STATE_RET_NULL(retcachea[i]->state);

        return true;
    }

    return false;
}

extern PLpgSQL_function* get_security_function(FunctionCallInfo fcinfo);

/*
 * Result cache of procedural language functions
 *
 * Currently, function inputs are difficult to predict because they often contain expressions
 * or even the output of other functions.
 *
 * Some scenarios of relation scan operators can estimate the number of calls more accurately,
 * but the distribution of parameters is still difficult to predict. Therefore, the design
 * considers fragmenting the cache in order to create as small a cache as possible.
 *
 * In practice, the cost of cache management in the PG environment is still considerable. For
 * the simplest function that contains only one native type parameter and directly returns the
 * result, the hit rate is less than about 15%, which will result in negative returns.
 *
 * The function cost is closely related to the management of the result cache, but unfortunately,
 * there are problems with the current function cost system and it is difficult to solve it for
 * the time being. The first phase of implementation uses a rough empirical coefficient.
 *
 * Fragmented cache, divided into 32 buckets, each with a capacity of 16, and a total cache of 512.
 * 1. Bucket slots are allocated at once
 * 2. There are many buckets, and some buckets may not be used when the result set is small,
 *    reducing memory allocation
 * 3. Four positions are allocated in the bucket each time, and the bucket can be not full when
 *    the result set is small, reducing memory allocation
 * 4. The bucket size is small, and the matching process is faster
 * 5. The parameters use the instruction set to calculate the CRC matching bucket, in order to be faster
 * Extreme scenarios, such as all input parameters are constants, only one bucket is used, 4 blocks.
 */
bool EStateFuncPutRetCache(FunctionCallInfo fcinfo, Datum ret)
{
    short i;
    short nargs;
    short buck;
    short nextvict;
    Oid datumtype;
    pg_crc32c argcrc;
    pg_crc32c *argcrcs;
    int	usagecount;
    Datum *fargs;
    bool *argnull;
    Datum *args;
    EState *es;
    FuncCache fncache;
    FuncRetBucket **retbuckets;
    FuncRetBucket *retbucket;
    FuncRetCache **retcache;
    FuncRetCache *buckcache;
    FuncRetCache *cret;
    FuncRetCache *rret;
    MemoryContext oldctx;

    Assert(fcinfo->fncache);
    Assert(fcinfo->top_estate);
    Assert(FNCACHE_NUMARGS(fcinfo->fncache->fcflags) == fcinfo->nargs);

    /* Autonomous transaction functions and subroutines do not support caching. */
    if (fcinfo->flinfo && fcinfo->flinfo->fn_extra) {
        PLpgSQL_function* func;
        if (fcinfo->fncache->security) {
            func = get_security_function(fcinfo);
        } else {
            func = (PLpgSQL_function*)fcinfo->flinfo->fn_extra;
        }
        if (func->action->isAutonomous || func->proc_list != NIL || OidIsValid(func->parent_oid)) {
            EStateFuncReclaimCache(fcinfo->fncache);

            fcinfo->fncache = NULL;
            return false;
        }
    }

    fncache = fcinfo->fncache;
    es = fcinfo->top_estate;
    nargs = fcinfo->nargs;

    if (nargs == 0) {
        /* No arguments, only cached one result. */
        FuncRetCache *funcretcache;

        Assert(fncache->cacheptr.retcache == NULL);

        oldctx = MemoryContextSwitchTo(ActivePortal->func_retcache_cxt);

        funcretcache = (FuncRetCache *)palloc0(sizeof(FuncRetCache));

        funcretcache->state = FR_VALID;

        if (fcinfo->isnull) {
            funcretcache->state |= FR_RETNULL;
        } else {
            funcretcache->retval = FunctionDatumCopy(ret, fncache->rettype);
        }

        MemoryContextSwitchTo(oldctx);

        fncache->cacheptr.retcache = funcretcache;

        return true;
    }

    /* CRC is invalid, FunctionArgCannotCache check considers the arguments to be uncached */
    if (!ARG_CRC32C_IS_VALID(fcinfo->arghash)) {
        return false;
    }

    argcrc = fcinfo->arghash;

    Assert(!FunctionArgCannotCache(fncache, fcinfo));

    /* Check if the function result can be cached. */
    if (!fcinfo->isnull && FunctionDatumToolong(ret, fncache->rettype)) {
        return true;
    }

    buck = argcrc % FR_CACHE_NUM_BUCKETS;

    oldctx = MemoryContextSwitchTo(ActivePortal->func_retcache_cxt);

    if (fncache->cacheptr.retbuckets) {
        retbuckets = fncache->cacheptr.retbuckets;
    } else {
        /* cache initial */
        retbuckets = (FuncRetBucket **)palloc0(sizeof(FuncRetBucket *) * FR_CACHE_NUM_BUCKETS);
        fncache->cacheptr.retbuckets = retbuckets;
    }

    if (retbuckets[buck]) {
        retbucket = retbuckets[buck];
    } else {
        /* bucket initial */
        retbucket = (FuncRetBucket *)palloc0(sizeof(FuncRetBucket));
        retbuckets[buck] = retbucket;

        retbucket->argcrcs = (pg_crc32c *)palloc0(sizeof(pg_crc32c) * FR_CACHE_SIZE_BUCKET);
        retbucket->retcache = (FuncRetCache **)palloc0(sizeof(FuncRetCache *) * FR_CACHE_SIZE_BUCKET);

        buckcache = (FuncRetCache *)palloc0(sizeof(FuncRetCache) * FR_CACHE_SIZE_SECTION);
        args = (Datum *)palloc0(sizeof(Datum) * FR_CACHE_SIZE_SECTION * nargs);

        for (i = 0; i < FR_CACHE_SIZE_SECTION; i++) {
            buckcache[i].args = args + nargs * i;
            retbucket->retcache[i] = buckcache + i;
        }
    }

    rret = NULL;
    nextvict = retbucket->nextvict;
    argcrcs = retbucket->argcrcs;
    retcache = retbucket->retcache;

    for (;;) {
        cret = retcache[nextvict];

        if (unlikely(cret == NULL)) {
            /* Initialize the next segment */
            Assert(!ARG_CRC32C_IS_VALID(argcrcs[nextvict]));
            Assert((nextvict % FR_CACHE_SIZE_SECTION) == 0);

            buckcache = (FuncRetCache *)palloc0(sizeof(FuncRetCache) * FR_CACHE_SIZE_SECTION);
            args = (Datum *)palloc0(sizeof(Datum) * FR_CACHE_SIZE_SECTION * nargs);

            for (i = 0; i < FR_CACHE_SIZE_SECTION; i++) {
                buckcache[i].args = args + nargs * i;
                retbucket->retcache[i + nextvict] = buckcache + i;
            }

            cret = buckcache;
        }

        /* Free to use */
        if (!FR_STATE_IS_VALID(cret->state)) {
            rret = cret;
            break;
        }

        usagecount = FCR_STATE_GET_USAGECOUNT(cret->state);

        /* If have a hit result, it will have one more chance to stay before being kicked out. */
        if (FR_STATE_HITTED(cret->state)) {
            cret->state &= ~FR_HITTED;
        } else if (usagecount > 0) {
            cret->state -= FCR_USAGECOUNT_ONE;
        } else {
            rret = cret;
            break;
        }

        nextvict++;
        if (nextvict >= FR_CACHE_SIZE_BUCKET) {
            nextvict = 0;
        }
    }

    if (rret == NULL) {
        MemoryContextSwitchTo(oldctx);

        return false;
    }

    argcrcs[nextvict] = argcrc;

    nextvict++;
    if (nextvict >= FR_CACHE_SIZE_BUCKET) {
        nextvict = 0;
    }
    retbucket->nextvict = nextvict;

    rret->state = FR_VALID;
    rret->state += FCR_USAGECOUNT_ONE;

    if (fcinfo->isnull) {
        rret->state |= FR_RETNULL;
    } else {
        datumtype = fncache->rettype;

        if (datumtype == NUMERICOID || datumtype == TEXTOID ||
                datumtype == VARCHAROID || datumtype == NVARCHAR2OID ||
                datumtype == BPCHAROID) {
            if (rret->retval) {
                pfree(DatumGetPointer(rret->retval));
            }
        }

        rret->retval = FunctionDatumCopy(ret, datumtype);
    }

    fargs = fcinfo->arg;
    argnull = fcinfo->argnull;

    rret->argisnull = 0;

    for (i = 0; i < nargs; i++) {
        if (argnull[i]) {
            rret->argisnull |= (1 << i);
            continue;
        }

        datumtype = fncache->argtypes[i];

        if (datumtype == NUMERICOID || datumtype == TEXTOID ||
                datumtype == VARCHAROID || datumtype == NVARCHAR2OID ||
                datumtype == BPCHAROID) {
            if (rret->args[i]) {
                pfree(DatumGetPointer(rret->args[i]));
            }
        }

        rret->args[i] = FunctionDatumCopy(fargs[i], datumtype);
    }

    MemoryContextSwitchTo(oldctx);

    return true;
}
