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
#include "executor/exec/execdebug.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "storage/tcap.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "optimizer/var.h"
#include "utils/resowner.h"
#include "miscadmin.h"

static bool get_last_attnums(Node* node, ProjectionInfo* projInfo);
static bool index_recheck_constraint(
    Relation index, Oid* constr_procs, Datum* existing_values, const bool* existing_isnull, Datum* new_values);
static void ShutdownExprContext(ExprContext* econtext, bool isCommit);
static bool check_violation(Relation heap, Relation index, IndexInfo *indexInfo, ItemPointer tupleid, Datum *values,
                            const bool *isnull, EState *estate, bool newIndex, bool errorOK, CheckWaitMode waitMode,
                            ConflictInfoData *conflictInfo, Oid partoid = InvalidOid, int2 bucketid = InvalidBktId,
                            Oid *conflictPartOid = NULL, int2 *conflictBucketid = NULL);

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
EState* CreateExecutorState(MemoryContext saveCxt)
{
    EState* estate = NULL;
    MemoryContext qcontext;
    MemoryContext oldcontext;

    /*
     * Create the per-query context for this Executor run.
     */
    if (saveCxt != NULL) {
        qcontext = saveCxt;
    } else {
        qcontext = AllocSetContextCreate(CurrentMemoryContext,
            "ExecutorState",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    /*
     * Make the EState node within the per-query context.  This way, we don't
     * need a separate pfree_ext() operation for it at shutdown.
     */
    oldcontext = MemoryContextSwitchTo(qcontext);

    estate = makeNode(EState);

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
void ExecAssignResultTypeFromTL(PlanState* planstate, TableAmType tam)
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
    tupDesc = ExecTypeFromTL(planstate->plan->targetlist, hasoid, false, tam);
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

                attr = inputDesc->attrs[variable->varattno - 1];
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

    if (exprlist == NIL) {
        projInfo->pi_itemIsDone = NULL; /* not needed */
    } else {
        projInfo->pi_itemIsDone = (ExprDoneCond*)palloc0(len * sizeof(ExprDoneCond));

        if (projInfo->pi_exprContext != NULL && projInfo->pi_exprContext->have_vec_set_fun) {
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

/* ----------------
 *		ExecBuildProjectionInfo
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
ProjectionInfo* ExecBuildProjectionInfo(
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

                attr = inputDesc->attrs[variable->varattno - 1];
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
    planstate->ps_ProjInfo = ExecBuildProjectionInfo(
        planstate->targetlist, planstate->ps_ExprContext, planstate->ps_ResultTupleSlot, inputDesc);
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
void ExecOpenIndices(ResultRelInfo* resultRelInfo, bool speculative)
{
    Relation resultRelation = resultRelInfo->ri_RelationDesc;
    List* indexoidlist = NIL;
    ListCell* l = NULL;
    int len, i;
    RelationPtr relationDescs;
    IndexInfo** indexInfoArray;

    resultRelInfo->ri_NumIndices = 0;
    resultRelInfo->ri_ContainGPI = false;

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
    foreach (l, indexoidlist) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc;
        IndexInfo* ii = NULL;

        indexDesc = index_open(indexOid, RowExclusiveLock);

        // ignore INSERT/UPDATE/DELETE on unusable index
        if (!IndexIsUsable(indexDesc->rd_index)) {
            index_close(indexDesc, RowExclusiveLock);
            continue;
        }

        /* Check index whether is global parition index, and save */
        if (RelationIsGlobalIndex(indexDesc)) {
            resultRelInfo->ri_ContainGPI = true;
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

    list_free_ext(indexoidlist);
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

    /*
     * XXX should free indexInfo array here too?  Currently we assume that
     * such stuff will be cleaned up automatically in FreeExecutorState.
     */
}

/*
 * Copied from ExecInsertIndexTuples
 */
void ExecDeleteIndexTuples(TupleTableSlot* slot, ItemPointer tupleid, EState* estate,
    Relation targetPartRel, Partition p, const Bitmapset *modifiedIdxAttrs, const bool inplaceUpdated)
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
                predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
                indexInfo->ii_PredicateState = predicate;
            }

            /* Skip this index-update if the predicate isn't satisfied */
            if (!ExecQual(predicate, econtext, false)) {
                continue;
            }
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        index_delete(actualindex, values, isnull, tupleid);
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
                              inplaceUpdated);
    } else {
        UHeapTuple tmpUtup = ExecGetUHeapTupleFromSlot(oldslot); // materialize the tuple
        tmpUtup->table_oid = RelationGetRelid(rel);
        ExecDeleteIndexTuples(oldslot,
                              tupleid,
                              exec_index_tuples_state.estate, exec_index_tuples_state.targetPartRel,
                              exec_index_tuples_state.p,
                              modifiedIdxAttrs,
                              inplaceUpdated);
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
            predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
            indexInfo->ii_PredicateState = predicate;
        }

        /* Skip this index-update if the predicate isn't satisfied */
        if (!ExecQual(predicate, econtext, false)) {
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
    RelationPtr relationDescs;
    Relation heapRelation;
    IndexInfo** indexInfoArray;
    ExprContext* econtext = NULL;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    Relation actualheap;
    bool ispartitionedtable = false;
    bool containGPI;
    List* partitionIndexOidList = NIL;

    /*
     * Get information from the result relation info structure.
     */
    resultRelInfo = estate->es_result_relation_info;
    numIndices = resultRelInfo->ri_NumIndices;
    relationDescs = resultRelInfo->ri_IndexRelationDescs;
    indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
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
        if (!p->pd_part->indisusable && !containGPI) {
            numIndices = 0;
        }
    } else {
        actualheap = heapRelation;
    }

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
    for (i = 0; i < numIndices; i++) {
        Relation indexRelation = relationDescs[i];
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

        indexInfo = indexInfoArray[i];

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
                actualindex,
                indexpartition,
                RowExclusiveLock);
            // skip unusable index
            if (!indexpartition->pd_part->indisusable) {
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
                predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
                indexInfo->ii_PredicateState = predicate;
            }

            /* Skip this index-update if the predicate isn't satisfied */
            if (!ExecQual(predicate, econtext, false)) {
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
        if (!indexRelation->rd_index->indisunique) {
            checkUnique = UNIQUE_CHECK_NO;
        } else if (conflict != NULL) {
            checkUnique = UNIQUE_CHECK_UPSERT;
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

        if ((IndexUniqueCheckNoError(checkUnique) || indexInfo->ii_ExclusionOps != NULL) && !satisfiesConstraint) {
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
    existing_slot = MakeSingleTupleTableSlot(RelationGetDescr(heap), false, heap->rd_tam_type);
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
static void ShutdownExprContext(ExprContext* econtext, bool isCommit)
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


int PthreadMutexLock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    HOLD_INTERRUPTS();
    if (owner)
        ResourceOwnerEnlargePthreadMutex(owner);

    int ret = pthread_mutex_lock(mutex);
    if (ret == 0 && trace && owner) {
        ResourceOwnerRememberPthreadMutex(owner, mutex);
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
    if (ret == 0 && trace && owner) {
        ResourceOwnerRememberPthreadMutex(owner, mutex);
    }
    RESUME_INTERRUPTS();
    return ret;
}

int PthreadMutexUnlock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    HOLD_INTERRUPTS();
    int ret = pthread_mutex_unlock(mutex);
    if (ret == 0 && trace && owner)
        ResourceOwnerForgetPthreadMutex(owner, mutex);
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