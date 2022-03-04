/* -------------------------------------------------------------------------
 *
 * nodeForeignscan.cpp
 *	  Routines to support scans of foreign tables
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeForeignscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *
 *		ExecForeignScan			scans a foreign table.
 *		ExecInitForeignScan		creates and initializes state info.
 *		ExecReScanForeignScan	rescans the foreign relation.
 *		ExecEndForeignScan		releases any resources allocated.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeForeignscan.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/memutils.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif

#ifdef PGXC
#include "utils/lsyscache.h"
#include "pgxc/pgxc.h"
#endif

#include "utils/knl_relcache.h"

static TupleTableSlot* ForeignNext(ForeignScanState* node);
static bool ForeignRecheck(ForeignScanState* node, TupleTableSlot* slot);

/* ----------------------------------------------------------------
 *		ForeignNext
 *
 *		This is a workhorse for ExecForeignScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ForeignNext(ForeignScanState* node)
{
    TupleTableSlot* slot = NULL;
    ForeignScan* plan = (ForeignScan*)node->ss.ps.plan;
    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    MemoryContext oldcontext;

    /* Call the Iterate function in short-lived context */
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    slot = node->fdwroutine->IterateForeignScan(node);
    MemoryContextSwitchTo(oldcontext);

    /*
     * If any system columns are requested, we have to force the tuple into
     * physical-tuple form to avoid "cannot extract system attribute from
     * virtual tuple" errors later.  We also insert a valid value for
     * tableoid, which is the only actually-useful system column.
     */
    if (plan->fsSystemCol && !TupIsNull(slot)) {
        HeapTuple tup = ExecMaterializeSlot(slot);

        tup->t_tableOid = RelationGetRelid(node->ss.ss_currentRelation);
        tup->t_bucketId = RelationGetBktid(node->ss.ss_currentRelation);
#ifdef PGXC
        tup->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif
    }

    return slot;
}

/*
 * ForeignRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool ForeignRecheck(ForeignScanState* node, TupleTableSlot* slot)
{
    /* There are no access-method-specific conditions to recheck. */
    return true;
}

/* ----------------------------------------------------------------
 *		ExecForeignScan(node)
 *
 *		Fetches the next tuple from the FDW, checks local quals, and
 *		returns it.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecForeignScan(ForeignScanState* node)
{
    return ExecScan((ScanState*)node, (ExecScanAccessMtd)ForeignNext, (ExecScanRecheckMtd)ForeignRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitForeignScan
 * ----------------------------------------------------------------
 */
ForeignScanState* ExecInitForeignScan(ForeignScan* node, EState* estate, int eflags)
{
    ForeignScanState* scanstate = NULL;
    Relation currentRelation;
    FdwRoutine* fdwroutine = NULL;
    errno_t rc;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    scanstate = makeNode(ForeignScanState);
    scanstate->ss.ps.plan = (Plan*)node;
    scanstate->ss.ps.state = estate;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
#ifdef ENABLE_MOT
    if ((estate->mot_jit_context == NULL) || IS_PGXC_COORDINATOR || !JitExec::IsMotCodegenEnabled()) {
#endif
        ExecAssignExprContext(estate, &scanstate->ss.ps);
#ifdef ENABLE_MOT
    }
#endif

    scanstate->ss.ps.ps_TupFromTlist = false;

    /*
     * This function ExecInitForeignScan will be called by ExecInitVecForeignScan.
     * If the node is VecForeignScan, do not need initialize here.
     */
    if (!IsA(node, VecForeignScan)) {
        /*
         * initialize child expressions
         */
        scanstate->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)node->scan.plan.targetlist, (PlanState*)scanstate);
        scanstate->ss.ps.qual = (List*)ExecInitExpr((Expr*)node->scan.plan.qual, (PlanState*)scanstate);
    }
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
    ExecInitScanTupleSlot(estate, &scanstate->ss);

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    if (node->rel == NULL) {
        currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);
    } else {
        if (node->in_compute_pool == false) {
            currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);
        } else {
            currentRelation = get_rel_from_meta(node->rel);
            scanstate->options = node->options;
        }
    }

    scanstate->ss.ss_currentRelation = currentRelation;

    /*
     * get the scan type from the relation descriptor.	(XXX at some point we
     * might want to let the FDW editorialize on the scan tupdesc.)
     */
    ExecAssignScanType(&scanstate->ss, RelationGetDescr(currentRelation));

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &scanstate->ss.ps,
            scanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);
    ExecAssignScanProjectionInfo(&scanstate->ss);
    Assert(scanstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

    /*
     * Acquire function pointers from the FDW's handler, and init fdw_state.
     */
    if (!node->rel) {
        fdwroutine = GetFdwRoutineForRelation(currentRelation, true);
    } else {
        ForeignDataWrapper* fdw = NULL;
        if (node->options->stype == T_OBS_SERVER || node->options->stype == T_HDFS_SERVER) {
            fdw = GetForeignDataWrapperByName(HDFS_FDW, false);
        } else {
            fdw = GetForeignDataWrapperByName(DIST_FDW, false);
        }

        fdwroutine = GetFdwRoutine(fdw->fdwhandler);

        /* Save the data for later reuse in LocalMyDBCacheMemCxt */
        FdwRoutine* cfdwroutine = (FdwRoutine*)MemoryContextAlloc(LocalMyDBCacheMemCxt(), sizeof(FdwRoutine));
        rc = memcpy_s(cfdwroutine, sizeof(FdwRoutine), fdwroutine, sizeof(FdwRoutine));
        securec_check(rc, "\0", "\0");
        currentRelation->rd_fdwroutine = cfdwroutine;
    }

    scanstate->fdwroutine = fdwroutine;
    scanstate->fdw_state = NULL;

#ifdef ENABLE_MOT
    if ((estate->mot_jit_context == NULL) || IS_PGXC_COORDINATOR || !JitExec::IsMotCodegenEnabled()) {
#endif
        scanstate->scanMcxt = AllocSetContextCreate(CurrentMemoryContext,
            "Foreign Scan",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        /*
         * Tell the FDW to initiate the scan.
         */
        fdwroutine->BeginForeignScan(scanstate, eflags);
#ifdef ENABLE_MOT
    }
#endif

    return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndForeignScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndForeignScan(ForeignScanState* node)
{
    /* Let the FDW shut down */
#ifdef ENABLE_MOT
    if (node->fdw_state != NULL) {
#endif
        node->fdwroutine->EndForeignScan(node);
#ifdef ENABLE_MOT
    }
#endif

    /* Free the exprcontext */
    ExecFreeExprContext(&node->ss.ps);

    /* clean out the tuple table */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /* close the relation. */
    ForeignScan* scan = (ForeignScan*)node->ss.ps.plan;
    if (NULL == scan->rel) {
        ExecCloseScanRelation(node->ss.ss_currentRelation);
    } else {
        if (false == scan->in_compute_pool) {
            ExecCloseScanRelation(node->ss.ss_currentRelation);
        }
    }

    /* clear obs sk key */
    if (node->options != NULL) {
        char* obskey = getFTOptionValue(node->options->fOptions, OPTION_NAME_OBSKEY);
        if (obskey != NULL) {
            errno_t rc = EOK;
            rc = memset_s(obskey, strlen(obskey), 0, strlen(obskey));
            securec_check(rc, "\0", "\0");
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanForeignScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanForeignScan(ForeignScanState* node)
{
    node->fdwroutine->ReScanForeignScan(node);

    ExecScanReScan(&node->ss);
}
