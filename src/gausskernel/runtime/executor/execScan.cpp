/* -------------------------------------------------------------------------
 *
 * execScan.cpp
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execScan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/*
 * ExecScanFetch -- fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.	If we aren't, just execute
 * the access method's next-tuple routine.
 */
static TupleTableSlot* ExecScanFetch(ScanState* node, ExecScanAccessMtd access_mtd, ExecScanRecheckMtd recheck_mtd)
{
    EState* estate = node->ps.state;

    if (estate->es_epqTuple != NULL) {
        /*
         * We are inside an EvalPlanQual recheck.  Return the test tuple if
         * one is available, after rechecking any access-method-specific
         * conditions.
         */
        Index scan_rel_id = ((Scan*)node->ps.plan)->scanrelid;

        if (scan_rel_id == 0) {
            /*
             * This is a ForeignScan which has pushed down a
             * join to the remote side.  The recheck method is responsible not
             * only for rechecking the scan/join quals but also for storing
             * the correct tuple in the slot.
             *
             * currently not support.
             */
            Assert(false);
        } else if (estate->es_epqTupleSet[scan_rel_id - 1]) {
            TupleTableSlot* slot = node->ss_ScanTupleSlot;

            /* Return empty slot if we already returned a tuple */
            if (estate->es_epqScanDone[scan_rel_id - 1])
                return ExecClearTuple(slot);
            /* Else mark to remember that we shouldn't return more */
            estate->es_epqScanDone[scan_rel_id - 1] = true;

            /* Return empty slot if we haven't got a test tuple */
            if (estate->es_epqTuple[scan_rel_id - 1] == NULL)
                return ExecClearTuple(slot);

            /* Store test tuple in the plan node's scan slot */
            (void)ExecStoreTuple(estate->es_epqTuple[scan_rel_id - 1], slot, InvalidBuffer, false);

            /* Check if it meets the access-method conditions */
            if (!(*recheck_mtd)(node, slot))
                (void)ExecClearTuple(slot); /* would not be returned by scan */

            return slot;
        }
    }

    /*
     * Run the node-type-specific access method function to get the next tuple
     */
    return (*access_mtd)(node);
}

/* ----------------------------------------------------------------
 *		ExecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple in the direction specified
 *		in the global variable ExecDirection.
 *		The access method returns the next tuple and execScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecScan(ScanState* node, ExecScanAccessMtd access_mtd, /* function returning a tuple */
    ExecScanRecheckMtd recheck_mtd)
{
    ExprContext* econtext = NULL;
    List* qual = NIL;
    ProjectionInfo* proj_info = NULL;
    ExprDoneCond is_done;
    TupleTableSlot* result_slot = NULL;

    if (node->isPartTbl && !PointerIsValid(node->partitions))
        return NULL;

    /*
     * Fetch data from node
     */
    qual = node->ps.qual;
    proj_info = node->ps.ps_ProjInfo;
    econtext = node->ps.ps_ExprContext;

    /*
     * If we have neither a qual to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
    if (qual == NULL && proj_info == NULL) {
        ResetExprContext(econtext);
        return ExecScanFetch(node, access_mtd, recheck_mtd);
    }

    /*
     * Check to see if we're still projecting out tuples from a previous scan
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->ps.ps_vec_TupFromTlist) {
        Assert(proj_info); /* can't get here if not projecting */
        result_slot = ExecProject(proj_info, &is_done);
        if (is_done == ExprMultipleResult)
            return result_slot;
        /* Done with that source tuple... */
        node->ps.ps_vec_TupFromTlist = false;
    }

    /*
     * @hdfs
     * Optimize scan bu using informational constraint.
     * if the is_scan_false is true, the iteration is over.
     */
    if (node->is_scan_end) {
        return NULL;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * get a tuple from the access method.	Loop until we obtain a tuple that
     * passes the qualification.
     */
    for (;;) {
        TupleTableSlot* slot = NULL;

        slot = ExecScanFetch(node, access_mtd, recheck_mtd);
        /* refresh qual every loop */
        qual = node->ps.qual;
        /*
         * if the slot returned by the accessMtd contains NULL, then it means
         * there is nothing more to scan so we just return an empty slot,
         * being careful to use the projection result slot so it has correct
         * tupleDesc.
         */
        if (TupIsNull(slot) || unlikely(executorEarlyStop())) {
            if (proj_info != NULL) {
                if (proj_info->pi_state.is_flt_frame) {
                    return ExecClearTuple(proj_info->pi_state.resultslot);
                } else {
                    return ExecClearTuple(proj_info->pi_slot);
                }
            } else {
                return NULL;
            }
        }

        /* place to filter Ndp page */
        if (node->ss_currentScanDesc && node->ss_currentScanDesc->ndp_pushdown_optimized) {
            HeapTuple tuple = (HeapTuple)slot->tts_tuple;
            if (tuple && tuple->t_data && (tuple->t_data->t_infomask & NDP_HANDLED_TUPLE)) {
                if (proj_info != NULL) {
                    return proj_info->pi_slot;
                } else {
                    return slot;
                }
            }
        }

        /*
         * place the current tuple into the expr context
         */
        econtext->ecxt_scantuple = slot;

        /*
         * check that the current tuple satisfies the qual-clause
         *
         * check for non-nil qual here to avoid a function call to ExecQual()
         * when the qual is nil ... saves only a few cycles, but they add up
         * ...
         */
        if (qual == NULL || ExecQual(qual, econtext)) {
            /*
             * Found a satisfactory scan tuple.
             */
            if (proj_info != NULL) {
                if (node->ps.state && node->ps.state->es_plannedstmt) {
                    econtext->can_ignore = node->ps.state->es_plannedstmt->hasIgnore;
                }
                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it --- unless we find we can project no tuples
                 * from this scan tuple, in which case continue scan.
                 */
                result_slot = ExecProject(proj_info, &is_done);
#ifdef PGXC
                /* Copy the xcnodeoid if underlying scanned slot has one */
                result_slot->tts_xcnodeoid = slot->tts_xcnodeoid;
#endif /* PGXC */

                if (is_done != ExprEndResult) {
                    node->ps.ps_vec_TupFromTlist = (is_done == ExprMultipleResult);

                    /*
                     * @hdfs
                     * Optimize foreign scan by using informational constraint.
                     */
                    if (IsA(node->ps.plan, ForeignScan)) {
                        ForeignScan* foreign_scan = (ForeignScan*)(node->ps.plan);
                        if (foreign_scan->scan.scan_qual_optimized) {
                            /*
                             * If we find a suitable tuple, set is_scan_end value is true.
                             * It means that we do not find suitable tuple in the next iteration,
                             * the iteration is over.
                             */
                            node->is_scan_end = true;
                        }
                    }
                    return result_slot;
                }
            } else {
                /*
                 * Optimize foreign scan by using informational constraint.
                 */
                if (IsA(node->ps.plan, ForeignScan)) {
                    ForeignScan* foreign_scan = (ForeignScan*)(node->ps.plan);
                    if (foreign_scan->scan.scan_qual_optimized) {
                        /*
                         * If we find a suitable tuple, set is_scan_end value is true.
                         * It means that we do not find suitable tuple in the next iteration,
                         * the iteration is over.
                         */
                        node->is_scan_end = true;
                    }
                }
                /*
                 * Here, we aren't projecting, so just return scan tuple.
                 */
                return slot;
            }
        } else
            InstrCountFiltered1(node, 1);

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);
    }
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * ExecAssignScanType must have been called already.
 */
void ExecAssignScanProjectionInfo(ScanState* node)
{
    Scan* scan = (Scan*)node->ps.plan;
    Index var_no;

    /* Vars in an index-only scan's tlist should be INDEX_VAR */
    if (IsA(scan, IndexOnlyScan))
        var_no = INDEX_VAR;
#ifdef USE_SPQ
    else if (IsA(scan, SpqIndexOnlyScan))
        var_no = INDEX_VAR;
#endif
    else
        var_no = scan->scanrelid;

    if (tlist_matches_tupdesc(&node->ps, scan->plan.targetlist, var_no, node->ss_ScanTupleSlot->tts_tupleDescriptor))
        node->ps.ps_ProjInfo = NULL;
    else
        ExecAssignProjectionInfo(&node->ps, node->ss_ScanTupleSlot->tts_tupleDescriptor);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 * This function is called by ExecInitExtensiblePlan to initialize projection info.
 * Usually the caller provides a targetlist describing the scan tuples, so we can
 * avoid a projection step by setting ps_ProjInfo to NULL. Such as "SELECT * FROM ...".
 */
void ExecAssignScanProjectionInfoWithVarno(ScanState* node, Index var_no)
{
    Scan* scan = (Scan*)node->ps.plan;

    if (tlist_matches_tupdesc(&node->ps, scan->plan.targetlist, var_no, node->ss_ScanTupleSlot->tts_tupleDescriptor))
        node->ps.ps_ProjInfo = NULL;
    else
        ExecAssignProjectionInfo(&node->ps, node->ss_ScanTupleSlot->tts_tupleDescriptor);
}

bool tlist_matches_tupdesc(PlanState* ps, List* tlist, Index var_no, TupleDesc tup_desc)
{
    int num_attrs = tup_desc->natts;
    int attr_no;
    bool has_oid = false;
    ListCell* tlist_item = list_head(tlist);

    /* Check the tlist attributes */
    for (attr_no = 1; attr_no <= num_attrs; attr_no++) {
        Form_pg_attribute att_tup = &tup_desc->attrs[attr_no - 1];
        Var* var = NULL;

        if (tlist_item == NULL)
            return false; /* tlist too short */
        var = (Var*)((TargetEntry*)lfirst(tlist_item))->expr;
        if (var == NULL || !IsA(var, Var))
            return false; /* tlist item not a Var */
        /* if these Asserts fail, planner messed up */
        Assert(var->varno == var_no);
        Assert(var->varlevelsup == 0);
        if (var->varattno != attr_no)
            return false; /* out of order */
        if (att_tup->attisdropped)
            return false; /* table contains dropped columns */

        /*
         * Note: usually the Var's type should match the tupdesc exactly, but
         * in situations involving unions of columns that have different
         * typmods, the Var may have come from above the union and hence have
         * typmod -1.  This is a legitimate situation since the Var still
         * describes the column, just not as exactly as the tupdesc does. We
         * could change the planner to prevent it, but it'd then insert
         * projection steps just to convert from specific typmod to typmod -1,
         * which is pretty silly.
         */
        if (var->vartype != att_tup->atttypid || (var->vartypmod != att_tup->atttypmod && var->vartypmod != -1))
            return false; /* type mismatch */

        tlist_item = lnext(tlist_item);
    }

    if (tlist_item != NULL)
        return false; /* tlist too long */

    /*
     * If the plan context requires a particular hasoid setting, then that has
     * to match, too.
     */
    if (ExecContextForcesOids(ps, &has_oid) && has_oid != tup_desc->tdhasoid)
        return false;

    return true;
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void ExecScanReScan(ScanState* node)
{
    EState* estate = node->ps.state;

    /* Stop projecting any tuples from SRFs in the targetlist */
    node->ps.ps_vec_TupFromTlist = false;

    /* Rescan EvalPlanQual tuple if we're inside an EvalPlanQual recheck */
    if (estate->es_epqScanDone != NULL) {
        Index scan_rel_id = ((Scan*)node->ps.plan)->scanrelid;

        Assert(scan_rel_id > 0);

        estate->es_epqScanDone[scan_rel_id - 1] = false;
    }
}
