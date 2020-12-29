/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * vecscan.cpp
 *    This code provides support for generalized vectorized relation scans.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "vecexecutor/vecexecutor.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/*
 * ExecScanFetch -- fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline VectorBatch* exec_vec_scan_fetch(
    ScanState* node, ExecVecScanAccessMtd access_mtd, ExecVecScanRecheckMtd recheck_mtd)
{
    // We don't support reevaluate plan
    //
    DBG_ASSERT(node->ps.state->es_epqTuple == NULL);

    // Run the node-type-specific access method function to get the next tuple
    //
    return (*access_mtd)(node);
}

/* ----------------------------------------------------------------
 *      ExecVecScan
 *
 *      Scans the relation using the 'access method' indicated and
 *      returns the next qualifying tuple in the direction specified
 *      in the global variable ExecDirection.
 *      The access method returns the next tuple and execScan() is
 *      responsible for checking the tuple returned against the qual-clause.
 *
 *      A 'recheck method' must also be provided that can check an
 *      arbitrary tuple of the relation against any qual conditions
 *      that are implemented internal to the access method.
 *
 *      Conditions:
 *        -- the "cursor" maintained by the AMI is positioned at the tuple
 *           returned previously.
 *
 *      Initial States:
 *        -- the relation indicated is opened for scanning so that the
 *           "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecScan(ScanState* node, ExecVecScanAccessMtd accessMtd, /* function returning a tuple */
    ExecVecScanRecheckMtd recheckMtd)
{
    ExprContext* econtext = NULL;
    ProjectionInfo* proj_info = NULL;
    VectorBatch* result_batch = NULL;

    /*
     * Fetch data from node
     */
    List* qual = node->ps.qual;
    proj_info = node->ps.ps_ProjInfo;
    econtext = node->ps.ps_ExprContext;

    /*
     * @hdfs
     * according to informational constraint, we will do not get
     * suitable tuple, so return here
     */
    if (node->is_scan_end) {
        ResetExprContext(econtext);
        return NULL;
    }

    /*
     * If we have neither a qual to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
    if (qual == NULL && proj_info == NULL) {
        ResetExprContext(econtext);
        return exec_vec_scan_fetch(node, accessMtd, recheckMtd);
    }

    // We don't support function returning set
    //
    DBG_ASSERT(!node->ps.ps_TupFromTlist);

    // Reset per-tuple memory context to free any expression evaluation
    // storage allocated in the previous batch cycle.  Note this can't happen
    // until we're done projecting out tuples from a scan tuple.
    //
    ResetExprContext(econtext);

    /*
     * get a tuple from the access method.  Loop until we obtain a tuple that
     * passes the qualification.
     */
    for (;;) {
        VectorBatch* batch = NULL;

        CHECK_FOR_INTERRUPTS();

        batch = exec_vec_scan_fetch(node, accessMtd, recheckMtd);
        /* Response to the stop query flag. */
        if (BatchIsNull(batch) || unlikely(true == executorEarlyStop())) {
            return NULL;
        }

        /*
         * check that the current tuple satisfies the qual-clause
         *
         * check for non-nil qual here to avoid a function call to ExecQual()
         * when the qual is nil ... saves only a few cycles, but they add up
         * ...
         */
        econtext->ecxt_scanbatch = batch;
        if (qual == NULL || ExecVecQual(qual, econtext, false)) {
            /*
             * Found a satisfactory scan tuple.
             */
            result_batch = batch;

            /*
             * The pack operator must be done defore the projection.
             */
            if (econtext->ecxt_scanbatch->m_sel) {
                econtext->ecxt_scanbatch->Pack(econtext->ecxt_scanbatch->m_sel);
            }

            if (proj_info != NULL) {
                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it --- unless we find we can project no tuples
                 * from this scan tuple, in which case continue scan.
                 */
                result_batch = ExecVecProject(proj_info);
                result_batch->m_rows = Min(result_batch->m_rows, batch->m_rows);
                result_batch->FixRowCount();
            }

            if (result_batch->m_rows > 0) {
                /*
                 * @hdfs
                 * Optimize foreign scan by using informational constraint.
                 */
                if (IsA(node->ps.plan, VecForeignScan)) {
                    ForeignScan* foreign_scan = NULL;
                    foreign_scan = (ForeignScan*)(node->ps.plan);
                    if (foreign_scan->scan.scan_qual_optimized) {
                        node->is_scan_end = true;
                        result_batch->m_rows = 1;
                    }
                }
                return result_batch;
            }
        }

        /*
         * Batch fails qual, so free per-batch memory and try again.
         */
        ResetExprContext(econtext);
    }
}

/*
 * ExecAssignVecScanProjectionInfo
 *      Set up projection info for a scan node, if necessary.
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
void ExecAssignVecScanProjectionInfo(ScanState* node)
{
    Scan* scan = (Scan*)node->ps.plan;

    if (tlist_matches_tupdesc(
            &node->ps, scan->plan.targetlist, scan->scanrelid, node->ss_ScanTupleSlot->tts_tupleDescriptor))
        node->ps.ps_ProjInfo = NULL;
    else
        node->ps.ps_ProjInfo = ExecBuildVecProjectionInfo(
            node->ps.targetlist, node->ps.plan->qual, node->ps.ps_ExprContext, node->ps.ps_ResultTupleSlot, NULL);
}
