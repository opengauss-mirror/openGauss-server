/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 *  vecforeignroutine.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/bulkload/vecforeignroutine.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "nodes/nodes.h"
#include "nodes/execnodes.h"
#include "foreign/fdwapi.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/memutils.h"
#include "vecexecutor/vecnodes.h"
#include "bulkload/foreignroutine.h"
#include "bulkload/dist_fdw.h"
#include "commands/copy.h"
#include "miscadmin.h"

extern bool DoAcceptOneError(DistImportExecutionState *festate);
extern bool DoAcceptOneError(CopyState cstate);
extern void EndDistImport(DistImportExecutionState *importstate);
extern void FormAndSaveImportError(CopyState cstate, Relation errRel, Datum begintime, ImportErrorLogger *elogger);
extern void FormAndSaveImportError(CopyState cstate, Relation errRel, Datum begintime, CopyErrorLogger *elogger);

/*
 * all stuffs used for bulkload, which comments being boundary.
 */
extern void SyncBulkloadStates(CopyState cstate);
extern void CleanBulkloadStates();  // all stuffs used for bulkload(end).

// Try to save importing error if needed
bool TrySaveImportError(DistImportExecutionState *importState, ForeignScanState *node)
{
    if ((ERRCODE_TO_CATEGORY((unsigned int)geterrcode()) == ERRCODE_DATA_EXCEPTION) && DoAcceptOneError(importState)) {
        if (geterrcode() == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE || geterrcode() == ERRCODE_UNTRANSLATABLE_CHARACTER)
            t_thrd.bulk_cxt.illegal_character_err_cnt++;

        ListCell *lc = NULL;

        foreach (lc, importState->elogger) {
            ImportErrorLogger *elogger = (ImportErrorLogger *)lfirst(lc);
            FormAndSaveImportError(importState, importState->errLogRel, importState->beginTime, elogger);
        }

        // clear error state
        //
        FlushErrorStateWithoutDeleteChildrenContext();
        return true;
    }
    return false;
}

bool TrySaveImportError(CopyState cstate)
{
    cstate->errorrows++;
    if ((ERRCODE_TO_CATEGORY((unsigned int)geterrcode()) == ERRCODE_DATA_EXCEPTION) && DoAcceptOneError(cstate)) {
        FormAndSaveImportError(cstate, cstate->err_table, cstate->copy_beginTime, cstate->logger);
        // clear error state
        //
        FlushErrorStateWithoutDeleteChildrenContext();
        return true;
    }
    return false;
}

VectorBatch *distExecVecImport(VecForeignScanState *node)
{
    DistImportExecutionState *importState = (DistImportExecutionState *)node->fdw_state;
    VectorBatch *batch = node->m_pScanBatch;
    bool found = false;
    ErrorContextCallback errcontext;
    Datum *values = node->m_values;
    bool *nulls = node->m_nulls;
    MemoryContext oldMemoryContext;
    MemoryContext scanMcxt = node->scanMcxt;

    /* Set up callback to identify error line number. */
    errcontext.callback = BulkloadErrorCallback;
    errcontext.arg = (void *)importState;
    errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcontext;

    /*
     * The protocol for loading a virtual tuple into a slot is first
     * ExecClearTuple, then fill the values/isnull arrays, then
     * ExecStoreVirtualTuple.  If we don't find another row in the file, we
     * just skip the last step, leaving the slot empty as required.
     *
     * We can pass ExprContext = NULL because we read all columns from the
     * file, so no need to evaluate default expressions.
     *
     * We can also pass tupleOid = NULL because we don't allow oids for
     * foreign tables.
     */
    batch->Reset(true);
    if (node->m_done) {
        /* Remove error callback. */
        t_thrd.log_cxt.error_context_stack = errcontext.previous;
        return batch;
    }

    MemoryContextReset(scanMcxt);
    oldMemoryContext = MemoryContextSwitchTo(scanMcxt);
#ifndef ENABLE_LITE_MODE
    SetObsMemoryContext(((CopyState)importState)->copycontext);
#endif
    for (batch->m_rows = 0; batch->m_rows < BatchMaxSize; batch->m_rows++) {
retry:
        PG_TRY();
        {
            /*
             * Synchronize the current bulkload states.
             */
            SyncBulkloadStates((CopyState)importState);
            found = NextCopyFrom((CopyState)importState, NULL, values, nulls, NULL);
        }
        PG_CATCH();
        {
            /*
             * Clean the current bulkload states.
             */
            CleanBulkloadStates();

            if (TrySaveImportError(importState, node)) {
                (void)MemoryContextSwitchTo(scanMcxt);
                MemoryContextReset(scanMcxt);
                CHECK_FOR_INTERRUPTS();
                goto retry;
            } else {
                /* clean copy state and re throw */
                importState->isExceptionShutdown = true;
                EndDistImport(importState);
                PG_RE_THROW();
            }
        }
        PG_END_TRY();

        /*
         * Clean the current bulkload states.
         */
        CleanBulkloadStates();

        if (found) {
            int rows = batch->m_rows;
            for (int i = 0; i < batch->m_cols; i++) {
                ScalarVector *vec = &(batch->m_arr[i]);
                if (nulls[i]) {
                    vec->m_rows++;
                    vec->SetNull(rows);
                    continue;
                }

                if (vec->m_desc.encoded)
                    vec->AddVar(values[i], rows);
                else
                    vec->m_vals[rows] = values[i];

                vec->m_rows++;
            }
        } else {
            node->m_done = true;
            break;
        }
    }

    (void)MemoryContextSwitchTo(oldMemoryContext);
    /* Remove error callback. */
    t_thrd.log_cxt.error_context_stack = errcontext.previous;

    return batch;
}
