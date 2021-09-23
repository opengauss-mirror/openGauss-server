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
 * vecimit.cpp
 *     Routines to handle limiting of query results where appropriate
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veclimit.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeLimit.h"
#include "nodes/nodeFuncs.h"
#include "vecexecutor/vecexecutor.h"

VectorBatch* ExecVecLimit(VecLimitState* node) {
    ScanDirection direction;
    VectorBatch* outer_batch = NULL;
    PlanState* outer_plan = NULL;

    /*
     * get information from the node
     */
    direction = node->ps.state->es_direction;
    outer_plan = outerPlanState(node);

    /*
     * The main logic is a simple state machine.
     */
    switch (node->lstate) {
        case LIMIT_INITIAL:

            /*
             * First call for this node, so compute limit/offset. (We can't do
             * this any earlier, because parameters from upper nodes will not
             * be set during ExecInitLimit.)  This also sets position = 0 and
             * changes the state to LIMIT_RESCAN.
             */
            recompute_limits(node);

            /* fall through */
        case LIMIT_RESCAN:

            /*
             * If backwards scan, just return NULL without changing state.
             */
            if (!ScanDirectionIsForward(direction))
                return NULL;

            /*
             * Check for empty window; if so, treat like empty subplan.
             */
            if (node->count <= 0 && !node->noCount) {
                node->lstate = LIMIT_EMPTY;
                return NULL;
            }

            /*
             * Fetch rows from subplan until we reach position > offset.
             * first scan after recomputing parameters
             */
            for (;;) {
                outer_batch = VectorEngine(outer_plan);
                if (BatchIsNull(outer_batch)) {
                    /*
                     * The subplan returns too few tuples for us to produce
                     * any output at all.
                     */
                    node->lstate = LIMIT_EMPTY;
                    return NULL;
                }
                node->subBatch = outer_batch;

                int pre_pos = node->position;
                node->position += outer_batch->m_rows;
                if (node->position > node->offset) {
                    bool selection[BatchMaxSize];
                    /* vaild values start from offset */
                    int offset = node->offset - pre_pos;
                    for (int i = 0; i < outer_batch->m_rows; i++) {
                        if (node->noCount) { /* ignore count */
                            if (i < offset) {
                                selection[i] = false;
                            } else {
                                selection[i] = true;
                            }
                        } else {
                            if (i < offset) {
                                selection[i] = false;
                            } else if (i < node->count + offset) {
                                selection[i] = true;
                            } else {
                                outer_batch->m_rows = i;
                                for (int k = 0; k < outer_batch->m_cols; k++) {
                                    outer_batch->m_arr[k].m_rows = i;
                                }
                                break;
                            }
                        }
                    }
                    outer_batch->Pack(selection);
                    break;
                }
            }

            /*
             * Okay, we have the first batch of the window.
             */
            node->lstate = LIMIT_INWINDOW;
            break;

        case LIMIT_EMPTY:

            /*
             * The subplan is known to return no tuples (or not more than
             * OFFSET tuples, in general).	So we return no tuples.
             */
            return NULL;

        case LIMIT_INWINDOW:
            if (ScanDirectionIsForward(direction)) {
                /*
                 * Forwards scan, so check for stepping off end of window. If
                 * we are at the end of the window, return NULL without
                 * advancing the subplan or the position variable; but change
                 * the state machine state to record having done so.
                 */
                if (!node->noCount && node->position - node->offset >= node->count) {
                    node->lstate = LIMIT_WINDOWEND;
                    return NULL;
                }

                /*
                 * Get next tuple from subplan, if any.
                 */
                outer_batch = VectorEngine(outer_plan);
                if (BatchIsNull(outer_batch)) {
                    node->lstate = LIMIT_SUBPLANEOF;
                    return NULL;
                }

                node->subBatch = outer_batch;
                int tmp_pos = node->position + outer_batch->m_rows;
                if (!node->noCount && tmp_pos - node->offset >= node->count) {
                    /* RowNum is need number of row */
                    int row_num = node->count - (node->position - node->offset);
                    int batch_col_num = outer_batch->m_cols;
                    outer_batch->m_rows = row_num;
                    for (int j = 0; j < batch_col_num; j++) {
                        outer_batch->m_arr[j].m_rows = row_num;
                    }
                    node->position += outer_batch->m_rows;
                } else {
                    node->position += outer_batch->m_rows;
                }
            } else {
                /*
                 * Backwards scan, so check for stepping off start of window.
                 * As above, change only state-machine status if so.
                 */
                if (node->position <= node->offset + 1) {
                    node->lstate = LIMIT_WINDOWSTART;
                    return NULL;
                }

                /*
                 * Get previous tuple from subplan; there should be one!
                 */
                outer_batch = VectorEngine(outer_plan);
                if (BatchIsNull(outer_batch))
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("LIMIT subplan failed to run backwards")));
                node->subBatch = outer_batch;
                int tmp_pos = node->position;
                if (tmp_pos - outer_batch->m_rows <= node->offset + 1) {
                    outer_batch->m_rows = node->position - node->offset - 1;
                    node->position -= outer_batch->m_rows;
                } else {
                    node->position -= outer_batch->m_rows;
                }
            }
            break;

        case LIMIT_SUBPLANEOF:
            if (ScanDirectionIsForward(direction)) {
                return NULL;
            }

            /*
             * Backing up from subplan EOF, so re-fetch previous tuple; there
             * should be one!  Note previous tuple must be in window.
             */
            outer_batch = VectorEngine(outer_plan);
            if (BatchIsNull(outer_batch)) {
                ereport(
                    ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("LIMIT subplan failed to run backwards")));
            }
            node->subBatch = outer_batch;
            node->lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't advance it before */
            break;

        case LIMIT_WINDOWEND:
            if (ScanDirectionIsForward(direction))
                return NULL;

            /*
             * Backing up from window end: simply re-return the last tuple
             * fetched from the subplan.
             */
            outer_batch = node->subBatch;
            node->lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't advance it before */
            break;

        case LIMIT_WINDOWSTART:
            if (!ScanDirectionIsForward(direction)) {
                return NULL;
            }

            /*
             * Advancing after having backed off window start: simply
             * re-return the last tuple fetched from the subplan.
             */
            outer_batch = node->subBatch;
            node->lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't change it before */
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("impossible LIMIT state: %d", (int)node->lstate)));
    }

    /* Return the current tuple */
    Assert(!BatchIsNull(outer_batch));

    return outer_batch;
}

/* ----------------------------------------------------------------
 *		ExecVecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
VecLimitState* ExecInitVecLimit(VecLimit* node, EState* estate, int eflags)
{
    VecLimitState* limit_state = NULL;
    Plan* left_plan = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    /*
     * create state structure
     */
    limit_state = makeNode(VecLimitState);
    limit_state->ps.plan = (Plan*)node;
    limit_state->ps.state = estate;
    limit_state->ps.vectorized = true;
    limit_state->lstate = LIMIT_INITIAL;

    /*
     * Miscellaneous initialization
     *
     * Limit nodes never call ExecQual or ExecProject, but they need an
     * exprcontext anyway to evaluate the limit/offset parameters in.
     */
    ExecAssignExprContext(estate, &limit_state->ps);

    /*
     * initialize child expressions
     */
    limit_state->limitOffset = ExecInitExpr((Expr*)node->limitOffset, (PlanState*)limit_state);
    limit_state->limitCount = ExecInitExpr((Expr*)node->limitCount, (PlanState*)limit_state);

    /*
     * Tuple table initialization (XXX not actually used...)
     */
    ExecInitResultTupleSlot(estate, &limit_state->ps);

    /*
     * then initialize outer plan
     */
    left_plan = outerPlan(node);
    outerPlanState(limit_state) = ExecInitNode(left_plan, estate, eflags);

    /*
     * limit nodes do no projections, so initialize projection info for this
     * node appropriately
     */
    ExecAssignResultTypeFromTL(
            &limit_state->ps,
            ExecGetResultType(outerPlanState(limit_state))->tdTableAmType);

    limit_state->ps.ps_ProjInfo = NULL;
    limit_state->subBatch = NULL;

    return limit_state;
}

/* ----------------------------------------------------------------
 *		ExecEndVecLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void ExecEndVecLimit(VecLimitState* node)
{
    ExecFreeExprContext(&node->ps);
    ExecEndNode(outerPlanState(node));
}

void ExecReScanVecLimit(VecLimitState* node)
{
    /*
     * Recompute limit/offset in case parameters changed, and reset the state
     * machine.  We must do this before rescanning our child node, in case
     * it's a Sort that we are passing the parameters down to.
     */
    recompute_limits(node);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL) {
        VecExecReScan(node->ps.lefttree);
    }
}
