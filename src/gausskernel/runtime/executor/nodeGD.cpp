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
 *---------------------------------------------------------------------------------------
 *
 *  nodeGD.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/nodeGD.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeGD.h"
#include "db4ai/gd.h"

//////////////////////////////////////////////////////////////////////////

GradientDescentHook_iteration gdhook_iteration = nullptr;

static bool transfer_slot(GradientDescentState* gd_state, TupleTableSlot* slot,
                                    int ith_tuple, Matrix* features, Matrix* dep_var)
{
    const GradientDescent* gd_node = gd_get_node(gd_state);
    Assert(ith_tuple < (int)features->rows);
    
    if (!slot->tts_isnull[gd_node->targetcol]) {
        int feature = 0;
        gd_float* w = features->data + ith_tuple * features->columns;
        for (int i = 0; i < get_natts(gd_state); i++) {
            if (i == gd_node->targetcol && !dep_var_is_continuous(gd_state->algorithm)) {
                if (!dep_var_is_binary(gd_state->algorithm)) {
                    ereport(ERROR,
                            (errmodule(MOD_DB4AI),
                                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("categorical dependent variable not implemented")));
                }                    
                    
                float dt = 0;
                if (get_atttypid(gd_state, gd_node->targetcol) == BOOLOID)
                    dt = DatumGetBool(slot->tts_values[gd_node->targetcol])
                                        ? gd_state->algorithm->max_class : gd_state->algorithm->min_class;
                else {
                    bool found = false;
                    for (int v = 0; v < gd_state->num_classes && !found; v++) {
                        found = datumIsEqual(slot->tts_values[gd_node->targetcol], gd_state->binary_classes[v],
                                            get_attbyval(gd_state, gd_node->targetcol),
                                            get_attlen(gd_state, gd_node->targetcol));
                        if (found)
                            dt = (v == 1 ? gd_state->algorithm->max_class : gd_state->algorithm->min_class);
                    }
                    if (!found) {
                        if (gd_state->num_classes == 2)
                            ereport(ERROR,
                                    (errmodule(MOD_DB4AI),
                                        errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                                        errmsg("too many target values for binary operator")));
                        
                        gd_state->binary_classes[gd_state->num_classes++] =
                                datumCopy(slot->tts_values[gd_node->targetcol],
                                        get_attbyval(gd_state, gd_node->targetcol),
                                        get_attlen(gd_state, gd_node->targetcol));
                    }
                }
                dep_var->data[ith_tuple] = dt;                               
            } else {
                gd_float value;
                if (slot->tts_isnull[i]) {
                    Assert(i != gd_node->targetcol);
                    value = 0.0; // default value for feature, it is not the target for sure
                }
                else
                    value = gd_datum_get_float(get_atttypid(gd_state, i), slot->tts_values[i]);

                if (i == gd_node->targetcol)
                    dep_var->data[ith_tuple] = value;
                else {
                    *w++ = value;
                    feature++;
                }
            }
        }
        Assert(feature == gd_state->n_features-1);
        *w = 1.0; // bias
        
        return true;
    }
    
    return false;
}

void exec_gd_batch(GradientDescentState* gd_state, int iter)
{
    // get information from the node
    const GradientDescent* gd_node = gd_get_node(gd_state);
    PlanState* outer_plan = outerPlanState(gd_state);
    Matrix* features;
    Matrix* dep_var;
    bool more = true;
    TupleTableSlot* slot = NULL;
    do {
        // read next batch
        features = gd_state->shuffle->get(gd_state->shuffle, &dep_var);

        int ith_tuple = 0;
        while (more && ith_tuple < gd_node->batch_size) {
            slot = ExecProcNode(outer_plan);
            if (TupIsNull(slot)) {
                more = false;
            } else {
                if (transfer_slot(gd_state, slot, ith_tuple, features, dep_var)) {
                    if (iter == 0)
                        gd_state->processed++;

                    ith_tuple++;
                } else {
                    if (iter == 0)
                        gd_state->discarded++;
                }
            }
        }

        // use the batch to test now in case the shuffle algorithm
        // releases it during unget
        if (iter > 0 && ith_tuple > 0) {
            if (ith_tuple < gd_node->batch_size) {
                matrix_resize(features, ith_tuple, gd_state->n_features);
                matrix_resize(dep_var, ith_tuple, 1);                        
            }
            
            double loss = gd_state->algorithm->test_callback(gd_node, features, dep_var, &gd_state->weights, &gd_state->scores);
            gd_state->loss += loss;
            ereport(DEBUG1,
                    (errmodule(MOD_DB4AI),
                        errmsg("iteration %d loss = %.6f (total %.6g)", iter, loss, gd_state->loss)));
            
            if (ith_tuple < gd_node->batch_size) {
                matrix_resize(features, gd_node->batch_size, gd_state->n_features);
                matrix_resize(dep_var, gd_node->batch_size, 1);                        
            }
        }
        
        // give back the batch to the shuffle algorithm
        gd_state->shuffle->unget(gd_state->shuffle, ith_tuple);
    } while (more);
}

void exec_gd_start_iteration(GradientDescentState* gd_state)
{
        if (gd_state->optimizer->start_iteration != nullptr)
            gd_state->optimizer->start_iteration(gd_state->optimizer);

        if (gd_state->shuffle->start_iteration != nullptr)
            gd_state->shuffle->start_iteration(gd_state->shuffle);
}

void exec_gd_end_iteration(GradientDescentState* gd_state)
{
        if (gd_state->shuffle->end_iteration != nullptr)
            gd_state->shuffle->end_iteration(gd_state->shuffle);
        
        if (gd_state->optimizer->end_iteration != nullptr)
            gd_state->optimizer->end_iteration(gd_state->optimizer);
}

/* ----------------------------------------------------------------
 *		ExecGradientDescent
 * ----------------------------------------------------------------
 * 
 * Training and test are interleaved to avoid a double scan over the data
 * for training and test. Iteration 0 only computes the initial weights, and
 * at each following iteration the model is tested with the current weights
 * and new weights are updated with the gradients. The optimization is clear:
 * for N iterations, the basic algorithm requires N*2 data scans, while the
 * interleaved train&test requires only N+1 data scans. When N=1 the number
 * of scans is the same (N*2 = N+1)
 */
TupleTableSlot* ExecGradientDescent(GradientDescentState* gd_state)
{
    // check if training is already finished
    if (gd_state->done)
        return NULL;
    
    // get information from the node
    const GradientDescent* gd_node = gd_get_node(gd_state);
    ScanDirection direction = gd_state->ss.ps.state->es_direction;
    PlanState* outer_plan = outerPlanState(gd_state);

    // If backwards scan, just return NULL without changing state.
    if (!ScanDirectionIsForward(direction))
        return NULL;

    // for counting execution time
    uint64_t start, finish, step;
    uint64_t iter_start, iter_finish;

    // iterations
    double prev_loss = 0;
    TupleTableSlot* slot = NULL;
    
    gd_state->processed = 0;
    gd_state->discarded = 0;

    uint64_t max_usecs = ULLONG_MAX;
    if (gd_node->max_seconds > 0)
        max_usecs = gd_node->max_seconds * 1000000ULL;
    
    bool stop = false;
    start = gd_get_clock_usecs();
    step = start;
    for (int iter = 0; !stop && iter <= gd_node->max_iterations; iter++) {
        iter_start = gd_get_clock_usecs();
        
        // init loss & scores
        scores_init(&gd_state->scores);
        gd_state->loss = 0;
        
        exec_gd_start_iteration(gd_state);
        exec_gd_batch(gd_state, iter);
        exec_gd_end_iteration(gd_state);

        iter_finish = gd_get_clock_usecs();

        // delta loss < loss tolerance?
        if (iter > 0)
            stop = (fabs(prev_loss - gd_state->loss) < gd_node->tolerance);

        if (!stop) {
            // continue with another iteration with the new weights
            int bytes = sizeof(gd_float) * gd_state->n_features;
            int rc = memcpy_s(gd_state->weights.data, gd_state->weights.allocated * sizeof(gd_float),
                            gd_state->optimizer->weights.data, bytes);
            securec_check(rc, "", "");
        
            if (iter > 0) {
                gd_state->n_iterations++;
                if (gdhook_iteration != nullptr)
                    gdhook_iteration(gd_state);
            }

            // timeout || max_iterations
            stop = (gd_get_clock_usecs()-start >= max_usecs)
                    || (iter == gd_node->max_iterations);
        }
            
        // trace at end or no more than once per second
        bool trace_iteration = gd_node->verbose || stop;
        if (!trace_iteration) {
            uint64_t now = gd_get_clock_usecs();
            uint64_t nusecs = now - step;
            if (nusecs > 1000000) {
                // more than one second
                trace_iteration = true;
                step = now;
            }
        }
        
        if (iter>0 && trace_iteration) {
            gd_float* w = gd_state->weights.data;
            StringInfoData buf;
            initStringInfo(&buf);
            for (int i=0 ; i<gd_state->n_features ; i++)
                appendStringInfo(&buf, "%.3f,", w[i]);
            
            ereport(DEBUG1,
                    (errmodule(MOD_DB4AI),
                        errmsg("ITERATION %d: test_loss=%.6f delta_loss=%.6f tolerance=%.3f  accuracy=%.3f tuples=%d coef=%s",
                                iter, gd_state->loss,
                                fabs(prev_loss - gd_state->loss), gd_node->tolerance,
                                get_accuracy(&gd_state->scores), gd_state->processed,
                                buf.data)));
            pfree(buf.data);
        }
        
        prev_loss = gd_state->loss;

        if (!stop)
            ExecReScan(outer_plan);  // for the next iteration
    }
    
    finish = gd_get_clock_usecs();
    
    gd_state->done = true;
    gd_state->usecs = finish - start;

    // return trainined model
    ExprDoneCond isDone;
    slot = ExecProject(gd_state->ss.ps.ps_ProjInfo, &isDone);
    
    return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitGradientDescent
 *
 *		This initializes the GradientDescent node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
GradientDescentState* ExecInitGradientDescent(GradientDescent* gd_node, EState* estate, int eflags)
{
    GradientDescentState* gd_state = NULL;
    Plan* outer_plan = outerPlan(gd_node);

    // check for unsupported flags
    Assert(!(eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // create state structure
    gd_state = makeNode(GradientDescentState);
    gd_state->ss.ps.plan = (Plan*)gd_node;
    gd_state->ss.ps.state = estate;

    // Tuple table initialization
    ExecInitScanTupleSlot(estate, &gd_state->ss);
    ExecInitResultTupleSlot(estate, &gd_state->ss.ps);

    // initialize child expressions
    ExecAssignExprContext(estate, &gd_state->ss.ps);
    gd_state->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)gd_node->plan.targetlist, (PlanState*)gd_state);
    
    // initialize outer plan
    outerPlanState(gd_state) = ExecInitNode(outer_plan, estate, eflags);

    // Initialize result tuple type and projection info.
    ExecAssignScanTypeFromOuterPlan(&gd_state->ss); // input tuples
    ExecAssignResultTypeFromTL(&gd_state->ss.ps);  // result tuple
    ExecAssignProjectionInfo(&gd_state->ss.ps, NULL);
    gd_state->ss.ps.ps_TupFromTlist = false;
    
    // select algorithm
    gd_state->algorithm = gd_get_algorithm(gd_node->algorithm);
   
    // Input tuple initialization
    gd_state->tupdesc = ExecGetResultType(outerPlanState(gd_state));
    
    int natts = gd_state->tupdesc->natts;
    gd_state->n_features = natts; // -1 dep_var, +1 bias (fixed as 1)
    
    for (int i = 0; i < natts; i++) {
        Oid oidtype = gd_state->tupdesc->attrs[i]->atttypid;
        if (i == gd_node->targetcol) {
            switch (oidtype) {
                case BITOID:
                case VARBITOID:
                case BYTEAOID:
                case CHAROID:
                case RAWOID:
                case NAMEOID:
                case TEXTOID:
                case BPCHAROID:
                case VARCHAROID:
                case NVARCHAR2OID:
                case CSTRINGOID:
                case INT1OID:
                case INT2OID:
                case INT4OID:
                case INT8OID:
                case FLOAT4OID:
                case FLOAT8OID:
                case NUMERICOID:
                case ABSTIMEOID:
                case DATEOID:
                case TIMEOID:
                case TIMESTAMPOID:
                case TIMESTAMPTZOID:
                case TIMETZOID:
                case SMALLDATETIMEOID:
                    // detect the different values while reading the data
                    gd_state->num_classes = 0;
                    break;
                
                case BOOLOID:
                    // values are known in advance
                    gd_state->binary_classes[0] = BoolGetDatum(false);
                    gd_state->binary_classes[1] = BoolGetDatum(true);
                    gd_state->num_classes = 2;
                    break;
                    
                default:
                    // unsupported datatypes
                    ereport(ERROR,
                            (errmodule(MOD_DB4AI),
                                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Datatype of target not supported")));
                    break;
            }
        }
    }
    
    // optimizer
    switch (gd_node->optimizer) {
        case OPTIMIZER_GD:
            gd_state->optimizer = gd_init_optimizer_gd(gd_state);
            break;
        case OPTIMIZER_NGD:
            gd_state->optimizer = gd_init_optimizer_ngd(gd_state);
            break;
        default:
            ereport(ERROR,
                    (errmodule(MOD_DB4AI),
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Optimizer %d not supported", gd_node->optimizer)));
            break;
    }
    matrix_init(&gd_state->optimizer->weights, gd_state->n_features);
    matrix_init(&gd_state->optimizer->gradients, gd_state->n_features);

    // shuffle
    gd_state->shuffle = gd_init_shuffle_cache(gd_state);
    gd_state->shuffle->optimizer = gd_state->optimizer;
    
    // training state initialization
    gd_state->done = false;
    gd_state->learning_rate = gd_node->learning_rate;
    gd_state->n_iterations = 0;
    gd_state->loss = 0;
    matrix_init(&gd_state->weights, gd_state->n_features);

    return gd_state;
}

/* ----------------------------------------------------------------
 *		ExecEndGradientDescent
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void ExecEndGradientDescent(GradientDescentState* gd_state)
{
    // release state
    matrix_release(&gd_state->weights);

    gd_state->shuffle->release(gd_state->shuffle);
    
    matrix_release(&gd_state->optimizer->gradients);
    matrix_release(&gd_state->optimizer->weights);
    gd_state->optimizer->release(gd_state->optimizer);
    
    ExecFreeExprContext(&gd_state->ss.ps);
    ExecEndNode(outerPlanState(gd_state));
    pfree(gd_state);
}

