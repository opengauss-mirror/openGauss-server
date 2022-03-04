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
 * gd.cpp
 *        Gradient descent is used for algorithm optimization.
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/gd.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"

#include "db4ai/gd.h"
#include "db4ai/db4ai_common.h"
#include "nodes/primnodes.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "db4ai/hyperparameter_validation.h"
#include "db4ai/kernel.h"

#define GD_TARGET_COL   0   // predefined, always the first

const char *gd_get_optimizer_name(OptimizerML optimizer)
{
    static const char* names[] = { "gd", "ngd" };
    if (optimizer >= INVALID_OPTIMIZER)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid optimizer %d", optimizer)));

    return names[optimizer];
}

GradientDescent *gd_get_algorithm(AlgorithmML algorithm)
{
    GradientDescent *gd_algorithm = nullptr;
    switch (algorithm) {
        case LOGISTIC_REGRESSION:
            gd_algorithm = &gd_logistic_regression;
            break;
        case SVM_CLASSIFICATION:
            gd_algorithm = &gd_svm_classification;
            break;
        case LINEAR_REGRESSION:
            gd_algorithm = &gd_linear_regression;
            break;
        case PCA:
            gd_algorithm = &gd_pca;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid algorithm %d", algorithm)));
            break;
    }
    return gd_algorithm;
}

// usage: svmc, logreg
MetricML *gd_metrics_accuracy(AlgorithmAPI *self, int *num_metrics)
{
    Assert(num_metrics != nullptr);
    static MetricML metrics[] = {
        METRIC_ML_ACCURACY, METRIC_ML_F1, METRIC_ML_PRECISION, METRIC_ML_RECALL, METRIC_ML_LOSS
    };
    *num_metrics = sizeof(metrics) / sizeof(MetricML);
    return metrics;
}

// usage: linreg
MetricML *gd_metrics_mse(AlgorithmAPI *self, int *num_metrics)
{
    Assert(num_metrics != nullptr);
    static MetricML metrics[] = { METRIC_ML_MSE };
    *num_metrics = sizeof(metrics) / sizeof(MetricML);
    return metrics;
}

// usage: pca
MetricML *gd_metrics_loss(AlgorithmAPI *self, int *num_metrics)
{
    Assert(num_metrics != nullptr);
    static MetricML metrics[] = { METRIC_ML_LOSS };
    *num_metrics = sizeof(metrics) / sizeof(MetricML);
    return metrics;
}

static double gd_get_score(GradientDescentState *gd_state, MetricML metric, bool *isNull)
{
    double score = 0;
    bool hasp, hasr;
    double precision, recall;

    *isNull = false;
    switch (metric) {
        case METRIC_ML_LOSS:
            score = gd_state->loss;
            break;
        case METRIC_ML_ACCURACY:
            score = get_accuracy(&gd_state->scores, &hasr);
            if (!hasr)
                *isNull = true;
            break;
        case METRIC_ML_F1: // 2 * (precision * recall) / (precision + recall)
            precision = get_precision(&gd_state->scores, &hasp);
            recall = get_recall(&gd_state->scores, &hasr);
            if ((hasp && precision > 0) || (hasr && recall > 0))
                score = 2.0 * precision * recall / (precision + recall);
            else
                *isNull = true;
            break;
        case METRIC_ML_PRECISION:
            score = get_precision(&gd_state->scores, &hasp);
            *isNull = !hasp;
            break;
        case METRIC_ML_RECALL:
            score = get_recall(&gd_state->scores, &hasr);
            *isNull = !hasr;
            break;
        case METRIC_ML_MSE:
            score = gd_state->scores.mse;
            break;
        default:
            *isNull = true;
            break;
    }

    return score;
}

void gd_copy_pg_array_data(float8 *dest, Datum const source_datum, int32_t const num_entries)
{
    ArrayType *pg_array = DatumGetArrayTypeP(source_datum);
    Oid array_type = ARR_ELEMTYPE(pg_array);
    /*
     * We expect the input to be an n-element array of float4/float8; verify that. We
     * don't need to use deconstruct_array() since the array data is just
     * going to look like a C array of n double values.
     */
    if (unlikely((ARR_NDIM(pg_array) != 1) || (ARR_DIMS(pg_array)[0] != num_entries) || ARR_HASNULL(pg_array) ||
                 !((array_type == FLOAT4OID) || (array_type == FLOAT8OID))))
        ereport(ERROR, (errmodule(MOD_DB4AI),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Input array must be 1-dimensional of %d elements, must not contain nulls, "
                       "and must be of type float8 or float.", num_entries)));
    
    bool const release_point = PointerGetDatum(pg_array) != source_datum;
    float8 value = 0.;
    if (array_type == FLOAT8OID) {
        int32_t const bytes = num_entries * sizeof(float8);
        int32_t const rc = memcpy_s(dest, bytes, ARR_DATA_PTR(pg_array), bytes);
        securec_check(rc, "\0", "\0");
    } else if (array_type == FLOAT4OID) {
        auto feature_array = reinterpret_cast<float *>(ARR_DATA_PTR(pg_array));
        for (int32_t f = 0; f < num_entries; ++f) {
            value = feature_array[f];
            dest[f] = value;
        }
    }
    // if we end up having a copy we gotta release it as to not leak memory
    if (unlikely(release_point))
        pfree(pg_array);
}

// when adding new algorithms make sure of changing the corresponding enum correctly (positions must match)
const char* optimizer_ml_str[] = {
    [OPTIMIZER_GD]      = "gd",
    [OPTIMIZER_NGD]     = "ngd",
    [INVALID_OPTIMIZER] = "INVALID_OPTIMIZER"
};
const int32_t optimizer_ml_str_size = ARRAY_LENGTH(optimizer_ml_str);
ASSERT_ELEMENTS_ENUM_TO_STR(optimizer_ml_str, INVALID_OPTIMIZER);

const char* optimizer_ml_to_string(OptimizerML optimizer_ml)
{
    return enum_to_string<OptimizerML, optimizer_ml_str, optimizer_ml_str_size>(optimizer_ml);
}


OptimizerML get_optimizer_ml(const char* str)
{
    return string_to_enum<OptimizerML, optimizer_ml_str, optimizer_ml_str_size, true>(str, "Invalid optimizer");
}

const char* gd_optimizer_ml[GD_NUM_OPTIMIZERS] = {"gd", "ngd"};

OptimizerGD *gd_init_optimizer(const GradientDescentState *pstate, HyperparametersGD *hyperp)
{
    OptimizerGD* poptimizer = nullptr;
    switch (hyperp->optimizer) {
        case OPTIMIZER_GD:
            poptimizer = gd_init_optimizer_gd(pstate, hyperp);
            break;
        case OPTIMIZER_NGD:
            poptimizer = gd_init_optimizer_ngd(pstate, hyperp);
            break;
        default:
            ereport(ERROR,
                    (errmodule(MOD_DB4AI),
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Optimizer %d not supported", hyperp->optimizer)));
            break;
    }
    return poptimizer;
}

void gd_multiclass_target(GradientDescent *algorithm, int target, float8 *dest, const float8 *src, int count)
{
    for (int f = 0; f < count; f++, dest++, src++)
        *dest = (*src == target ? algorithm->max_class : algorithm->min_class);
}

ModelHyperparameters *gd_make_hyperparameters(AlgorithmAPI *self)
{
    HyperparametersGD *hyperp = (HyperparametersGD *) palloc0(sizeof(HyperparametersGD));
    return &hyperp->mhp;
}

void gd_update_hyperparameters(AlgorithmAPI *self, ModelHyperparameters *hyperp)
{
    HyperparametersGD *gd_hyperp = (HyperparametersGD *)hyperp;

    // choose a random seed for default (value 0)
    // for sure it is not set to zero again because zero is the starting epoch
    if (gd_hyperp->seed == 0)
        gd_hyperp->seed = time(NULL);
}

TrainModelState* gd_create(AlgorithmAPI *self, const TrainModel *pnode)
{
    if (pnode->configurations != 1)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("multiple hyperparameter configurations for gradient descent not yet supported")));

    HyperparametersGD *hyperp = (HyperparametersGD *) pnode->hyperparameters[0];

    // create state structure
    GradientDescentState *pstate =
        (GradientDescentState *)makeNodeWithSize(TrainModelState, sizeof(GradientDescentState));

    // training state initialization
    pstate->init = false;
    pstate->learning_rate = hyperp->learning_rate;
    pstate->n_iterations = 0;
    pstate->loss = 0;
    pstate->allocated_classes = 0;
    pstate->kernel = nullptr; // default, linear kernel

    // the tupdesc sometimes does not contain enough information, e.g. array size
    // then the remaining part of the initialization is deferred to the
    // first iteration to avoid the execution of the whole subplan during the initialization,
    // for example in EXPLAIN

    return &pstate->tms;
}

static bool transfer_slot(GradientDescentState *pstate, int ith_tuple, Matrix *features, Matrix *dep_var,
                          HyperparametersGD *hyperp)
{
    Assert(ith_tuple < (int)features->rows);

    GradientDescent *gd_algo = (GradientDescent*)pstate->tms.algorithm;
    Assert(!gd_is_supervised(gd_algo) || dep_var != nullptr);
    
    ModelTuple *tuple = &pstate->tms.tuple;
    float8 value = 0;
    int feature = 0;
    int const n_features = features->columns;
    float8 *w;

    if (pstate->kernel == nullptr) {
        // just put the values into the destination vector
        w = features->data + ith_tuple * n_features;
    } else {
        // first put the values into a temporary vector
        w = pstate->aux_input.data;
    }

    for (int i = 0; i < tuple->ncolumns; i++) {
        Oid typid = tuple->typid[i];
        bool isnull = tuple->isnull[i];
        Datum datum = tuple->values[i];
        
        if (i == GD_TARGET_COL && gd_is_supervised(gd_algo)) {
            // ignore row when target column is null for supervised
            if (isnull)
                return false;
            
            if (gd_dep_var_is_binary(gd_algo)) {
                bool byval = tuple->typbyval[i];
                int attlen = tuple->typlen[i];
                
                if (pstate->target_typid == BOOLOID)
                    value = DatumGetBool(datum) ? gd_algo->max_class : gd_algo->min_class;
                else {
                    bool found = false;
                    for (int v = 0; v < pstate->num_classes && !found; v++) {
                        found = DatumImageEq(datum, pstate->value_classes[v], byval, attlen);
                        if (found)
                            value = (v == 1 ? gd_algo->max_class : gd_algo->min_class);
                    }
                    if (!found) {
                        if (pstate->num_classes == 2)
                            ereport(ERROR,
                                    (errmodule(MOD_DB4AI),
                                            errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                                            errmsg("too many target values for binary operator")));
                        
                        value = (pstate->num_classes == 1 ? gd_algo->max_class : gd_algo->min_class);
                        pstate->value_classes[pstate->num_classes++] = datumCopy(datum, byval, attlen);
                    }
                }
            } else {
                if (gd_dep_var_is_continuous(gd_algo)) {
                    // continuous, get just the float value
                    value = datum_get_float8(typid, datum);
                } else {
                    bool byval = tuple->typbyval[i];
                    int attlen = tuple->typlen[i];

                    Oid typoutput;
                    bool typIsVarlena;
                    getTypeOutputInfo(tuple->typid[i], &typoutput, &typIsVarlena);

                    bool found = false;
                    for (int v = 0; v < pstate->num_classes && !found; v++) {
                        if (DatumImageEq(datum, pstate->value_classes[v], byval, attlen)) {
                            value = v;
                            found = true;
                        }
                    }
                    if (!found) {
                        // new category
                        if (pstate->num_classes == pstate->allocated_classes) {
                            pstate->allocated_classes += pstate->allocated_classes / 2; // at least one
                            pstate->value_classes =
                                (Datum *)repalloc(pstate->value_classes, pstate->allocated_classes * sizeof(Datum));
                        }
                        pstate->value_classes[pstate->num_classes] = datumCopy(datum, byval, attlen);
                        value = pstate->num_classes++;
                    }
                }
            }
            
            dep_var->data[ith_tuple] = value;
        } else {
            if (typid == FLOAT8ARRAYOID || typid == FLOAT4ARRAYOID) {
                /*
                 * in here there is a single attribute - which is an array - and all
                 * features are read directly from it
                 *
                 * depending on the current state of the array, we might end up having a copy
                 */
                Assert(feature == 0);
                
                // ignore row when there are no features
                if (isnull)
                    return false;
                
                // expected array length, the intercept/bias does not come with the data
                feature = n_features;
                if (gd_algo->add_bias)
                    feature--;
                
                gd_copy_pg_array_data(w, datum, feature);
                w += feature;
            } else {
                if (isnull)
                    value = gd_algo->def_feature;
                else
                    value = datum_get_float8(typid, datum);
                
                *w++ = value;
                feature++;
            }
        }
    }
    
    if (pstate->kernel != nullptr) {
        // now apply the kernel transformation
        Assert(feature == pstate->aux_input.rows);
        
        // transform using a fake vector
        Matrix tmp;
        matrix_init(&tmp, pstate->kernel->coefficients);
        w = features->data + ith_tuple * n_features;
        
        // make sure the transformation points to the destination vector
        float8 *p = matrix_swap_data(&tmp, w);
        pstate->kernel->transform(pstate->kernel, &pstate->aux_input, &tmp);
        matrix_swap_data(&tmp, p);
        matrix_release(&tmp);

        // update counters and pointers of data
        feature = pstate->kernel->coefficients;
        w += feature;
    }

    if (gd_algo->add_bias) {
        Assert(feature == pstate->n_features - 1);
        *w = 1.0; // bias
        feature++;
    }
    
    Assert(feature == pstate->n_features);
    return true;
}

static void exec_gd_batch(GradientDescentState* pstate, int iter, bool has_snapshot, HyperparametersGD *hyperp)
{
    GradientDescent *gd_algo = (GradientDescent*)pstate->tms.algorithm;

    void *algo_data = nullptr;
    if (gd_algo->start_iteration != nullptr)
        algo_data = gd_algo->start_iteration(pstate, iter);
        
    Matrix* features;
    Matrix* dep_var;
    bool more = true;
    do {
        // prepare next batch
        features = pstate->shuffle->get(pstate->shuffle, &dep_var);

        int ith_tuple = 0;
        if (!has_snapshot) {
            while (more && ith_tuple < hyperp->batch_size) {
                if (pstate->reuse_tuple)
                    pstate->reuse_tuple = false;
                else
                    more = pstate->tms.fetch(pstate->tms.callback_data, &pstate->tms.tuple);

                if (more) {
                    if (transfer_slot(pstate, ith_tuple, features, dep_var, hyperp)) {
                        if (iter == 0)
                            pstate->processed++;

                        ith_tuple++;
                    } else {
                        if (iter == 0)
                            pstate->discarded++;
                    }
                }
            }
        } else {
            Assert(iter > 0);
            if (features != nullptr)
                ith_tuple = features->rows;
            else
                more = false;
        }

        // use the batch to test now in case the shuffle algorithm
        // releases it during unget
        if (iter > 0 && ith_tuple > 0) {
            if (ith_tuple < hyperp->batch_size) {
                matrix_resize(features, ith_tuple, pstate->n_features);
                if (gd_is_supervised(gd_algo))
                    matrix_resize(dep_var, ith_tuple, 1);
            }
            
            double loss = gd_algo->test(pstate, features, dep_var, &pstate->weights, &pstate->scores);
            pstate->loss += loss;
            ereport(DEBUG1,
                    (errmodule(MOD_DB4AI),
                        errmsg("iteration %d loss = %.6f (total %.6g)", iter, loss, pstate->loss)));
            
            if (ith_tuple < hyperp->batch_size) {
                matrix_resize(features, hyperp->batch_size, pstate->n_features);
                if (gd_is_supervised(gd_algo))
                    matrix_resize(dep_var, hyperp->batch_size, 1);
            }
        }
        
        if (ith_tuple > 0 || !has_snapshot) {
            // give back the batch to the shuffle algorithm
            pstate->shuffle->unget(pstate->shuffle, ith_tuple);
        }
    } while (more);

    if (gd_algo->end_iteration != nullptr)
        gd_algo->end_iteration(algo_data);
}

static void exec_gd_start_iteration(GradientDescentState* pstate)
{
    if (pstate->optimizer->start_iteration != nullptr)
        pstate->optimizer->start_iteration(pstate->optimizer);

    if (pstate->shuffle->start_iteration != nullptr)
        pstate->shuffle->start_iteration(pstate->shuffle);
}

static bool exec_gd_end_iteration(GradientDescentState* pstate)
{
    if (pstate->shuffle->end_iteration != nullptr)
        pstate->shuffle->end_iteration(pstate->shuffle);

    if (pstate->optimizer->end_iteration != nullptr)
        pstate->optimizer->end_iteration(pstate->optimizer);
        
    if (pstate->shuffle->has_snapshot != nullptr)
        return pstate->shuffle->has_snapshot(pstate->shuffle);
    
    return false;
}

static bool exec_gd_stop_iterating(GradientDescentState const *pstate, double const prev_loss, double const tolerance)
{
    return (fabs(prev_loss - pstate->loss) < tolerance);
}

static void exec_gd_finalize(GradientDescentState *pstate)
{
    if (pstate->optimizer->finalize != nullptr)
        pstate->optimizer->finalize(pstate->optimizer);
}

static void deferred_init(TrainModelState *pstate, HyperparametersGD *hyperp)
{
    GradientDescentState *gd_state = (GradientDescentState *)pstate;
    Assert(!gd_state->init);

    GradientDescent *gd_algo = (GradientDescent*)pstate->algorithm;

    if (gd_is_supervised(gd_algo)) {
        gd_state->allocated_classes = 2; // minimum for binary
        gd_state->value_classes = (Datum*)palloc(gd_state->allocated_classes * sizeof(Datum));
    }

    // read the first tuple
    if (!pstate->fetch(pstate->callback_data, &pstate->tuple))
        ereport(ERROR,
                (errmodule(MOD_DB4AI),
                    errcode(ERRCODE_NO_DATA_FOUND),
                    errmsg("Input data is empty")));

    gd_state->reuse_tuple = true;

    // compute the number of features and validate the dependent var
    // of supervised algorithms
    gd_state->n_features = 0;
    gd_state->target_typid = gd_algo->def_ret_typid;
    for (int i = 0; i < pstate->tuple.ncolumns; i++) {
        Oid oidtype = pstate->tuple.typid[i];
        if (gd_is_supervised(gd_algo) && i == GD_TARGET_COL) {
            gd_state->target_typid = oidtype;
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
                    gd_state->value_classes[0] = BoolGetDatum(false);
                    gd_state->value_classes[1] = BoolGetDatum(true);
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
        } else {
            if (oidtype == FLOAT8ARRAYOID || oidtype == FLOAT4ARRAYOID) {
                if (gd_state->n_features == 0) {
                    if (pstate->tuple.isnull[i])
                        ereport(ERROR, (errmodule(MOD_DB4AI),
                                errmsg("Input array required")));

                    Datum dt = pstate->tuple.values[i];
                    ArrayType *parray = DatumGetArrayTypeP(dt);
                    if (((ARR_NDIM(parray) != 1) || ARR_HASNULL(parray)))
                        ereport(ERROR, (errmodule(MOD_DB4AI),
                                errmsg("Input array must be 1-dimensional and must not contain nulls")));
                    
                    gd_state->n_features = ARR_DIMS(parray)[0];

                    // if we end up having a copy we gotta release it as to not leak memory
                    if (PointerGetDatum(parray) != dt)
                        pfree(parray);
                } else
                    ereport(ERROR,
                            (errmodule(MOD_DB4AI),
                                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Input array cannot be combined with other features")));
            } else {
                if (oidtype != BOOLOID
                        && oidtype != INT1OID
                        && oidtype != INT2OID
                        && oidtype != INT4OID
                        && oidtype != INT8OID
                        && oidtype != FLOAT4OID
                        && oidtype != FLOAT8OID
                        && oidtype != NUMERICOID)
                    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Oid type %d not yet supported", oidtype)));

                gd_state->n_features++;
            }
        }
    }
    
    if (gd_state->n_features == 0)
        ereport(ERROR,
                (errmodule(MOD_DB4AI),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("At least one feature is required")));

    // now it is possible to prepare the kernel
    if (gd_algo->prepare_kernel != nullptr) {
        gd_state->kernel = gd_algo->prepare_kernel(gd_state->n_features, hyperp);
        if (gd_state->kernel != nullptr) {
            matrix_init(&gd_state->aux_input, gd_state->n_features);
            gd_state->n_features = gd_state->kernel->coefficients;
        }
    }

    // extra bias/intercept column fixed to 1
    if (gd_algo->add_bias)
        gd_state->n_features++;

    // some algorithms require a previous scan over the data
    // it has to be done before the optimizer is initialized
    if (gd_algo->scan != nullptr) {
        Matrix feat_temp, depvar_temp;
        matrix_init(&feat_temp, gd_state->n_features);
        matrix_init(&depvar_temp, gd_state->n_features);

        // just read the rows and call
        bool stop = false;
        do {
            if (gd_state->reuse_tuple)
                gd_state->reuse_tuple = false;
            else {
                if (!pstate->fetch(pstate->callback_data, &pstate->tuple))
                    break;
            }

            if (transfer_slot(gd_state, 0, &feat_temp, &depvar_temp, hyperp))
                stop = gd_algo->scan(&feat_temp, &depvar_temp);
        } while (!stop);
        
        matrix_release(&feat_temp);
        matrix_release(&depvar_temp);
        pstate->rescan(pstate->callback_data);
    }

    // initiate the optimizer
    gd_state->optimizer = gd_algo->prepare_optimizer(gd_state, hyperp);

    // initiate the cache
    gd_state->shuffle = gd_init_shuffle_cache(gd_state, hyperp);
    gd_state->shuffle->optimizer = gd_state->optimizer;

    matrix_init_clone(&gd_state->weights, &gd_state->optimizer->weights);

    gd_state->init = true;
}

void check_gd_state(GradientDescentState *gd_state)
{
    int dt_cnt = gd_state->weights.rows * gd_state->weights.columns;
    for (int i = 0; i < dt_cnt; i++) {
        if (isnan(gd_state->weights.data[i])) {
            ereport(NOTICE, (errmodule(MOD_DB4AI), errmsg("Training warning: weights are out of range, try again with "
                                                          "another hyperparameter configuration")));
            break;
        }
    }
}

/* Training and test are interleaved to avoid a double scan over the data
 * for training and test. Iteration 0 only computes the initial weights, and
 * at each following iteration the model is tested with the current weights
 * and new weights are updated with the gradients. The optimization is clear:
 * for N iterations, the basic algorithm requires N*2 data scans, while the
 * interleaved train&test requires only N+1 data scans. When N=1 the number
 * of scans is the same (N*2 = N+1)
 */

void gd_run(AlgorithmAPI *self, TrainModelState* pstate, Model **models)
{
    Assert(pstate->finished == 0);

    GradientDescent *gd_algo = (GradientDescent*)pstate->algorithm;

    // get information from the node
    GradientDescentState *gd_state = (GradientDescentState *)pstate;
    Assert(pstate->config->configurations == 1);
    HyperparametersGD *hyperp = (HyperparametersGD *) pstate->config->hyperparameters[0];

    // deferred initialization, read and process the first row
    deferred_init(pstate, hyperp);

    // for counting execution time
    uint64_t start, finish;
    uint64_t iter_start, iter_finish;

    // iterations
    double prev_loss = DBL_MAX;
    
    gd_state->processed = 0;
    gd_state->discarded = 0;

    uint64_t max_usecs = ULLONG_MAX;
    if (hyperp->max_seconds > 0)
        max_usecs = hyperp->max_seconds * 1000000ULL;
    
    bool has_snapshot = false;
    bool stop = false;
    start = gd_get_clock_usecs();
    int32_t const current_bytes =
            gd_state->optimizer->weights.rows * gd_state->optimizer->weights.columns * sizeof(float8);
    int32_t const max_bytes = gd_state->weights.allocated * sizeof(float8);
    for (int iter = 0; !stop && iter <= hyperp->max_iterations; iter++) {
        iter_start = gd_get_clock_usecs();
        
        // init loss & scores
        scores_init(&gd_state->scores);
        gd_state->loss = 0;
        
        exec_gd_start_iteration(gd_state);
        exec_gd_batch(gd_state, iter, has_snapshot, hyperp);
        has_snapshot = exec_gd_end_iteration(gd_state);

        if (iter == 0) {
            if (gd_state->processed == 0)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_NO_DATA_FOUND),
                                errmsg("Input data is empty")));

            if (gd_is_supervised(gd_algo) && !gd_dep_var_is_continuous(gd_algo) && gd_state->num_classes < 2)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_NO_DATA_FOUND),
                                errmsg("At least two categories are needed")));
        }

        iter_finish = gd_get_clock_usecs();

        // delta loss < loss tolerance?
        if (iter > 0)
            stop = exec_gd_stop_iterating(gd_state, prev_loss, hyperp->tolerance);

        if (!stop) {
            // continue with another iteration with the new weights
            int rc = memcpy_s(gd_state->weights.data, max_bytes,
                              gd_state->optimizer->weights.data, current_bytes);
            securec_check(rc, "", "");
        
            if (iter > 0)
                gd_state->n_iterations++;

            // timeout || max_iterations
            stop = (gd_get_clock_usecs()-start >= max_usecs)
                    || (iter == hyperp->max_iterations);
        }
            
        if (hyperp->verbose && iter > 0) {
            StringInfoData buf;
            initStringInfo(&buf);

            appendStringInfo(&buf, "test_loss=%.6f delta_loss=", gd_state->loss);

            if (iter > 1)
                appendStringInfo(&buf, "%.16g", fabs(prev_loss - gd_state->loss));
            else
                appendStringInfoChar(&buf, '-');

            bool has;
            double accuracy = get_accuracy(&gd_state->scores, &has);
            appendStringInfo(&buf, " tolerance=%.3f accuracy=%.3f tuples=%d coef=", hyperp->tolerance,
                             has ? accuracy : -1, gd_state->processed);

            matrix_print(&gd_state->weights, &buf, false);
            
            ereport(NOTICE, (errmodule(MOD_DB4AI), errmsg("ITERATION %d: %s", iter, buf.data)));
            pfree(buf.data);
        }
        
        prev_loss = iter > 0 ? gd_state->loss : DBL_MAX;

        if (!stop && !has_snapshot) {
            // for the next iteration
            gd_state->tms.rescan(gd_state->tms.callback_data);
        }
    }
    
    exec_gd_finalize(gd_state);
    
    finish = gd_get_clock_usecs();
    
    gd_state->usecs = finish - start;
    pstate->finished++;

    // return trainined model
    Model *model = models[0];
    MemoryContext oldcxt = MemoryContextSwitchTo(model->memory_context);

    model->return_type = gd_state->target_typid;
    model->exec_time_secs = gd_state->usecs / 1.0e6;
    model->pre_time_secs = 0;
    model->processed_tuples = gd_state->processed;
    model->discarded_tuples = gd_state->discarded;

    model->num_actual_iterations = gd_state->n_iterations;

    // scores
    bool isNull;
    int nmetrics;
    double score;
    MetricML* metrics = self->get_metrics(self, &nmetrics);
    model->scores = nullptr;
    for (int m = 0; m < nmetrics; m++) {
        score = gd_get_score(gd_state, metrics[m], &isNull);
        if (!isNull) {
            TrainingScore *pscore = (TrainingScore *)palloc0(sizeof(TrainingScore));
            pscore->name = metric_ml_to_string(metrics[m]);
            pscore->value = score;
            model->scores = lappend(model->scores, pscore);
        }
    }

    // serialize internal data:
    Datum dt;
    struct varlena *categories = NULL;
    Assert(!gd_state->weights.transposed);
    float8 *pw = gd_state->weights.data;
    int count = gd_state->weights.rows * gd_state->weights.columns;

    model->data.version = DB4AI_MODEL_V01;
    model->data.size = sizeof(GradientDescentModelV01)
                        + sizeof(float8) * count;

    // prepare categories for supervised categorial/binary
    if (gd_is_supervised(gd_algo) && !gd_dep_var_is_continuous(gd_algo)) {
        ArrayBuildState *astate = NULL;
        for (int i = 0; i < gd_state->num_classes; i++)
            astate = accumArrayResult(astate, gd_state->value_classes[i], false, gd_state->target_typid,
                                      CurrentMemoryContext);

        dt = makeArrayResult(astate, CurrentMemoryContext);
        categories = pg_detoast_datum((struct varlena *)DatumGetPointer(dt));
        model->data.size += VARSIZE(categories);
    }

    // does it require extra data?
    int extra_size = 0;
    void *extra_data = nullptr;
    if (gd_algo->get_extra_data != nullptr) {
        extra_data = gd_algo->get_extra_data(gd_state, &extra_size);
        model->data.size += extra_size;
    }

    // serialize data
    GradientDescentModelV01 *mdata = (GradientDescentModelV01*)palloc(model->data.size);
    model->data.raw_data = mdata;

    errno_t rc = memcpy_s(&mdata->hyperparameters, sizeof(HyperparametersGD), hyperp, sizeof(HyperparametersGD));
    securec_check_ss(rc, "\0", "\0");

    mdata->features = gd_state->n_features;
    if (gd_state->kernel != nullptr)
        mdata->input = gd_state->aux_input.rows;
    else {
        mdata->input = mdata->features;
        if (gd_algo->add_bias)
            mdata->input--;
    }
    mdata->dimensions[0] = gd_state->weights.rows;
    mdata->dimensions[1] = gd_state->weights.columns;

    int8_t *ptr = (int8_t *)(mdata + 1);
    int avail = model->data.size - sizeof(GradientDescentModelV01);
    count *= sizeof(float8);
    rc = memcpy_s(ptr, avail, pw, count);
    securec_check_ss(rc, "\0", "\0");

    if (categories != NULL) {
        mdata->categories_size = VARSIZE(categories);

        ptr += count;
        avail -= count;

        rc = memcpy_s(ptr, avail, categories, mdata->categories_size);
        securec_check_ss(rc, "\0", "\0");

        if (categories != (struct varlena *)DatumGetPointer(dt))
            pfree(categories);
    } else
        mdata->categories_size = 0;

    // extra data for prediction
    if (extra_size > 0) {
        Assert(extra_data != nullptr);
        ptr += mdata->categories_size;
        rc = memcpy_s(ptr, avail, extra_data, extra_size);
        securec_check_ss(rc, "\0", "\0");
    }

    // DEPRECATED
    model->weights = 0;
    model->train_info = nullptr;
    model->model_data = 0;

    model->status = ERRCODE_SUCCESSFUL_COMPLETION;
    check_gd_state(gd_state);
    MemoryContextSwitchTo(oldcxt);
}

void gd_end(AlgorithmAPI *self, TrainModelState* pstate)
{
    GradientDescentState *gd_state = (GradientDescentState *)pstate;

    if (gd_state->allocated_classes > 0)
        pfree(gd_state->value_classes);

    // release state
    if (gd_state->init) {
        matrix_release(&gd_state->weights);

        if (gd_state->kernel != nullptr) {
            gd_state->kernel->release(gd_state->kernel);
            pfree(gd_state->kernel);
            matrix_release(&gd_state->aux_input);
        }

        gd_state->shuffle->release(gd_state->shuffle);
        
        matrix_release(&gd_state->optimizer->gradients);
        matrix_release(&gd_state->optimizer->weights);
        gd_state->optimizer->release(gd_state->optimizer);

        gd_state->init = false;
    }
}

void gd_deserialize(GradientDescent *gd, const SerializedModel *model, Oid return_type, SerializedModelGD *gdm)
{
    gdm->algorithm = gd;
    gdm->return_type = return_type;
    
    int avail = model->size;
    if (model->version == DB4AI_MODEL_V01) {
        if (avail < (int)sizeof(GradientDescentModelV01))
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                    errmsg("Model data corrupted reading header")));

        GradientDescentModelV01 *mdata = (GradientDescentModelV01 *)model->raw_data;
        avail -= sizeof(GradientDescentModelV01);

        gdm->input = mdata->input;
        gdm->kernel = nullptr;

        errno_t rc = memcpy_s(&gdm->hyperparameters, sizeof(HyperparametersGD), &mdata->hyperparameters,
                              sizeof(HyperparametersGD));
        securec_check_ss(rc, "\0", "\0");

        matrix_init(&gdm->features, mdata->features);
        matrix_init(&gdm->weights, mdata->dimensions[0], mdata->dimensions[1]);

        // read the coeeficients
        int bc = (gdm->weights.rows * gdm->weights.columns) * sizeof(float8);
        if (avail < bc)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                    errmsg("Model data corrupted reading coefficient")));

        uint8_t *ptr = (uint8_t *)(mdata + 1);
        rc = memcpy_s(gdm->weights.data, gdm->weights.allocated * sizeof(float8), ptr, bc);
        securec_check(rc, "\0", "\0");
        avail -= bc;

        // read the categories
        gdm->ncategories = 0;
        gdm->categories = nullptr;
        if (mdata->categories_size > 0) {
            ArrayType *arr = (ArrayType *)(ptr + bc);
            if (avail < (int)VARSIZE(arr))
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Model data corrupted reading categories")));

            gdm->ncategories = ARR_DIMS(arr)[0];
            gdm->categories = (Datum *)palloc(gdm->ncategories * sizeof(Datum));

            Datum dt;
            bool isnull;
            int cat = 0;
            ArrayIterator it = array_create_iterator(arr, 0);
            while (array_iterate(it, &dt, &isnull)) {
                Assert(!isnull);
                gdm->categories[cat++] = dt;
            }
            array_free_iterator(it);
            avail -= VARSIZE(arr);
        }
        if (avail > 0) {
            // extra data, copy the end
            gdm->extra_data = palloc(avail);
            rc = memcpy_s(gdm->extra_data, avail, (uint8_t*)model->raw_data + model->size - avail, avail);
            securec_check(rc, "\0", "\0");
        } else
            gdm->extra_data = nullptr;
    } else
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                errmsg("Invalid Gradient Descent model version")));
}

List *gd_explain(struct AlgorithmAPI *self, const SerializedModel *model, Oid return_type)
{
    List *infos = NULL;

    // extract serialized model
    SerializedModelGD gds;
    gd_deserialize((GradientDescent*)self, model, return_type, &gds);

    // training metadata
    TrainingInfo *info;

    // weights
    ArrayBuildState *astate = NULL;
    float8 *pw = gds.weights.data;
    info = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
    if (gds.weights.columns == 1) {
        // vector of weights
        int count = gds.weights.rows * gds.weights.columns;
        while (count-- > 0)
            astate = accumArrayResult(astate, Float8GetDatum(*pw++), false, FLOAT8OID, CurrentMemoryContext);
    } else {
        // matrix of weights
        for (int r = 0; r < gds.weights.rows; r++) {
            ArrayBuildState *astate2 = NULL;
            for (int c = 0; c < gds.weights.columns; c++)
                astate2 = accumArrayResult(astate2, Float8GetDatum(*pw++), false, FLOAT8OID, CurrentMemoryContext);

            astate = accumArrayResult(astate, makeArrayResult(astate2, CurrentMemoryContext),
                                    false, FLOAT8ARRAYOID, CurrentMemoryContext);
        }
    }
    info->value = makeArrayResult(astate, CurrentMemoryContext);
    info->type = FLOAT8ARRAYOID;
    info->name = "weights";
    infos = lappend(infos, info);

    // categories
    if (gd_is_supervised(gds.algorithm) && !gd_dep_var_is_continuous(gds.algorithm)) {
        info = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        astate = NULL;
        for (int i = 0; i < gds.ncategories; i++)
            astate =
                accumArrayResult(astate, gds.categories[i], false, gds.return_type, CurrentMemoryContext);

        info->value = makeArrayResult(astate, CurrentMemoryContext);
        info->type =  get_array_type(gds.return_type);
        info->name = "categories";
        infos = lappend(infos, info);
    }

    // release
    matrix_release(&gds.weights);
    matrix_release(&gds.features);
    if (gds.categories != nullptr)
        pfree(gds.categories);

    return infos;
}
