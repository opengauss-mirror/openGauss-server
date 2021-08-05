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
 *  gd.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/gd.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GD_H
#define GD_H

#include "nodes/execnodes.h"
#include "db4ai/predict_by.h"

// returns the current time in microseconds
inline uint64_t
gd_get_clock_usecs() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

enum {
    GD_DEPENDENT_VAR_CONTINUOUS    = 0x00000000,
    GD_DEPENDENT_VAR_BINARY        = 0x00000001,
    GD_DEPENDENT_VAR_CATEGORICAL   = 0x00000002,
};

enum {
    METRIC_ACCURACY             = 0x0001, // (tp + tn) / n
    METRIC_F1                   = 0x0002, // 2 * (precision * recall) / (precision + recall)
    METRIC_PRECISION            = 0x0004, // tp / (tp + fp)
    METRIC_RECALL               = 0x0008, // tp / (tp + fn)
    METRIC_LOSS                 = 0x0010, // defined by each algorithm
    METRIC_MSE                  = 0x0020, // sum((y-y')^2)) / n
};

char* gd_get_metric_name(int metric);

struct GradientDescent;

typedef void (*f_gd_gradients)(const GradientDescent* gd_node, const Matrix* features, const Matrix* dep_var,
                                        Matrix* weights, Matrix* gradients);
typedef double (*f_gd_test)(const GradientDescent* gd_node, const Matrix* features, const Matrix* dep_var,
                                        const Matrix* weights, Scores* scores);
typedef gd_float (*f_gd_predict)(const Matrix* features, const Matrix* weights);

typedef struct GradientDescentAlgorithm {
    const char*         name;
    int                 flags;
    int                 metrics;
    // values for binary algorithms
    // e.g. (0,1) for logistic regression or (-1,1) for svm classifier
    gd_float            min_class;
    gd_float            max_class;
    // callbacks for hooks
    f_gd_gradients      gradients_callback;
    f_gd_test           test_callback;
    f_gd_predict        predict_callback;
} GradientDescentAlgorithm;

GradientDescentAlgorithm* gd_get_algorithm(AlgorithmML algorithm);

inline bool dep_var_is_continuous(const GradientDescentAlgorithm* algo) {
    return ((algo->flags & (GD_DEPENDENT_VAR_BINARY | GD_DEPENDENT_VAR_CATEGORICAL)) == 0);
}

inline bool dep_var_is_binary(const GradientDescentAlgorithm* algo) {
    return ((algo->flags & GD_DEPENDENT_VAR_BINARY) != 0);
}

gd_float gd_datum_get_float(Oid type, Datum datum);
Datum gd_float_get_datum(Oid type, gd_float value);

inline Oid
get_atttypid(GradientDescentState* gd_state, int col) {
    return gd_state->tupdesc->attrs[col]->atttypid;
}

inline bool
get_attbyval(GradientDescentState* gd_state, int col) {
    return gd_state->tupdesc->attrs[col]->attbyval;
}

inline int
get_attlen(GradientDescentState* gd_state, int col) {
    return gd_state->tupdesc->attrs[col]->attlen;
}

inline int
get_atttypmod(GradientDescentState* gd_state, int col) {
    return gd_state->tupdesc->attrs[col]->atttypmod;
}

inline int
get_natts(GradientDescentState* gd_state) {
    return gd_state->tupdesc->natts;
}

struct Model;

ModelPredictor gd_predict_prepare(const Model* model);
Datum gd_predict(ModelPredictor pred, Datum* values, bool* isnull, Oid* types, int values_size);

inline const GradientDescent* gd_get_node(const GradientDescentState* gd_state) {
    return (const GradientDescent*)gd_state->ss.ps.plan;
}

const char* gd_get_expr_name(GradientDescentExprField field);
Datum ExecEvalGradientDescent(GradientDescentExprState* mlstate, ExprContext* econtext,
                            bool* isNull, ExprDoneCond* isDone);
							
////////////////////////////////////////////////////////////////////////
// optimizers

typedef struct OptimizerGD {
    // shared members, initialized by the caller and not by the specialization
    Matrix weights;
    Matrix gradients;
    
    // specialized methods
    void (*start_iteration)(OptimizerGD* optimizer);
    void (*update_batch)(OptimizerGD* optimizer, const Matrix* features, const Matrix* dep_var);
    void (*end_iteration)(OptimizerGD* optimizer);
    void (*release)(OptimizerGD* optimizer);
} OptimizerGD;

OptimizerGD* gd_init_optimizer_gd(const GradientDescentState* gd_state);
OptimizerGD* gd_init_optimizer_ngd(const GradientDescentState* gd_state);

const char* gd_get_optimizer_name(OptimizerML optimizer);

////////////////////////////////////////////////////////////////////////////
// shuffle

typedef struct ShuffleGD {
    OptimizerGD* optimizer;
    void (*start_iteration)(ShuffleGD* shuffle);
    Matrix* (*get)(ShuffleGD* shuffle, Matrix** dep_var);
    void (*unget)(ShuffleGD* shuffle, int tuples);
    void (*end_iteration)(ShuffleGD* shuffle);
    void (*release)(ShuffleGD* shuffle);
} ShuffleGD;

ShuffleGD* gd_init_shuffle_cache(const GradientDescentState* gd_state);


#endif /* GD_H */
