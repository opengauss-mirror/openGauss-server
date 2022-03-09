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
#include "db4ai/matrix.h"
#include "db4ai/predict_by.h"
#include "db4ai_common.h"
#include "db4ai/db4ai_api.h"
#include "db4ai/aifuncs.h"

// returns the current time in microseconds
inline uint64_t
gd_get_clock_usecs() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

struct TrainModel;

///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

// GD node: used for train models using Gradient Descent

struct GradientDescent;
struct OptimizerGD;
struct ShuffleGD;
struct HyperparametersGD;
struct KernelTransformer;

typedef struct GradientDescentState {
    TrainModelState         tms;

    // solvers
    OptimizerGD             *optimizer;
    ShuffleGD               *shuffle;
    KernelTransformer       *kernel;
    Matrix                  aux_input; // only for non-linear kernels

    // tuple information
    bool                    reuse_tuple; // only after initialization
    int                     n_features; // number of features
    Oid                     target_typid;

    // dependent var categories
    int                     num_classes;
    int                     allocated_classes;
    Datum                   *value_classes;

    // training state
    bool                    init;       // training has started, not just for explain
    Matrix                  weights;
    double                  learning_rate;
    int                     n_iterations;
    int                     usecs;          // execution time
    int                     processed;  // tuples
    int                     discarded;
    float                   loss;
    Scores                  scores;
} GradientDescentState;

typedef struct GradientsConfig {
    const HyperparametersGD *hyperp;
    const Matrix*           features;
    Matrix*                 weights;
    Matrix*                 gradients;
} GradientsConfig;

typedef struct GradientsConfigGD {
    GradientsConfig         hdr;
    const Matrix*           dep_var;
} GradientsConfigGD;

typedef struct GradientsConfigPCA {
    GradientsConfig         hdr;
    IncrementalStatistics *eigenvalues_stats = nullptr;
    double batch_error = 0.;
    Matrix *dot_products = nullptr;
} GradientsConfigPCA;

typedef struct GradientDescent {
    AlgorithmAPI        algo;
    bool                add_bias;
    // default values
    Oid                 def_ret_typid;
    float8            def_feature;
    // values for binary algorithms
    // e.g. (0,1) for logistic regression or (-1,1) for svm classifier
    float8            min_class;
    float8            max_class;
    // callbacks
    bool (*scan)(const Matrix* features, const Matrix* dep_var); // return true to abort
    OptimizerGD* (*prepare_optimizer)(const GradientDescentState *gd_state, HyperparametersGD *hyperp);
    KernelTransformer* (*prepare_kernel)(int features, HyperparametersGD *hyperp);
    void* (*start_iteration)(GradientDescentState* gd_state, int iter);
    void (*end_iteration)(void* data);
    void (*compute_gradients)(GradientsConfig *cfg);
    double (*test)(const GradientDescentState* gd_state, const Matrix* features, const Matrix* dep_var,
                    const Matrix* weights, Scores* scores);
    Datum (*predict)(const Matrix* features, const Matrix* weights,
                    Oid return_type, void *extra_data, bool max_binary, bool* categorize);
    void* (*get_extra_data)(GradientDescentState* gd_state, int *size);
} GradientDescent;

#define GD_SVM_CLASSIFICATION_NAME  "svm_classification"
#define GD_LOGISTIC_REGRESSION_NAME "logistic_regression"

extern GradientDescent gd_logistic_regression;
extern GradientDescent gd_svm_classification;
extern GradientDescent gd_linear_regression;
extern GradientDescent gd_pca;
extern GradientDescent gd_multiclass;

GradientDescent* gd_get_algorithm(AlgorithmML algorithm);

inline bool gd_is_supervised(const GradientDescent* gd) {
    return ((gd->algo.flags & ALGORITHM_ML_UNSUPERVISED) == 0);
}

inline bool gd_dep_var_is_continuous(const GradientDescent* gd) {
    return ((gd->algo.flags & ALGORITHM_ML_TARGET_CONTINUOUS) != 0);
}

inline bool gd_dep_var_is_binary(const GradientDescent* gd) {
    return ((gd->algo.flags & (ALGORITHM_ML_TARGET_MULTICLASS | ALGORITHM_ML_TARGET_CONTINUOUS)) == 0);
}

void gd_copy_pg_array_data(float8 *dest, Datum const source, int32_t const num_entries);

void gd_multiclass_target(GradientDescent *algorithm, int target, float8 *dest, const float8 *src, int count);

////////////////////////////////////////////////////////////////////////
// optimizers

// GD optimizers
typedef enum {
    OPTIMIZER_GD = 0,   // simple mini-batch
    OPTIMIZER_NGD,  // normalized gradient descent
    INVALID_OPTIMIZER,
} OptimizerML;

typedef struct OptimizerGD {
    // shared members
    const GradientDescentState *gd_state;
    const HyperparametersGD *hyperp;
    Matrix weights;
    Matrix gradients;
    
    // specialized methods
    void (*start_iteration)(OptimizerGD* optimizer);
    void (*update_batch)(OptimizerGD* optimizer, const Matrix* features, const Matrix* dep_var);
    void (*end_iteration)(OptimizerGD* optimizer);
    void (*release)(OptimizerGD* optimizer);
    void (*finalize)(OptimizerGD *optimizer);
} OptimizerGD;

typedef struct OptimizerOVA {
    OptimizerGD opt;
    OptimizerGD *parent;
    GradientDescent *algorithm;
} OptimizerOVA;

OptimizerGD* gd_init_optimizer_gd(const GradientDescentState* gd_state, HyperparametersGD *hyperp);
OptimizerGD* gd_init_optimizer_ngd(const GradientDescentState* gd_state, HyperparametersGD *hyperp);
OptimizerGD* gd_init_optimizer_pca(const GradientDescentState* gd_state, HyperparametersGD *hyperp);
OptimizerGD* gd_init_optimizer_ova(const GradientDescentState* gd_state, HyperparametersGD *hyperp);
OptimizerGD* gd_init_optimizer(const GradientDescentState *gd_state, HyperparametersGD *hyperp);

const char* gd_get_optimizer_name(OptimizerML optimizer);

const char *optimizer_ml_to_string(OptimizerML optimizer_ml);
OptimizerML get_optimizer_ml(const char *str);

inline const char *optimizer_ml_getter(void *optimizer_ml)
{
    return optimizer_ml_to_string(*static_cast<OptimizerML *>(optimizer_ml));
}

inline void optimizer_ml_setter(const char *str, void *optimizer_ml)
{
    *static_cast<OptimizerML *>(optimizer_ml) = get_optimizer_ml(str);
}

////////////////////////////////////////////////////////////////////////////
// shuffle

typedef struct ShuffleGD {
    OptimizerGD* optimizer;
    bool supervised;
    int num_batches;
    void (*start_iteration)(ShuffleGD* shuffle);
    Matrix* (*get)(ShuffleGD* shuffle, Matrix** dep_var);
    void (*unget)(ShuffleGD* shuffle, int tuples);
    void (*end_iteration)(ShuffleGD* shuffle);
    void (*release)(ShuffleGD* shuffle);
    bool (*has_snapshot)(ShuffleGD* shuffle);
} ShuffleGD;

ShuffleGD* gd_init_shuffle_cache(const GradientDescentState* gd_state, HyperparametersGD *hyperp);

//////////////////////////////////////////////////////////////////////////

typedef enum {
    SVM_KERNEL_LINEAR,      // default
    SVM_KERNEL_GAUSSIAN,
    SVM_KERNEL_POLYNOMIAL,
    SVM_NUM_KERNELS
} KernelSVM;

typedef struct HyperparametersGD {
    ModelHyperparameters mhp;

    // generic hyperparameters
    OptimizerML optimizer;      // default GD/mini-batch
    int         max_seconds;    // 0 to disable
    bool        verbose;
    int         max_iterations; // maximum number of iterations
    int         batch_size;
    double      learning_rate;
    double      decay;          // (0:1], learning rate decay
    double      tolerance;      // [0:1], 0 means to run all iterations
    int         seed;           // [0:N], random seed

    // for SVM
    double      lambda;         // regularization strength
    KernelSVM   kernel;         // default linear (0)
    double      gamma;          // for gaussian kernel
    int         degree;         // [2:9] for polynomial kernel
    double      coef0;          // [0:N] for polynomial kernel
    int         components;     // for gaussian and polynomial kernels, default 0 => Max(128, 2 * num_features)

    // for PCA (for now)
    int         number_dimensions; // dimension of the sub-space to be computed

    // for multiclass
    AlgorithmML classifier;
} HyperparametersGD;

#define GD_HYPERPARAMETERS_SUPERVISED \
    HYPERPARAMETER_INT4("batch_size", 1000, 1, true, MAX_BATCH_SIZE, true, \
                        HyperparametersGD, batch_size, \
                        HP_AUTOML_INT(1, 10000, 4, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_FLOAT8("decay", 0.95, 0.0, false, DBL_MAX, true, \
                        HyperparametersGD, decay, \
                        HP_AUTOML_FLOAT(1E-6, 1E3, 9, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_FLOAT8("learning_rate", 0.8, 0.0, false, DBL_MAX, true, \
                        HyperparametersGD, learning_rate, \
                        HP_AUTOML_FLOAT(1E-6, 1E3, 9, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_INT4("max_iterations", 100, 1, true, ITER_MAX, true, \
                        HyperparametersGD, max_iterations, \
                        HP_AUTOML_INT(1, 100, 10, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_INT4("max_seconds", 0, 0, true, INT32_MAX, true, \
                        HyperparametersGD, max_seconds, \
                        HP_NO_AUTOML()), \
    HYPERPARAMETER_ENUM("optimizer", "gd", gd_optimizer_ml, GD_NUM_OPTIMIZERS, \
                        optimizer_ml_getter, optimizer_ml_setter, \
                        HyperparametersGD, optimizer, \
                        HP_AUTOML_ENUM()), \
    HYPERPARAMETER_FLOAT8("tolerance", 0.0005, 0.0, false, DBL_MAX, true, \
                        HyperparametersGD, tolerance, \
                        HP_AUTOML_FLOAT(1E-6, 1E3, 9, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_INT4("seed", 0, 0, true, INT32_MAX, true, \
                        HyperparametersGD, seed, \
                        HP_AUTOML_INT(1, INT32_MAX, 1, ProbabilityDistribution::UNIFORM_RANGE)), \
    HYPERPARAMETER_BOOL("verbose", false, \
                        HyperparametersGD, verbose, \
                        HP_NO_AUTOML()),

extern const char* svm_kernel_str[SVM_NUM_KERNELS];
const char *svm_kernel_getter(void *kernel);
void svm_kernel_setter(const char *str, void *kernel);

#define GD_HYPERPARAMETERS_SVM_CLASSIFICATION \
    GD_HYPERPARAMETERS_SUPERVISED \
    HYPERPARAMETER_FLOAT8("lambda", 0.01, 0.0, false, DBL_MAX, true, \
                        HyperparametersGD, lambda, \
                        HP_AUTOML_FLOAT(1E-6, 1E3, 9, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_ENUM("kernel", "linear", \
                        svm_kernel_str, SVM_NUM_KERNELS, \
                        svm_kernel_getter, svm_kernel_setter, \
                        HyperparametersGD, kernel, \
                        HP_AUTOML_ENUM()), \
    HYPERPARAMETER_INT4("components", 0, 0, true, INT32_MAX, true, \
                        HyperparametersGD, components, \
                        HP_AUTOML_INT(0, INT32_MAX, 1, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_FLOAT8("gamma", 0.5, 0.0, false, DBL_MAX, true, \
                        HyperparametersGD, gamma, \
                        HP_AUTOML_FLOAT(1E-3, 1E3, 9, ProbabilityDistribution::LOG_RANGE)), \
    HYPERPARAMETER_INT4("degree", 2, 2, true, 9, true, \
                        HyperparametersGD, degree, \
                        HP_AUTOML_INT(2, 9, 1, ProbabilityDistribution::UNIFORM_RANGE)), \
    HYPERPARAMETER_FLOAT8("coef0", 1.0, 0.0, true, DBL_MAX, true, \
                        HyperparametersGD, coef0, \
                        HP_AUTOML_FLOAT(1E-2, 1E1, 9, ProbabilityDistribution::LOG_RANGE)),

//////////////////////////////////////////////////////////////////////////
// serialization

typedef struct GradientDescentModelV01 {
    HyperparametersGD hyperparameters;
    int input; // number of input columns
    int features; // may be different from columns due to bias, kernel, ...
    int dimensions[2]; // weights
    int categories_size;
    // the following come in binary format
    //  - float8 weights[rows * columns]
    //  - uint8_t categories[categories_size]
    //  - uint8_t extra_data[] -- for prediction
} GradientDescentModelV01;

//////////////////////////////////////////////////////////////////////////
// shared by multiple algorithms

#define GD_NUM_OPTIMIZERS   2
extern const char* gd_optimizer_ml[GD_NUM_OPTIMIZERS];

TrainModelState* gd_create(AlgorithmAPI *self, const TrainModel *pnode);
void gd_run(AlgorithmAPI *self, TrainModelState* pstate, Model **models);
void gd_end(AlgorithmAPI *self, TrainModelState* pstate);

ModelHyperparameters* gd_make_hyperparameters(AlgorithmAPI *self);
void gd_update_hyperparameters(AlgorithmAPI *self, ModelHyperparameters* hyperp);

const HyperparameterDefinition* gd_get_hyperparameters_regression(AlgorithmAPI *self, int *definitions_size);

MetricML *gd_metrics_accuracy(AlgorithmAPI *self, int *num_metrics);
MetricML *gd_metrics_mse(AlgorithmAPI *self, int *num_metrics);
MetricML *gd_metrics_loss(AlgorithmAPI *self, int *num_metrics);

ModelPredictor gd_predict_prepare(AlgorithmAPI *self, const SerializedModel* model, Oid return_type);
Datum gd_predict(AlgorithmAPI *self, ModelPredictor pred, Datum* values, bool* isnull, Oid* types, int values_size);

typedef struct SerializedModelGD {
    GradientDescent *algorithm;
    HyperparametersGD hyperparameters;
    int ncategories;
    int input; // number of input columns, may be different from number of features
    Oid return_type;
    Matrix weights;
    Matrix features;
    Datum *categories;
    void *extra_data;
    KernelTransformer *kernel;
    Matrix aux_input; // for kernels
} SerializedModelGD;

void gd_deserialize(GradientDescent *gd, const SerializedModel *model, Oid return_type, SerializedModelGD *gdm);

List* gd_explain(struct AlgorithmAPI *self, const SerializedModel *model, Oid return_type);

#endif /* GD_H */
