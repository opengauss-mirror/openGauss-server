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
 * xgboost.cpp
 *        Main file implemented for the xgboost algorithm
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/xgboost/xgboost.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "postgres_ext.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include <dlfcn.h>

#include "db4ai/xgboost.h"
#include "db4ai/aifuncs.h"
#include "db4ai/model_warehouse.h"
#include "db4ai/predict_by.h"
#include "db4ai/db4ai_common.h"

#include "xgboost/c_api.h"

double total_exec_time = 0.0;
struct timespec exec_start_time, exec_end_time;

#define XGBOOST_LIB_NAME "libxgboost.so"

typedef const int (*XGBoosterSetParam_Sym)(BoosterHandle handle, const char *name, const char *value);
typedef const int (*XGDMatrixCreateFromMat_Sym)(const float *data, bst_ulong nrow, bst_ulong ncol,
                   float missing, DMatrixHandle *out);
typedef const int (*XGDMatrixSetFloatInfo_Sym)(DMatrixHandle handle, const char *field, const float *array, bst_ulong len);
typedef const int (*XGBoosterCreate_Sym)(const DMatrixHandle dmats[], bst_ulong len, BoosterHandle *out);
typedef const int (*XGBoosterUnserializeFromBuffer_Sym)(BoosterHandle handle, const void *buf, bst_ulong len);
typedef const int (*XGBoosterUpdateOneIter_Sym)(BoosterHandle handle, int iter, DMatrixHandle dtrain);
typedef const int (*XGBoosterEvalOneIter_Sym)(BoosterHandle handle, int iter, DMatrixHandle dmats[], 
                   const char *evnames[], bst_ulong len, const char **out_result);
typedef const int (*XGBoosterSerializeToBuffer_Sym)(BoosterHandle handle, bst_ulong *out_len, const char **out_dptr);
typedef const int (*XGDMatrixFree_Sym)(DMatrixHandle handle);
typedef const int (*XGBoosterFree_Sym)(BoosterHandle handle);
typedef const int (*XGBoosterPredict_Sym)(BoosterHandle handle, DMatrixHandle dmat, int option_mask, unsigned ntree_limit,
                   int training, bst_ulong *out_len, const float **out_result);
typedef const char* (*XGBGetLastError_Sym)(void);

typedef struct {
    XGBoosterSetParam_Sym XGBoosterSetParam;
    XGDMatrixCreateFromMat_Sym XGDMatrixCreateFromMat;
    XGDMatrixSetFloatInfo_Sym XGDMatrixSetFloatInfo;
    XGBoosterCreate_Sym XGBoosterCreate;
    XGBoosterUnserializeFromBuffer_Sym XGBoosterUnserializeFromBuffer;
    XGBoosterUpdateOneIter_Sym XGBoosterUpdateOneIter;
    XGBoosterEvalOneIter_Sym XGBoosterEvalOneIter;
    XGBoosterSerializeToBuffer_Sym XGBoosterSerializeToBuffer;
    XGDMatrixFree_Sym XGDMatrixFree;
    XGBoosterFree_Sym XGBoosterFree;
    XGBoosterPredict_Sym XGBoosterPredict;
    XGBGetLastError_Sym XGBGetLastError;
} xgboostApi;  //xgboost API function

static void *g_xgboost_handle = NULL; //dynamic library handler
static xgboostApi *g_xgboostApi = NULL; // function symbols of xgboost library
static MemoryContext g_xgboostMcxt = NULL;

#define safe_xgboost(call) { \
    int err = (call);        \
    if (err != 0) {          \
        char *str = const_cast<char*>(g_xgboostApi->XGBGetLastError());    \
        char *res = strchr(str, '\n');       \
        *res = '\0';                        \
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                errmsg("%s", strrchr(str, ':') + 1))); \
        }                                                                                                         \
    }

double parseDoubleFromErrMetric(const char *str)
{
    // skip all the way to `:'
    while (*str != ':')
        ++str;
    // skip `:' and parse the number
    return strtod(++str, NULL);
}

struct xg_data_t {
    float* labels{nullptr};
    float* features{nullptr};
    char* raw_model{nullptr};
    int ft_rows{0};
    int ft_cols{0};
    int lb_rows{0};
    double validation_score{0};
    uint64_t raw_model_size{0};

    inline void set_raw_model(char *raw_model_, uint64_t len_)
    {
        if (raw_model_ == 0 || len_ <= 0)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("Xgboost failed loading raw model.")));

        /* check if we need to release the old buffer */
        if (raw_model)
            pfree((void *)raw_model);

        raw_model_size = len_;
        raw_model = (char *)palloc0(sizeof(char) * len_);
        errno_t rc = memcpy_s(raw_model, len_, raw_model_, len_);
        securec_check(rc, "\0", "\0");
    }
};

typedef struct XgboostModelV01 {
    int ft_cols;
} XgboostModelV01;

/*
 * Auxiliary method for printing tuples, useful for debugging
 */
void printXGData(const xg_data_t &chunk, const int n_tuples)
{
    StringInfoData buf;
    initStringInfo(&buf);

    for (int i = 0; i < n_tuples; ++i) {
        appendStringInfo(&buf, "%4.2f\t", chunk.labels[i]);
        for (int j = 0; j < chunk.ft_cols; ++j) {
            appendStringInfo(&buf, "%4.2f", *(chunk.features + i * chunk.ft_cols + j));
            appendStringInfoChar(&buf, '\t');
        }
        appendStringInfoChar(&buf, '\n');
        elog(NOTICE, "%s", buf.data);
        resetStringInfo(&buf);
    }
    pfree(buf.data);
}

typedef struct HyperparamsXGBoost {
    ModelHyperparameters mhp; /* place-holder */
    /* hyperparameters */
    uint32_t     n_iterations;
    uint32_t     batch_size;
    uint32_t     max_depth;
    uint32_t     min_child_weight;
    uint32_t     nthread;
    uint32_t     seed;
    uint32_t     verbosity;
    double      eta;
    double      gamma;
    const char* booster{nullptr};
    const char* tree_method{nullptr};
    const char* eval_metric{nullptr};
} HyperparamsXGBoost;

/*
 * XGBoost state
 */
typedef struct XGBoostState {
    TrainModelState tms;
    // tuple description
    Oid *oids;
    bool done = false;
    uint32_t processed_tuples = 0U;
    double execution_time = 0.;
} XGBoostState;

typedef struct SerializedModelXgboost {
    int ft_cols;
    SerializedModel *model = nullptr;
    BoosterHandle booster = nullptr;
}SerializedModelXgboost;

extern XGBoost xg_reg_logistic;
extern XGBoost xg_bin_logistic;
extern XGBoost xg_reg_sqe;
extern XGBoost xg_reg_gamma;

XGBoost *xgboost_get_algorithm(AlgorithmML algorithm)
{
    XGBoost *xgboost_algorithm = nullptr;
    switch (algorithm) {
        case XG_REG_LOGISTIC:
            xgboost_algorithm = &xg_reg_logistic;
            break;
        case XG_BIN_LOGISTIC:
            xgboost_algorithm = &xg_bin_logistic;
            break;
        case XG_REG_SQE:
            xgboost_algorithm = &xg_reg_sqe;
            break;
        case XG_REG_GAMMA:
            xgboost_algorithm = &xg_reg_gamma;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid algorithm %d", algorithm)));
            break;
    }
    return xgboost_algorithm;
}

// Hyperparameter and algorithm definitions
MetricML *xgboost_metrics_accuracy(AlgorithmAPI *self, int *num_metrics)
{
    Assert(num_metrics != nullptr);
    static MetricML metrics[] = {
        METRIC_ML_AUC, METRIC_ML_AUC_PR, METRIC_ML_MAP,  METRIC_ML_RMSE, METRIC_ML_RMSLE, METRIC_ML_MAE
    };
    *num_metrics = sizeof(metrics) / sizeof(MetricML);
    return metrics;
}

#define BOOST_GBLINEAR_IDX 1
const char *xgboost_boost_str[]       = {"gbtree", "gblinear", "dart"};
const char *xgboost_tree_method_str[] = {"auto", "exact", "approx", "hist", "gpu_hist"};
const char *xgboost_eval_metric_str[] = {"rmse", "rmsle", "map", "mae", "auc", "aucpr" };
static HyperparameterDefinition xgboost_hyperparameter_definitions[] = {
    HYPERPARAMETER_INT4("n_iter", 10, 1, true, ITER_MAX, true, HyperparamsXGBoost, n_iterations, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("batch_size", 10000, 1, true, MAX_BATCH_SIZE, true, HyperparamsXGBoost, batch_size,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("max_depth", 5, 0, true, INT32_MAX, true, HyperparamsXGBoost, max_depth, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("min_child_weight", 1, 0, true, INT32_MAX, true, HyperparamsXGBoost, min_child_weight,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_FLOAT8("gamma", 0.0, 0.0, true, DBL_MAX, true, HyperparamsXGBoost, gamma, HP_NO_AUTOML()),
    HYPERPARAMETER_FLOAT8("eta", 0.3, 0.0, true, 1, true, HyperparamsXGBoost, eta, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("nthread", 1, 0, true, 100, true, HyperparamsXGBoost, nthread, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("verbosity", 1, 0, true, 3, true, HyperparamsXGBoost, verbosity, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("seed", 0, 0, true, INT32_MAX, true, HyperparamsXGBoost, seed,
                        HP_AUTOML_INT(1, INT32_MAX, 1, ProbabilityDistribution::UNIFORM_RANGE)),
    HYPERPARAMETER_STRING("booster", "gbtree", xgboost_boost_str, ARRAY_LENGTH(xgboost_boost_str), HyperparamsXGBoost,
                          booster, HP_NO_AUTOML()),
    HYPERPARAMETER_STRING("tree_method", "auto", xgboost_tree_method_str, ARRAY_LENGTH(xgboost_tree_method_str),
                          HyperparamsXGBoost, tree_method, HP_NO_AUTOML()),
    HYPERPARAMETER_STRING("eval_metric", "rmse", xgboost_eval_metric_str, ARRAY_LENGTH(xgboost_eval_metric_str),
                          HyperparamsXGBoost, eval_metric, HP_NO_AUTOML()),
};

static const HyperparameterDefinition* xgboost_get_hyperparameters(AlgorithmAPI *self, int *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(xgboost_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return xgboost_hyperparameter_definitions;
}

static ModelHyperparameters *xgboost_make_hyperparameters(AlgorithmAPI *self)
{
    auto xgboost_hyperp = reinterpret_cast<HyperparamsXGBoost *>(palloc0(sizeof(HyperparamsXGBoost)));

    return &xgboost_hyperp->mhp;
}

void load_xgboost_library(void)
{
    if (g_xgboost_handle != NULL) {
        return; // have load it
    }

    // get the library path from GAUSSHOME/lib directory
    char* gausshome = getGaussHome();
    StringInfo libPath = makeStringInfo();
    appendStringInfo(libPath, "%s/lib/%s", gausshome, XGBOOST_LIB_NAME);

    (void)LWLockAcquire(XGBoostLibLock, LW_EXCLUSIVE);
    if (g_xgboost_handle != NULL) { //check again to avoid double dlopen
        LWLockRelease(XGBoostLibLock);
        return; // have load it
    }

    g_xgboost_handle = dlopen(libPath->data, RTLD_NOW | RTLD_GLOBAL);
    if (g_xgboost_handle == NULL) {
        LWLockRelease(XGBoostLibLock);
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED),
                    errmsg("Call dlopen to load library file %s failed. error: %s", libPath->data, dlerror())));
    }

    g_xgboostMcxt = AllocSetContextCreate(g_instance.instance_context,
                "xgboostApiMemoryContext",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE,
                SHARED_CONTEXT);
    g_xgboostApi = (xgboostApi *)MemoryContextAlloc(g_xgboostMcxt, sizeof(xgboostApi));

    g_xgboostApi->XGBoosterSetParam = (XGBoosterSetParam_Sym)dlsym(g_xgboost_handle, "XGBoosterSetParam");
    g_xgboostApi->XGDMatrixCreateFromMat = (XGDMatrixCreateFromMat_Sym)dlsym(g_xgboost_handle, "XGDMatrixCreateFromMat");
    g_xgboostApi->XGDMatrixSetFloatInfo = (XGDMatrixSetFloatInfo_Sym)dlsym(g_xgboost_handle, "XGDMatrixSetFloatInfo");
    g_xgboostApi->XGBoosterCreate = (XGBoosterCreate_Sym)dlsym(g_xgboost_handle, "XGBoosterCreate");
    g_xgboostApi->XGBoosterUnserializeFromBuffer = (XGBoosterUnserializeFromBuffer_Sym)dlsym(g_xgboost_handle, "XGBoosterUnserializeFromBuffer");
    g_xgboostApi->XGBoosterUpdateOneIter = (XGBoosterUpdateOneIter_Sym)dlsym(g_xgboost_handle, "XGBoosterUpdateOneIter");
    g_xgboostApi->XGBoosterEvalOneIter = (XGBoosterEvalOneIter_Sym)dlsym(g_xgboost_handle, "XGBoosterEvalOneIter");
    g_xgboostApi->XGBoosterSerializeToBuffer = (XGBoosterSerializeToBuffer_Sym)dlsym(g_xgboost_handle, "XGBoosterSerializeToBuffer");
    g_xgboostApi->XGDMatrixFree = (XGDMatrixFree_Sym)dlsym(g_xgboost_handle, "XGDMatrixFree");
    g_xgboostApi->XGBoosterFree = (XGBoosterFree_Sym)dlsym(g_xgboost_handle, "XGBoosterFree");
    g_xgboostApi->XGBoosterPredict = (XGBoosterPredict_Sym)dlsym(g_xgboost_handle, "XGBoosterPredict");
    g_xgboostApi->XGBGetLastError = (XGBGetLastError_Sym)dlsym(g_xgboost_handle, "XGBGetLastError");

    if (g_xgboostApi->XGBoosterSetParam == NULL || g_xgboostApi->XGDMatrixCreateFromMat == NULL ||
        g_xgboostApi->XGDMatrixSetFloatInfo == NULL || g_xgboostApi->XGBoosterCreate == NULL ||
        g_xgboostApi->XGBoosterUnserializeFromBuffer == NULL || g_xgboostApi->XGBoosterUpdateOneIter == NULL ||
        g_xgboostApi->XGBoosterEvalOneIter == NULL || g_xgboostApi->XGBoosterSerializeToBuffer == NULL ||
        g_xgboostApi->XGDMatrixFree == NULL || g_xgboostApi->XGBoosterFree == NULL ||
        g_xgboostApi->XGBoosterPredict == NULL || g_xgboostApi->XGBGetLastError == NULL) {
        (void)dlclose(g_xgboost_handle);
        g_xgboost_handle = NULL;
        LWLockRelease(XGBoostLibLock);
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED),
                    errmsg("Call dlsym to the symbol of load library file %s failed. error: %s", libPath->data, dlerror())));
    }

    LWLockRelease(XGBoostLibLock);

    /* clear any existing error */
    (void)dlerror();

    pfree(libPath->data); 
}

/*
 * This method is reposnsible for setting hyperparams for the XGBoost algorithm.
 * For hyperparams which are not set, XGBoost takes default values.
 */
void set_hyperparams(AlgorithmAPI *alg, const HyperparamsXGBoost *xg_hyp, void *booster)
{
    StringInfoData buf;
    initStringInfo(&buf);

    switch (alg->algorithm) {
        case XG_BIN_LOGISTIC:
            safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "objective", "binary:logistic"));
            break;
        case XG_REG_LOGISTIC:
            safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "objective", "reg:logistic"));
            break;
        case XG_REG_SQE:
            safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "objective", "reg:squarederror"));
            break;
        case XG_REG_GAMMA:
            safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "objective", "reg:gamma"));
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Unknown XG Algorithm %d!", alg->algorithm)));
            break;
    }

    if (xg_hyp->booster) {
        safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "booster", xg_hyp->booster));
    }
    if (xg_hyp->tree_method) {
        safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "tree_method", xg_hyp->tree_method));
    }
    if (strcmp(xg_hyp->eval_metric, "") != 0) {
        safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "eval_metric", xg_hyp->eval_metric));
    }
    if (xg_hyp->seed != 0) {
        appendStringInfo(&buf, "%d", xg_hyp->seed);
        safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "seed", buf.data));
        resetStringInfo(&buf);
    }
    if (xg_hyp->verbosity != 1) {
        appendStringInfo(&buf, "%d", xg_hyp->verbosity);
        safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "verbosity", buf.data));
        resetStringInfo(&buf);
    }

    appendStringInfo(&buf, "%d", xg_hyp->nthread);
    safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "nthread", buf.data));
    resetStringInfo(&buf);

    appendStringInfo(&buf, "%d", xg_hyp->max_depth);
    safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "max_depth", buf.data));
    resetStringInfo(&buf);

    appendStringInfo(&buf, "%f", xg_hyp->gamma);
    safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "gamma", buf.data));
    resetStringInfo(&buf);

    appendStringInfo(&buf, "%f", xg_hyp->eta);
    safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "eta", buf.data));
    resetStringInfo(&buf);

    appendStringInfo(&buf, "%d", xg_hyp->min_child_weight);
    safe_xgboost(g_xgboostApi->XGBoosterSetParam(booster, "min_child_wight", buf.data));
    resetStringInfo(&buf);

    pfree(buf.data);
}

template <bool alloc = false>
void setup_xg_chunk(xg_data_t &xg_data)
{
    check_hyper_bounds(sizeof(float), xg_data.lb_rows, "batch_size");

    /* allocate the labels array */
    uint lb_size = sizeof(float) * xg_data.lb_rows;
    if (alloc) {
        xg_data.labels = (float*)palloc0(lb_size);
    } else {
        /* reset labels for the next iteration */
        errno_t rc = memset_s(xg_data.labels, lb_size, 0, lb_size);
        securec_check(rc, "\0", "\0");
    }

    check_hyper_bounds(xg_data.ft_rows, xg_data.ft_cols, "batch_size");
    check_hyper_bounds(sizeof(float), xg_data.ft_rows * xg_data.ft_cols, "batch_size");

    uint ft_size = sizeof(float) * xg_data.ft_rows * xg_data.ft_cols;
    if (alloc) {
        xg_data.features = (float*)palloc0(ft_size);
    } else {
        /* reset features for next iteration */
        errno_t rc = memset_s(xg_data.features, ft_size, 0, ft_size);
        securec_check(rc, "\0", "\0");
    }
}

/*
 * this function initializes the algorithm
 */
static TrainModelState *xgboost_create(AlgorithmAPI *self, const TrainModel *pnode)
{
    if (pnode->configurations != 1)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("multiple hyper-parameter configurations for xgboost are not yet supported")));

    auto xg_state = reinterpret_cast<XGBoostState *>(makeNodeWithSize(TrainModelState, sizeof(XGBoostState)));
    xg_state->done = false;
    return &xg_state->tms;
}

/* ----------------------------------------------------------------
 * XBoost train operation.
 *
 * Node that the allocated chunk size may differ from the number of
 * actual tuples, i.e., number of tuples can be less in the last
 * chunk.
 * ----------------------------------------------------------------
 */
void trainXG(AlgorithmAPI *alg, const HyperparamsXGBoost *xg_hyp, xg_data_t *chunk, const int n_tuples,
             bool first_call = true)
{
    /* XGoost DMatrix handle */
    DMatrixHandle dtrain, dtest;

    /* load DTrain matrix */
    safe_xgboost(g_xgboostApi->XGDMatrixCreateFromMat((float *)chunk->features,  // input data
                                        n_tuples,                  // # rows
                                        chunk->ft_cols,            // # columns in the input
                                        -1,                        // filler for missing values
                                        &dtrain));                 // handle of the DMatrix

    /* load DTest matrix */
    safe_xgboost(g_xgboostApi->XGDMatrixCreateFromMat((float *)chunk->features, n_tuples, chunk->ft_cols, -1, &dtest));

    /* load the labels */
    safe_xgboost(g_xgboostApi->XGDMatrixSetFloatInfo(dtrain, "label", chunk->labels, n_tuples));
    safe_xgboost(g_xgboostApi->XGDMatrixSetFloatInfo(dtest, "label", chunk->labels, n_tuples));

    DMatrixHandle eval_dmats[2] = {dtrain, dtest};

    /* create the booster and load the desired parameters */
    BoosterHandle booster;
    safe_xgboost(g_xgboostApi->XGBoosterCreate(eval_dmats, 2, &booster));

    if (!first_call) {
        safe_xgboost(g_xgboostApi->XGBoosterUnserializeFromBuffer(booster, chunk->raw_model, chunk->raw_model_size));
    } else {
        set_hyperparams(alg, xg_hyp, booster);
    }

    /* evaluation structures */
    const char* eval_names[2] = {"train", "test"};
    const char* eval_result   = nullptr;

    for (uint32_t iter = 0; iter < xg_hyp->n_iterations; ++iter) {
        safe_xgboost(g_xgboostApi->XGBoosterUpdateOneIter(booster, iter, dtrain));
        safe_xgboost(g_xgboostApi->XGBoosterEvalOneIter(booster, iter, eval_dmats, eval_names, 2, &eval_result));
    }

    /* get evaluation results */
    chunk->validation_score = parseDoubleFromErrMetric(eval_result);
    uint64_t raw_model_len;
    char *raw_model;
    safe_xgboost(g_xgboostApi->XGBoosterSerializeToBuffer(booster, &raw_model_len, (const char **)&raw_model));
    chunk->set_raw_model(raw_model, raw_model_len);
    /* free xgboost structures */
    safe_xgboost(g_xgboostApi->XGDMatrixFree(dtrain));
    safe_xgboost(g_xgboostApi->XGDMatrixFree(dtest));
    safe_xgboost(g_xgboostApi->XGBoosterFree(booster));
}

static void check_data_cnt(uint32_t tuple_count, uint32_t pos_cnt, HyperparamsXGBoost *xg_hyperp)
{
    // only have positive numbers or negative numbers
    if ((strcmp(xg_hyperp->eval_metric, "auc") == 0 || strcmp(xg_hyperp->eval_metric, "aucpr") == 0) &&
        (pos_cnt == 0 || pos_cnt == tuple_count)) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("The dataset only contains pos or neg samples for auc or aucpr")));
    }
}

void xgboost_serialize(SerializedModel *data, xg_data_t *chunk_ptr)
{
    data->size = chunk_ptr->raw_model_size + sizeof(XgboostModelV01);
    XgboostModelV01 *mdata = (XgboostModelV01 *)palloc0(data->size);

    data->raw_data = mdata;

    mdata->ft_cols = chunk_ptr->ft_cols;
    int8_t *ptr = (int8_t *)(mdata + 1); 
    int avail = data->size - sizeof(XgboostModelV01);

    int rc = memcpy_s(ptr, avail, chunk_ptr->raw_model, chunk_ptr->raw_model_size);
    securec_check_ss(rc, "\0", "\0");
} 
static void xgboost_run(AlgorithmAPI *self, TrainModelState *pstate, Model **models)
{
    Assert(pstate->finished == 0);

    clock_gettime(CLOCK_MONOTONIC, &exec_start_time);
    auto xg_state  = reinterpret_cast<XGBoostState*>(pstate);
    auto xg_hyperp = const_cast<HyperparamsXGBoost *>(
                    reinterpret_cast<HyperparamsXGBoost const *>(pstate->config->hyperparameters[0]));

    ModelTuple const *outer_tuple_slot = nullptr;

    load_xgboost_library();

    // data holder for in-between (chunk-wise) invocation of XGBoost training algorithm
    xg_data_t chunk;
    chunk.lb_rows = chunk.ft_rows = xg_hyperp->batch_size;
    chunk.ft_cols = pstate->tuple.ncolumns - 1; // minus 1 for labels!
    setup_xg_chunk<true>(chunk);
    
    uint32_t pos_cnt = 0; // number of positive numbers
    uint32_t tuple_count = 0, batch_count = 1;
    while (true) {
        /* retrieve tuples from the outer plan until there are no more */
        outer_tuple_slot = pstate->fetch(pstate->callback_data, &pstate->tuple)
                            ? &pstate->tuple : nullptr;
        /* if no more tuples in the pipeline exit the loop */
        if (outer_tuple_slot == nullptr) {
            break;
        }

        /* skip null rows */
        if (outer_tuple_slot->isnull[0]) {
            continue;
        }

        /* first attribute `0' always corresponds to the label (i.e. target) */
        float label =
            datum_get_float8(outer_tuple_slot->typid[XG_TARGET_COLUMN], outer_tuple_slot->values[XG_TARGET_COLUMN]);
        chunk.labels[tuple_count] = label;

        bool col_is_null = false;
        for (int j = 1; j < pstate->tuple.ncolumns; ++j) {
            if (outer_tuple_slot->isnull[j]) {
                col_is_null = true;
                break;
            }
            *(chunk.features + tuple_count * chunk.ft_cols + j - 1) =
                datum_get_float8(outer_tuple_slot->typid[j], outer_tuple_slot->values[j]);
        }
        if (col_is_null) {
            continue;
        }
        if (label > 0) {
            pos_cnt += 1;
        }
        ++tuple_count;
        ++xg_state->processed_tuples;

        /* train this batch */
        if (tuple_count == xg_hyperp->batch_size) {
            check_data_cnt(tuple_count, pos_cnt, xg_hyperp);
            trainXG(self, xg_hyperp, &chunk, tuple_count, (batch_count == 1));
            setup_xg_chunk(chunk);
            tuple_count = 0;
            pos_cnt = 0;
            ++batch_count;
        }
    }

    /* process any remaining tuples */
    if (tuple_count > 0) {
        check_data_cnt(tuple_count, pos_cnt, xg_hyperp);
        trainXG(self, xg_hyperp, &chunk, tuple_count, (batch_count == 1));
    }

    /* record the execution time of this method */
    clock_gettime(CLOCK_MONOTONIC, &exec_end_time);

    /* processing stats */
    xg_state->done = true;
    xg_state->execution_time = interval_to_sec(time_diff(&exec_end_time, &exec_start_time));
    if (xg_state->processed_tuples == 0)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_NO_DATA_FOUND),
                errmsg("Training data is empty, please check the input data.")));

    // number of configurations already run
    ++pstate->finished;
    // store the model
    Model* model = models[0];
    MemoryContext oldcxt = MemoryContextSwitchTo(model->memory_context);

    model->data.version = DB4AI_MODEL_V01;
    
    xgboost_serialize(&model->data, &chunk);
    
    model->exec_time_secs = xg_state->execution_time;
    model->processed_tuples = xg_state->processed_tuples;
    model->num_actual_iterations = xg_hyperp->n_iterations;
    model->return_type = FLOAT8OID;
    // store the score
    TrainingScore* pscore = (TrainingScore*)palloc0(sizeof(TrainingScore));
    pscore->name = xg_hyperp->eval_metric;
    pscore->value = chunk.validation_score;
    model->scores = lappend(model->scores, pscore);
    model->status = ERRCODE_SUCCESSFUL_COMPLETION;
    MemoryContextSwitchTo(oldcxt);
}

static void xgboost_end(AlgorithmAPI *self, TrainModelState *pstate)
{
    auto xg_state_node  = reinterpret_cast<XGBoostState*>(pstate);
    if (xg_state_node->oids != nullptr)
        pfree(xg_state_node->oids);
}


// ---------------------------------------------------------------------------------------------------
//  Prediction part
// ---------------------------------------------------------------------------------------------------
void print1DFeature(float *arr, const int cols)
{
    StringInfoData buf;
    initStringInfo(&buf);

    for (int i = 0; i < cols; ++i) {
        appendStringInfo(&buf, "%4.2f\t", arr[i]);
    }

    appendStringInfoChar(&buf, '\n');
    elog(NOTICE, "%s", buf.data);

    pfree(buf.data);
}

void xgboost_deserialize(SerializedModel *xg_model, SerializedModelXgboost *xgboostm)
{
    int avail = xg_model->size;
    if (xg_model->version == DB4AI_MODEL_V01) {
        if (avail < (int)sizeof(XgboostModelV01))
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                    errmsg("Model data corrupted reading header")));
        XgboostModelV01 *mdata = (XgboostModelV01 *)xg_model->raw_data;
        xgboostm->ft_cols = mdata->ft_cols;
        avail -= (int)sizeof(XgboostModelV01);

        void *placeholder = palloc0(avail);
        uint8_t *ptr = (uint8_t *)(mdata + 1);
        int rc = memcpy_s(placeholder, avail, ptr, avail);
        securec_check(rc, "\0", "\0");

        xg_model->raw_data = placeholder;
        xg_model->size = avail;
        xgboostm->model = xg_model;
    }
}
ModelPredictor xgboost_predict_prepare(AlgorithmAPI *, SerializedModel const *model, Oid return_type)
{
    if (unlikely(!model))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Xgboost predict prepare: model cannot be null")));

    load_xgboost_library();

    auto xg_model = const_cast<SerializedModel *>(model);
    SerializedModelXgboost *xgboostm = (SerializedModelXgboost *)palloc0(sizeof(SerializedModelXgboost));
    xgboost_deserialize(xg_model, xgboostm);
    return reinterpret_cast<ModelPredictor>(xgboostm);
}

Datum xgboost_predict(AlgorithmAPI *, ModelPredictor model, Datum *values, bool *isnull, Oid *types, int ncolumns)
{
    SerializedModelXgboost *xgboostm = (SerializedModelXgboost *)model;

    /* init XGBoost predictor */
    safe_xgboost(g_xgboostApi->XGBoosterCreate(nullptr, 0, &xgboostm->booster));
    /* load the decoded model */
    safe_xgboost(g_xgboostApi->XGBoosterUnserializeFromBuffer(xgboostm->booster, xgboostm->model->raw_data, xgboostm->model->size));
    /* sanity checks */
    Assert(xgboostm->booster != nullptr);
    if (ncolumns != xgboostm->ft_cols)
        ereport(ERROR, (errmodule(MOD_DB4AI),
            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid number of features for prediction, provided %d, expected %d",
                ncolumns, xgboostm->ft_cols)));

    load_xgboost_library();

    float features[ncolumns];
    for (int col = 0; col < ncolumns; ++col)
        features[col] = isnull[col] ? 0.0 : datum_get_float8(types[col], values[col]);

    DMatrixHandle dmat;
    /* convert to DMatrix */
    safe_xgboost(g_xgboostApi->XGDMatrixCreateFromMat((float *) features, 1, ncolumns, -1, &dmat));

    bst_ulong out_len;
    const float *out_result;
    safe_xgboost(g_xgboostApi->XGBoosterPredict(xgboostm->booster, dmat, 0, 0, 0, &out_len, &out_result));

    /* currently predict tuple-at-the-time */
    double prediction = out_result[0];

    /* release memory of xgboost dmatrix structure */
    safe_xgboost(g_xgboostApi->XGDMatrixFree(dmat));
    safe_xgboost(g_xgboostApi->XGBoosterFree(xgboostm->booster));
    return Float8GetDatum(prediction);
}

/*
 * used in EXPLAIN MODEL
 */
List *xgboost_explain(AlgorithmAPI *self, SerializedModel const *model, Oid return_type)
{
    if (unlikely(!model))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("xgboost explain: model cannot be null")));

    if (unlikely(model->version != DB4AI_MODEL_V01))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("xgboost explain: currently only model V01 is supported")));

    List *model_info = nullptr;

    auto xg_model = const_cast<SerializedModel*>(model);

    TrainingInfo *info = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
    info->value = Int32GetDatum(xg_model->size);
    info->type = INT4OID;
    info->name = "model size";
    model_info = lappend(model_info, info);

    return model_info;
}

XGBoost xg_reg_logistic = {
    // AlgorithmAPI
    {
        XG_REG_LOGISTIC,
        "xgboost_regression_logistic",
        ALGORITHM_ML_DEFAULT,
        xgboost_metrics_accuracy,
        xgboost_get_hyperparameters,
        xgboost_make_hyperparameters,
        nullptr,
        xgboost_create,
        xgboost_run,
        xgboost_end,
        xgboost_predict_prepare,
        xgboost_predict,
        xgboost_explain
    },
};

XGBoost xg_bin_logistic = {
    // AlgorithmAPI
    {
        XG_BIN_LOGISTIC,
        "xgboost_binary_logistic",
        ALGORITHM_ML_DEFAULT,
        xgboost_metrics_accuracy,
        xgboost_get_hyperparameters,
        xgboost_make_hyperparameters,
        nullptr,
        xgboost_create,
        xgboost_run,
        xgboost_end,
        xgboost_predict_prepare,
        xgboost_predict,
        xgboost_explain
    },
};

XGBoost xg_reg_sqe = {
    // AlgorithmAPI
    {
        XG_REG_SQE,
        "xgboost_regression_squarederror",
        ALGORITHM_ML_DEFAULT,
        xgboost_metrics_accuracy,
        xgboost_get_hyperparameters,
        xgboost_make_hyperparameters,
        nullptr,
        xgboost_create,
        xgboost_run,
        xgboost_end,
        xgboost_predict_prepare,
        xgboost_predict,
        xgboost_explain
    },
};

XGBoost xg_reg_gamma = {
    // AlgorithmAPI
    {
        XG_REG_GAMMA,
        "xgboost_regression_gamma",
        ALGORITHM_ML_DEFAULT,
        xgboost_metrics_accuracy,
        xgboost_get_hyperparameters,
        xgboost_make_hyperparameters,
        nullptr,
        xgboost_create,
        xgboost_run,
        xgboost_end,
        xgboost_predict_prepare,
        xgboost_predict,
        xgboost_explain
    },
};
