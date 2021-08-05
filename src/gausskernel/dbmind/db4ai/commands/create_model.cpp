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
 *  command.h
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/commands/create_model.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "db4ai/create_model.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "db4ai/model_warehouse.h"
#include "db4ai/hyperparameter_validation.h"
#include "catalog/indexing.h"
#include "executor/executor.h"
#include "executor/nodeKMeans.h"
#include "nodes/value.h"
#include "parser/analyze.h"
#include "rewrite/rewriteHandler.h"
#include "utils/snapmgr.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "workload/workload.h"
#include "executor/nodeGD.h"
#include "db4ai/aifuncs.h"
#include "utils/builtins.h"

extern void exec_simple_plan(PlannedStmt *plan);            // defined in postgres.cpp
bool verify_pgarray(ArrayType const * pg_array, int32_t n); // defined in kmeans.cpp

/*
 * Common setup needed by both normal execution and EXPLAIN ANALYZE.
 * This setup is adapted from SetupForCreateTableAs
 */
static Query *setup_for_create_model(Query *query, /* IntoClause *into, */ const char *queryString,
    ParamListInfo params                           /* , DestReceiver *dest */)
{
    List *rewritten = NIL;

    Assert(query->commandType == CMD_SELECT);

    /*
     * Parse analysis was done already, but we still have to run the rule
     * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
     * came straight from the parser, or suitable locks were acquired by
     * plancache.c.
     *
     * Because the rewriter and planner tend to scribble on the input, we make
     * a preliminary copy of the source querytree.  This prevents problems in
     * the case that CTAS is in a portal or plpgsql function and is executed
     * repeatedly.  (See also the same hack in EXPLAIN and PREPARE.)
     */
    rewritten = QueryRewrite((Query *)copyObject(query));

    /* SELECT should never rewrite to more or less than one SELECT query */
    if (list_length(rewritten) != 1) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Unexpected rewrite result for CREATE MODEL statement")));
    }

    query = (Query *)linitial(rewritten);

    return query;
}

// Create an GradientDescent execution node with a given configuration
static GradientDescent *create_gd_node(AlgorithmML algorithm, List *hyperparameters, DestReceiverTrainModel *dest)
{
    GradientDescent *gd_node = makeNode(GradientDescent);
    gd_node->algorithm = algorithm;

    configure_hyperparameters(algorithm, hyperparameters, dest->model, gd_node);
    if (gd_node->seed == 0) {
        gd_node->seed = time(NULL); // it is not set to zero again (zero is the epoch in the past)
        update_model_hyperparameter(dest->model, "seed", INT4OID, Int32GetDatum(gd_node->seed));
    }

    gd_node->plan.type = T_GradientDescent;
    gd_node->plan.targetlist = makeGradientDescentExpr(algorithm, nullptr, 1);
    dest->targetlist = gd_node->plan.targetlist;

    return gd_node;
}

// Add a GradientDescent operator at the root of the plan
static PlannedStmt *add_GradientDescent_to_plan(PlannedStmt *plan, AlgorithmML algorithm, List *hyperparameters,
    DestReceiverTrainModel *dest)
{
    GradientDescent *gd_node = create_gd_node(algorithm, hyperparameters, dest);
    gd_node->plan.lefttree = plan->planTree;
    plan->planTree = &gd_node->plan;

    return plan;
}

static DistanceFunction get_kmeans_distance(const char *distance_func)
{
    DistanceFunction distance = KMEANS_L2_SQUARED;

    if (strcmp(distance_func, "L1") == 0)
        distance = KMEANS_L1;
    else if (strcmp(distance_func, "L2") == 0)
        distance = KMEANS_L2;
    else if (strcmp(distance_func, "L2_Squared") == 0)
        distance = KMEANS_L2_SQUARED;
    else if (strcmp(distance_func, "Linf") == 0)
        distance = KMEANS_LINF;
    else {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("No known distance function chosen. Current candidates are: "
            "L1, L2, L2_Squared (default), Linf")));
    }

    return distance;
}

static SeedingFunction get_kmeans_seeding(const char *seeding_func)
{
    SeedingFunction seeding = KMEANS_RANDOM_SEED;

    if (strcmp(seeding_func, "Random++") == 0)
        seeding = KMEANS_RANDOM_SEED;
    else if (strcmp(seeding_func, "KMeans||") == 0)
        seeding = KMEANS_BB;
    else {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("No known seeding function chosen. Current candidates are: Random++ (default), KMeans||")));
    }
    return seeding;
}

static KMeans *create_kmeans_node(AlgorithmML const algorithm, List *hyperparameters, DestReceiverTrainModel *dest)
{
    KMeans *kmeans_node = makeNode(KMeans);
    char *distance_func = nullptr;
    char *seeding_func = nullptr;
    double tolerance = 0.;
    int32_t num_iterations = 0;
    int32_t num_centroids = 0;
    int32_t const max_num_centroids = 1000000;
    int32_t batch_size = 0;
    int32_t num_features = 0;
    int32_t external_seed = 0;
    int32_t verbosity = 0;
    auto kmeans_model = reinterpret_cast<ModelKMeans *>(dest->model);
    HyperparameterValidation validation;
    memset_s(&validation, sizeof(HyperparameterValidation), 0, sizeof(HyperparameterValidation));

    kmeans_node->algorithm = algorithm;
    kmeans_node->plan.type = T_KMeans;
    kmeans_model->model.return_type = INT4OID;

    set_hyperparameter<int32_t>("max_iterations", &num_iterations, hyperparameters, 10, dest->model, &validation);

    if (unlikely(num_iterations <= 0)) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("max_iterations must be in [1, %d]", INT_MAX)));
    } else {
        kmeans_node->parameters.num_iterations = num_iterations;
    }
    set_hyperparameter<int32_t>("num_centroids", &num_centroids, hyperparameters, 10, dest->model, &validation);

    if (unlikely((num_centroids <= 0) || (num_centroids > max_num_centroids))) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("num_centroids must be in [1, %d]", max_num_centroids)));
    } else {
        kmeans_node->parameters.num_centroids = num_centroids;
    }
    set_hyperparameter<double>("tolerance", &tolerance, hyperparameters, 0.00001, dest->model, &validation);

    if (unlikely((tolerance <= 0.) || (tolerance > 1.))) {
        ereport(ERROR,
            (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("tolerance must be in (0, 1.0]")));
    } else {
        kmeans_node->parameters.tolerance = tolerance;
    }
    set_hyperparameter<int32_t>("batch_size", &batch_size, hyperparameters, 10, dest->model, &validation);

    if (unlikely((batch_size <= 0) || (batch_size > max_num_centroids))) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("batch_size must be in [1, %d]", max_num_centroids)));
    } else {
        kmeans_node->description.batch_size = batch_size;
    }
    set_hyperparameter<int32_t>("num_features", &num_features, hyperparameters, 2, dest->model, &validation);

    if (unlikely((num_features <= 0) || (num_features > max_num_centroids))) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("num_features must be in [1, %d]", max_num_centroids)));
    } else {
        kmeans_node->description.n_features = num_features;
    }
    set_hyperparameter<char *>("distance_function", &distance_func, hyperparameters, "L2_Squared", dest->model,
        &validation);

    kmeans_node->description.distance = get_kmeans_distance(distance_func);

    set_hyperparameter<char *>("seeding_function", &seeding_func, hyperparameters, "Random++", dest->model,
        &validation);

    kmeans_node->description.seeding = get_kmeans_seeding(seeding_func);

    set_hyperparameter<int32_t>("verbose", &verbosity, hyperparameters, false, dest->model, &validation);

    if (verbosity < 0 || verbosity > 2)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Verbosity level must be between 0 (no output), 1 (less output), or 2 (full output)")));
    else
        kmeans_node->description.verbosity = static_cast<Verbosity>(verbosity);

    /*
     * unfortunately the system parses an int64_t as T_Float whenever the it does not fit into a int32_t
     * thus, an int64_t might be internally parsed as a T_Integer or a T_Float depending on whether
     * the precision fits into an int32_t. thus, we accept a small seed (int32_t) that is xor'ed with
     * a random long internal seed.
     */
    set_hyperparameter<int32_t>("seed", &external_seed, hyperparameters, 0, dest->model, &validation);

    /*
     * the seed used for the algorithm is the xor of the seed provided by the user with
     * a random (but fixed) internal seed. as long as the internal seed is kept unchanged
     * results will be reproducible (see nodeKMeans.cpp)
     */
    if (external_seed < 0)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("seed must be in [0, %d]", INT_MAX)));
    else
        kmeans_node->parameters.external_seed = static_cast<uint64_t>(external_seed);
        
    /*
     * these fields are propagated all the way to store_model and used for prediction
     * the value of fields of ModelKMeans not set here change during execution
     * and thus are set in the very end, when the model is about to be stored
     */
    kmeans_model->dimension = kmeans_node->description.n_features;
    kmeans_model->original_num_centroids = kmeans_node->parameters.num_centroids;
    kmeans_model->distance_function_id = kmeans_node->description.distance;

    pfree(distance_func);
    pfree(seeding_func);   
    return kmeans_node;
}

// Add a k-means operator at the root of the plan
static PlannedStmt *add_kmeans_to_plan(PlannedStmt *plan, AlgorithmML algorithm, List *hyperparameters,
    DestReceiverTrainModel *dest)
{
    if (unlikely(algorithm != KMEANS)) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Algorithm is not the expected %u (k-means). Provided %u", KMEANS, algorithm)));
    }

    KMeans *kmeans_node = create_kmeans_node(algorithm, hyperparameters, dest);

    kmeans_node->plan.lefttree = plan->planTree;
    plan->planTree = &kmeans_node->plan;

    return plan;
}


// Add the ML algorithm at the root of the plan according to the CreateModelStmt
static PlannedStmt *add_create_model_to_plan(CreateModelStmt *stmt, PlannedStmt *plan, DestReceiverTrainModel *dest)
{
    PlannedStmt *result = NULL;
    switch (stmt->algorithm) {
        case LOGISTIC_REGRESSION:
        case SVM_CLASSIFICATION:
        case LINEAR_REGRESSION: {
            result = add_GradientDescent_to_plan(plan, stmt->algorithm, stmt->hyperparameters, dest);
            break;
        }
        case KMEANS: {
            result = add_kmeans_to_plan(plan, stmt->algorithm, stmt->hyperparameters, dest);
            break;
        }
        case INVALID_ALGORITHM_ML:
        default: {
            char *s = "logistic_regression, svm_classification, linear_regression, kmeans";
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Architecture %s is not supported. Supported architectures: %s", stmt->architecture, s)));
        }
    }
    return result;
}

// Create the query plan with the appropriate machine learning model
PlannedStmt *plan_create_model(CreateModelStmt *stmt, const char *query_string, ParamListInfo params,
    DestReceiver *dest)
{
    Query *query = (Query *)stmt->select_query;
    PlannedStmt *plan = NULL;

    query = setup_for_create_model(query, query_string, params);

    /* plan the query */
    plan = pg_plan_query(query, 0, params);

    // Inject the GradientDescent node at the root of the plan
    plan = add_create_model_to_plan(stmt, plan, (DestReceiverTrainModel *)dest);

    return plan;
}

// Prepare the DestReceiver for training
void configure_dest_receiver_train_model(DestReceiverTrainModel *dest, AlgorithmML algorithm, const char *model_name,
    const char *sql)
{
    switch (algorithm) {
        case LOGISTIC_REGRESSION:
        case LINEAR_REGRESSION:
        case SVM_CLASSIFICATION: {
            dest->model = (Model *)palloc0(sizeof(ModelGradientDescent));
            break;
        }
        case KMEANS: {
            dest->model = (Model *)palloc0(sizeof(ModelKMeans));
            break;
        }
        default: {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Unsupported model type in model warehouse %d", algorithm)));
        }
    }
    dest->model->algorithm = algorithm;
    dest->model->model_name = pstrdup(model_name);
    dest->model->sql = sql;
    dest->targetlist = nullptr;
}


// /*
// * ExecCreateTableAs -- execute a CREATE TABLE AS command
// */
void exec_create_model(CreateModelStmt *stmt, const char *queryString, ParamListInfo params, char *completionTag)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("No support for distributed scenarios yet.")));
#endif
    DestReceiverTrainModel *dest = NULL;

    PlannedStmt *plan = NULL;
    QueryDesc *queryDesc = NULL;
    ScanDirection dir;

    /*
     * Create the tuple receiver object and insert hyperp it will need
     */
    dest = (DestReceiverTrainModel *)CreateDestReceiver(DestTrainModel);
    configure_dest_receiver_train_model(dest, (AlgorithmML)stmt->algorithm, stmt->model, queryString);

    plan = plan_create_model(stmt, queryString, params, (DestReceiver *)dest);

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.	(This could only matter if
     * the planner executed an allegedly-stable function that changed the
     * database contents, but let's do it anyway to be parallel to the EXPLAIN
     * code path.)
     */
    PushCopiedSnapshot(GetActiveSnapshot());
    UpdateActiveSnapshotCommandId();

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    queryDesc = CreateQueryDesc(plan, queryString, GetActiveSnapshot(), InvalidSnapshot, &dest->dest, params, 0);

#ifdef ENABLE_MULTIPLE_NODES
    if (ENABLE_WORKLOAD_CONTROL && (IS_PGXC_COORDINATOR)) {
#else
    if (ENABLE_WORKLOAD_CONTROL) {
#endif
        /* Check if need track resource */
        u_sess->exec_cxt.need_track_resource = WLMNeedTrackResource(queryDesc);
    }

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, 0);

    /* workload client manager */
    if (ENABLE_WORKLOAD_CONTROL) {
        WLMInitQueryPlan(queryDesc);
        dywlm_client_manager(queryDesc);
    }

    dir = ForwardScanDirection;

    /* run the plan */
    ExecutorRun(queryDesc, dir, 0L);

    /* save the rowcount if we're given a completionTag to fill */
    if (completionTag != NULL) {
        errno_t rc;
        rc = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "MODEL CREATED. PROCESSED %lu", queryDesc->estate->es_processed);
        securec_check_ss(rc, "\0", "\0");
    }

    /* and clean up */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);

    PopActiveSnapshot();
}

static void store_gd_expr_in_model(Datum dt, Oid type, int col, GradientDescentExprField field, Model *model, 
                                   ModelGradientDescent *model_gd, TupleDesc tupdesc)
{
    switch (field) {
        case GD_EXPR_ALGORITHM:
            Assert(type == INT4OID);
            model_gd->model.algorithm = (AlgorithmML)DatumGetInt32(dt);
            break;
        case GD_EXPR_OPTIMIZER:
            break; // Ignore field
        case GD_EXPR_RESULT_TYPE:
            Assert(type == OIDOID);
            model->return_type = DatumGetUInt32(dt);
            break;
        case GD_EXPR_NUM_ITERATIONS:
            Assert(type == INT4OID);
            model->num_actual_iterations = DatumGetInt32(dt);
            break;
        case GD_EXPR_EXEC_TIME_MSECS:
            Assert(type == FLOAT4OID);
            model->exec_time_secs = DatumGetFloat4(dt) / 1000.0;
            break;
        case GD_EXPR_PROCESSED_TUPLES:
            Assert(type == INT4OID);
            model->processed_tuples = DatumGetInt32(dt);
            break;
        case GD_EXPR_DISCARDED_TUPLES:
            Assert(type == INT4OID);
            model->discarded_tuples = DatumGetInt32(dt);
            break;
        case GD_EXPR_WEIGHTS:
            Assert(type == FLOAT4ARRAYOID);
            model_gd->weights = datumCopy(dt, tupdesc->attrs[col]->attbyval, tupdesc->attrs[col]->attlen);
            break;
        case GD_EXPR_CATEGORIES: {
            ArrayType *arr = (ArrayType *)DatumGetPointer(dt);
            model_gd->ncategories = ARR_DIMS(arr)[0];
            model_gd->categories =
                datumCopy(dt, tupdesc->attrs[col]->attbyval, tupdesc->attrs[col]->attlen);
        } break;
        default:
            (void)type;
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Model warehouse for GradientDescent field %d not implemented", field)));
            break;
    }
}

static void store_tuple_gd_in_model_warehouse(TupleTableSlot *slot, DestReceiverTrainModel *dest)
{
    Assert(dest->targetlist != nullptr);

    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    Model *model = dest->model;
    model->pre_time_secs = 0.0;

    ModelGradientDescent *model_gd = (ModelGradientDescent *)model;
    model_gd->ncategories = 0;
    model_gd->model.algorithm = INVALID_ALGORITHM_ML; // undefined

    int col = 0;
    ListCell *lc;
    foreach (lc, dest->targetlist) {
        TargetEntry *target = lfirst_node(TargetEntry, lc);
        GradientDescentExpr *expr = (GradientDescentExpr *)target->expr;
        if (!slot->tts_isnull[col]) {
            Datum dt = slot->tts_values[col];
            Oid type = tupdesc->attrs[col]->atttypid;
            if ((expr->field & GD_EXPR_SCORE) != 0) {
                Assert(type == FLOAT4OID);
                TrainingScore *score = (TrainingScore *)palloc0(sizeof(TrainingScore));
                score->name = pstrdup(target->resname);
                score->value = DatumGetFloat4(dt);
                model->scores = lappend(model->scores, score);
            } else {
                store_gd_expr_in_model(dt, type, col, expr->field, model, model_gd, tupdesc);
            }
        }
        col++;
    }
}

static void store_kmeans_data_in_model(uint32_t natts, TupleDesc tupdesc, Datum *values, bool *nulls,
                                       Model *model, ModelKMeans *model_kmeans)
{
    ArrayType *centroid_ids = nullptr;
    ArrayType *centroid_coordinates = nullptr;
    ArrayType *objective_functions = nullptr;
    ArrayType *avg_distances = nullptr;
    ArrayType *min_distances = nullptr;
    ArrayType *max_distances = nullptr;
    ArrayType *std_dev_distances = nullptr;
    ArrayType *cluster_sizes = nullptr;
    /* these are the inner-facing arrays */
    int32_t *centroid_ids_data = nullptr;
    double *centroid_coordiates_data = nullptr;
    double *objective_functions_data = nullptr;
    double *avg_distances_data = nullptr;
    double *min_distances_data = nullptr;
    double *max_distances_data = nullptr;
    double *std_dev_distances_data = nullptr;
    int64_t *cluster_sizes_data = nullptr;
    Oid oid = 0;
    Datum attr = 0;

    /*
     * for tuple at a time we only use one centroid at a time
     */
    for (uint32_t a = 0; a < natts; ++a) {
        if (unlikely(nulls[a])) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("Encountered null attribute %u when serializing k-means model when it should not", a)));
        }

        oid = tupdesc->attrs[a]->atttypid;
        attr = values[a];

        /*
         * this switch has to match exactly the schema of the row we return (see nodeKMeans.cpp)
         * there is a single row (quite big in general) thus the switch executes only once
         */
        switch (a) {
            case 0:
                // centroids ids of type INT4ARRAYOID
                Assert(oid == INT4ARRAYOID);
                centroid_ids = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                centroid_ids_data = reinterpret_cast<int32_t *>(ARR_DATA_PTR(centroid_ids));
                break;
            case 1:
                // centroids coordinates of type FLOAT8ARRAYOID
                Assert(oid == FLOAT8ARRAYOID);
                /*
                 * in tuple at a time, this memory reference is valid until we store the centroid
                 * in the model warehouse
                 */
                centroid_coordinates = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                centroid_coordiates_data = reinterpret_cast<double *>(ARR_DATA_PTR(centroid_coordinates));
                break;
            case 2:
                // value of the objective functions (per cluster) of type FLOAT8ARRAYOID
                Assert(oid == FLOAT8ARRAYOID);
                objective_functions = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                objective_functions_data = reinterpret_cast<double *>(ARR_DATA_PTR(objective_functions));
                break;
            case 3:
                // avg distance of the clusters of type FLOAT8ARRAYOID
                Assert(oid == FLOAT8ARRAYOID);
                avg_distances = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                avg_distances_data = reinterpret_cast<double *>(ARR_DATA_PTR(avg_distances));
                break;
            case 4:
                // min distance of the clusters of type FLOAT8ARRAYOID
                Assert(oid == FLOAT8ARRAYOID);
                min_distances = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                min_distances_data = reinterpret_cast<double *>(ARR_DATA_PTR(min_distances));
                break;
            case 5:
                // max distance of the clusters of type FLOAT8ARRAYOID
                Assert(oid == FLOAT8ARRAYOID);
                max_distances = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                max_distances_data = reinterpret_cast<double *>(ARR_DATA_PTR(max_distances));
                break;
            case 6:
                // standard deviation of clusters of type FLOAT8ARRAYOID
                Assert(oid == FLOAT8ARRAYOID);
                std_dev_distances = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                std_dev_distances_data = reinterpret_cast<double *>(ARR_DATA_PTR(std_dev_distances));
                break;
            case 7:
                // cluster sizes of type INT8ARRAYOID
                Assert(oid == INT8ARRAYOID);
                cluster_sizes = reinterpret_cast<ArrayType *>(DatumGetPointer(attr));
                cluster_sizes_data = reinterpret_cast<int64_t *>(ARR_DATA_PTR(cluster_sizes));
                break;
            case 8:
                // num good points of type INT8OID
                Assert(oid == INT8OID);
                model->processed_tuples = DatumGetInt64(attr);
                break;
            case 9:
                // num bad point of type INT8OID
                Assert(oid == INT8OID);
                model->discarded_tuples = DatumGetInt64(attr);
                break;
            case 10:
                // seedings time (secs) of type FLOAT8OID
                Assert(oid == FLOAT8OID);
                model->pre_time_secs = DatumGetFloat8(attr);
                break;
            case 11:
                // execution time (secs) of type FLOAT8OID
                Assert(oid == FLOAT8OID);
                model->exec_time_secs = DatumGetFloat8(attr);
                break;
            case 12:
                // actual number of iterations INT4OID
                Assert(oid == INT4OID);
                model->num_actual_iterations = DatumGetInt32(attr);
                break;
            case 13:
                // actual number of centroids INT4OID
                Assert(oid == INT4OID);
                model_kmeans->actual_num_centroids = DatumGetInt32(attr);
                break;
            case 14:
                // seed used for computations
                Assert(oid == INT8OID);
                model_kmeans->seed = DatumGetInt64(attr);
                break;
            default:
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Unknown attribute %u when serializing k-means model", a)));
        }
    }

    uint32_t const actual_num_centroids = model_kmeans->actual_num_centroids;
    uint32_t const dimension = model_kmeans->dimension;
    uint32_t centroid_coordinates_offset = 0;
    WHCentroid *current_centroid = nullptr;

    /*
     * at this point we have extracted all the attributes and the memory representation
     * of the model can be constructed so that it can be stored in the model warehouse
     */
    model_kmeans->centroids = reinterpret_cast<WHCentroid *>(palloc0(sizeof(WHCentroid) * actual_num_centroids));

    /*
     * we fill in the information of every centroid
     */
    for (uint32_t current_centroid_idx = 0; current_centroid_idx < actual_num_centroids; ++current_centroid_idx) {
        current_centroid = model_kmeans->centroids + current_centroid_idx;
        current_centroid->id = centroid_ids_data[current_centroid_idx];
        current_centroid->objective_function = objective_functions_data[current_centroid_idx];
        current_centroid->avg_distance_to_centroid = avg_distances_data[current_centroid_idx];
        current_centroid->min_distance_to_centroid = min_distances_data[current_centroid_idx];
        current_centroid->max_distance_to_centroid = max_distances_data[current_centroid_idx];
        current_centroid->std_dev_distance_to_centroid = std_dev_distances_data[current_centroid_idx];
        current_centroid->cluster_size = cluster_sizes_data[current_centroid_idx];
        current_centroid->coordinates = centroid_coordiates_data + centroid_coordinates_offset;
        centroid_coordinates_offset += dimension;
    }
}

static void store_tuple_kmeans_in_model_warehouse(TupleTableSlot *slot, DestReceiverTrainModel *dest)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);
    Assert(slot->tts_nvalid == NUM_ATTR_OUTPUT);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(!TTS_HAS_PHYSICAL_TUPLE(slot));

    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    auto model_kmeans = reinterpret_cast<ModelKMeans *>(dest->model);
    Model *model = &model_kmeans->model;

    if (unlikely(slot->tts_isempty))
        return;

    uint32_t const natts = slot->tts_nvalid;
    /*
     * the slot contains a virtual tuple and thus we can access its attributs directly
     */
    Datum *values = slot->tts_values;
    bool *nulls = slot->tts_isnull;

    if (unlikely(!values && !nulls)) {
        ereport(ERROR,
            (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Empty arrays values and nulls")));
    }

    store_kmeans_data_in_model(natts, tupdesc, values, nulls, model, model_kmeans);
}

static void store_tuple_in_model_warehouse(TupleTableSlot *slot, DestReceiver *self)
{
    DestReceiverTrainModel *dest = (DestReceiverTrainModel *)self;
    Model *model = dest->model;

    switch (model->algorithm) {
        case LOGISTIC_REGRESSION:
        case SVM_CLASSIFICATION:
        case LINEAR_REGRESSION:
            store_tuple_gd_in_model_warehouse(slot, dest);
            break;

        case KMEANS:
            store_tuple_kmeans_in_model_warehouse(slot, dest);
            break;

        case INVALID_ALGORITHM_ML:
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Unsupported model type %d", static_cast<int>(model->algorithm))));
            break;
    }

    store_model(model);
}


static void do_nothing_startup(DestReceiver *self, int operation, TupleDesc typehyperp)
{
    /* do nothing */
}


static void do_nothing_cleanup(DestReceiver *self)
{
    /* this is used for both shutdown and destroy methods */
}

DestReceiver *CreateTrainModelDestReceiver()
{
    DestReceiverTrainModel *dr = (DestReceiverTrainModel *)palloc0(sizeof(DestReceiverTrainModel));
    DestReceiver *result = &dr->dest;

    result->rStartup = do_nothing_startup;
    result->receiveSlot = store_tuple_in_model_warehouse;
    result->rShutdown = do_nothing_cleanup;
    result->rDestroy = do_nothing_cleanup;
    result->mydest = DestTrainModel;

    return result;
}
