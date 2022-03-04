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
#include "nodes/value.h"
#include "parser/analyze.h"
#include "rewrite/rewriteHandler.h"
#include "utils/snapmgr.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "workload/workload.h"
#include "executor/node/nodeTrainModel.h"
#include "db4ai/aifuncs.h"
#include "db4ai/db4ai_api.h"
#include "utils/builtins.h"
#include "db4ai/gd.h"
#include "db4ai/fp_ops.h"
#include "optimizer/planmain.h"

extern void exec_simple_plan(PlannedStmt *plan);            // defined in postgres.cpp

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


static void add_operator_to_plan(PlannedStmt *plan, Plan *plan_operator)
{
    plan_operator->lefttree = plan->planTree;
    plan->planTree = plan_operator;
}


// Optimize the original query subplan, acoording to the train model operation
static void optimize_train_model_subplan(PlannedStmt *plan, TrainModel *train_model, AlgorithmAPI *api)
{
    Plan *original_plan       = plan->planTree;

    // Check if the plan could be improved by adding a materialize node before train model
    bool already_materializes = ExecMaterializesOutput(original_plan->type) ||
                                (IsA(original_plan, SubqueryScan) &&
                                 ExecMaterializesOutput(((SubqueryScan *) original_plan)->subplan->type));
    if ((api->flags & ALGORITHM_ML_RESCANS_DATA) &&
        u_sess->attr.attr_sql.enable_material && !already_materializes) {

        Plan *materialized_plan = materialize_finished_plan(plan->planTree, true);
        add_operator_to_plan(plan, materialized_plan);
    }
}


// Add a train model operator at the root of the plan
static void add_train_model_to_plan(PlannedStmt *plan, AlgorithmML algorithm, List *hyperparameters,
                                    MemoryContext cxt, DestReceiverTrainModel *dest)
{
    TrainModel *pnode = makeNode(TrainModel);
    pnode->algorithm = algorithm;
    pnode->configurations = 1;
    pnode->hyperparameters = (const ModelHyperparameters **)palloc(sizeof(ModelHyperparameters *));
    pnode->cxt = cxt;
    
    AlgorithmAPI *api = get_algorithm_api(algorithm);
    Assert(api->make_hyperparameters != nullptr);
    Assert(api->get_hyperparameters_definitions != nullptr);
    
    ModelHyperparameters *hyperp = api->make_hyperparameters(api);
    pnode->hyperparameters[0] = hyperp;
    
    // put the hyperparameters into the node using default values when needed
    // and register the hyperparameter values also into the output structures
    int32_t definitions_size;
    const HyperparameterDefinition *definitions = api->get_hyperparameters_definitions(api, &definitions_size);
    init_hyperparameters_with_defaults(definitions, definitions_size, hyperp);
    configure_hyperparameters_vset(definitions, definitions_size, hyperparameters, hyperp);
    if (api->update_hyperparameters != nullptr)
        api->update_hyperparameters(api, hyperp);

    dest->hyperparameters = prepare_model_hyperparameters(definitions, definitions_size, hyperp, dest->memcxt);

    optimize_train_model_subplan(plan, pnode, api);
    
    add_operator_to_plan(plan, &pnode->plan);
}

// Add the ML algorithm at the root of the plan according to the CreateModelStmt
static void add_create_model_to_plan(CreateModelStmt *stmt, PlannedStmt *plan,
                                     DestReceiverTrainModel *dest, MemoryContext cxt)
{
    if (stmt->algorithm >= INVALID_ALGORITHM_ML) {
        char *s = "logistic_regression, svm_classification, linear_regression, kmeans, pca, "
                  "xgboost_regression_logistic, xgboost_binary_logistic, xgboost_regression_squarederror, "
                  "xgboost_regression_gamma, dectree_classification, dectree_regression";
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Architecture %s is not supported. Supported architectures: %s", stmt->architecture, s)));
    }
    
    add_train_model_to_plan(plan, stmt->algorithm, stmt->hyperparameters, cxt, dest);
}

// Create the query plan with the appropriate machine learning model
PlannedStmt *plan_create_model(CreateModelStmt *stmt, const char *query_string, ParamListInfo params,
                               DestReceiver *dest, MemoryContext cxt)
{
    Query *query = (Query *)stmt->select_query;
    PlannedStmt *plan = NULL;
    
    query = setup_for_create_model(query, query_string, params);
    
    /* plan the query */
    plan = pg_plan_query(query, 0, params);
    
    // Inject the GradientDescent node at the root of the plan
    DestReceiverTrainModel *dest_train_model = (DestReceiverTrainModel *)dest;
    
    add_create_model_to_plan(stmt, plan, dest_train_model, cxt);
    
    return plan;
}

// Prepare the DestReceiver for training
void configure_dest_receiver_train_model(DestReceiverTrainModel *dest, MemoryContext context, AlgorithmML algorithm,
                                         const char *model_name, const char *sql, bool automatic_save)
{
    if (algorithm >= INVALID_ALGORITHM_ML)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Unsupported model type in model warehouse %d", algorithm)));
    
    MemoryContext old_context = MemoryContextSwitchTo(context);
    dest->memcxt = context;
    dest->algorithm = algorithm;
    dest->model_name = pstrdup(model_name);
    dest->sql = sql;
    dest->hyperparameters = NULL;
    dest->targetlist = nullptr;
    dest->save_model = automatic_save;
    MemoryContextSwitchTo(old_context);
}


void exec_create_model_planned(QueryDesc *queryDesc, char *completionTag)
{
    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, 0);
    
    /* workload client manager */
    if (ENABLE_WORKLOAD_CONTROL) {
        WLMInitQueryPlan(queryDesc);
        dywlm_client_manager(queryDesc);
    }
    
    ScanDirection dir = ForwardScanDirection;
    
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
}


void exec_create_model(CreateModelStmt *stmt, const char *queryString, ParamListInfo params, char *completionTag)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("No support for distributed scenarios yet.")));
#endif
    DestReceiverTrainModel *dest = NULL;
    
    PlannedStmt *plan = NULL;
    QueryDesc *queryDesc = NULL;
    
    // We create the model, and all AutoML structures in the db4ai context. This context
    // is normally allocated as a subcontext of portal context, and lasts longer than the execution
    // of the query plan for training
    MemoryContext db4ai_context = AllocSetContextCreate(CurrentMemoryContext, "db4ai_context",
                                                        ALLOCSET_DEFAULT_MINSIZE,
                                                        ALLOCSET_DEFAULT_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old = MemoryContextSwitchTo(db4ai_context);
    
    /*
     * Create the tuple receiver object and insert hyperp it will need
     */
    dest = (DestReceiverTrainModel *)CreateDestReceiver(DestTrainModel);
    configure_dest_receiver_train_model(dest, CurrentMemoryContext,
                                        (AlgorithmML)stmt->algorithm, stmt->model, queryString,
                                        true);
    
    plan = plan_create_model(stmt, queryString, params, (DestReceiver *)dest, db4ai_context);
    
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
    print_hyperparameters(DEBUG1, dest->hyperparameters);
    exec_create_model_planned(queryDesc, completionTag);
    
    FreeQueryDesc(queryDesc);
    PopActiveSnapshot();
    
    MemoryContextSwitchTo(old);
}


static void store_tuple_in_model_warehouse(TupleTableSlot *slot, DestReceiver *self)
{
    DestReceiverTrainModel *dest = (DestReceiverTrainModel *)self;
    
    if (dest->algorithm >= INVALID_ALGORITHM_ML)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Unsupported model type %d", static_cast<int>(dest->algorithm))));

    Model *model = (Model*)DatumGetPointer(slot->tts_values[0]);
    model->algorithm = dest->algorithm;
    model->model_name = dest->model_name;
    model->sql = dest->sql;
    model->hyperparameters = dest->hyperparameters;
    
    if (dest->save_model)
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
