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
 * create_model.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/create_model.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CREATE_MODEL_H
#define CREATE_MODEL_H

#include "postgres.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/parsenodes_common.h"
#include "nodes/plannodes.h"
#include "tcop/dest.h"

struct Model;
struct QueryDesc;

struct DestReceiverTrainModel {
    DestReceiver dest;
    MemoryContext memcxt;
    AlgorithmML algorithm;
    const char* model_name;
    const char* sql;
    List* hyperparameters;      // List of Hyperparamters
    List *targetlist; // for gradient descent
    bool  save_model; // Set to save automatically the model into the modle warehouse
};


void configure_dest_receiver_train_model(DestReceiverTrainModel *dest, MemoryContext context, AlgorithmML algorithm,
                                        const char* model_name, const char* sql, bool automatic_save);

// Create a DestReceiver object for training model operators
DestReceiver *CreateTrainModelDestReceiver();

// Rewrite a create model query, and plan the query. This method is used in query execution
// and for explain statements
PlannedStmt *plan_create_model(CreateModelStmt *stmt, const char *query_string, ParamListInfo params,
    DestReceiver *dest, MemoryContext cxt);

// Call executor
void exec_create_model(CreateModelStmt *stmt, const char *queryString, ParamListInfo params, char *completionTag);

// Execute a query plan for create model
void exec_create_model_planned(QueryDesc *queryDesc, char *completionTag);

#endif
