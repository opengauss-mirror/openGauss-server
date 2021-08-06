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
 *
 *
 * IDENTIFICATION
 *        src/include/db4ai/hyperparameter_validation.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef DB4AI_HYPERPARAMETER_VALIDATION_H
#define DB4AI_HYPERPARAMETER_VALIDATION_H

#include "postgres.h"

#include "db4ai/model_warehouse.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"


#include <float.h>


struct HyperparameterDefinition {
    const char* name;
    Datum default_value;
    Datum min_value;
    Datum max_value;
    const char** valid_values;
    void (* enum_setter)(const char* s, void* enum_addr);
    Oid type;
    int32_t valid_values_size;
    int32_t offset;
    bool min_inclusive;
    bool max_inclusive;
};

struct HyperparameterValidation {
    void* min_value;
    bool min_inclusive;
    void* max_value;
    bool max_inclusive;
    const char** valid_values;
    int32_t valid_values_size;
};

// changes the final value of a hyperparameter into the model warehouse output
void update_model_hyperparameter(Model* model, const char* name, Oid type, Datum value);

void configure_hyperparameters(AlgorithmML algorithm,
                               List* hyperparameters, Model* model, void* hyperparameter_struct);

// Set int hyperparameter
void set_hyperparameter_value(const char* name, int* hyperparameter,
                              Value* value, VariableSetKind kind, int default_value, Model* model,
                              HyperparameterValidation* validation);


// Set double hyperparameter
void set_hyperparameter_value(const char* name, double* hyperparameter, Value* value,
                              VariableSetKind kind, double default_value, Model* model,
                              HyperparameterValidation* validation);


// Set string hyperparameter (no const)
void set_hyperparameter_value(const char* name, char** hyperparameter, Value* value,
                              VariableSetKind kind, char* default_value, Model* model,
                              HyperparameterValidation* validation);


// Set boolean hyperparameter
void set_hyperparameter_value(const char* name, bool* hyperparameter,
                              Value* value, VariableSetKind kind, bool default_value, Model* model,
                              HyperparameterValidation* validation);


// General purpouse method to set the hyperparameters
// Locate the hyperparameter in the list by name and set it to the selected value
// Return the index in the list of the hyperparameters. If not found return -1
template<typename T>
int set_hyperparameter(const char* name, T* hyperparameter, List* hyperparameters, T default_value, Model* model,
                       HyperparameterValidation* validation) {
    int result = 0;
    foreach_cell(it, hyperparameters) {
        VariableSetStmt* current = lfirst_node(VariableSetStmt, it);
        
        if (strcmp(current->name, name) == 0) {
            if (list_length(current->args) > 1) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Hyperparameter %s cannot be a list", current->name)));
            }
            
            Value* value = NULL;
            if (current->args != NULL) {
                A_Const* aconst = NULL;
                aconst = linitial_node(A_Const, current->args);
                value = &aconst->val;
            }
            
            set_hyperparameter_value(name, hyperparameter, value, current->kind, default_value, model, validation);
            return result;
        }
        result++;
    }
    
    // If not set by user, set the default value
    set_hyperparameter_value(name, hyperparameter, NULL, VAR_SET_DEFAULT, default_value, model, validation);
    return -1;
}

#endif
