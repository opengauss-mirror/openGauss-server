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


// Probability distributions for HPO
enum class ProbabilityDistribution {
    UNIFORM_RANGE,
    LOG_RANGE,
    INVALID_DISTRIBUTION // internal, for checking
};

struct HyperparameterAutoML{
    bool enable;            // Set if AutoML can tune the hyperparameter
    ProbabilityDistribution distribution;
    int32_t steps;          // Steps considered for AutoML
    Datum min_value_automl; // Minimum value for AutoML (inclusive)
    Datum max_value_automl; // Max value for AutoML (exclusive)
};

struct HyperparameterValidation {
    Datum min_value;
    Datum max_value;
    bool min_inclusive;
    bool max_inclusive;
    const char **valid_values;
    int32_t valid_values_size;
    const char* (* enum_getter)(void *enum_addr);
    void (* enum_setter)(const char *s, void *enum_addr);
};


struct HyperparameterDefinition {
    const char *name;
    Oid type;
    Datum default_value;
    HyperparameterValidation validation;
    HyperparameterAutoML automl;
    int32_t offset;
    bool min_inclusive;
    bool max_inclusive;
};


struct AlgorithmConfiguration{
    AlgorithmML algorithm;
    HyperparameterDefinition* hyperparameters;
    int32_t hyperparameters_size;
    MetricML* available_metrics;
    int32_t available_metrics_size;
    bool is_supervised;
};

// Prepare the set of hyperparameter of a the model
List *prepare_model_hyperparameters(const HyperparameterDefinition definitions[], int32_t definitions_size,
                                    void* hyperparameter_struct, MemoryContext memcxt);

// Configure the hyperparameters of an algorithm or automl, using a list of VariableSetStmt
void configure_hyperparameters_vset(const HyperparameterDefinition definitions[],
                int32_t definitions_size, List *hyperparameters, void *configuration);

// Configure the hyperparameters of an algorithm or automl, using a list of Hyperparameter
void configure_hyperparameters(const HyperparameterDefinition definitions[],
                int32_t definitions_size, const Hyperparameter *hyperparameters, int nhyperp, void *configuration);

// Configure the hyperparameters of an algorithm or automl, using a list of Hyperparameter from the model warehouse
void configure_hyperparameters_modelw(const HyperparameterDefinition definitions[],
                int32_t definitions_size, List *hyperparameters, void *configuration);

// Extract the value for a hyperparameter from a VariableSetStmt
Datum extract_datum_from_variable_set_stmt(VariableSetStmt* stmt, const HyperparameterDefinition* definition);

// Return a hyperparameter with the given name from the hyperparameter list. Returns null if none is available
const HyperparameterDefinition* find_hyperparameter_definition(const HyperparameterDefinition definitions[],
            int32_t definitions_size, const char* hyperparameter_name);

// Get the hyperparameter definitions for one specific algorithm
const HyperparameterDefinition* get_hyperparameter_definitions(AlgorithmML algorithm,
                int32_t *result_size);

// Get the confgiuration parameters for an architecture
AlgorithmConfiguration* get_algorithm_configuration(AlgorithmML algorithm);

// Initialize a hyperparameter struct with the default values
void init_hyperparameters_with_defaults(const HyperparameterDefinition definitions[],
                int32_t defintions_size, void* hyperparameter_struct);

// Print the list of hyperparameters
void print_hyperparameters(int level, List* /*<Hyperparameter*>*/ hyperparameters);

// changes the final value of a hyperparameter into the model warehouse output
void update_model_hyperparameter(MemoryContext memcxt, List *hyperparameters, const char* name, Oid type, Datum value);

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

// AutoML Configuration for a hyperparameter
#define HP_AUTOML(automl_tuning, min_value_automl, max_value_automl, steps, distribution) \
            automl_tuning, min_value_automl, max_value_automl, steps, distribution
#define HP_AUTOML_INT(min_value_automl, max_value_automl, steps, distribution) \
            HP_AUTOML(true, min_value_automl, max_value_automl, steps, distribution)
#define HP_AUTOML_FLOAT(min_value_automl, max_value_automl, steps, distribution) \
            HP_AUTOML(true, min_value_automl, max_value_automl, steps, distribution)
#define HP_AUTOML_BOOL() HP_AUTOML(true,  0, 0, 0, ProbabilityDistribution::UNIFORM_RANGE)
#define HP_AUTOML_ENUM() HP_AUTOML(true,  0, 0, 0, ProbabilityDistribution::UNIFORM_RANGE)
#define HP_NO_AUTOML()   HP_AUTOML(false, 0, 0, 0, ProbabilityDistribution::INVALID_DISTRIBUTION)


// Definitions of hyperparameters. The following macors have an indirection level to allow
// definitions using other macros such as HP_AUTOML
#define HYPERPARAMETER_BOOL_INTERNAL(name, default_value,                                          \
                struct_name, attribute,                                                            \
                automl_tuning, min_value_automl, max_value_automl, steps, distribution)            \
    {                                                                                              \
        name, BOOLOID, BoolGetDatum(default_value),                                                \
        {PointerGetDatum(NULL), PointerGetDatum(NULL), false, false,                               \
        NULL, 0, NULL, NULL},                                                                      \
        {automl_tuning, distribution, steps, min_value_automl, max_value_automl},                  \
        offsetof(struct_name, attribute)                                                           \
    }

#define HYPERPARAMETER_BOOL(name, default_value, struct_name, attribute, automl)                   \
        HYPERPARAMETER_BOOL_INTERNAL(name, default_value, struct_name, attribute, automl)

///////////////////////////////////////////////////////////////////////////////

#define HYPERPARAMETER_ENUM_INTERNAL(name, default_value,                                              \
                enum_values, enum_values_size, enum_getter, enum_setter, struct_name, attribute,       \
                automl_tuning, min_value_automl, max_value_automl, steps, distribution)                \
    {                                                                                                  \
        name, ANYENUMOID, CStringGetDatum(default_value),                                              \
        {PointerGetDatum(NULL), PointerGetDatum(NULL), false, false,                                   \
        enum_values, enum_values_size, enum_getter, enum_setter},                                      \
        {automl_tuning, distribution, steps, min_value_automl, max_value_automl},                      \
        offsetof(struct_name, attribute)                                                               \
    }

#define HYPERPARAMETER_ENUM(name, default_value, enum_values, enum_values_size, enum_getter, enum_setter,           \
                struct_name, attribute, automl)                                                        \
        HYPERPARAMETER_ENUM_INTERNAL(name, default_value, enum_values, enum_values_size, enum_getter, enum_setter,  \
                struct_name, attribute, automl)

///////////////////////////////////////////////////////////////////////////////

#define HYPERPARAMETER_INT4_INTERNAL(name, default_value,                                              \
                min, min_inclusive, max, max_inclusive, struct_name, attribute,                        \
                automl_tuning, min_value_automl, max_value_automl, steps, distribution)                \
    {                                                                                                  \
        name, INT4OID, Int32GetDatum(default_value),                                                   \
        {Int32GetDatum(min), Int32GetDatum(max), min_inclusive, max_inclusive,                         \
        NULL, 0,  NULL, NULL},                                                                         \
        {automl_tuning, distribution, steps, Int32GetDatum(min_value_automl), Int32GetDatum(max_value_automl)}, \
        offsetof(struct_name, attribute)                                                               \
    }

#define HYPERPARAMETER_INT4(name, default_value, min, min_inclusive, max, max_inclusive,               \
                struct_name, attribute, automl)                                                        \
        HYPERPARAMETER_INT4_INTERNAL(name, default_value, min, min_inclusive, max, max_inclusive,      \
        struct_name, attribute, automl)

///////////////////////////////////////////////////////////////////////////////

#define HYPERPARAMETER_INT8_INTERNAL(name, default_value,                                              \
                min, min_inclusive, max, max_inclusive, struct_name, attribute,                        \
                automl_tuning, min_value_automl, max_value_automl, steps, distribution)                \
    {                                                                                                  \
        name, INT8OID, Int64GetDatum(default_value),                                                   \
        {Int64GetDatum(min), Int64GetDatum(max), min_inclusive, max_inclusive,                         \
        NULL, 0, NULL, NULL},                                                                          \
        {automl_tuning, distribution, steps, Int64GetDatum(min_value_automl), Int64GetDatum(max_value_automl)}, \
        offsetof(struct_name, attribute)                                                               \
    }

#define HYPERPARAMETER_INT8(name, default_value, min, min_inclusive, max, max_inclusive,               \
                struct_name, attribute, automl)                                                        \
        HYPERPARAMETER_INT8_INTERNAL(name, default_value, min, min_inclusive, max, max_inclusive,      \
        struct_name, attribute, automl)

///////////////////////////////////////////////////////////////////////////////

#define HYPERPARAMETER_FLOAT8_INTERNAL(name, default_value,                                                           \
                min, min_inclusive, max, max_inclusive, struct_name, attribute,                                       \
                automl_tuning, min_value_automl, max_value_automl, steps, distribution)                               \
    {                                                                                                                 \
        name, FLOAT8OID, Float8GetDatum(default_value),                                                               \
            {Float8GetDatum(min), Float8GetDatum(max), min_inclusive, max_inclusive,                                  \
            NULL, 0, NULL, NULL},                                                                                     \
            {automl_tuning, distribution, steps, Float8GetDatum(min_value_automl), Float8GetDatum(max_value_automl)}, \
            offsetof(struct_name, attribute)                                                                          \
    }

#define HYPERPARAMETER_FLOAT8(name, default_value, min, min_inclusive, max, max_inclusive,          \
                struct_name, attribute, automl)                                                     \
        HYPERPARAMETER_FLOAT8_INTERNAL(name, default_value, min, min_inclusive, max, max_inclusive, \
        struct_name, attribute, automl)

///////////////////////////////////////////////////////////////////////////////

#define HYPERPARAMETER_STRING_INTERNAL(name, default_value,                                \
                str_values, str_values_size, struct_name, attribute,                       \
                automl_tuning, min_value_automl, max_value_automl, steps, distribution)    \
    {                                                                                      \
        name, CSTRINGOID, CStringGetDatum(default_value),                                  \
        {PointerGetDatum(NULL), PointerGetDatum(NULL), false, false,                       \
        str_values, str_values_size, NULL, NULL},                                          \
        {automl_tuning, distribution, steps, min_value_automl, max_value_automl},          \
        offsetof(struct_name, attribute)                                                   \
    }

#define HYPERPARAMETER_STRING(name, default_value, str_values, str_values_size,            \
                struct_name, attribute, automl)                                            \
        HYPERPARAMETER_STRING_INTERNAL(name, default_value, str_values, str_values_size,   \
                struct_name, attribute, automl)

#endif
