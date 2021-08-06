/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 *
 *
 * IDENTIFICATION
 * src/gausskernel/dbmind/db4ai/executor/hyperparameter_validation.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/hyperparameter_validation.h"

#include "nodes/plannodes.h"


#define ARRAY_LENGTH(x) sizeof(x) / sizeof((x)[0])

static void init_hyperparameter_validation(HyperparameterValidation *v, void *min_value, bool min_inclusive,
    void *max_value, bool max_inclusive, const char **valid_values, int32_t valid_values_size)
{
    v->min_value = min_value;
    v->min_inclusive = min_inclusive;
    v->max_value = max_value;
    v->max_inclusive = max_inclusive;
    v->valid_values = valid_values;
    v->valid_values_size = valid_values_size;
}

// Definitions of hyperparameters
#define HYPERPARAMETER_BOOL(name, default_value, struct_name, attribute)                                              \
    {                                                                                                                 \
        name, BoolGetDatum(default_value), PointerGetDatum(NULL), PointerGetDatum(NULL), NULL, NULL, BOOLOID, 0,      \
        offsetof(struct_name, attribute), false, false                                                                \
    }


#define HYPERPARAMETER_ENUM(name, default_value, enum_values, enum_values_size, enum_setter, struct_name, attribute)  \
    {                                                                                                                 \
        name, CStringGetDatum(default_value), PointerGetDatum(NULL), PointerGetDatum(NULL), enum_values, enum_setter, \
        ANYENUMOID, enum_values_size, offsetof(struct_name, attribute), false, false                                  \
    }


#define HYPERPARAMETER_INT4(name, default_value, min, min_inclusive, max, max_inclusive, struct_name, attribute) \
    {                                                                                                            \
        name, Int32GetDatum(default_value), Int32GetDatum(min), Int32GetDatum(max), NULL, NULL, INT4OID, 0,      \
        offsetof(struct_name, attribute), min_inclusive, max_inclusive                                           \
    }

#define HYPERPARAMETER_FLOAT8(name, default_value, min, min_inclusive, max, max_inclusive, struct_name, attribute) \
    {                                                                                                              \
        name, Float8GetDatum(default_value), Float8GetDatum(min), Float8GetDatum(max), NULL, NULL, FLOAT8OID, 0,   \
        offsetof(struct_name, attribute), min_inclusive, max_inclusive                                             \
    }

const char* gd_optimizer_ml[] = {"gd", "ngd"};
// Used by linear regression and logistic regression
HyperparameterDefinition logistic_regression_hyperparameter_definitions[] = {
    HYPERPARAMETER_INT4("batch_size", 1000, 1, true, INT32_MAX, true,
                        GradientDescent, batch_size),
                            HYPERPARAMETER_FLOAT8("decay", 0.95, 0.0, false, DBL_MAX, true,
                        GradientDescent, decay),
                            HYPERPARAMETER_FLOAT8("learning_rate", 0.8, 0.0, false, DBL_MAX, true,
                        GradientDescent, learning_rate),
                            HYPERPARAMETER_INT4("max_iterations", 100, 1, true, INT32_MAX, true,
                        GradientDescent, max_iterations),
                            HYPERPARAMETER_INT4("max_seconds", 0, 0, true, INT32_MAX, true,
                        GradientDescent, max_seconds),
                            HYPERPARAMETER_ENUM("optimizer", "gd", gd_optimizer_ml, ARRAY_LENGTH(gd_optimizer_ml), optimizer_ml_setter,
                        GradientDescent, optimizer),
                            HYPERPARAMETER_FLOAT8("tolerance", 0.0005, 0.0, false, DBL_MAX, true,
                        GradientDescent, tolerance),
                            HYPERPARAMETER_INT4("seed", 0, 0, true, INT32_MAX, true,
                        GradientDescent, seed),
                            HYPERPARAMETER_BOOL("verbose", false,
                        GradientDescent, verbose),
                                                                            };

HyperparameterDefinition svm_hyperparameter_definitions[] = {
    HYPERPARAMETER_INT4("batch_size", 1000, 1, true, INT32_MAX, true,
                        GradientDescent, batch_size),
                            HYPERPARAMETER_FLOAT8("decay", 0.95, 0.0, false, DBL_MAX, true,
                        GradientDescent, decay),
                            HYPERPARAMETER_FLOAT8("lambda", 0.01, 0.0, false, DBL_MAX, true,
                        GradientDescent, lambda),
                            HYPERPARAMETER_FLOAT8("learning_rate", 0.8, 0.0, false, DBL_MAX, true,
                        GradientDescent, learning_rate),
                            HYPERPARAMETER_INT4("max_iterations", 100, 1, true, INT32_MAX, true,
                        GradientDescent, max_iterations),
                            HYPERPARAMETER_INT4("max_seconds", 0, 0, true, INT32_MAX, true,
                        GradientDescent, max_seconds),
                            HYPERPARAMETER_ENUM("optimizer", "gd", gd_optimizer_ml, ARRAY_LENGTH(gd_optimizer_ml), optimizer_ml_setter,
                        GradientDescent, optimizer),
                            HYPERPARAMETER_FLOAT8("tolerance", 0.0005, 0.0, false, DBL_MAX, true,
                        GradientDescent, tolerance),
                            HYPERPARAMETER_INT4("seed", 0, 0, true, INT32_MAX, true,
                        GradientDescent, seed),
                            HYPERPARAMETER_BOOL("verbose", false,
                        GradientDescent, verbose),
                                                            };

HyperparameterDefinition kmeans_hyperparameter_definitions[] = {
    /* nothing to do now, will do when needing */
};


void get_hyperparameter_definitions(AlgorithmML algorithm, HyperparameterDefinition **result, int32_t *result_size)
{
    switch (algorithm) {
        case LOGISTIC_REGRESSION:
        case LINEAR_REGRESSION:
            *result = logistic_regression_hyperparameter_definitions;
            *result_size = ARRAY_LENGTH(logistic_regression_hyperparameter_definitions);
            break;

        case SVM_CLASSIFICATION:
            *result = svm_hyperparameter_definitions;
            *result_size = ARRAY_LENGTH(svm_hyperparameter_definitions);
            break;

        case KMEANS:
            *result = kmeans_hyperparameter_definitions;
            *result_size = ARRAY_LENGTH(kmeans_hyperparameter_definitions);
            break;

        case INVALID_ALGORITHM_ML:
        default:
            char *s = "logistic_regression, svm_classification, linear_regression, kmeans";
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Architecture is not supported. Supported architectures: %s", s)));
    }
}


static void add_model_hyperparameter(Model *model, const char *name, Oid type, Datum value)
{
    Hyperparameter *hyperp = (Hyperparameter *)palloc0(sizeof(Hyperparameter));
    hyperp->name = pstrdup(name);
    hyperp->type = type;
    hyperp->value = value;
    model->hyperparameters = lappend(model->hyperparameters, hyperp);
}

void update_model_hyperparameter(Model *model, const char *name, Oid type, Datum value) {
    ListCell *lc;
    foreach(lc, model->hyperparameters) {
        Hyperparameter *hyperp = lfirst_node(Hyperparameter, lc);
        if (strcmp(hyperp->name, name) == 0) {
            hyperp->type = type;
            hyperp->value = value;
            return;
        }
    }
    Assert(false);
}

// Set int hyperparameter
void set_hyperparameter_value(const char *name, int *hyperparameter, Value *value, VariableSetKind kind,
    int default_value, Model *model, HyperparameterValidation *validation)
{
    if (kind == VAR_SET_DEFAULT) {
        *hyperparameter = default_value;
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value DEFAULT (%d)", name, *hyperparameter)));
    } else if (kind == VAR_SET_VALUE) {
        if (value == NULL) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s cannot take NULL value", name)));
        } else if (value->type != T_Integer) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s must be an integer", name)));
        }
        *hyperparameter = intVal(value);
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value %d", name, *hyperparameter)));
    } else {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid hyperparameter value for %s ", name)));
    }

    add_model_hyperparameter(model, name, INT4OID, Int32GetDatum(*hyperparameter));
    if (validation->min_value != NULL && validation->max_value != NULL) {
        bool out_of_range =
            (*hyperparameter < *(int *)validation->min_value || *hyperparameter > *(int *)validation->max_value);
        if (!validation->min_inclusive && *hyperparameter <= *(int *)validation->min_value)
            out_of_range = true;
        if (!validation->max_inclusive && *hyperparameter >= *(int *)validation->max_value)
            out_of_range = true;
        if (out_of_range) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s must be in the range %c%d,%d%c", name, validation->min_inclusive ? '[' : '(',
                *(int *)validation->min_value, *(int *)validation->max_value, validation->max_inclusive ? ']' : ')')));
        }
    }
}


// Set double hyperparameter
void set_hyperparameter_value(const char *name, double *hyperparameter, Value *value, VariableSetKind kind,
    double default_value, Model *model, HyperparameterValidation *validation)
{
    if (kind == VAR_SET_DEFAULT) {
        *hyperparameter = default_value;
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value DEFAULT (%f)", name, *hyperparameter)));
    } else if (kind == VAR_SET_VALUE) {
        if (value == NULL) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s cannot take NULL value", name)));
        } else if (value->type == T_Float) {
            *hyperparameter = floatVal(value);
        } else if (value->type == T_Integer) {
            *hyperparameter = (double)intVal(value);
        } else {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s must be a floating point number", name)));
        }
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value %f", name, *hyperparameter)));
    } else {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid hyperparameter value for %s ", name)));
    }

    add_model_hyperparameter(model, name, FLOAT8OID, Float8GetDatum(*hyperparameter));
    if (validation->min_value != NULL && validation->max_value != NULL) {
        bool out_of_range =
            (*hyperparameter < *(double *)validation->min_value || *hyperparameter > *(double *)validation->max_value);
        if (!validation->min_inclusive && *hyperparameter <= *(double *)validation->min_value)
            out_of_range = true;
        if (!validation->max_inclusive && *hyperparameter >= *(double *)validation->max_value)
            out_of_range = true;
        if (out_of_range) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s must be in the range %c%.8g,%.8g%c", name,
                validation->min_inclusive ? '[' : '(', *(double *)validation->min_value,
                *(double *)validation->max_value, validation->max_inclusive ? ']' : ')')));
        }
    }
}


// Set string hyperparameter (no const)
void set_hyperparameter_value(const char *name, char **hyperparameter, Value *value, VariableSetKind kind,
    char *default_value, Model *model, HyperparameterValidation *validation)
{
    if (kind == VAR_SET_DEFAULT) {
        *hyperparameter = (char*)palloc((strlen(default_value) + 1) * sizeof(char));
        errno_t err = strcpy_s(*hyperparameter, strlen(default_value) + 1, default_value);
        securec_check(err, "\0", "\0");
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value DEFAULT (%s)", name, *hyperparameter)));
    } else if (kind == VAR_SET_VALUE) {
        if (value == NULL) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s cannot take NULL value", name)));
        } else if (value->type != T_String) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s must be a string", name)));
        }
        *hyperparameter = strVal(value);
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value %s", name, *hyperparameter)));
    } else {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid hyperparameter value for %s ", name)));
    }

    if (validation->valid_values != NULL) {
        bool found = false;
        for (int i = 0; i < validation->valid_values_size; i++) {
            if (0 == strcmp(validation->valid_values[i], *hyperparameter)) {
                found = true;
                break;
            }
        }
        if (!found) {
            StringInfo str = makeStringInfo();
            for (int i = 0; i < validation->valid_values_size; i++) {
                if (i != 0)
                    appendStringInfoString(str, ", ");
                appendStringInfoString(str, (validation->valid_values)[i]);
            }
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid hyperparameter value for %s. Valid values are: %s. (default is %s)", name, str->data,
                default_value)));
        }
    }
    add_model_hyperparameter(model, name, VARCHAROID, PointerGetDatum(cstring_to_text(*hyperparameter)));
}


inline const char *bool_to_str(bool value)
{
    return value ? "TRUE" : "FALSE";
}

// Set boolean hyperparameter
void set_hyperparameter_value(const char *name, bool *hyperparameter, Value *value, VariableSetKind kind,
    bool default_value, Model *model, HyperparameterValidation *validation)
{
    if (kind == VAR_SET_DEFAULT) {
        *hyperparameter = default_value;
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value DEFAULT (%s)", name, bool_to_str(*hyperparameter))));
    } else if (kind == VAR_SET_VALUE) {
        if (value == NULL) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s cannot take NULL value", name)));
        } else if (value->type == T_String) {
            char *str = strVal(value);
            if (strcmp(str, "true") == 0) {
                *hyperparameter = true;
            } else if (strcmp(str, "false") == 0) {
                *hyperparameter = false;
            } else {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Hyperparameter %s is not a valid string for boolean (i.e. 'true' or 'false')", name)));
            }
        } else if (value->type == T_Integer) {
            *hyperparameter = (intVal(value) != 0);
        } else {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s must be a boolean or integer", name)));
        }
        ereport(NOTICE, (errmsg("Hyperparameter %s takes value %s", name, bool_to_str(*hyperparameter))));
    } else {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid hyperparameter value for %s ", name)));
    }

    add_model_hyperparameter(model, name, BOOLOID, BoolGetDatum(*hyperparameter));
}


// Set the hyperparameters according to the definitions. In the process, the range and values of
// each parameter is validated
void configure_hyperparameters(AlgorithmML algorithm, List *hyperparameters, Model *model, void *hyperparameter_struct)
{
    HyperparameterDefinition *definitions;
    int32_t definitions_size;
    get_hyperparameter_definitions(algorithm, &definitions, &definitions_size);

    HyperparameterValidation validation;
    for (int32_t i = 0; i < definitions_size; i++) {
        switch (definitions[i].type) {
            case INT4OID: {
                int *value_addr = (int *)((char *)hyperparameter_struct + definitions[i].offset);
                int value_min = DatumGetInt32(definitions[i].min_value);
                int value_max = DatumGetInt32(definitions[i].max_value);
                init_hyperparameter_validation(&validation, &value_min, definitions[i].min_inclusive, &value_max,
                    definitions[i].max_inclusive, NULL, 0);
                set_hyperparameter<int>(definitions[i].name, value_addr, hyperparameters,
                    DatumGetInt32(definitions[i].default_value), model, &validation);
                break;
            }

            case FLOAT8OID: {
                double *value_addr = (double *)((char *)hyperparameter_struct + definitions[i].offset);
                double value_min = DatumGetFloat8(definitions[i].min_value);
                double value_max = DatumGetFloat8(definitions[i].max_value);
                init_hyperparameter_validation(&validation, &value_min, definitions[i].min_inclusive, &value_max,
                    definitions[i].max_inclusive, NULL, 0);
                set_hyperparameter<double>(definitions[i].name, value_addr, hyperparameters,
                    DatumGetFloat8(definitions[i].default_value), model, &validation);
                break;
            }

            case BOOLOID: {
                bool *value_addr = (bool *)((char *)hyperparameter_struct + definitions[i].offset);
                set_hyperparameter<bool>(definitions[i].name, value_addr, hyperparameters,
                    DatumGetBool(definitions[i].default_value), model, NULL);
                break;
            }

            case ANYENUMOID: {
                void *value_addr = (void *)((char *)hyperparameter_struct + definitions[i].offset);
                char *str = NULL;
                init_hyperparameter_validation(&validation, NULL, NULL, NULL, NULL, definitions[i].valid_values,
                    definitions[i].valid_values_size);
                set_hyperparameter<char *>(definitions[i].name, &str, hyperparameters,
                    DatumGetCString(definitions[i].default_value), model, &validation);
                definitions[i].enum_setter(str, value_addr);
                break;
            }


            default: {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Invalid hyperparameter OID %d for hyperparameter %s", definitions[i].type,
                    definitions[i].name)));
            }
        }
    }
}
