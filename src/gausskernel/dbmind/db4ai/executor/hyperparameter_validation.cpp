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
 * hyperparameter_validation.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/dbmind/db4ai/executor/hyperparameter_validation.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/hyperparameter_validation.h"

#include "db4ai/aifuncs.h"
#include "instruments/generate_report.h"
#include "nodes/plannodes.h"
#include "db4ai/db4ai_api.h"


///////////////////////////////////////////////////////////////////////////////

#define ARCHITECTURE_CONFIGURATION(name, hyperparameters, metrics, is_supervised)              \
    {                                                                                          \
        name, hyperparameters, ARRAY_LENGTH(hyperparameters), metrics, ARRAY_LENGTH(metrics),  \
        is_supervised                                                                          \
    }


const HyperparameterDefinition* get_hyperparameter_definitions(AlgorithmML algorithm, int32_t *result_size)
{
    AlgorithmAPI* api = get_algorithm_api(algorithm);
    return api->get_hyperparameters_definitions(api, result_size);
}

AlgorithmConfiguration *get_algorithm_configuration(AlgorithmML algorithm)
{
    switch (algorithm) {
        case INVALID_ALGORITHM_ML:
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Algorithm %d has no registered configuration", algorithm)));
    }
    return NULL;
}

const HyperparameterDefinition *find_hyperparameter_definition(const HyperparameterDefinition definitions[],
                                                               int32_t definitions_size,
                                                               const char *hyperparameter_name)
{
    for (int i = 0; i < definitions_size; i++) {
        if (0 == strcmp(definitions[i].name, hyperparameter_name)) {
            return &definitions[i];
        }
    }
    return NULL;
}

// Set the value of a hyperparameter structure
static void set_hyperparameter_datum(Hyperparameter *hyperp, Oid type, Datum value)
{
    if (type == ANYENUMOID) {  // Outside of hyperparameter module, treat them as strings
        hyperp->type = VARCHAROID;
        const char* str = DatumGetPointer(value);
        hyperp->value = PointerGetDatum(cstring_to_text(str));
    } else {
        hyperp->type = type;
        hyperp->value = value;
    }
}

static List *add_model_hyperparameter(List *hyperparameters, MemoryContext memcxt, const char *name, Oid type,
                                      Datum value)
{
    MemoryContext old_context = MemoryContextSwitchTo(memcxt);
    Hyperparameter *hyperp = (Hyperparameter *)palloc0(sizeof(Hyperparameter));
    hyperp->name = pstrdup(name);
    set_hyperparameter_datum(hyperp, type, value);

    hyperparameters = lappend(hyperparameters, hyperp);
    MemoryContextSwitchTo(old_context);

    return hyperparameters;
}

void update_model_hyperparameter(MemoryContext memcxt, List *hyperparameters, const char *name, Oid type, Datum value)
{
    MemoryContext old_context = MemoryContextSwitchTo(memcxt);
    ListCell *lc;
    foreach(lc, hyperparameters) {
        Hyperparameter *hyperp = lfirst_node(Hyperparameter, lc);
        if (strcmp(hyperp->name, name) == 0) {
            set_hyperparameter_datum(hyperp, type, value);
            break;
        }
    }
    MemoryContextSwitchTo(old_context);
}


inline const char *bool_to_str(bool value)
{
    return value ? "TRUE" : "FALSE";
}

static void ereport_hyperparameter(int level, const char *name, Datum value, Oid type)
{
    switch (type) {
        case INT4OID: {
            ereport(level, (errmsg("Hyperparameter %s takes value %d", name, DatumGetInt32(value))));
            break;
        }

        case INT8OID: {
            ereport(level, (errmsg("Hyperparameter %s takes value %ld", name, DatumGetInt64(value))));
            break;
        }

        case FLOAT8OID: {
            ereport(level, (errmsg("Hyperparameter %s takes value %f", name, DatumGetFloat8(value))));
            break;
        }

        case BOOLOID: {
            ereport(level, (errmsg("Hyperparameter %s takes value %s", name, bool_to_str(DatumGetBool(value)))));
            break;
        }

        case CSTRINGOID: {
            ereport(level, (errmsg("Hyperparameter %s takes value %s", name, DatumGetCString(value))));
            break;
        }

        case VARCHAROID: {
            char *str = Datum_to_string(value, type, false);
            ereport(level, (errmsg("Hyperparameter %s takes value %s", name, str)));
            pfree(str);
            break;
        }

        case ANYENUMOID: {
            ereport(level, (errmsg("Hyperparameter %s takes value %s", name, DatumGetCString(value))));
            break;
        }

        default: {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Hyperparameter type %d not yet supported", type)));
        }
    }
}



// Get hyperparameter in hyperparameter struct to the givne value in the datum. Definition is used for metadata
static Datum get_hyperparameter(const HyperparameterDefinition *definition, void *hyperparameter_struct)
{
    switch (definition->type) {
        case INT4OID: {
            int32_t *value_addr = (int32_t *)((char *)hyperparameter_struct + definition->offset);
            return Int32GetDatum(*value_addr);
            break;
        }

        case INT8OID: {
            int64_t *value_addr = (int64_t *)((char *)hyperparameter_struct + definition->offset);
            return Int64GetDatum(*value_addr);
            break;
        }

        case FLOAT8OID: {
            double *value_addr = (double *)((char *)hyperparameter_struct + definition->offset);
            return Float8GetDatum(*value_addr);
            break;
        }

        case BOOLOID: {
            bool *value_addr = (bool *)((char *)hyperparameter_struct + definition->offset);
            return BoolGetDatum(*value_addr);
            break;
        }

        case CSTRINGOID: {
            char **value_addr = (char **)((char *)hyperparameter_struct + definition->offset);
            return CStringGetDatum(*value_addr);
            break;
        }

        case ANYENUMOID: {
            void *value_addr = (void *)((char *)hyperparameter_struct + definition->offset);
            return CStringGetDatum(definition->validation.enum_getter(value_addr));
            break;
        }

        default:
            break;
    }
    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Hyperparameter type %d not yet supported", definition->type)));

    return PointerGetDatum(NULL);
}


// Set hyperparameter in hyperparameter struct to the givne value in the datum. Definition is used for metadata
static void set_hyperparameter(const HyperparameterDefinition *definition, Datum value, void *hyperparameter_struct)
{
    switch (definition->type) {
        case INT4OID: {
            int32_t *value_addr = (int32_t *)((char *)hyperparameter_struct + definition->offset);
            *value_addr = DatumGetInt32(value);
            break;
        }

        case INT8OID: {
            int64_t *value_addr = (int64_t *)((char *)hyperparameter_struct + definition->offset);
            *value_addr = DatumGetInt64(value);
            break;
        }

        case FLOAT8OID: {
            double *value_addr = (double *)((char *)hyperparameter_struct + definition->offset);
            *value_addr = DatumGetFloat8(value);
            break;
        }

        case BOOLOID: {
            bool *value_addr = (bool *)((char *)hyperparameter_struct + definition->offset);
            *value_addr = DatumGetBool(value);
            break;
        }

        case CSTRINGOID: {
            char **value_addr = (char **)((char *)hyperparameter_struct + definition->offset);
            *value_addr = DatumGetCString(value);
            break;
        }

        case ANYENUMOID: {
            void *value_addr = (void *)((char *)hyperparameter_struct + definition->offset);
            definition->validation.enum_setter(DatumGetCString(value), value_addr);
            break;
        }

        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Hyperparameter type %d not yet supported", definition->type)));
    }
}

static void validate_hyperparameter_string(const char *name, const char *value, const char *valid_values[],
                                           int32_t valid_values_size)
{
    if (valid_values != NULL) {
        bool found = false;
        for (int i = 0; i < valid_values_size; i++) {
            if (0 == strcmp(valid_values[i], value)) {
                found = true;
                break;
            }
        }
        if (!found) {
            StringInfo str = makeStringInfo();
            for (int i = 0; i < valid_values_size; i++) {
                if (i != 0) {
                    appendStringInfoString(str, ", ");
                }
                appendStringInfoString(str, (valid_values)[i]);
            }
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid hyperparameter value for %s. Valid values are: %s.", name, str->data)));
        }
    }
}

static void validate_hyperparameter(Datum value, Oid type, const HyperparameterValidation *validation, const char *name)
{
    switch (type) {
        case INT4OID: {
            bool out_of_range = (DatumGetInt32(value) < DatumGetInt32(validation->min_value) ||
                                 DatumGetInt32(value) > DatumGetInt32(validation->max_value));
            if (!validation->min_inclusive && DatumGetInt32(value) <= DatumGetInt32(validation->min_value))
                out_of_range = true;
            if (!validation->max_inclusive && DatumGetInt32(value) >= DatumGetInt32(validation->max_value))
                out_of_range = true;
            if (out_of_range) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be in the range %c%d,%d%c", name,
                                       validation->min_inclusive ? '[' : '(', DatumGetInt32(validation->min_value),
                                       DatumGetInt32(validation->max_value), validation->max_inclusive ? ']' : ')')));
            }
            break;
        }

        case INT8OID: {
            bool out_of_range = (DatumGetInt64(value) < DatumGetInt64(validation->min_value) ||
                                 DatumGetInt64(value) > DatumGetInt64(validation->max_value));
            if (!validation->min_inclusive && DatumGetInt64(value) <= DatumGetInt64(validation->min_value))
                out_of_range = true;
            if (!validation->max_inclusive && DatumGetInt64(value) >= DatumGetInt64(validation->max_value))
                out_of_range = true;
            if (out_of_range) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be in the range %c%ld,%ld%c", name,
                                       validation->min_inclusive ? '[' : '(', DatumGetInt64(validation->min_value),
                                       DatumGetInt64(validation->max_value), validation->max_inclusive ? ']' : ')')));
            }
            break;
        }

        case FLOAT8OID: {
            bool out_of_range = (DatumGetFloat8(value) < DatumGetFloat8(validation->min_value) ||
                                 DatumGetFloat8(value) > DatumGetFloat8(validation->max_value));
            if (!validation->min_inclusive && DatumGetFloat8(value) <= DatumGetFloat8(validation->min_value))
                out_of_range = true;
            if (!validation->max_inclusive && DatumGetFloat8(value) >= DatumGetFloat8(validation->max_value))
                out_of_range = true;
            if (out_of_range) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be in the range %c%.8g,%.8g%c", name,
                                       validation->min_inclusive ? '[' : '(', DatumGetFloat8(validation->min_value),
                                       DatumGetFloat8(validation->max_value), validation->max_inclusive ? ']' : ')')));
            }
            break;
        }

        case BOOLOID:  // No validation
            break;

        case CSTRINGOID:
            validate_hyperparameter_string(name, DatumGetCString(value), validation->valid_values,
                                           validation->valid_values_size);
            break;

        case ANYENUMOID:
            validate_hyperparameter_string(name, DatumGetCString(value), validation->valid_values,
                                           validation->valid_values_size);
            break;

        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Hyperparameter type %d not yet supported", type)));
    }
}

static Value *extract_value_from_variable_set_stmt(VariableSetStmt *stmt)
{
    if (list_length(stmt->args) > 1) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Hyperparameter %s cannot be a list", stmt->name)));
    }

    Value* value = NULL;
    if (stmt->args != NULL) {
        A_Const* aconst = NULL;
        aconst = linitial_node(A_Const, stmt->args);
        value = &aconst->val;
    }
    return value;
}

static Datum value_to_datum(Value *value, Oid expected_type, const char *name)
{
    Datum result = (Datum)0;
    if (value == NULL) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Hyperparameter %s cannot take NULL value", name)));
    }

    switch (expected_type) {
        case INT4OID: {
            if (value->type != T_Integer) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be an integer", name)));
            }
            result = Int32GetDatum(intVal(value));
            break;
        }

        case INT8OID: {
            if (value->type != T_Integer) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be an integer", name)));
            }
            result = Int64GetDatum(intVal(value));
            break;
        }

        case FLOAT8OID: {
            if (value->type == T_Float) {
                result = Float8GetDatum(floatVal(value));
            } else if (value->type == T_Integer) {
                result = Float8GetDatum((double)intVal(value));
            } else {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be a floating point number", name)));
            }
            break;
        }

        case BOOLOID: {
            if (value->type == T_String) {
                char *str = strVal(value);
                if (strcmp(str, "true") == 0) {
                    result = BoolGetDatum(true);
                } else if (strcmp(str, "false") == 0) {
                    result = BoolGetDatum(false);
                } else {
                    ereport(
                        ERROR,
                        (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("Hyperparameter %s is not a valid string for boolean (i.e. 'true' or 'false')", name)));
                }
            } else if (value->type == T_Integer) {
                result = BoolGetDatum((intVal(value) != 0));
            } else {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be a boolean or integer", name)));
            }
            break;
        }

        case CSTRINGOID: {
            if (value->type != T_String) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be a string", name)));
            }
            result = CStringGetDatum(strVal(value));
            break;
        }

        case ANYENUMOID: {
            if (value->type != T_String) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Hyperparameter %s must be a string", name)));
            }
            result = CStringGetDatum(strVal(value));
            break;
        }

        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Hyperparameter type %d not yet supported", expected_type)));
    }

    return result;
}

Datum extract_datum_from_variable_set_stmt(VariableSetStmt *stmt, const HyperparameterDefinition *definition)
{
    Datum selected_value = (Datum)0;
    if (stmt->kind == VAR_SET_DEFAULT) {
        selected_value = definition->default_value;
    } else if (stmt->kind == VAR_SET_VALUE) {
        Value* value = extract_value_from_variable_set_stmt(stmt);
        selected_value = value_to_datum(value, definition->type, definition->name);
    }

    return selected_value;
}

void configure_hyperparameters_vset(const HyperparameterDefinition definitions[], int32_t definitions_size,
                                    List *hyperparameters, void *configuration)
{
    foreach_cell(it, hyperparameters) {
        VariableSetStmt* current = lfirst_node(VariableSetStmt, it);

        const HyperparameterDefinition *definition =
            find_hyperparameter_definition(definitions, definitions_size, current->name);
        if (definition != NULL) {
            Datum selected_value = (Datum)0;
            selected_value = extract_datum_from_variable_set_stmt(current, definition);

            validate_hyperparameter(selected_value, definition->type, &definition->validation, definition->name);
            set_hyperparameter(definition, selected_value, configuration);
        } else
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid hyperparameter %s", current->name)));
    }
}

void configure_hyperparameters_modelw(const HyperparameterDefinition definitions[], int32_t definitions_size,
                                      List *hyperparameters, void *configuration)
{
    foreach_cell(it, hyperparameters) {
        Hyperparameter* current = lfirst_node(Hyperparameter, it);

        const HyperparameterDefinition *definition =
            find_hyperparameter_definition(definitions, definitions_size, current->name);
        if (definition != NULL) {
            if (definition->type == ANYENUMOID) {
                // special case
                if (current->type != VARCHAROID)
                    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Unexpected type for enum hyperparameter %s", current->name)));

                char* str = Datum_to_string(current->value, VARCHAROID, false);
                void *value_addr = (void*)((char *)configuration + definition->offset);
                definition->validation.enum_setter(str, value_addr);
                pfree(str);
            } else {
                if (definition->type != current->type)
                    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Unexpected type for hyperparameter %s", current->name)));

                set_hyperparameter(definition, current->value, configuration);
            }
        } else
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid hyperparameter %s", current->name)));
    }
}

void configure_hyperparameters(const HyperparameterDefinition definitions[], int32_t definitions_size,
                               const Hyperparameter *hyperparameters, int nhyperp, void *configuration)
{

    const Hyperparameter* current = hyperparameters;
    for (int h = 0; h < nhyperp; h++, current++) {
        const HyperparameterDefinition *definition =
            find_hyperparameter_definition(definitions, definitions_size, current->name);
        if (definition != NULL) {
            if (current->type != definition->type)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid hyperparameter value for %s", current->name)));
            
            validate_hyperparameter(current->value, definition->type, &definition->validation, definition->name);
            set_hyperparameter(definition, current->value, configuration);
        } else
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid hyperparameter %s", current->name)));
    }
}

List *prepare_model_hyperparameters(const HyperparameterDefinition *definitions, int32_t definitions_size,
                                    void *hyperparameter_struct, MemoryContext memcxt)
{
    List *hyperparameters = NULL;
    for (int i = 0; i < definitions_size; i++) {
        Datum current_value = get_hyperparameter(&definitions[i], hyperparameter_struct);
        hyperparameters =
            add_model_hyperparameter(hyperparameters, memcxt, definitions[i].name, definitions[i].type, current_value);
    }
    return hyperparameters;
}

void init_hyperparameters_with_defaults(const HyperparameterDefinition definitions[], int32_t definitions_size,
                                        void *hyperparameter_struct)
{
    for (int i = 0; i < definitions_size; i++) {
        set_hyperparameter(&definitions[i], definitions[i].default_value, hyperparameter_struct);
    }
}

void print_hyperparameters(int level, List *hyperparameters)
{
    foreach_cell(it, hyperparameters) {
        Hyperparameter* current = (Hyperparameter*) lfirst(it);
        ereport_hyperparameter(level, current->name, current->value, current->type);
    }
}