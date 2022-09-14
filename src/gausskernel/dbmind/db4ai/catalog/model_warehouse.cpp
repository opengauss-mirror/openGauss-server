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
 * model_warehouse.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/catalog/model_warehouse.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/model_warehouse.h"
#include "db4ai/gd.h"
#include "db4ai/aifuncs.h"
#include "db4ai/db4ai_api.h"
#include "access/tableam.h"
#include "catalog/gs_model.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "instruments/generate_report.h"
#include "lib/stringinfo.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/bytea.h"

typedef enum ListType {
    HYPERPARAMETERS = 0,
    COEFS,
    SCORES,
} ListType;


template <ListType ltype> void ListToTuple(List *list, Datum *name, Datum *value, Datum *oid);

template <ListType listType> void TupleToList(Model *model, Datum *names, Datum *values, Datum *oids);

template <ListType ltype> static void add_model_parameter(Model *model, const char *name, Oid type, Datum value);

// Store the model in the catalog tables
void store_model(const Model *model)
{
    HeapTuple tuple;
    int rc;
    Relation rel = NULL;
    Oid extOwner = GetUserId();
    Datum values[Natts_gs_model_warehouse];
    bool nulls[Natts_gs_model_warehouse];
    Datum ListNames, ListValues, ListOids;

    if (SearchSysCacheExists1(DB4AI_MODEL, CStringGetDatum(model->model_name))) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The model name \"%s\" already exists in gs_model_warehouse.", model->model_name)));
    }

    rel = heap_open(ModelRelationId, RowExclusiveLock);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_gs_model_model_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(model->model_name));
    values[Anum_gs_model_owner_oid - 1] = ObjectIdGetDatum(extOwner);
    values[Anum_gs_model_create_time - 1] = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    values[Anum_gs_model_processedTuples - 1] = Int64GetDatum(model->processed_tuples);
    values[Anum_gs_model_discardedTuples - 1] = Int64GetDatum(model->discarded_tuples);
    values[Anum_gs_model_process_time_secs - 1] = Float4GetDatum(model->pre_time_secs);
    values[Anum_gs_model_exec_time_secs - 1] = Float4GetDatum(model->exec_time_secs);
    values[Anum_gs_model_iterations - 1] = Int64GetDatum(model->num_actual_iterations);
    values[Anum_gs_model_outputType - 1] = ObjectIdGetDatum(model->return_type);
    values[Anum_gs_model_query - 1] = CStringGetTextDatum(model->sql);

    values[Anum_gs_model_model_type - 1] = CStringGetTextDatum(algorithm_ml_to_string(model->algorithm));

    if (model->hyperparameters == nullptr) {
        nulls[Anum_gs_model_hyperparametersNames - 1] = true;
        nulls[Anum_gs_model_hyperparametersValues - 1] = true;
        nulls[Anum_gs_model_hyperparametersOids - 1] = true;
    } else {
        ListToTuple<ListType::HYPERPARAMETERS>(model->hyperparameters, &ListNames, &ListValues, &ListOids);
        values[Anum_gs_model_hyperparametersNames - 1] = ListNames;
        values[Anum_gs_model_hyperparametersValues - 1] = ListValues;
        values[Anum_gs_model_hyperparametersOids - 1] = ListOids;
    }

    if (model->scores == nullptr) {
        nulls[Anum_gs_model_trainingScoresName - 1] = true;
        nulls[Anum_gs_model_trainingScoresValue - 1] = true;
    } else {
        ListToTuple<ListType::SCORES>(model->scores, &ListNames, &ListValues, &ListOids);
        values[Anum_gs_model_trainingScoresName - 1] = ListNames;
        values[Anum_gs_model_trainingScoresValue - 1] = ListValues;
    }

    if (model->data.version != DB4AI_MODEL_UNDEFINED) {
        if (model->data.version >= DB4AI_MODEL_INVALID)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Invalid model version %d", model->data.version)));

        // prepare an array with the version and the content in hexadecimal format
        // add extra two for '\x', two for version
        int bc = VARHDRSZ + (model->data.size * 2) + 4;
        bytea* arr = (bytea*)palloc(bc);
        SET_VARSIZE(arr, bc);

        char* pdata = (char*)VARDATA(arr);
        *pdata++ = '\\';
        *pdata++ = 'x';

        char ch = (char)model->data.version;
        pdata += hex_encode(&ch, 1, pdata);
        hex_encode((char*)model->data.raw_data, model->data.size, pdata);

        values[Anum_gs_model_modelData - 1] = PointerGetDatum(arr);
    } else {
        if (model->model_data == 0)
            nulls[Anum_gs_model_modelData - 1] = true;
        else
            values[Anum_gs_model_modelData - 1] = model->model_data;
    }

    // DEPRECATED
    nulls[Anum_gs_model_weight - 1] = true;
    nulls[Anum_gs_model_modeldescribe - 1] = true;
    nulls[Anum_gs_model_coefNames - 1] = true;
    nulls[Anum_gs_model_coefValues - 1] = true;
    nulls[Anum_gs_model_coefOids - 1] = true;

    // create tuple and insert into model warehouse
    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    (void)simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(rel, RowExclusiveLock);
}

/* get SGD model */
void get_sgd_model_data(HeapTuple *tuple, ModelGradientDescent *resGD, Form_gs_model_warehouse tuplePointer)
{
    char *strValues;
    Datum dtValues;
    ArrayBuildState *astate = NULL;
    bool isnull = false;

    /*  weight */
    resGD->weights = SysCacheGetAttr(DB4AI_MODEL, *tuple, Anum_gs_model_weight, &isnull);

    /* categories */
    resGD->ncategories = 0;
    Datum dtCat = SysCacheGetAttr(DB4AI_MODEL, *tuple, Anum_gs_model_coefValues, &isnull);

    if (!isnull) {
        ArrayType *arrValues = DatumGetArrayTypeP(dtCat);
        ArrayIterator itValue = array_create_iterator(arrValues, 0);
        while (array_iterate(itValue, &dtValues, &isnull)) {
            resGD->ncategories++;
            strValues = TextDatumGetCString(dtValues);
            dtValues = string_to_datum(strValues, tuplePointer->outputtype);
            astate = accumArrayResult(astate, dtValues, false, tuplePointer->outputtype, CurrentMemoryContext);
        }
        resGD->categories = makeArrayResult(astate, CurrentMemoryContext);
    } else {
        resGD->categories = PointerGetDatum(NULL);
    }
}

char *splitStringFillCoordinates(WHCentroid *curseCent, char *strCoordinates, int dimension)
{
    char *cur, *context = NULL;
    Datum dtCur;
    int iter = 0;
    double *res = (double *)palloc0(dimension * sizeof(double));

    while (iter < dimension) {
        if (iter == 0) {
            cur = strtok_r(strCoordinates, ")(,", &context);
        } else {
            cur = strtok_r(NULL, ")(,", &context);
        }

        if (cur != NULL) {
            dtCur = string_to_datum(cur, FLOAT8OID);
            res[iter] = DatumGetFloat8(dtCur);
            iter++;
        } else {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("the Coordinates result seems not match their dimension or actual_num_centroids.")));
        }
    }
    curseCent->coordinates = res;

    return context;
}

void splitStringFillCentroid(WHCentroid *curseCent, char *strDescribe)
{
    char *cur, *name, *context = NULL;
    Datum dtCur;

    name = strtok_r(strDescribe, ":,", &context);
    cur = strtok_r(NULL, ":,", &context);
    while (cur != NULL and name != NULL) {
        if (strcmp(name, "id") == 0) {
            dtCur = string_to_datum(cur, INT8OID);
            curseCent->id = DatumGetUInt32(dtCur);
        } else if (strcmp(name, "objective_function") == 0) {
            dtCur = string_to_datum(cur, FLOAT8OID);
            curseCent->objective_function = DatumGetFloat8(dtCur);
        } else if (strcmp(name, "avg_distance_to_centroid") == 0) {
            dtCur = string_to_datum(cur, FLOAT8OID);
            curseCent->avg_distance_to_centroid = DatumGetFloat8(dtCur);
        } else if (strcmp(name, "min_distance_to_centroid") == 0) {
            dtCur = string_to_datum(cur, FLOAT8OID);
            curseCent->min_distance_to_centroid = DatumGetFloat8(dtCur);
        } else if (strcmp(name, "max_distance_to_centroid") == 0) {
            dtCur = string_to_datum(cur, FLOAT8OID);
            curseCent->max_distance_to_centroid = DatumGetFloat8(dtCur);
        } else if (strcmp(name, "std_dev_distance_to_centroid") == 0) {
            dtCur = string_to_datum(cur, FLOAT8OID);
            curseCent->std_dev_distance_to_centroid = DatumGetFloat8(dtCur);
        } else if (strcmp(name, "cluster_size") == 0) {
            dtCur = string_to_datum(cur, INT8OID);
            curseCent->cluster_size = DatumGetUInt64(dtCur);
        } else {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("this description should not be here in KMEANS: %s", cur)));
        }
        name = strtok_r(NULL, ":,", &context);
        cur = strtok_r(NULL, ":,", &context);
    }
}

void get_kmeans_model_data(HeapTuple *tuple, ModelKMeans *modelKmeans)
{
    Datum dtValue, dtName;
    bool isnull;
    char *strValue, *strName, *coordinates = NULL;
    uint32_t coefContainer;
    int offset = 0;
    WHCentroid *curseCent;

    modelKmeans->model.algorithm = KMEANS;

    /* coef */
    Datum dtCoefValues = SysCacheGetAttr(DB4AI_MODEL, *tuple, Anum_gs_model_coefValues, &isnull);
    ArrayType *arrValues = DatumGetArrayTypeP(dtCoefValues);
    ArrayIterator itValue = array_create_iterator(arrValues, 0);

    Datum dtCoefNames = SysCacheGetAttr(DB4AI_MODEL, *tuple, Anum_gs_model_coefNames, &isnull);
    ArrayType *arrNames = DatumGetArrayTypeP(dtCoefNames);
    ArrayIterator itName = array_create_iterator(arrNames, 0);

    int decimal_scale = 10;
    while (array_iterate(itName, &dtName, &isnull)) {
        array_iterate(itValue, &dtValue, &isnull);
        strName = TextDatumGetCString(dtName);
        strValue = TextDatumGetCString(dtValue);
        coefContainer = strtol(strValue, NULL, decimal_scale);
        if (strcmp(strName, "original_num_centroids") == 0) {
            modelKmeans->original_num_centroids = coefContainer;
        } else if (strcmp(strName, "actual_num_centroids") == 0) {
            modelKmeans->actual_num_centroids = coefContainer;
        } else if (strcmp(strName, "seed") == 0) {
            modelKmeans->seed = coefContainer;
        } else if (strcmp(strName, "dimension") == 0) {
            modelKmeans->dimension = coefContainer;
        } else if (strcmp(strName, "distance_function_id") == 0) {
            modelKmeans->distance_function_id = coefContainer;
        } else if (strcmp(strName, "coordinates") == 0) {
            coordinates = strValue;
        } else {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("the coef should not be here in KMEANS: %s", strName)));
        }
    }

    modelKmeans->centroids =
        reinterpret_cast<WHCentroid *>(palloc0(sizeof(WHCentroid) * modelKmeans->actual_num_centroids));

    /* describe */
    Datum dtDescribe = SysCacheGetAttr(DB4AI_MODEL, *tuple, Anum_gs_model_modeldescribe, &isnull);
    ArrayType *arrDescribe = DatumGetArrayTypeP(dtDescribe);
    ArrayIterator itDescribe = array_create_iterator(arrDescribe, 0);

    while (array_iterate(itDescribe, &dtName, &isnull)) {
        curseCent = modelKmeans->centroids + offset;
        strName = TextDatumGetCString(dtName);
        coordinates = splitStringFillCoordinates(curseCent, coordinates, modelKmeans->dimension);
        splitStringFillCentroid(curseCent, strName);
        offset++;
    }
}

void setup_model_data_v0(Model *model, const bool &only_model, const AlgorithmML &algorithm, const char *modelType,
                         HeapTuple tuple, Form_gs_model_warehouse tuplePointer, void *result)
{
    switch (algorithm) {
        case LOGISTIC_REGRESSION:
        case SVM_CLASSIFICATION:
        case LINEAR_REGRESSION: {
            result = palloc0(sizeof(ModelGradientDescent));
            ModelGradientDescent *resGD = (ModelGradientDescent *)result;
            get_sgd_model_data(&tuple, resGD, tuplePointer);
            model = &(resGD->model);
        } break;
        case KMEANS: {
            result = palloc0(sizeof(ModelKMeans));
            ModelKMeans *resKmeans = (ModelKMeans *)result;
            get_kmeans_model_data(&tuple, resKmeans);
            model = &(resKmeans->model);
        } break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("the type of model is invalid: %s", modelType)));
            break;
    }
}

void setup_model_data_v1(Model *model, HeapTuple &tuple, const bool only_model, bool &isnull)
{
    if (!only_model) {
        model->model_data = SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_modelData, &isnull);
        if (isnull) {
            model->model_data = 0;
            return;
        }

        bytea *arr = (bytea *)pg_detoast_datum((struct varlena *)DatumGetPointer(model->model_data));
        char *pdata = (char *)VARDATA(arr);

        char ch;
        hex_decode(pdata + 2, 2, &ch);  // skip '\x'
        model->data.version = (SerializedModelVersion)ch;
        if (model->data.version == DB4AI_MODEL_UNDEFINED || model->data.version >= DB4AI_MODEL_INVALID)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                            errmsg("Invalid model version %d", model->data.version)));

        model->data.size = (VARSIZE(arr) - 4 - VARHDRSZ) / 2;
        model->data.raw_data = palloc(model->data.size);
        hex_decode(pdata + 4, model->data.size * 2, (char *)model->data.raw_data);

        if (PointerGetDatum(arr) != model->model_data) pfree(arr);
    }
}

static const size_t model_gradient_descent_size = sizeof(ModelGradientDescent);
static const size_t model_kmeans_size = sizeof(ModelKMeans);
static const size_t model_size[] = {[LOGISTIC_REGRESSION] = model_gradient_descent_size,
                                    [SVM_CLASSIFICATION] = model_gradient_descent_size,
                                    [LINEAR_REGRESSION] = model_gradient_descent_size,
                                    [PCA] = 0,
                                    [KMEANS] = model_kmeans_size};

// Get the model from the catalog tables
const Model *get_model(const char *model_name, bool only_model)
{
    void *result = NULL;
    Model *model = NULL;
    Datum ListNames, ListValues, ListOids;
    bool isnull = false;
    bool isnullValue = false;
    bool isnullOid = false;
    AlgorithmML algorithm;

    if (t_thrd.proc->workingVersionNum < 92366) {
        ereport(WARNING, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before GRAND VERSION NUM 92366, we do not support gs_model_warehouse.")));
        return NULL;
    }

    HeapTuple tuple = SearchSysCache1(DB4AI_MODEL, CStringGetDatum(model_name));
    if (!HeapTupleIsValid(tuple)) {
        ereport(WARNING, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("There is no model called \"%s\".", model_name)));
        return NULL;
    }

    Form_gs_model_warehouse tuplePointer = (Form_gs_model_warehouse)GETSTRUCT(tuple);
    const char *modelType = TextDatumGetCString(SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_model_type, &isnull));

    algorithm = get_algorithm_ml(modelType);
    if (algorithm >= INVALID_ALGORITHM_ML) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the type of model is invalid: %s", modelType)));
    }

    model = (Model *) palloc0(sizeof(Model));
    setup_model_data_v1(model, tuple, only_model, isnull);
    if (model->model_data == 0) {
        pfree(model);
        if (only_model) {
            result = palloc0(sizeof(Model));
            model = (Model *)result;
        } else if (algorithm == LOGISTIC_REGRESSION || algorithm == SVM_CLASSIFICATION ||
                   algorithm == LINEAR_REGRESSION || algorithm == KMEANS) {
            result = palloc0(model_size[algorithm]);
            setup_model_data_v0(model, only_model, algorithm, modelType, tuple, tuplePointer, result);
            model->data.version = DB4AI_MODEL_V00;
        } else {
            result = palloc0(sizeof(Model));
            model = (Model *)result;
        }
    }

    model->algorithm = algorithm;
    model->model_name = model_name;
    model->exec_time_secs = tuplePointer->exectime;
    model->pre_time_secs = tuplePointer->preprocesstime;
    model->processed_tuples = tuplePointer->processedtuples;
    model->discarded_tuples = tuplePointer->discardedtuples;
    model->return_type = tuplePointer->outputtype;
    model->num_actual_iterations = tuplePointer->iterations;
    model->sql = TextDatumGetCString(SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_query, &isnull));
    model->memory_context = CurrentMemoryContext;

    ListNames = SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_hyperparametersNames, &isnull);
    ListValues = SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_hyperparametersValues, &isnullValue);
    ListOids = SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_hyperparametersOids, &isnullOid);
    if (!isnull && !isnullValue && !isnullOid) {
        TupleToList<ListType::HYPERPARAMETERS>(model, &ListNames, &ListValues, &ListOids);
    }

    ListNames = SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_trainingScoresName, &isnull);
    ListValues = SysCacheGetAttr(DB4AI_MODEL, tuple, Anum_gs_model_trainingScoresValue, &isnullValue);
    if (!isnull && !isnullValue) {
        TupleToList<ListType::SCORES>(model, &ListNames, &ListValues, NULL);
    }

    // DEPRECATED
    model->weights = 0;

    ReleaseSysCache(tuple);
    return model;
}

void elog_model(int level, const Model *model)
{
    Oid typoutput;
    bool typIsVarlena;
    ListCell *lc;
    StringInfoData buf;
    initStringInfo(&buf);
    const char* model_type = algorithm_ml_to_string(model->algorithm);

    appendStringInfo(&buf, "\n:type %s", model_type);
    if (model->sql != NULL)
        appendStringInfo(&buf, "\n:sql %s", model->sql);
    if (model->hyperparameters != nullptr) {
        appendStringInfoString(&buf, "\n:hyperparameters");
        foreach (lc, model->hyperparameters) {
            Hyperparameter *hyperp = lfirst_node(Hyperparameter, lc);
            getTypeOutputInfo(hyperp->type, &typoutput, &typIsVarlena);
            appendStringInfo(&buf, "\n   :%s %s", hyperp->name, OidOutputFunctionCall(typoutput, hyperp->value));
        }
    }
    appendStringInfo(&buf, "\n:return type %u", model->return_type);
    appendStringInfo(&buf, "\n:pre-processing time %lf s", model->pre_time_secs);
    appendStringInfo(&buf, "\n:exec time %lf s", model->exec_time_secs);
    appendStringInfo(&buf, "\n:processed %ld tuples", model->processed_tuples);
    appendStringInfo(&buf, "\n:discarded %ld tuples", model->discarded_tuples);
    if (model->train_info != nullptr) {
        appendStringInfoString(&buf, "\n:info");
        foreach (lc, model->train_info) {
            TrainingInfo *info = lfirst_node(TrainingInfo, lc);
            getTypeOutputInfo(info->type, &typoutput, &typIsVarlena);
            appendStringInfo(&buf, "\n   :%s %s", info->name, OidOutputFunctionCall(typoutput, info->value));
        }
    }
    if (model->scores != nullptr) {
        appendStringInfoString(&buf, "\n:scores");
        foreach (lc, model->scores) {
            TrainingScore *score = lfirst_node(TrainingScore, lc);
            appendStringInfo(&buf, "\n   :%s %.16g", score->name, score->value);
        }
    }

    AlgorithmAPI* api = get_algorithm_api(model->algorithm);
    if (api->explain != nullptr) {
        Oid typoutput;
        bool typIsVarlena;
        ListCell* lc;
        List* infos = api->explain(api, &model->data, model->return_type);
        foreach(lc, infos) {
            TrainingInfo* info = lfirst_node(TrainingInfo, lc);
            getTypeOutputInfo(info->type, &typoutput, &typIsVarlena);
            appendStringInfo(&buf, "\n:%s %s", info->name, OidOutputFunctionCall(typoutput, info->value));
        }
    }

    elog(level, "Model=%s%s", model->model_name, buf.data);
    pfree(buf.data);
}

template <ListType ltype> void ListToTuple(List *list, Datum *name, Datum *value, Datum *oid)
{
    text *t_names, *t_values;
    Datum *array_container = nullptr;
    int iter = 0;
    ArrayBuildState *astateName = NULL, *astateValue = NULL;

    array_container = (Datum *)palloc0(list->length * sizeof(Datum));
    foreach_cell(it, list)
    {
        switch (ltype) {
            case ListType::HYPERPARAMETERS: {
                Hyperparameter *cell = (Hyperparameter *)lfirst(it);
                t_names = cstring_to_text(cell->name);
                t_values = cstring_to_text(Datum_to_string(cell->value, cell->type, false));
                array_container[iter] = ObjectIdGetDatum(cell->type);
                astateName =
                    accumArrayResult(astateName, PointerGetDatum(t_names), false, TEXTOID, CurrentMemoryContext);
                astateValue =
                    accumArrayResult(astateValue, PointerGetDatum(t_values), false, TEXTOID, CurrentMemoryContext);
                iter++;
            } break;
            case ListType::COEFS: {
                Oid typeOut;
                bool isvarlena;
                TrainingInfo *cell = (TrainingInfo *)lfirst(it);
                t_names = cstring_to_text(cell->name);

                getTypeOutputInfo(cell->type, &typeOut, &isvarlena);
                t_values = cstring_to_text(OidOutputFunctionCall(typeOut, cell->value));

                array_container[iter] = ObjectIdGetDatum(cell->type);
                astateName =
                    accumArrayResult(astateName, PointerGetDatum(t_names), false, TEXTOID, CurrentMemoryContext);
                astateValue =
                    accumArrayResult(astateValue, PointerGetDatum(t_values), false, TEXTOID, CurrentMemoryContext);
                iter++;
            } break;
            case ListType::SCORES: {
                TrainingScore *cell = (TrainingScore *)lfirst(it);
                t_names = cstring_to_text(cell->name);
                array_container[iter] = Float4GetDatum(cell->value);
                astateName =
                    accumArrayResult(astateName, PointerGetDatum(t_names), false, TEXTOID, CurrentMemoryContext);
                iter++;
            } break;
            default: {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Not support saving this data into model warehouse.")));
            } break;
        }
    }

    switch (ltype) {
        case ListType::HYPERPARAMETERS:
        case ListType::COEFS: {
            *name = makeArrayResult(astateName, CurrentMemoryContext);
            *value = makeArrayResult(astateValue, CurrentMemoryContext);
            ArrayType *oid_array = construct_array(array_container, list->length, OIDOID, sizeof(Oid), true, 'i');
            *oid = PointerGetDatum(oid_array);
        } break;
        case ListType::SCORES: {
            *name = makeArrayResult(astateName, CurrentMemoryContext);
            ArrayType *value_array =
                construct_array(array_container, list->length, FLOAT4OID, sizeof(float4), FLOAT4PASSBYVAL, 'i');
            *value = PointerGetDatum(value_array);
        } break;
        default: {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support saving this data into model warehouse.")));
            break;
        }
    }
}

template <ListType listType> void TupleToList(Model *model, Datum *names, Datum *values, Datum *oids)
{
    char *strNames, *strValues;
    Oid tranOids;
    Datum dtNames, dtValues, dtOid;
    ArrayType *arrNames, *arrValues, *arrOids;
    bool isnull;
    ArrayIterator itOid = NULL;

    arrNames = DatumGetArrayTypeP(*names);
    arrValues = DatumGetArrayTypeP(*values);

    ArrayIterator itName = array_create_iterator(arrNames, 0);
    ArrayIterator itValue = array_create_iterator(arrValues, 0);
    if (oids != NULL) {
        arrOids = DatumGetArrayTypeP(*oids);
        itOid = array_create_iterator(arrOids, 0);
    }
    while (array_iterate(itName, &dtNames, &isnull)) {
        array_iterate(itValue, &dtValues, &isnull);
        switch (listType) {
            case ListType::HYPERPARAMETERS: {
                array_iterate(itOid, &dtOid, &isnull);
                strNames = TextDatumGetCString(dtNames);
                strValues = TextDatumGetCString(dtValues);
                tranOids = DatumGetObjectId(dtOid);
                dtValues = string_to_datum(strValues, tranOids);
                add_model_parameter<listType>(model, strNames, tranOids, dtValues);
            } break;
            case ListType::COEFS: {
                array_iterate(itOid, &dtOid, &isnull);
                strNames = TextDatumGetCString(dtNames);
                strValues = TextDatumGetCString(dtValues);

                Oid typInput, typIOParam;
                getTypeInputInfo(dtOid, &typInput, &typIOParam);

                dtValues = OidInputFunctionCall(typInput, strValues, typIOParam, -1);

                tranOids = DatumGetObjectId(dtOid);
                add_model_parameter<listType>(model, strNames, tranOids, dtValues);
            } break;
            case ListType::SCORES: {
                strNames = TextDatumGetCString(dtNames);
                add_model_parameter<listType>(model, strNames, 0, dtValues);
            } break;
            default: {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Not support fetching this data from model warehouse.")));
                return;
            }
        }
    }
    return;
}

template <ListType ltype> static void add_model_parameter(Model *model, const char *name, Oid type, Datum value)
{
    switch (ltype) {
        case ListType::HYPERPARAMETERS: {
            Hyperparameter *hyperp = (Hyperparameter *)palloc0(sizeof(Hyperparameter));
            hyperp->name = pstrdup(name);
            hyperp->type = type;
            hyperp->value = value;
            model->hyperparameters = lappend(model->hyperparameters, hyperp);
        } break;
        case ListType::COEFS: {
            TrainingInfo *tinfo = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
            tinfo->name = pstrdup(name);
            tinfo->type = type;
            tinfo->value = value;
            model->train_info = lappend(model->train_info, tinfo);
        } break;
        case ListType::SCORES: {
            TrainingScore *tscore = (TrainingScore *)palloc0(sizeof(TrainingScore));
            tscore->name = pstrdup(name);
            tscore->value = DatumGetFloat4(value);
            model->scores = lappend(model->scores, tscore);
        } break;
        default: {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support put this data into Model-struct.")));
            return;
        }
    }
}
