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
 * -------------------------------------------------------------------------
 *
 * ml_model.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/optimizer/util/learn/ml_model.cpp
 *
 * DESCRIPTION
 * The general utilities and APIs of machine learning models
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "cjson/cJSON.h"
#include "funcapi.h"
#include "optimizer/comm.h"
#include "optimizer/dynsmp.h"
#include "optimizer/encoding.h"
#include "optimizer/learn.h"
#include "optimizer/ml_model.h"
#include "optimizer/plan_tree_model.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

static void ModelTrainInternal(const char* templateName, const char* modelName, ModelAccuracy** mAcc);
static ModelPredictInfo* ModelPredictInternal(const char* templateName, const char* modelName, char* filepath);
static bool ConfigurationIsValid(const Form_gs_opt_model modelinfo);
static ModelTrainInfo* GetModelTrainInfo(const cJSON* root);
static List* GetPlanstateList(PlanState* resultPlan);
static void HandleSubPlan(PlanState* resultPlan, PlannedStmt* pstmt, uint64 qid, FILE* fpout);
static int64* JsonGetInt64Array(cJSON* root, int* length);
static ModelPredictInfo* GetModelPredictInfo(const cJSON* root);
static void SubPlanAssignPrediction(PlanState* resultPlan, ModelPredictInfo* info);
static TupleDesc InitTupleVal(int modelTrainAttrnum);
static void UpdateTrainRes(Datum* values, Datum* datumsMax, Datum* datumsAcc, int nLabel, ModelAccuracy** mAcc,
    const ModelTrainInfo* info, char* labels);

/* *
 * @Description: registered function for model training procedure
 * @in templateName: template name of machine learning model to call
 * modelName: model instance name to call
 * @return set of record which contains all results
 */
Datum model_train_opt(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("AiEngine is not available in multipule nodes mode")));
#endif
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to use model_train_opt()")));
    }

    const static int modelTrainAttrnum = 4;
    char* templateName = NULL;
    char* modelName = NULL;
    /* function args */
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("TemplateName should not be NULL.")));
    } else {
        templateName = (char*)(text_to_cstring(PG_GETARG_TEXT_P(0)));
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("ModelName should not be NULL.")));
    } else {
        modelName = (char*)(text_to_cstring(PG_GETARG_TEXT_P(1)));
    }

    /* initialize tuple to return */
    TupleDesc tupdesc = InitTupleVal(modelTrainAttrnum);
    Datum values[modelTrainAttrnum];
    bool nulls[modelTrainAttrnum];

    HeapTuple tuple;
    Datum result;
    int i = -1;

    /* calls the worker */
    ModelAccuracy* mAcc = (ModelAccuracy*)palloc0(sizeof(ModelAccuracy));
    ModelTrainInternal((const char*)templateName, (const char*)modelName, &mAcc);
    errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* fill in all attribute value */
    if (mAcc->startup_time_accuracy >= 0) {
        values[++i] = Float8GetDatum(mAcc->startup_time_accuracy);
    } else {
        nulls[++i] = true;
    }

    if (mAcc->total_time_accuracy >= 0) {
        values[++i] = Float8GetDatum(mAcc->total_time_accuracy);
    } else {
        nulls[++i] = true;
    }

    if (mAcc->rows_accuracy >= 0) {
        values[++i] = Float8GetDatum(mAcc->rows_accuracy);
    } else {
        nulls[++i] = true;
    }

    if (mAcc->peak_memory_accuracy >= 0) {
        values[++i] = Float8GetDatum(mAcc->peak_memory_accuracy);
    } else {
        nulls[++i] = true;
    }
    pfree(mAcc);
    pfree_ext(templateName);
    pfree_ext(modelName);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    PG_RETURN_DATUM(result);
}

/* *
 * @Description: registered function for to track model training process
 * @in templateName:
 * modelName: model instance name to call
 * @return file path of model training log
 */
Datum track_model_train_opt(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("AiEngine is not available in multipule nodes mode")));
#endif
    char* templateName = NULL;
    char* modelName = NULL;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("TemplateName should not be NULL.")));
    } else {
        templateName = (char*)(text_to_cstring(PG_GETARG_TEXT_P(0)));
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("ModelName should not be NULL.")));
    } else {
        modelName = (char*)(text_to_cstring(PG_GETARG_TEXT_P(1)));
    }
    
    char* labels = NULL;
    int nLabel;
    char* buf = (char*)palloc0(sizeof(char) * CURL_BUF_SIZE);
    Form_gs_opt_model modelinfo = CheckModelTargets(templateName, modelName, &labels, &nLabel);
    AiEngineConnInfo* conninfo = (AiEngineConnInfo*)palloc0(sizeof(AiEngineConnInfo));
    char portStr[PORT_LEN] = {'\0'};
    errno_t ret = sprintf_s(portStr, PORT_LEN, "%d", modelinfo->port);
    securec_check_ss(ret, "\0", "\0");
    conninfo->host = pstrdup(modelinfo->ip.data);
    conninfo->port = pstrdup(portStr);
    conninfo->request_api = pstrdup(PYTHON_SERVER_ROUTE_TRACK);
    conninfo->header = pstrdup(PYTHON_SERVER_HEADER_JSON);
    cJSON* jsonObj = cJSON_CreateObject();
    cJSON_AddStringToObject(jsonObj, "modelName", modelName);
    conninfo->json_string = pstrdup(cJSON_Print(jsonObj));
    cJSON_Delete(jsonObj);
    if (!TryConnectRemoteServer(conninfo, &buf)) {
        pfree_ext(buf);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("AI engine connection failed.")));
    }
    switch (buf[0]) {
        case 'F': {
            pfree_ext(buf);
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("The training log of %s hasn't been generated by AiEngine.", modelName)));
            break;
        }
        case 'M': {
            pfree_ext(buf);
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("AI engine internal error: missing or wrong key.")));
            break;
        }
        default: {
            break;
        }
    }
    ereport(INFO, (errmodule(MOD_OPT_AI),
                   errmsg("The training process of %s is recorded in the following file:", modelName)));
    
    pfree_ext(templateName);
    pfree_ext(modelName);

    PG_RETURN_TEXT_P(cstring_to_text(buf));
}

static void UpdateTrainRes(Datum* values, Datum* datumsMax, Datum* datumsAcc, int nLabel, ModelAccuracy** mAcc,
    const ModelTrainInfo* info, char* labels)
{
    ArrayType* arrMax = NULL;
    ArrayType* arrAcc = NULL;
    int index = 0;
    values[Anum_gs_opt_model_feature_size - 1] = Int32GetDatum(info->feature_size);
    values[Anum_gs_opt_model_available - 1] = BoolGetDatum(info->available);
    values[Anum_gs_opt_model_is_training - 1] = BoolGetDatum(false);

    for (char* label = labels; *label; label++) {
        switch (*label) {
            case gs_opt_model_label_startup_time:
                (*mAcc)->startup_time_accuracy = info->acc[LABEL_START_TIME_INDEX];
                datumsMax[index] = Int64GetDatum(info->max[LABEL_START_TIME_INDEX]);
                datumsAcc[index++] = Float4GetDatum(info->acc[LABEL_START_TIME_INDEX]);
                break;
            case gs_opt_model_label_total_time:
                (*mAcc)->total_time_accuracy = info->acc[LABEL_TOTAL_TIME_INDEX];
                datumsMax[index] = Int64GetDatum(info->max[LABEL_TOTAL_TIME_INDEX]);
                datumsAcc[index++] = Float4GetDatum(info->acc[LABEL_TOTAL_TIME_INDEX]);
                break;
            case gs_opt_model_label_rows:
                (*mAcc)->rows_accuracy = info->acc[LABEL_ROWS_INDEX];
                datumsMax[index] = Int64GetDatum(info->max[LABEL_ROWS_INDEX]);
                datumsAcc[index++] = Float4GetDatum(info->acc[LABEL_ROWS_INDEX]);
                break;
            case gs_opt_model_label_peak_memory:
                (*mAcc)->peak_memory_accuracy = info->acc[LABEL_PEAK_MEMEORY_INDEX];
                datumsMax[index] = Int64GetDatum(info->max[LABEL_PEAK_MEMEORY_INDEX]);
                datumsAcc[index++] = Float4GetDatum(info->acc[LABEL_PEAK_MEMEORY_INDEX]);
                break;
            default:
                break;
        }
    }

    arrMax = construct_array(datumsMax, nLabel, INT8OID, sizeof(int64), FLOAT8PASSBYVAL, 'i');
    arrAcc = construct_array(datumsAcc, nLabel, FLOAT4OID, sizeof(float4), FLOAT4PASSBYVAL, 'i');
    values[Anum_gs_opt_model_max - 1] = PointerGetDatum(arrMax);
    values[Anum_gs_opt_model_acc - 1] = PointerGetDatum(arrAcc);
}

static void ModelTrainInternal(const char* templateName, const char* modelName, ModelAccuracy** mAcc)
{
    char* labels = NULL;
    int nLabel;
    /* 0. search in catalog to get model information */
    Form_gs_opt_model modelinfo = CheckModelTargets(templateName, modelName, &labels, &nLabel);
    if (!ConfigurationIsValid(modelinfo)) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Model configuration contains illegal values, please check for template name %s "
                       "model name %s", templateName, modelName)));
    }
    /* 1. calls related model_train PG_FUNCTION */
    char* trainResultJson = TreeModelTrain(modelinfo, labels);
    /* 2. parse the returned json cstring and handle exceptions */
    ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("AIEngine Returns JSON:\n%s", trainResultJson)));

    cJSON* jsonObj = cJSON_Parse(trainResultJson);
    if (jsonObj == NULL) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NODE_STATE),
            errmsg("Output from AIEngine ia not in JSON format. Output is \n%s", trainResultJson)));
    }
    ModelTrainInfo* info = GetModelTrainInfo(jsonObj);
    cJSON_Delete(jsonObj);
    /* 3. update catalog and form the struct to return */
    Relation modelRel = heap_open(OptModelRelationId, RowExclusiveLock);
    Datum values[Natts_gs_opt_model];
    bool nulls[Natts_gs_opt_model];
    bool replaces[Natts_gs_opt_model];
    Datum* datumsMax = (Datum*)palloc(sizeof(Datum) * nLabel);
    Datum* datumsAcc = (Datum*)palloc(sizeof(Datum) * nLabel);
    for (int i = 0; i < Natts_gs_opt_model; i++) {
        nulls[i] = false;
        if (i == Anum_gs_opt_model_feature_size - 1 || i == Anum_gs_opt_model_max - 1 ||
            i == Anum_gs_opt_model_acc - 1 || i == Anum_gs_opt_model_available - 1 ||
            i == Anum_gs_opt_model_is_training - 1)
            replaces[i] = true;
        else
            replaces[i] = false;
    }
    UpdateTrainRes(values, datumsMax, datumsAcc, nLabel, mAcc, info, labels);

    HeapTuple modelTuple = SearchSysCache1(OPTMODEL, CStringGetDatum(modelName));
    if (!HeapTupleIsValid(modelTuple)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("OPT Model not found for model name %s", modelName)));
    }
    HeapTuple newTuple = heap_modify_tuple(modelTuple, RelationGetDescr(modelRel), values, nulls, replaces);
    simple_heap_update(modelRel, &newTuple->t_self, newTuple);
    CatalogUpdateIndexes(modelRel, newTuple);

    /* 4. release locks and set things free */
    ReleaseSysCache(modelTuple);
    heap_freetuple_ext(newTuple);
    heap_close(modelRel, RowExclusiveLock);
    pfree_ext(info);
    pfree_ext(datumsMax);
    pfree_ext(datumsAcc);
}

/* *
 * @Description: easy to use API for explain to call.
 * @in root - PlanState root for the query to predict
 * @in fileName - csv file generated by PreModelPredict
 * @out pred_xxx for Plans of root
 */
void ModelPredictForExplain(PlanState* root, char* fileName, const char* modelName)
{
    bool isNULL = false;

    /* check if model exists and get template name */
    HeapTuple modelTuple = SearchSysCache1(OPTMODEL, CStringGetDatum(modelName));
    if (!HeapTupleIsValid(modelTuple)) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("OPT Model not found for model name %s", modelName)));
    }
    Datum val = SysCacheGetAttr(OPTMODEL, modelTuple, Anum_gs_opt_model_template_name, &isNULL);
    if (isNULL) {
        ReleaseSysCache(modelTuple);
        ereport(
            ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Model targets is null")));
    }
    if (access(fileName, F_OK) == 0) {
        const char* templateName = DatumGetCString(val);
        ModelPredictInfo* mpinfo = ModelPredictInternal(templateName, modelName, fileName);
        SubPlanAssignPrediction(root, mpinfo);
        ReleaseSysCache(modelTuple);
        pfree_ext(mpinfo);
        if (unlink(fileName) < 0) {
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode_for_file_access(),
                    errmsg("could not unlink file \"%s\": %m", fileName)));
        } else {
            ereport(LOG, (errmsg("Unlinked file: \"%s\": %m", fileName)));
        }
    } else {
        ReleaseSysCache(modelTuple);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI), errcode_for_file_access(), errmsg("could not access file \"%s\": %m", fileName)));
    }
}

/* *
 * @Description: internal routine for model predicting procedure.
 * returned mpinfo should be freed by caller
 * @in data_id -  id of dataset to be predicted
 * @return ModelPredictInfo that contains all prediction information
 */
static ModelPredictInfo* ModelPredictInternal(const char* templateName, const char* modelName, char* filepath)
{
    ModelPredictInfo* mpinfo = NULL;
    /* 0. gather model configurations and check validity */
    char* labels = NULL;
    int nLabel;
    Form_gs_opt_model modelinfo = CheckModelTargets(templateName, modelName, &labels, &nLabel);
    /* 1. calls related model_predict PG_FUNCTION */
    char* predictResultJson = TreeModelPredict(modelName, filepath, modelinfo->ip.data, modelinfo->port);
    /* 2. parse the returned json */
    ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("AIEngine Returns JSON:\n %s", predictResultJson)));
    cJSON* jsonObj = cJSON_Parse(predictResultJson);
    if (jsonObj == NULL) {
        (void)unlink(filepath);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("Output from AIEngine ia not in JSON format. Output is \n%s", predictResultJson)));
    }
    mpinfo = GetModelPredictInfo(jsonObj);
    cJSON_Delete(jsonObj);
    /* 3. check if results matches target labels */
    for (char* label = labels; *label; label++) {
        switch (*label) {
            case gs_opt_model_label_startup_time:
                if (mpinfo->startup_time == NULL)
                    goto FAIL_MISMATCH_LABEL;
                break;
            case gs_opt_model_label_total_time:
                if (mpinfo->total_time == NULL)
                    goto FAIL_MISMATCH_LABEL;
                break;
            case gs_opt_model_label_rows:
                if (mpinfo->rows == NULL)
                    goto FAIL_MISMATCH_LABEL;
                break;
            case gs_opt_model_label_peak_memory:
                if (mpinfo->peak_memory == NULL)
                    goto FAIL_MISMATCH_LABEL;
                break;
            default:
                break;
        }
    }
    return mpinfo;
    /* if model prediction does not match the model targets handle errors below */
FAIL_MISMATCH_LABEL:
    (void)unlink(filepath);
    ereport(ERROR,
        (errmodule(MOD_OPT_AI),
            errcode(ERRCODE_UNEXPECTED_NODE_STATE),
            errmsg("AIEngine internal error: Prediction mismatch the model's label targets.")));
    return mpinfo;
}

/*
 * @Description: we should only accept labels [S, T, R, M] for now, check here.
 * @return: true if labels are [S, T, R, M].
 */
static bool CheckLabels(char** labels, int* nLabel)
{
    bool flagStartup = false;
    bool flagTotal = false;
    bool flagRow = false;
    bool flagMem = false;

    for (char* label = *labels; *label; label++) {
        switch (*label) {
            case gs_opt_model_label_startup_time: {
                if (flagStartup == true) {
                    return false;
                }
                flagStartup = true;
                (*nLabel)++;
                break;
            }
            case gs_opt_model_label_total_time: {
                if (flagTotal == true) {
                    return false;
                }
                flagTotal = true;
                (*nLabel)++;
                break;
            }
            case gs_opt_model_label_rows: {
                if (flagRow == true) {
                    return false;
                }
                flagRow = true;
                (*nLabel)++;
                break;
            }
            case gs_opt_model_label_peak_memory:
                if (flagMem == true) {
                    return false;
                }
                flagMem = true;
                (*nLabel)++;
                break;
            default:
                ereport(LOG,
                    (errmodule(MOD_OPT_AI),
                        errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("Model targets element only supports S/T/R/M, please check")));
                return false;
        }
    }
    return true;
}

Form_gs_opt_model CheckModelTargets(const char* templateName, const char* modelName, char** labels, int* nLabel)
{
    ArrayType* labelArray = NULL;
    int nElement;
    bool isNULL = false;
    Form_gs_opt_model modelinfo = NULL;
    *nLabel = 0;
    if (templateName == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("templateName must not be NULL."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
    }
    /* check templateName, for only rlstm is supported */
    if (strcmp(templateName, RLSTM_TEMPLATE_NAME) != 0) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("%s is not supported as template_name.", templateName)));
    }
    /* gather model configurations and check validity */
    HeapTuple modelTuple = SearchSysCache1(OPTMODEL, CStringGetDatum(modelName));
    if (!HeapTupleIsValid(modelTuple)) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("OPT_Model not found for model name %s", modelName)));
    }

    /*
     * Handle the model targets array. The targets array is expected to be a 1-D char array.
     */
    Datum val = SysCacheGetAttr(OPTMODEL, modelTuple, Anum_gs_opt_model_label, &isNULL);
    if (isNULL) {
        ReleaseSysCache(modelTuple);
        ereport(
            ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Model targets is null")));
    }
    labelArray = DatumGetArrayTypeP(val);
    nElement = ARR_DIMS(labelArray)[0];
    /* Sanity check for labels attribute */
    if ((ARR_NDIM(labelArray) != 1) || (nElement <= 0) || (ARR_HASNULL(labelArray)) ||
        (ARR_ELEMTYPE(labelArray) != CHAROID)) {
        ReleaseSysCache(modelTuple);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Model labels are not a 1-D char array.")));
    }

    *labels = (char*)palloc0((nElement + 1) * sizeof(char));
    errno_t rc = memcpy_s(*labels, (nElement + 1) * sizeof(char), ARR_DATA_PTR(labelArray), nElement * sizeof(char));
    securec_check(rc, "\0", "\0");

    bool isLegal = CheckLabels(labels, nLabel);
    if (isLegal) {
        modelinfo = (Form_gs_opt_model)GETSTRUCT(modelTuple);
        ReleaseSysCache(modelTuple);
    } else {
        pfree_ext(*labels);
        ReleaseSysCache(modelTuple);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("Repetitive labels found in model labels attribute for template name"
                       " %s model name %s",
                    templateName,
                    modelName)));
    }
    return modelinfo;
}

static bool ConfigurationIsValid(const Form_gs_opt_model modelinfo)
{
    if (modelinfo->max_epoch < 0 || modelinfo->learning_rate < 0 || modelinfo->hidden_units < 0 ||
        modelinfo->batch_size < 0) {
        return false;
    }
    return true;
}

/* returned mtinfo shouldb be freed by caller */
static ModelTrainInfo* GetModelTrainInfo(const cJSON* root)
{
    /* caller should make sure that root is not null */
    Assert(root);
    cJSON* item = NULL;
    ModelTrainInfo* info = (ModelTrainInfo*)palloc0(sizeof(ModelTrainInfo));
    item = cJSON_GetObjectItem(root, "converged");
    info->available = (item == NULL) ? (false) : (item->valueint);
    item = cJSON_GetObjectItem(root, "max_startup");
    info->max[LABEL_START_TIME_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "max_total");
    info->max[LABEL_TOTAL_TIME_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "max_row");
    info->max[LABEL_ROWS_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "max_mem");
    info->max[LABEL_PEAK_MEMEORY_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "re_startup");
    info->acc[LABEL_START_TIME_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "re_total");
    info->acc[LABEL_TOTAL_TIME_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "re_row");
    info->acc[LABEL_ROWS_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "re_mem");
    info->acc[LABEL_PEAK_MEMEORY_INDEX] = (item == NULL) ? (-1) : (item->valueint);
    item = cJSON_GetObjectItem(root, "feature_length");
    info->feature_size = (item == NULL) ? (-1) : (item->valueint);
    return info;
}

static ModelPredictInfo* GetModelPredictInfo(const cJSON* root)
{
    /* caller should make sure root is not NULL */
    Assert(root);
    static const int arrLen = 4;
    cJSON* item = NULL;
    int length[arrLen] = {0};
    int lengthLegit = 0;
    int i = 0;
    ModelPredictInfo* info = (ModelPredictInfo*)palloc0(sizeof(ModelPredictInfo));
    item = cJSON_GetObjectItem(root, "pred_startup");
    info->startup_time = JsonGetInt64Array(item, &length[i++]);
    item = cJSON_GetObjectItem(root, "pred_total");
    info->total_time = JsonGetInt64Array(item, &length[i++]);
    item = cJSON_GetObjectItem(root, "pred_rows");
    info->rows = JsonGetInt64Array(item, &length[i++]);
    item = cJSON_GetObjectItem(root, "pred_mem");
    info->peak_memory = JsonGetInt64Array(item, &length[i]);
    /* check if all prediction array has the same length, length is 0 if target not in labels entry */
    for (i = 0; i < arrLen; i++) {
        if (length[i] != 0 && lengthLegit != 0) {
            if (length[i] != lengthLegit) {
                pfree_ext(info);
                ereport(ERROR,
                    (errmodule(MOD_OPT_AI),
                        errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("Predictions have different dimensions.")));
            }
        } else if (length[i] != 0 && lengthLegit == 0) {
            lengthLegit = length[i];
        }
    }
    info->length = lengthLegit;
    return info;
}

/* *
 * Since flask.jsonify cannot handle int array, the int array is returned in string format.
 * The conversion is done below.
 */
static int64* JsonGetInt64Array(cJSON* root, int* length)
{
    if (root == NULL) {
        return NULL;
    }
    char* buf = root->valuestring;
    char delim[] = "[] ,";
    char* token = NULL;
    char* nextToken = NULL;
    int index = 0;
    if (buf == NULL) {
        return NULL;
    }
    /* Count number of elements as [num(,) + 1] */
    int count = 1;
    for (uint i = 0; i < strlen(buf); i++) {
        if (buf[i] == ',')
            count++;
    }
    *length = count;
    /* Convert char* to int8 array via strtok_s */
    int64* array = (int64*)palloc0(sizeof(int64) * count);
    token = strtok_s(buf, delim, &nextToken);
    while (token != NULL) {
        array[index++] = atol(token);
        token = strtok_s(NULL, delim, &nextToken);
    }
    return array;
}

/* *
 * @Description: prepare plan data for the prediction routine, should be called after the plan
 * @in PlanState and PlannedStmt
 * @out data saved in gs_wlm_plan_operator_info table.
 * @return the query id to locate the entry.
 */
char* PreModelPredict(PlanState* resultPlan, PlannedStmt* pstmt)
{
    uint64 qid = u_sess->instr_cxt.gs_query_id->queryId;
    char* filename = (char*)palloc0(sizeof(char) * MAX_LEN_TEXT);
    FILE* fpout = NULL;
    /* set qid as the file name to avoid conflicts */
    errno_t ret = sprintf_s(filename, MAX_LEN_TEXT, "/tmp/gaussdb_%lu.csv", qid);
    securec_check_ss(ret, "\0", "\0");
    /* remove file if already exists with the same name */
    int rc = unlink(filename);
    if (rc < 0 && errno != ENOENT) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI), errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", filename)));
    }
    /*
     * Open the file to write out the encodings and related info
     */
    fpout = AllocateFile(filename, PG_BINARY_A);
    if (fpout == NULL) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("could not open temporary statistics file \"%s\": %m", filename)));
    }
    ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("Saving feature to temp file %s", filename)));
    HandleSubPlan(resultPlan, pstmt, qid, fpout);

    if (FreeFile(fpout)) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("could not close temporary statistics file \"%s\": %m", filename)));
    }
    return filename;
}

/* *
 * @Description: 1. recursively extracts the plan node info
 * 2. append information into the temporary file
 * @in PlanState, PlannedStmt, qeury id
 * @out A file that contains all information needed to make prediction.
 * @return file name
 *
 */
static void HandleSubPlan(PlanState* resultPlan, PlannedStmt* pstmt, uint64 qid, FILE* fpout)
{
    OperatorPlanInfo* optPlanInfo = ExtractOperatorPlanInfo(resultPlan, pstmt);

    char buf[MAX_LEN_ROW] = {'\0'};
    char* encode = NULL;
    errno_t ret = EOK;
    size_t rc = 0;

    /* to be released later */
    text* textOpr = cstring_to_text(optPlanInfo->operation);
    text* textOrn = cstring_to_text(optPlanInfo->orientation);
    text* textStr = cstring_to_text(optPlanInfo->strategy);
    text* textOpt = cstring_to_text(optPlanInfo->options);
    text* textCnd = cstring_to_text(optPlanInfo->condition);
    text* textPrj = cstring_to_text(optPlanInfo->projection);

    /* get one-hot encoded plan node */
    encode = text_to_cstring(DatumGetTextP(DirectFunctionCall7(encode_plan_node,
        PointerGetDatum(textOpr),
        PointerGetDatum(textOrn),
        PointerGetDatum(textStr),
        PointerGetDatum(textOpt),
        Int32GetDatum(resultPlan->plan->dop),
        PointerGetDatum(textCnd),
        PointerGetDatum(textPrj))));
    /* append the data node information to target file. */
    ret = sprintf_s(buf,
        sizeof(buf),
        "%lu, %d, %d, %s, %d, %d, %d, %d\n",
        qid,
        resultPlan->plan->plan_node_id,
        optPlanInfo->parent_node_id,
        encode,
        -1,
        -1,
        -1,
        -1);
    securec_check_ss(ret, "\0", "\0");
    rc = fwrite(buf, strlen(buf), 1, fpout);
    if (rc != 1) {
        pfree_ext(encode);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode_for_file_access(),
                errmsg("could not write to file: \"%lu.csv\": %m", qid)));
    }
    ListCell* lc = NULL;
    List* list = GetPlanstateList(resultPlan);
    foreach (lc, list) {
        PlanState* subplan = (PlanState*)lfirst(lc);
        HandleSubPlan(subplan, pstmt, qid, fpout);
    }
    pfree_ext(encode);
    pfree_ext(textOpr);
    pfree_ext(textOrn);
    pfree_ext(textStr);
    pfree_ext(textOpt);
    pfree_ext(textCnd);
    pfree_ext(textPrj);
    list_free(list);
}

static void SubPlanAssignPrediction(PlanState* resultPlan, ModelPredictInfo* info)
{
    int index = resultPlan->plan->plan_node_id - 1;
    if (info->rows != NULL)
        resultPlan->plan->pred_rows = (double)info->rows[index];
    else
        resultPlan->plan->pred_rows = -1;

    if (info->startup_time != NULL)
        resultPlan->plan->pred_startup_time = (double)info->startup_time[index];
    else
        resultPlan->plan->pred_startup_time = -1;

    if (info->total_time != NULL)
        resultPlan->plan->pred_total_time = (double)info->total_time[index];
    else
        resultPlan->plan->pred_total_time = -1;

    if (info->peak_memory != NULL)
        resultPlan->plan->pred_max_memory = (long)info->peak_memory[index];
    else
        resultPlan->plan->pred_max_memory = -1;

    ListCell* lc = NULL;
    List* list = GetPlanstateList(resultPlan);
    foreach (lc, list) {
        PlanState* subplan = (PlanState*)lfirst(lc);
        SubPlanAssignPrediction(subplan, info);
    }
    list_free(list);
}

void SetNullPrediction(PlanState* resultPlan)
{
    resultPlan->plan->pred_rows = -1;
    resultPlan->plan->pred_startup_time = -1;
    resultPlan->plan->pred_total_time = -1;
    resultPlan->plan->pred_max_memory = -1;
    ListCell* lc = NULL;
    List* list = GetPlanstateList(resultPlan);
    foreach (lc, list) {
        PlanState* subplan = (PlanState*)lfirst(lc);
        SetNullPrediction(subplan);
    }
    list_free(list);
}

static List* AddPlanStates(const List* srcPlanList, List* planstateList)
{
    ListCell* lst = NULL;
    foreach (lst, srcPlanList) {
        SubPlanState* sps = (SubPlanState*)lfirst(lst);
        if (sps->planstate == NULL) {
            continue;
        }
        planstateList = lappend(planstateList, sps->planstate);
    }
    return planstateList;
}

static List* AddModifyTableStates(ModifyTableState* mt, List* planstateList)
{
    for (int i = 0; i < mt->mt_nplans; i++) {
        PlanState* plan = mt->mt_plans[i];
        planstateList = lappend(planstateList, plan);
    }
    return planstateList;
}

static List* AddAppendStates(AppendState* append, List* planstateList)
{
    for (int i = 0; i < append->as_nplans; i++) {
        PlanState* plan = append->appendplans[i];
        planstateList = lappend(planstateList, plan);
    }
    return planstateList;
}

static List* AddMergeAppendStates(MergeAppendState* ma, List* planstateList)
{
    for (int i = 0; i < ma->ms_nplans; i++) {
        PlanState* plan = ma->mergeplans[i];
        planstateList = lappend(planstateList, plan);
    }
    return planstateList;
}

static List* AddBitmapAndStates(BitmapAndState* ba, List* planstateList)
{
    for (int i = 0; i < ba->nplans; i++) {
        PlanState* plan = ba->bitmapplans[i];
        planstateList = lappend(planstateList, plan);
    }
    return planstateList;
}

static List* AddBitmapOrStates(BitmapOrState* bo, List* planstateList)
{
    for (int i = 0; i < bo->nplans; i++) {
        PlanState* plan = bo->bitmapplans[i];
        planstateList = lappend(planstateList, plan);
    }
    return planstateList;
}

static List* GetPlanstateList(PlanState* resultPlan)
{
    List* planstateList = NIL;

    switch (nodeTag(resultPlan->plan)) {
        case T_ModifyTable:
        case T_VecModifyTable:
            planstateList = AddModifyTableStates((ModifyTableState*)resultPlan, planstateList);
            break;
        case T_Append:
        case T_VecAppend:
            planstateList = AddAppendStates((AppendState*)resultPlan, planstateList);
            break;
        case T_MergeAppend:
        case T_VecMergeAppend:
            planstateList = AddMergeAppendStates((MergeAppendState*)resultPlan, planstateList);
            break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd:
            planstateList = AddBitmapAndStates((BitmapAndState*)resultPlan, planstateList);
            break;
        case T_BitmapOr:
        case T_CStoreIndexOr:
            planstateList = AddBitmapOrStates((BitmapOrState*)resultPlan, planstateList);
            break;
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScanState* ss = (SubqueryScanState*)resultPlan;
            planstateList = lappend(planstateList, ss->subplan);
            break;
        }
        default: {
            if (resultPlan->lefttree != NULL) {
                planstateList = lappend(planstateList, resultPlan->lefttree);
            }
            if (resultPlan->righttree != NULL) {
                planstateList = lappend(planstateList, resultPlan->righttree);
            }
            break;
        }
    }

    planstateList = AddPlanStates(resultPlan->initPlan, planstateList);
    planstateList = AddPlanStates(resultPlan->subPlan, planstateList);

    return planstateList;
}

bool PredictorIsValid(const char* modelName)
{
    HeapTuple modelTuple = SearchSysCache1(OPTMODEL, CStringGetDatum(modelName));
    bool res = true;
    if (!HeapTupleIsValid(modelTuple)) {
        ereport(WARNING, (errmodule(MOD_OPT_AI), errmsg("MLModel not found for model name %s", modelName)));
        return false;
    }
    Form_gs_opt_model modelinfo = (Form_gs_opt_model)GETSTRUCT(modelTuple);
    char* currentDatname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    char* modelDatname = NameStr(modelinfo->datname);
    if (currentDatname == NULL || modelDatname == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Unexpected NULL value for database name.")));
    }
    if (strcmp(currentDatname, modelDatname) != 0) {
        ereport(WARNING, (errmodule(MOD_OPT_AI), errmsg("Predictor %s is not for current database", modelName)));
        res = false;
    } else if (modelinfo->is_training) {
        ereport(WARNING, (errmodule(MOD_OPT_AI), errmsg("Predictor %s is still training", modelName)));
        res = false;
    } else if (!modelinfo->available) {
        ereport(WARNING,
            (errmodule(MOD_OPT_AI), errmsg("Predictor %s is not converged, results are not reliable", modelName)));
    } else if (modelinfo->available) {
        ereport(NOTICE, (errmodule(MOD_OPT_AI), errmsg("Predictor %s is converged, results are reliable", modelName)));
    }
    ReleaseSysCache(modelTuple);
    pfree_ext(currentDatname);
    return res;
}

static TupleDesc InitTupleVal(int modelTrainAttrnum)
{
    TupleDesc tupdesc;
    int i = 0;
    tupdesc = CreateTemplateTupleDesc(modelTrainAttrnum, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "startup_time_accuracy", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_time_accuracy", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "rows_accuracy", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "peak_memory_accuracy", FLOAT8OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);
    return tupdesc;
}
