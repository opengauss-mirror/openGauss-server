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
 * -------------------------------------------------------------------------
 *
 * plan_tree_model.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/util/learn/plan_tree_model.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cjson/cJSON.h"
#include "funcapi.h"
#include "optimizer/comm.h"
#include "optimizer/encoding.h"
#include "optimizer/learn.h"
#include "optimizer/plan_tree_model.h"
#include "utils/timestamp.h"

static char* FormConfigureJson(const Form_gs_opt_model modelinfo, const char* labels);
static char* FormSetupJson(const char* modelName);
static void ConfigureModel(Form_gs_opt_model modelinfo, const char* labels, char** filename);
static char* train_model(const Form_gs_opt_model modelinfo, char* filename);
static void Unlinkfile(char* filename);

/**
 * @Description: registered function for model plan tree model training procedure
 * @in maxEpoch -  max number of epoch to train
 * @in learningRate -  learning rate for neural network back propagation
 * @in hiddenUnits -  number of hidden units in the fully-connect layer
 * @out catalog gs_opt_model's related columns will be updated accordingly
 * @return sucessful or other error/warning messages
 */
char* TreeModelTrain(Form_gs_opt_model modelinfo, char* labels)
{
    char* filename = (char*)palloc0(sizeof(char) * MAX_LEN_TEXT);
    char* buf = NULL;
    /* 1. configure the remote server for training to see if the server is ready */
    ConfigureModel(modelinfo, labels, &filename);

    /* 2. save encoded data to file */
    SaveDataToFile(filename);

    /* 3. send saved file to server to trigger training */
    buf = train_model(modelinfo, filename);
    return buf;
}

/**
 * @Description: function for plan tree model predicting procedure
 * @in data_id -  id of dataset to be predicted
 * @return sucessful or other error/warning messages
 */
char* TreeModelPredict(const char* modelName, char* filepath, const char* ip, int port)
{
    /* 1. setup the model for prediction */
    char* buf = (char*)palloc0(sizeof(char) * CURL_BUF_SIZE);
    AiEngineConnInfo* conninfo = (AiEngineConnInfo*)palloc0(sizeof(AiEngineConnInfo));
    char portStr[PORT_LEN] = {'\0'};
    errno_t ret = sprintf_s(portStr, PORT_LEN, "%d", port);
    securec_check_ss(ret, "\0", "\0");
    conninfo->host = pstrdup(ip);
    conninfo->port = pstrdup(portStr);
    conninfo->request_api = pstrdup(PYTHON_SERVER_ROUTE_PREPREDICT);
    conninfo->header = pstrdup(PYTHON_SERVER_HEADER_JSON);
    conninfo->json_string = FormSetupJson(modelName);
    if (!TryConnectRemoteServer(conninfo, &buf)) {
        DestroyConnInfo(conninfo);
        ParseResBuf(buf, filepath, "AI engine connection failed.");
        return buf;
    }

    switch (buf[0]) {
        case '0': {
            ereport(NOTICE, (errmodule(MOD_OPT_AI), errmsg("Model setup successfully.")));
            break;
        }
        case 'M': {
            ParseResBuf(buf, filepath, "Internal error: missing compulsory key.");
            break;
        }
        case 'i': {
            ParseResBuf(buf, filepath, "Internal error: failed to load model, please retrain.");
            break;
        }
        case 'N': {
            ParseResBuf(buf, filepath, "Internal error: model not found, please make sure the model is trained.");
            break;
        }
        case 'R': {
            ParseResBuf(buf, filepath, "Another session is running on AIEngine. If not, please restart AIEngine");
            break;
        }
        default: {
            ParseResBuf(buf, filepath, "Internal error: unknown error.");
            break;
        }
    }
    /* 2. send saved file to server to trigger prediction */
    conninfo->request_api = pstrdup(PYTHON_SERVER_ROUTE_PREDICT);
    conninfo->url = NULL;
    conninfo->header = NULL;
    conninfo->json_string = NULL;
    conninfo->file_tag = pstrdup("file");
    conninfo->file_path = pstrdup(filepath);
    if (!TryConnectRemoteServer(conninfo, &buf)) {
        ParseResBuf(buf, filepath, "AI engine connection failed.");
        return buf;
    }
    switch (buf[0]) {
        case 'M': {
            ParseResBuf(buf, filepath, "Internal error: fail to load the file to predict.");
            break;
        }
        case 'S': {
            ParseResBuf(buf, filepath, "Internal error: session is not loaded, model setup required.");
            break;
        }
        default: {
            break;
        }
    }
    return buf;
}

static char* FormConfigureJson(const Form_gs_opt_model modelinfo, const char* labels)
{
    char* result = (char*)palloc0(sizeof(char) * MAX_LEN_JSON);
    cJSON* jsonObj = cJSON_CreateObject();
    cJSON_AddStringToObject(jsonObj, "template_name", modelinfo->template_name.data);
    cJSON_AddStringToObject(jsonObj, "model_name", modelinfo->model_name.data);
    cJSON_AddNumberToObject(jsonObj, "max_epoch", modelinfo->max_epoch);
    cJSON_AddNumberToObject(jsonObj, "learning_rate", modelinfo->learning_rate);
    cJSON_AddNumberToObject(jsonObj, "dim_red", modelinfo->dim_red);
    cJSON_AddNumberToObject(jsonObj, "hidden_units", modelinfo->hidden_units);
    cJSON_AddNumberToObject(jsonObj, "batch_size", modelinfo->batch_size);
    cJSON_AddStringToObject(jsonObj, "labels", labels);
    char* buf = cJSON_Print(jsonObj);
    if (buf != NULL) {
        result = pstrdup(buf);
        cJSON_free(buf);
    }
    cJSON_Delete(jsonObj);
    return result;
}

static char* FormSetupJson(const char* modelName)
{
    char* result = (char*)palloc0(sizeof(char) * MAX_LEN_JSON);
    cJSON* jsonObj = cJSON_CreateObject();
    cJSON_AddStringToObject(jsonObj, "model_name", modelName);
    char* buf = cJSON_Print(jsonObj);
    if (buf != NULL) {
        result = pstrdup(buf);
        cJSON_free(buf);
    }
    cJSON_Delete(jsonObj);
    return result;
}

/* configure the remote server for training to see if the server is ready */
static void ConfigureModel(Form_gs_opt_model modelinfo, const char* labels, char** filename)
{
    char* buf = NULL;
    errno_t ret = sprintf_s(
        *filename, MAX_LEN_TEXT - 1, "%s-%s", TRAIN_DATASET_FILEPATH, timestamptz_to_str(GetCurrentTimestamp()));
    securec_check_ss(ret, "\0", "\0");
    AiEngineConnInfo* conninfo = (AiEngineConnInfo*)palloc0(sizeof(AiEngineConnInfo));
    char portStr[PORT_LEN] = {'\0'};
    ret = sprintf_s(portStr, PORT_LEN, "%d", modelinfo->port);
    securec_check_ss(ret, "\0", "\0");
    conninfo->host = pstrdup(modelinfo->ip.data);
    conninfo->port = pstrdup(portStr);
    conninfo->request_api = pstrdup(PYTHON_SERVER_ROUTE_PRETRAIN);
    conninfo->header = pstrdup(PYTHON_SERVER_HEADER_JSON);
    conninfo->json_string = FormConfigureJson(modelinfo, labels);
    if (!TryConnectRemoteServer(conninfo, &buf)) {
        ParseConfigBuf(buf, conninfo, "AI engine connection failed.");
    }

    switch (buf[0]) {
        case '0': {
            pfree_ext(buf);
            DestroyConnInfo(conninfo);
            ereport(NOTICE,
                (errmodule(MOD_OPT_AI),
                    errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("Model configured successfully.")));
            break;
        }
        case 'F': {
            ParseConfigBuf(buf, conninfo, "AIEngine internal error: configuration contains type error.");
            break;
        }
        case 'I': {
            ParseConfigBuf(buf, conninfo, "AIEngine internal error: key not unrecognized.");
            break;
        }
        case 'M': {
            ParseConfigBuf(buf, conninfo, "AIEngine internal error: missing compulsory key.");
            break;
        }
        default: {
            ParseConfigBuf(buf, conninfo, "AIEngine internal error: unknown error.");
            break;
        }
    }
}

static char* train_model(const Form_gs_opt_model modelinfo, char* filename)
{
    char* buf = NULL;
    char portStr[PORT_LEN] = {'\0'};
    errno_t ret = sprintf_s(portStr, PORT_LEN, "%d", modelinfo->port);
    securec_check_ss(ret, "\0", "\0");
    AiEngineConnInfo* conninfo = (AiEngineConnInfo*)palloc0(sizeof(AiEngineConnInfo));
    conninfo->host = pstrdup(modelinfo->ip.data);
    conninfo->port = pstrdup(portStr);
    conninfo->request_api = pstrdup(PYTHON_SERVER_ROUTE_TRAIN);
    conninfo->url = NULL;
    conninfo->header = NULL;
    conninfo->json_string = NULL;
    conninfo->file_tag = pstrdup("file");
    conninfo->file_path = pstrdup(filename);
    if (!TryConnectRemoteServer(conninfo, &buf)) {
        DestroyConnInfo(conninfo);
        pfree_ext(buf);
        ereport(ERROR,
            (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("AI engine connection failed.")));
        return buf;
    }

    DestroyConnInfo(conninfo);
    Unlinkfile(filename);
    switch (buf[0]) {
        case 'M': {
            pfree_ext(buf);
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("AIEngine internal error: missing compulsory key.")));
            break;
        }
        case 'R': {
            pfree_ext(buf);
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("Another session is running on AIEngine. If not, please restart AIEngine")));
            break;
        }
        default: {
            break;
        }
    }
    return buf;
}

static void Unlinkfile(char* filename)
{
    if (access(filename, F_OK) == 0) {
        if (unlink(filename) < 0) {
            ereport(ERROR,
                (errmodule(MOD_OPT_AI),
                    errcode_for_file_access(),
                    errmsg("could not unlink file \"%s\": %m", filename)));
        } else {
            ereport(LOG, (errmsg("Unlinked file: \"%s\"", filename)));
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT_AI), errcode_for_file_access(), errmsg("could not access file \"%s\": %m", filename)));
    }
}
