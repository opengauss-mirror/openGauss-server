/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * ogai_openai.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_openai.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/palloc.h"
#include "cjson/cJSON.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_httpscan.h"
#include "access/datavec/ogai_utils.h"
#include "access/datavec/ogai_openai.h"

EmbeddingClient* CreateOpenAIEmbeddingClient(ModelConfig* config)
{
    if (!config) return NULL;
    return New(CurrentMemoryContext) OpenAIEmbeddingClient(config);
}

GenerateClient* CreateOpenAIGenerateClient(ModelConfig* config)
{
    if (!config) return NULL;
    return New(CurrentMemoryContext) OpenAIGenerateClient(config);
}

RerankClient* CreateOpenAIRerankClient(ModelConfig* config)
{
    elog(LOG, "OpenAI does not support rerank API.");
    return NULL;
}

char* OpenAIEmbeddingClient::BuildEmbeddingReqBody(OGAIString* texts, size_t textNum)
{
    char* body = NULL;

    cJSON* root = cJSON_CreateObject();
    if (root == NULL) {
        elog(ERROR, "build embedding body error for openai.");
    }

    cJSON_AddStringToObject(root, "model", config->modelName);
    cJSON_AddNumberToObject(root, "dimensions", config->dimension);

    if (textNum == 1) {
        cJSON_AddStringToObject(root, "input", texts[0]);
    } else {
        cJSON* input = cJSON_CreateArray();
        if (input == NULL) {
            cJSON_Delete(root);
            elog(ERROR, "build embedding input array error for openai.");
            return NULL;
        }
        for (size_t i = 0; i < textNum; i++) {
            cJSON_AddItemToArray(input, cJSON_CreateString(texts[i]));
        }
        cJSON_AddItemToObject(root, "input", input);
    }

    char* jsonStr = cJSON_PrintUnformatted(root);
    if (jsonStr == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build embedding json body error for openai.");
        return NULL;
    }

    body = pg_strdup(jsonStr);
    cJSON_free(jsonStr);
    cJSON_Delete(root);
    return body;
}

void OpenAIEmbeddingClient::ParseEmbeddingRespBody(char* respBody, Vector** results, size_t start, size_t textNum)
{
    cJSON* root = cJSON_Parse(respBody);
    if (root == NULL) {
        elog(ERROR, "parse embedding response json body error for openai: %s.", respBody);
    }

    cJSON* data = cJSON_GetObjectItem(root, "data");
    if (data == NULL || !cJSON_IsArray(data)) {
        cJSON_Delete(root);
        elog(ERROR, "parse data response error for openai: %s.", respBody);
    }

    size_t embeddingCount = cJSON_GetArraySize(data);
    if (embeddingCount != textNum) {
        cJSON_Delete(root);
        elog(ERROR,
            "The number of texts[%zu] and embeddings[%zu] is inconsistent for openai.", textNum, embeddingCount);
    }

    for (size_t i = 0; i < embeddingCount; i++) {
        cJSON* item = cJSON_GetArrayItem(data, i);
        if (item == NULL || !cJSON_IsObject(item)) {
            cJSON_Delete(root);
            elog(ERROR, "data item at index %zu is not an object for openai.", i);
        }

        cJSON* embedding = cJSON_GetObjectItem(item, "embedding");
        if (embedding == NULL || !cJSON_IsArray(embedding)) {
            cJSON_Delete(root);
            elog(ERROR, "embedding at index %zu not found or not array for openai.", i);
        }

        config->dimension = cJSON_GetArraySize(embedding);
        if (config->dimension == 0) {
            cJSON_Delete(root);
            elog(ERROR, "embedding dimension is 0 at index %zu for openai.", i);
        }

        Vector* vec = InitVector(config->dimension);
        for (size_t j = 0; j < config->dimension; j++) {
            cJSON* val = cJSON_GetArrayItem(embedding, j);
            if (val == NULL) {
                cJSON_Delete(root);
                elog(ERROR, "embedding value at index [%zu][%zu] is NULL for openai.", i, j);
            }
            vec->x[j] = (float)val->valuedouble;
        }

        results[start + i] = vec;
    }

    cJSON_Delete(root);
}

OGAIString OpenAIGenerateClient::BuildGenerateReqBody(OGAIString query)
{
    char* body = NULL;
    cJSON* root = cJSON_CreateObject();
    if (root == NULL) {
        elog(ERROR, "build generate body error for openai.");
    }

    cJSON_AddStringToObject(root, "model", config->modelName);

    cJSON* messages = cJSON_CreateArray();
    if (messages == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build messages array error for openai.");
        return NULL;
    }

    cJSON* system_msg = cJSON_CreateObject();
    cJSON_AddStringToObject(system_msg, "role", "system");
    cJSON_AddStringToObject(system_msg, "content", "You are a helpful assistant.");
    cJSON_AddItemToArray(messages, system_msg);

    cJSON* user_msg = cJSON_CreateObject();
    cJSON_AddStringToObject(user_msg, "role", "user");
    cJSON_AddStringToObject(user_msg, "content", query);
    cJSON_AddItemToArray(messages, user_msg);

    cJSON_AddItemToObject(root, "messages", messages);

    char* jsonStr = cJSON_PrintUnformatted(root);
    if (jsonStr == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build generate json body error for openai.");
        return NULL;
    }

    body = pg_strdup(jsonStr);
    cJSON_free(jsonStr);
    cJSON_Delete(root);
    return body;
}

OGAIString OpenAIGenerateClient::ParseGenerateRespBody(OGAIString respBody)
{
    char* result = NULL;

    cJSON* root = cJSON_Parse(respBody);
    if (root == NULL) {
        elog(ERROR, "parse generate response json body error for openai: %s.", respBody);
    }

    cJSON* choices = cJSON_GetObjectItem(root, "choices");
    if (choices == NULL || !cJSON_IsArray(choices) || cJSON_GetArraySize(choices) == 0) {
        cJSON_Delete(root);
        elog(ERROR, "parse choices error for openai: %s.", respBody);
    }

    cJSON* choice = cJSON_GetArrayItem(choices, 0);
    if (choice == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "parse choice[0] error for openai.");
    }

    cJSON* message = cJSON_GetObjectItem(choice, "message");
    if (message == NULL || !cJSON_IsObject(message)) {
        cJSON_Delete(root);
        elog(ERROR, "parse message error for openai.");
    }

    cJSON* content = cJSON_GetObjectItem(message, "content");
    if (content == NULL || !cJSON_IsString(content) || content->valuestring == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "parse content error for openai: not found or not string.");
    }

    result = pg_strdup(content->valuestring);
    cJSON_Delete(root);
    return result;
}

OGAIString OpenAIRerankClient::BuildRerankReqBody(OGAIString query, InputDocuments* inputDocuments)
{
    elog(ERROR, "OpenAI does not support rerank API.");
    return NULL;
}

RerankResults* OpenAIRerankClient::ParseRerankRespBody(OGAIString respBody, InputDocuments* inputDocuments)
{
    elog(ERROR, "OpenAI does not support rerank API.");
    return NULL;
}

