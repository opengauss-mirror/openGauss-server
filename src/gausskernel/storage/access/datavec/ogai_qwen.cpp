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
 * ogai_qwen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_qwen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/palloc.h"
#include "cjson/cJSON.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_httpscan.h"
#include "access/datavec/ogai_utils.h"
#include "access/datavec/ogai_qwen.h"

#define QWEN_DEFAULT_TEMPERATURE 0.7
#define QWEN_DEFAULT_MAX_TOKENS 2000

EmbeddingClient* CreateQwenEmbeddingClient(ModelConfig* config)
{
    if (!config) return NULL;
    return New(CurrentMemoryContext) QwenEmbeddingClient(config);
}

GenerateClient* CreateQwenGenerateClient(ModelConfig* config)
{
    if (!config) return NULL;
    return New(CurrentMemoryContext) QwenGenerateClient(config);
}

RerankClient* CreateQwenRerankClient(ModelConfig* config)
{
    if (!config) return NULL;
    return New(CurrentMemoryContext) QwenRerankClient(config);
}

char* QwenEmbeddingClient::BuildEmbeddingReqBody(OGAIString* texts, size_t textNum)
{
    char* body = NULL;

    cJSON* root = cJSON_CreateObject();
    if (root == NULL) {
        elog(ERROR, "build embedding body error.");
    }

    cJSON* input = cJSON_CreateObject();
    if (input == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build embedding input object error.");
    }

    cJSON_AddStringToObject(root, "model", config->modelName);
    cJSON* textsArr = cJSON_CreateArray();
    for (size_t i = 0; i < textNum; i++) {
        cJSON_AddItemToArray(textsArr, cJSON_CreateString(texts[i]));
    }
    cJSON_AddItemToObject(input, "texts", textsArr);
    cJSON_AddItemToObject(root, "input", input);

    if (config->dimension > 0) {
        cJSON* params = cJSON_CreateObject();
        cJSON_AddNumberToObject(params, "dimension", config->dimension);
        cJSON_AddItemToObject(root, "parameters", params);
    }

    char* jsonStr = cJSON_PrintUnformatted(root);
    if (jsonStr == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build embedding json body error.");
    }

    body = pg_strdup(jsonStr);
    cJSON_free(jsonStr);
    cJSON_Delete(root);
    return body;
}

void QwenEmbeddingClient::ParseEmbeddingRespBody(char* respBody, Vector** results, size_t start, size_t textNum)
{
    cJSON* root = cJSON_Parse(respBody);
    if (root == NULL) {
        elog(ERROR, "parse embedding response json body error: %s.", respBody);
    }

    cJSON* output = cJSON_GetObjectItem(root, "output");
    if (output == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "parse embedding output error: %s.", respBody);
    }

    cJSON* embeddings = cJSON_GetObjectItem(output, "embeddings");
    if (embeddings == NULL || !cJSON_IsArray(embeddings)) {
        cJSON_Delete(root);
        elog(ERROR, "parse embeddings error.");
    }
    size_t embeddingCount = cJSON_GetArraySize(embeddings);
    if (textNum != embeddingCount) {
        cJSON_Delete(root);
        elog(ERROR, "The number of texts[%d] and vectors[%d] is inconsistent", textNum, embeddingCount);
    }

    for (size_t i = 0; i < embeddingCount; i++) {
        cJSON* item = cJSON_GetArrayItem(embeddings, i);
        cJSON* embedding = cJSON_GetObjectItem(item, "embedding");
        config->dimension = cJSON_GetArraySize(embedding);
        Vector* vec = InitVector(config->dimension);
        for (size_t j = 0; j < config->dimension; j++) {
            cJSON* val = cJSON_GetArrayItem(embedding, j);
            vec->x[j] = (float)val->valuedouble;
        }
        results[start + i] = vec;
    }
    cJSON_Delete(root);
}

OGAIString QwenGenerateClient::BuildGenerateReqBody(OGAIString query)
{
    char* body = NULL;
    cJSON* root = cJSON_CreateObject();
    float temperature = 0.7;
    if (root == NULL) {
        elog(ERROR, "build generate body error.");
    }

    cJSON_AddStringToObject(root, "model", config->modelName);

    cJSON* input = cJSON_CreateObject();
    if (input == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build input object error.");
        return NULL;
    }

    cJSON* messages = cJSON_CreateArray();
    if (messages == NULL) {
        cJSON_Delete(root);
        cJSON_Delete(input);
        elog(ERROR, "build messages array error.");
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

    cJSON_AddItemToObject(input, "messages", messages);
    cJSON_AddItemToObject(root, "input", input);

    cJSON* params = cJSON_CreateObject();
    cJSON_AddNumberToObject(params, "temperature", QWEN_DEFAULT_TEMPERATURE);
    cJSON_AddNumberToObject(params, "max_tokens", QWEN_DEFAULT_MAX_TOKENS);
    cJSON_AddItemToObject(root, "parameters", params);

    char* jsonStr = cJSON_PrintUnformatted(root);
    if (jsonStr == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build generate json body error.");
        return NULL;
    }

    body = pg_strdup(jsonStr);
    cJSON_free(jsonStr);
    cJSON_Delete(root);
    return body;
}

static char* ParseChoicesFormat(cJSON* output)
{
    cJSON* choices = cJSON_GetObjectItem(output, "choices");
    if (choices == NULL || !cJSON_IsArray(choices) || cJSON_GetArraySize(choices) == 0) {
        return NULL;
    }

    cJSON* choice = cJSON_GetArrayItem(choices, 0);
    if (choice == NULL) {
        return NULL;
    }

    cJSON* message = cJSON_GetObjectItem(choice, "message");
    if (message == NULL || !cJSON_IsObject(message)) {
        return NULL;
    }

    cJSON* content = cJSON_GetObjectItem(message, "content");
    if (content == NULL || !cJSON_IsString(content) || content->valuestring == NULL) {
        return NULL;
    }

    return pg_strdup(content->valuestring);
}

OGAIString QwenGenerateClient::ParseGenerateRespBody(OGAIString respBody)
{
    char* result = NULL;

    cJSON* root = cJSON_Parse(respBody);
    if (root == NULL) {
        elog(ERROR, "parse generate response json body error: %s.", respBody);
    }

    cJSON* output = cJSON_GetObjectItem(root, "output");
    if (output == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "parse generate output error: %s.", respBody);
    }

    cJSON* text = cJSON_GetObjectItem(output, "text");
    if (text != NULL && cJSON_IsString(text) && text->valuestring != NULL) {
        result = pg_strdup(text->valuestring);
        cJSON_Delete(root);
        return result;
    }

    result = ParseChoicesFormat(output);
    if (result != NULL) {
        cJSON_Delete(root);
        return result;
    }

    cJSON_Delete(root);
    elog(ERROR, "parse generate response error: neither 'text' nor 'choices' format found.");
    return NULL;
}

OGAIString QwenRerankClient::BuildRerankReqBody(OGAIString query, InputDocuments* inputDocuments)
{
    char* body = NULL;
    cJSON* root = cJSON_CreateObject();
    if (root == NULL) {
        elog(ERROR, "build rerank body error.");
    }

    cJSON_AddStringToObject(root, "model", config->modelName);
    cJSON* input = cJSON_CreateObject();
    cJSON_AddStringToObject(input, "query", query);
    cJSON* documents = cJSON_CreateArray();
    if (documents == NULL) {
        cJSON_Delete(root);
        cJSON_Delete(input);
        elog(ERROR, "build documents array error.");
        return NULL;
    }

    for (int i = 0; i < inputDocuments->docCount; ++i) {
        cJSON_AddItemToArray(documents, cJSON_CreateString(inputDocuments->docArray[i]));
    }
    cJSON_AddItemToObject(input, "documents", documents);
    cJSON_AddItemToObject(root, "input", input);

    /* set top-n, default equal to document counts */
    cJSON* params = cJSON_CreateObject();
    cJSON_AddNumberToObject(params, "top_n", inputDocuments->docCount);
    cJSON_AddItemToObject(root, "parameters", params);

    char* jsonStr = cJSON_PrintUnformatted(root);
    if (jsonStr == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build embedding json body error.");
        return NULL;
    }

    body = pg_strdup(jsonStr);
    cJSON_free(jsonStr);
    cJSON_Delete(root);
    return body;
}

RerankResults* QwenRerankClient::ParseRerankRespBody(OGAIString respBody, InputDocuments* inputDocuments)
{
    RerankResults* rerankResults = NULL;
    RerankDocument* rerankDocuments = NULL;
    cJSON* root = NULL;
    cJSON* output = NULL;
    cJSON* results = NULL;

    root = cJSON_Parse(respBody);
    if (root == NULL) {
        elog(WARNING, "build rerank response json body error: %s.", respBody);
        return rerankResults;
    }

    output = cJSON_GetObjectItem(root, "output");
    if (output == NULL) {
        cJSON_Delete(root);
        elog(WARNING, "parse rerank output error: %s.", respBody);
    }

    results = cJSON_GetObjectItem(output, "results");
    if (results == NULL || !cJSON_IsArray(results)) {
        cJSON_Delete(root);
        elog(WARNING, "parse rerank results error.");
        return rerankResults;
    }

    size_t rerankCount = cJSON_GetArraySize(results);
    if (rerankCount != inputDocuments->docCount) {
        cJSON_Delete(root);
        elog(WARNING, "The number of rerankCount[%d] and documentsCount[%d] is inconsistent",
            rerankCount, inputDocuments->docCount);
        return rerankResults;
    }

    rerankResults = (RerankResults*)palloc0(sizeof(RerankResults));
    rerankDocuments = (RerankDocument*)palloc0(sizeof(RerankDocument) * rerankCount);
    for (size_t i = 0; i < rerankCount; ++i) {
        cJSON* item = cJSON_GetArrayItem(results, i);
        cJSON* index = cJSON_GetObjectItem(item, "index");
        cJSON* relevanceScore = cJSON_GetObjectItem(item, "relevance_score");
        rerankDocuments[i].document = pg_strdup(inputDocuments->docArray[index->valueint]);
        rerankDocuments[i].rerankScore = relevanceScore->valuedouble;
        rerankDocuments[i].originIndex = index->valueint;
    }
    rerankResults->documents = rerankDocuments;
    rerankResults->docCount = rerankCount;
    cJSON_Delete(root);
    return rerankResults;
}
