/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * ogai_ollama.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_ollama.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <cfloat>
#include <cmath>
#include <stdlib.h>
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "cjson/cJSON.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_httpscan.h"
#include "access/datavec/ogai_utils.h"
#include "access/datavec/ogai_ollama.h"

static const long OLLAMA_HTTP_SUCCESS_MIN = 200;

EmbeddingClient* CreateOllamaEmbeddingClient(ModelConfig* config)
{
    if (!config) {
        return NULL;
    }
    return New(CurrentMemoryContext) OllamaEmbeddingClient(config);
}

GenerateClient* CreateOllamaGenerateClient(ModelConfig* config)
{
    if (!config) {
        return NULL;
    }
    return New(CurrentMemoryContext) OllamaGenerateClient(config);
}

RerankClient* CreateOllamaRerankClient(ModelConfig* config)
{
    elog(LOG, "Ollama does not support rerank api.");
    return NULL;
}

static void CheckOllamaEmbeddingInputs(const ModelConfig* config, OGAIString* texts, size_t textNum, const int* dim)
{
    if (config == NULL || texts == NULL || textNum == 0 || dim == NULL || *dim <= 0 ||
        *dim > VECTOR_MAX_DIM || config->dimension != (size_t)*dim || config->maxBatch == 0 ||
        textNum > MaxAllocSize / sizeof(Vector*)) {
        elog(ERROR, "Invalid Ollama embedding parameters or dimension.");
    }
    for (size_t i = 0; i < textNum; i++) {
        if (texts[i] == NULL) {
            elog(ERROR, "Ollama embedding input must not be NULL.");
        }
    }
}

static void FreeOllamaEmbeddingRequest(HttpMgr* http, KmErr* error, char* body)
{
    /* The request uses pg_strdup; the response belongs to HttpMgr. */
    free(body);
    httpmgr_free(http);
    km_err_free(error);
}

static void RequestOllamaEmbedding(OllamaEmbeddingClient* client, const char* url,
    OGAIString* texts, size_t count, Vector** results)
{
    KmErr* error = km_err_new(OGAI_ERROR_BUF_SZ);
    if (error == NULL) {
        elog(ERROR, "Could not allocate Ollama HTTP error buffer.");
    }
    HttpMgr* http = GetHttpMgr(error, url, client->config->apiKey, PROVIDER_OLLAMA);
    char* volatile body = NULL;
    PG_TRY();
    {
        body = client->BuildEmbeddingReqBody(texts, count);
        httpmgr_set_req_header(http, httpmgr_get_req_body_len(http, body));
        httpmgr_set_req_body(http, body);
        httpmgr_receive(http);
        if (km_err_catch(error)) {
            elog(ERROR, "Ollama HTTP request failed: %s.", error->buf);
        }
        long status = httpmgr_get_status(http);
        if (status < OLLAMA_HTTP_SUCCESS_MIN || status >= HTTP_MIN_UNEXCEPTED_CODE) {
            elog(ERROR, "Ollama model request returned HTTP status %ld.", status);
        }
        char* response = httpmgr_get_res_body(http);
        if (response == NULL) {
            elog(ERROR, "Ollama embedding response is empty.");
        }
        client->ParseEmbeddingRespBody(response, results, 0, count);
    }
    PG_CATCH();
    {
        FreeOllamaEmbeddingRequest(http, error, body);
        PG_RE_THROW();
    }
    PG_END_TRY();
    FreeOllamaEmbeddingRequest(http, error, body);
}

Vector** OllamaEmbeddingClient::BatchEmbed(OGAIString* texts, size_t textNum, int* dim)
{
    CheckOllamaEmbeddingInputs(config, texts, textNum, dim);
    size_t maxBatch = GetOllamaEmbeddingProtocol(config->baseUrl) == OLLAMA_EMBED_LEGACY ? 1 : config->maxBatch;
    char* url = ResolveOllamaEmbeddingUrl(config->baseUrl);
    Vector** results = (Vector**)palloc0(sizeof(Vector*) * textNum);
    PG_TRY();
    {
        for (size_t i = 0; i < textNum;) {
            size_t batch = Min(maxBatch, textNum - i);
            RequestOllamaEmbedding(this, url, &texts[i], batch, &results[i]);
            i += batch;
        }
    }
    PG_CATCH();
    {
        for (size_t i = 0; i < textNum; i++) {
            pfree_ext(results[i]);
        }
        pfree(results);
        pfree(url);
        PG_RE_THROW();
    }
    PG_END_TRY();
    pfree(url);
    return results;
}

static void CheckOllamaJson(cJSON* value)
{
    if (value == NULL) {
        elog(ERROR, "Could not allocate Ollama JSON request.");
    }
}

char* OllamaEmbeddingClient::BuildEmbeddingReqBody(OGAIString* texts, size_t textNum)
{
    OllamaEmbeddingProtocol protocol = GetOllamaEmbeddingProtocol(config->baseUrl);
    if (protocol == OLLAMA_EMBED_LEGACY && textNum != 1) {
        elog(ERROR, "Legacy Ollama embeddings endpoint accepts one text per request.");
    }
    cJSON* root = cJSON_CreateObject();
    CheckOllamaJson(root);
    char* body = NULL;
    char* volatile json = NULL;
    PG_TRY();
    {
        CheckOllamaJson(cJSON_AddStringToObject(root, "model", config->modelName));
        if (protocol == OLLAMA_EMBED_LEGACY) {
            CheckOllamaJson(cJSON_AddStringToObject(root, "prompt", texts[0]));
        } else {
            cJSON* input = cJSON_AddArrayToObject(root, "input");
            CheckOllamaJson(input);
            for (size_t i = 0; i < textNum; i++) {
                cJSON* item = cJSON_CreateString(texts[i]);
                CheckOllamaJson(item);
                cJSON_AddItemToArray(input, item);
            }
        }
        json = cJSON_PrintUnformatted(root);
        if (json == NULL) {
            elog(ERROR, "Could not serialize Ollama embedding request.");
        }
        body = pg_strdup(json);
    }
    PG_CATCH();
    {
        cJSON_free(json);
        cJSON_Delete(root);
        PG_RE_THROW();
    }
    PG_END_TRY();
    cJSON_free(json);
    cJSON_Delete(root);
    return body;
}

static Vector* ParseOllamaVector(cJSON* embedding, size_t expectedDim)
{
    if (!cJSON_IsArray(embedding)) {
        elog(ERROR, "Ollama embedding is missing or is not an array.");
    }
    size_t dimension = (size_t)cJSON_GetArraySize(embedding);
    if (dimension == 0) {
        elog(ERROR, "Ollama embedding dimension must not be zero.");
    }
    if (dimension != expectedDim || dimension > VECTOR_MAX_DIM) {
        elog(ERROR, "Ollama embedding dimension %zu does not match requested dimension %zu.", dimension, expectedDim);
    }
    cJSON* value = NULL;
    cJSON_ArrayForEach(value, embedding) {
        if (!cJSON_IsNumber(value) || !std::isfinite(value->valuedouble) || std::fabs(value->valuedouble) > FLT_MAX) {
            elog(ERROR, "Ollama embedding values must be finite numbers representable as float.");
        }
    }
    Vector* result = InitVector((int)dimension);
    size_t i = 0;
    cJSON_ArrayForEach(value, embedding) {
        result->x[i++] = (float)value->valuedouble;
    }
    return result;
}

struct OllamaEmbeddingResponse {
    Vector** results;
    size_t start;
    size_t textNum;
    size_t dimension;
    OllamaEmbeddingProtocol protocol;
    bool* seen;
};

static size_t GetOllamaEmbeddingIndex(cJSON* item, size_t defaultIndex, size_t textNum)
{
    cJSON* position = cJSON_GetObjectItemCaseSensitive(item, "index");
    if (position == NULL) {
        return defaultIndex;
    }
    if (!cJSON_IsNumber(position) || !std::isfinite(position->valuedouble) ||
        position->valuedouble < 0 || position->valuedouble >= (double)textNum ||
        std::floor(position->valuedouble) != position->valuedouble) {
        elog(ERROR, "Ollama OpenAI-compatible embedding index is invalid.");
    }
    return (size_t)position->valuedouble;
}

static void ParseOllamaEmbeddingArray(cJSON* root, const OllamaEmbeddingResponse* response)
{
    cJSON* embeddings = cJSON_GetObjectItemCaseSensitive(root,
        response->protocol == OLLAMA_EMBED_OPENAI ? "data" : "embeddings");
    if (!cJSON_IsArray(embeddings)) {
        elog(ERROR, "Ollama embedding response is missing the expected embedding array.");
    }
    size_t count = (size_t)cJSON_GetArraySize(embeddings);
    if (count != response->textNum) {
        elog(ERROR, "Ollama returned %zu embeddings for %zu input texts.", count, response->textNum);
    }
    size_t i = 0;
    cJSON* item = NULL;
    cJSON_ArrayForEach(item, embeddings) {
        size_t index = response->protocol == OLLAMA_EMBED_OPENAI ?
            GetOllamaEmbeddingIndex(item, i, response->textNum) : i;
        cJSON* embedding = response->protocol == OLLAMA_EMBED_OPENAI ?
            cJSON_GetObjectItemCaseSensitive(item, "embedding") : item;
        if (response->seen[index]) {
            elog(ERROR, "Ollama returned duplicate embedding indexes.");
        }
        response->seen[index] = true;
        response->results[response->start + index] = ParseOllamaVector(embedding, response->dimension);
        i++;
    }
}

void OllamaEmbeddingClient::ParseEmbeddingRespBody(char* respBody, Vector** results, size_t start, size_t textNum)
{
    OllamaEmbeddingProtocol protocol = GetOllamaEmbeddingProtocol(config->baseUrl);
    bool* seen = (bool*)palloc0(sizeof(bool) * textNum);
    cJSON* root = cJSON_ParseWithOpts(respBody, NULL, true);
    if (root == NULL) {
        pfree(seen);
        elog(ERROR, "Ollama embedding response is not valid JSON.");
    }
    OllamaEmbeddingResponse response = {results, start, textNum, config->dimension, protocol, seen};
    PG_TRY();
    {
        if (protocol == OLLAMA_EMBED_LEGACY) {
            if (textNum != 1) {
                elog(ERROR, "Legacy Ollama embedding response requires one input text.");
            }
            results[start] = ParseOllamaVector(cJSON_GetObjectItemCaseSensitive(root, "embedding"), config->dimension);
        } else {
            ParseOllamaEmbeddingArray(root, &response);
        }
    }
    PG_CATCH();
    {
        cJSON_Delete(root);
        pfree(seen);
        PG_RE_THROW();
    }
    PG_END_TRY();
    cJSON_Delete(root);
    pfree(seen);
}

OGAIString OllamaGenerateClient::BuildGenerateReqBody(OGAIString query)
{
    char* body = NULL;
    cJSON* root = cJSON_CreateObject();
    if (root == NULL) {
        elog(ERROR, "build generate api body error for ollama.");
    }

    cJSON_AddStringToObject(root, "model", config->modelName);
    cJSON* messages = cJSON_CreateArray();
    if (messages == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build generate api messages error for ollama.");
        return NULL;
    }

	// add system prompt
    cJSON* system_msg = cJSON_CreateObject();
    cJSON_AddStringToObject(system_msg, "role", "system");
    cJSON_AddStringToObject(system_msg, "content", "You are a helpful assistant.");
    cJSON_AddItemToArray(messages, system_msg);

    cJSON* user_msg = cJSON_CreateObject();
    cJSON_AddStringToObject(user_msg, "role", "user");
    cJSON_AddStringToObject(user_msg, "content", query);
    cJSON_AddItemToArray(messages, user_msg);

    cJSON_AddItemToObject(root, "messages", messages);
    cJSON_AddBoolToObject(root, "stream", false);

    char* jsonStr = cJSON_PrintUnformatted(root);
    if (jsonStr == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "build generate json body error for ollama.");
        return NULL;
    }

    body = pg_strdup(jsonStr);
    cJSON_free(jsonStr);
    cJSON_Delete(root);
    return body;
}

OGAIString OllamaGenerateClient::ParseGenerateRespBody(OGAIString respBody)
{
    char* result = NULL;

    cJSON* root = cJSON_Parse(respBody);
    if (root == NULL) {
        elog(ERROR, "parse generate api response json body error for ollama: %s.", respBody);
    }

    cJSON* message = cJSON_GetObjectItem(root, "message");
    if (message == NULL || !cJSON_IsObject(message)) {
        cJSON_Delete(root);
        elog(ERROR, "parse generate api response message error for ollama: %s.", respBody);
    }

    cJSON* content = cJSON_GetObjectItem(message, "content");
    if (content == NULL || !cJSON_IsString(content) || content->valuestring == NULL) {
        cJSON_Delete(root);
        elog(ERROR, "parse generate api response content error for ollama.");
    }

    result = pg_strdup(content->valuestring);
    cJSON_Delete(root);
    return result;
}

OGAIString OllamaRerankClient::BuildRerankReqBody(OGAIString query, InputDocuments* inputDocuments)
{
    elog(ERROR, "Ollama does not support rerank api now.");
    return NULL;
}

RerankResults* OllamaRerankClient::ParseRerankRespBody(OGAIString respBody, InputDocuments* inputDocuments)
{
    elog(ERROR, "Ollama does not support rerank api now.");
    return NULL;
}
