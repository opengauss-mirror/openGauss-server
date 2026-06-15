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
 * ogai_model_manager.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_model_manager.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/spi.h"
#include "access/htup.h"
#include "utils/builtins.h"
#include "cipher.h"
#include "access/datavec/ogai_model_framework.h"
#include "access/datavec/ogai_onnx_embedding.h"
#include "access/datavec/ogai_model_manager.h"

/*
 * DecryptApiKey - Decrypt api_key if it is encrypted
 *
 * @IN apiKey: api_key string from database (may be encrypted or plain text)
 * @RETURN: decrypted api_key string (caller should free it), or NULL if input is NULL
 */
static char* DecryptApiKey(const char* apiKey)
{
    if (apiKey == NULL) {
        return NULL;
    }

    /* Check if the api_key is encrypted */
    if (IsECEncryptedString(apiKey)) {
        char* plainApiKey = NULL;
        if (!decryptECString(apiKey, &plainApiKey, OGAI_MODE)) {
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                     errmsg("Failed to decrypt api_key")));
        }
        return plainApiKey;
    }

    /* Return a copy of plain text api_key */
    return pstrdup(apiKey);
}

static ProviderClientCreators providerCreators[] = {
    [PROVIDER_OPENAI] = {
        .createEmbedding = CreateOpenAIEmbeddingClient,
        .createGenerate = CreateOpenAIGenerateClient,
        .createRerank = CreateOpenAIRerankClient
    },
    [PROVIDER_QWEN] = {
        .createEmbedding = CreateQwenEmbeddingClient,
        .createGenerate = CreateQwenGenerateClient,
        .createRerank = CreateQwenRerankClient
    },
    [PROVIDER_OLLAMA] = {
        .createEmbedding = CreateOllamaEmbeddingClient,
        .createGenerate = CreateOllamaGenerateClient,
        .createRerank = CreateOllamaRerankClient
    },
    [PROVIDER_ONNX] = {
        .createEmbedding = CreateONNXEmbeddingClient,
        .createGenerate = NULL,
        .createRerank = NULL
    },
};

static void SetModelConfigFromDB(ModelConfig* config, const char* modelKey)
{
    char query[1024];
    int ret;
    int nrows = 0;
    HeapTuple tuple;
    MemoryContext originCtx = CurrentMemoryContext;

    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(ERROR, "Failed to connect to SPI");
    }

    errno_t nRet = snprintf_s(query, sizeof(query), sizeof(query) - 1,
                 "SELECT model_name, model_provider, api_key, url "
                 "FROM ogai.model_sources WHERE model_key = $1");
    securec_check_ss_c(nRet, "", "");
    Datum params[1];
    Oid paramtypes[1] = {TEXTOID};
    params[0] = CStringGetTextDatum(modelKey);
    ret = SPI_execute_with_args(query,
                                1,
                                paramtypes,
                                params,
                                NULL,
                                true,
                                0,
								NULL,
								NULL);
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_exec failed: %d", ret);

    nrows = SPI_processed;

    if (nrows != 1) {
        SPI_finish();
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ROWS),
        	errmsg("unexpected records found for model_key: %s, count: %d", modelKey, nrows)));
    }

    tuple = SPI_tuptable->vals[0];
    char* modelName = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
    char* modelProvider = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
    char* apiKey = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 3);
    char* url = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 4);
	
    if (modelName == NULL) {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("model_name cannot be NULL for model_key: %s", modelKey)));
    }
	
    if (modelProvider == NULL) {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("model_provider cannot be NULL for model_key: %s", modelKey)));
    }
	
    if (url == NULL) {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("url cannot be NULL for model_key: %s", modelKey)));
    }

    MemoryContext spiCtx = MemoryContextSwitchTo(originCtx);
	
    config->modelName = pstrdup(modelName);
    if (pg_strcasecmp(modelProvider, "openai") == 0) {
        config->provider = PROVIDER_OPENAI;
    } else if (pg_strcasecmp(modelProvider, "qwen") == 0) {
        config->provider = PROVIDER_QWEN;
    } else if (pg_strcasecmp(modelProvider, "ollama") == 0) {
        config->provider = PROVIDER_OLLAMA;
    } else if (pg_strcasecmp(modelProvider, "onnx") == 0) {
        config->provider = PROVIDER_ONNX;
    } else {
        MemoryContextSwitchTo(spiCtx);
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unknown provider: %s for model_key: %s", modelProvider, modelKey)));
    }
	
    config->apiKey = DecryptApiKey(apiKey);
    config->baseUrl = pstrdup(url);
    config->modelKey = pstrdup(modelKey);
    config->ownerName = pstrdup(GetUserNameFromId(GetUserId()));
    MemoryContextSwitchTo(spiCtx);

    SPI_finish();
}


static void AsyncSetModelConfigFromDB(
    ModelConfig* config, const char* modelKey, const char* ownerName)
{
    char query[1024];
    int ret;
    int nrows = 0;
    HeapTuple tuple;
    MemoryContext originCtx = CurrentMemoryContext;

    errno_t nRet = snprintf_s(query, sizeof(query), sizeof(query) - 1,
                 "SELECT model_name, model_provider, api_key, url "
                 "FROM ogai.model_sources "
                 "WHERE model_key = $1 AND owner_name = $2;");
    securec_check_ss_c(nRet, "", "");
    Datum params[2];
    Oid paramtypes[2] = {TEXTOID, TEXTOID};
    params[0] = CStringGetTextDatum(modelKey);
    params[1] = CStringGetTextDatum(ownerName);
    /* 2 means the sql above needs two input parameters */
    ret = SPI_execute_with_args(query,
                                2,
                                paramtypes,
                                params,
                                NULL,
                                true,
                                0,
								NULL,
								NULL);
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_exec failed: %d", ret);

    nrows = SPI_processed;

    if (nrows != 1) {
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ROWS),
        	errmsg("unexpected records found for model_key: %s, count: %d", modelKey, nrows)));
    }

    tuple = SPI_tuptable->vals[0];
    char* modelName = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
    char* modelProvider = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
    char* apiKey = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 3);
    char* url = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 4);
	
    if (modelName == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("model_name cannot be NULL for model_key: %s", modelKey)));
    }
	
    if (modelProvider == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("model_provider cannot be NULL for model_key: %s", modelKey)));
    }
	
    if (url == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("url cannot be NULL for model_key: %s", modelKey)));
    }

    MemoryContext spiCtx = MemoryContextSwitchTo(originCtx);
	
    config->modelName = pstrdup(modelName);
    if (pg_strcasecmp(modelProvider, "openai") == 0) {
        config->provider = PROVIDER_OPENAI;
    } else if (pg_strcasecmp(modelProvider, "qwen") == 0) {
        config->provider = PROVIDER_QWEN;
    } else if (pg_strcasecmp(modelProvider, "ollama") == 0) {
        config->provider = PROVIDER_OLLAMA;
    } else if (pg_strcasecmp(modelProvider, "onnx") == 0) {
        config->provider = PROVIDER_ONNX;
    } else {
        MemoryContextSwitchTo(spiCtx);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unknown provider: %s for model_key: %s", modelProvider, modelKey)));
    }
	
    config->apiKey = DecryptApiKey(apiKey);
    config->baseUrl = pstrdup(url);
    config->modelKey = pstrdup(modelKey);
    config->ownerName = pstrdup(ownerName);
    MemoryContextSwitchTo(spiCtx);
}


void GenerateModelConfig(ModelConfig* config, const char* modelKey)
{
    if (config == NULL || modelKey == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("model config or model_key cannot be NULL")));
    }
	
    SetModelConfigFromDB(config, modelKey);
}

void AsyncGenerateModelConfig(
        ModelConfig* config, const char* modelKey, const char* ownerName)
{
    if (config == NULL || modelKey == NULL || ownerName == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("model config or model_key cannot be NULL")));
    }
	
    AsyncSetModelConfigFromDB(config, modelKey, ownerName);
}

EmbeddingClient* CreateEmbeddingClient(ModelConfig* config)
{
    if (config->provider < 0 || config->provider >= sizeof(providerCreators) / sizeof(providerCreators[0])) {
        elog(ERROR, "invalid model provider: %d", config->provider);
    }

    EmbeddingClientCreator creator = providerCreators[config->provider].createEmbedding;
    if (creator == NULL) {
        elog(ERROR, "unsupported model provider for embedding client");
    }

    return creator(config);
}

GenerateClient* CreateGenerateClient(ModelConfig* config)
{
    if (config->provider < 0 ||  config->provider >= sizeof(providerCreators) / sizeof(providerCreators[0])) {
        elog(ERROR, "invalid model provider: %d", config->provider);
    }

    GenerateClientCreator creator = providerCreators[config->provider].createGenerate;
    if (creator == NULL) {
        elog(ERROR, "unsupported model provider for generate client");
    }

    return creator(config);
}

RerankClient* CreateRerankClient(ModelConfig* config)
{
    if (config->provider < 0 || config->provider >= sizeof(providerCreators) / sizeof(providerCreators[0])) {
        elog(ERROR, "invalid model provider: %d", config->provider);
    }

    RerankClientCreator creator = providerCreators[config->provider].createRerank;
    if (creator == NULL) {
        elog(ERROR, "unsupported model provider for rerank client");
    }

    return creator(config);
}
