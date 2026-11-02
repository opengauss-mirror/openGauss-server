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
 * ogai_ollama.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ogai_ollama.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef OGAI_OLLAMA_H
#define OGAI_OLLAMA_H

#include "access/datavec/ogai_model_framework.h"

EmbeddingClient* CreateOllamaEmbeddingClient(ModelConfig* config);
GenerateClient* CreateOllamaGenerateClient(ModelConfig* config);
RerankClient* CreateOllamaRerankClient(ModelConfig* config);

class OllamaEmbeddingClient : public EmbeddingClient {
public:
    OllamaEmbeddingClient(ModelConfig* modelConfig)
    {
        config = modelConfig;
    }
    Vector** BatchEmbed(OGAIString* texts, size_t textNum, int* dim) override;
    char* BuildEmbeddingReqBody(OGAIString* texts, size_t textNum) override;
    void ParseEmbeddingRespBody(char* respBody, Vector** results, size_t start, size_t textNum) override;
};

class OllamaGenerateClient : public GenerateClient {
public:
    OllamaGenerateClient(ModelConfig* modelConfig)
    {
        config = modelConfig;
    }
    OGAIString BuildGenerateReqBody(OGAIString query) override;
    OGAIString ParseGenerateRespBody(OGAIString respBody)  override;
};

class OllamaRerankClient : public RerankClient {
public:
    OllamaRerankClient(ModelConfig* modelConfig)
    {
        config = modelConfig;
    }

    OGAIString BuildRerankReqBody(OGAIString query, InputDocuments* inputDocuments)  override;
    RerankResults* ParseRerankRespBody(OGAIString respBody, InputDocuments* inputDocuments) override;
};

#endif // OGAI_OLLAMA_H
