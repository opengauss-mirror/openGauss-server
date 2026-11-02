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
 * ogai_utils.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ogai_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef OGAI_UTILS_H
#define OGAI_UTILS_H

#include "postgres.h"

#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_httpscan.h"
#include "access/datavec/ogai_model_framework.h"

#define OGAI_ERROR_BUF_SZ 4096

typedef enum {
    OLLAMA_EMBED_NATIVE,
    OLLAMA_EMBED_LEGACY,
    OLLAMA_EMBED_OPENAI
} OllamaEmbeddingProtocol;

OllamaEmbeddingProtocol GetOllamaEmbeddingProtocol(const char* url);
char* ResolveOllamaEmbeddingUrl(const char* url);

HttpMgr* GetHttpMgr(KmErr* errBuf, const char* url, const char* apiKey, ModelProvider provider);

#endif // OGAI_UTILS_H
