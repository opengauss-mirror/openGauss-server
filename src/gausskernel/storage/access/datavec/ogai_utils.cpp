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
 * ogai_utils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <curl/curl.h>
#include "postgres.h"
#include "keymgr/comm/security_error.h"
#include "access/datavec/ogai_utils.h"

#define OLLAMA_TIMEOUT 300
#define OLLAMA_CONNECT_TIMEOUT 10

static const char OGAI_HTTP_SCHEME[] = "http://";
static const char OGAI_HTTPS_SCHEME[] = "https://";

struct OGAIUrlPath {
    const char* start;
    const char* end;
    const char* suffix;
};

static OGAIUrlPath GetOGAIUrlPath(const char* url)
{
    if (url == NULL || url[0] == '\0') {
        elog(ERROR, "OGAI model URL must not be empty.");
    }
    size_t schemeLength = 0;
    if (pg_strncasecmp(url, OGAI_HTTPS_SCHEME, sizeof(OGAI_HTTPS_SCHEME) - 1) == 0) {
        schemeLength = sizeof(OGAI_HTTPS_SCHEME) - 1;
    } else if (pg_strncasecmp(url, OGAI_HTTP_SCHEME, sizeof(OGAI_HTTP_SCHEME) - 1) == 0) {
        schemeLength = sizeof(OGAI_HTTP_SCHEME) - 1;
    } else {
        elog(ERROR, "OGAI model URL must use HTTP or HTTPS.");
    }
    const char* authority = url + schemeLength;
    const char* path = strpbrk(authority, "/?#");
    if (path == NULL) {
        path = url + strlen(url);
    }
    if (path == authority) {
        elog(ERROR, "OGAI model URL must include a host.");
    }
    const char* suffix = strpbrk(path, "?#");
    if (suffix == NULL) {
        suffix = url + strlen(url);
    }
    const char* end = suffix;
    while (end > path && end[-1] == '/') {
        --end;
    }
    OGAIUrlPath result = {path, end, suffix};
    return result;
}

static bool PathEndsWith(const OGAIUrlPath& path, const char* suffix)
{
    size_t length = strlen(suffix);
    return (size_t)(path.end - path.start) >= length &&
        strncmp(path.end - length, suffix, length) == 0;
}

OllamaEmbeddingProtocol GetOllamaEmbeddingProtocol(const char* url)
{
    OGAIUrlPath path = GetOGAIUrlPath(url);
    if (PathEndsWith(path, "/api/embeddings")) {
        return OLLAMA_EMBED_LEGACY;
    }
    if (PathEndsWith(path, "/v1") || PathEndsWith(path, "/v1/embeddings")) {
        return OLLAMA_EMBED_OPENAI;
    }
    return OLLAMA_EMBED_NATIVE;
}

char* ResolveOllamaEmbeddingUrl(const char* url)
{
    OGAIUrlPath path = GetOGAIUrlPath(url);
    const char* addition = "";
    if (path.start == path.end) {
        addition = "/api/embed";
    } else if (PathEndsWith(path, "/api")) {
        addition = "/embed";
    } else if (PathEndsWith(path, "/v1")) {
        addition = "/embeddings";
    } else if (!PathEndsWith(path, "/api/embed") && !PathEndsWith(path, "/api/embeddings") &&
        !PathEndsWith(path, "/v1/embeddings")) {
        return pstrdup(url);
    }
    return psprintf("%.*s%s%s", (int)(path.end - url), url, addition, path.suffix);
}

template <typename T>
static void SetOllamaCurlOption(HttpMgr* http, CURLoption option, T value)
{
    CURLcode result = curl_easy_setopt(http->client, option, value);
    if (result != CURLE_OK) {
        km_err_msg(http->err, "Could not configure Ollama request: %s.", curl_easy_strerror(result));
    }
}

static void SetOllamaRequestLine(HttpMgr* http, const char* url)
{
    /* Model requests do not use the IAM/KMS HTTPS-only and explicit-CA policy. */
    http->url = km_strdup(url);
    http->method = CURLOPT_POST;
    if (http->url == NULL) {
        km_err_msg(http->err, "Could not copy Ollama request URL.");
        return;
    }
    SetOllamaCurlOption(http, CURLOPT_URL, http->url);
    SetOllamaCurlOption(http, CURLOPT_POST, 1L);
    SetOllamaCurlOption(http, CURLOPT_PROTOCOLS, (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
    SetOllamaCurlOption(http, CURLOPT_REDIR_PROTOCOLS, (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
    SetOllamaCurlOption(http, CURLOPT_SSL_VERIFYPEER, 1L);
    SetOllamaCurlOption(http, CURLOPT_SSL_VERIFYHOST, 2L);
    const char* caBundle = gs_getenv_r("CURL_CA_BUNDLE");
    if (caBundle != NULL && caBundle[0] != '\0') {
        SetOllamaCurlOption(http, CURLOPT_CAINFO, caBundle);
    }
    http->timeout = OLLAMA_TIMEOUT;
    SetOllamaCurlOption(http, CURLOPT_TIMEOUT, (long)http->timeout);
    SetOllamaCurlOption(http, CURLOPT_CONNECTTIMEOUT, (long)OLLAMA_CONNECT_TIMEOUT);
}

HttpMgr* GetHttpMgr(KmErr* errBuf, const char* url, const char* apiKey, ModelProvider provider)
{
    if (provider == PROVIDER_OLLAMA) {
        (void)GetOGAIUrlPath(url);
    }
    HttpMgr* httpMgr = NULL;
    /* remain resbody */
    char remain[3] = {0, 0, 1};

    httpMgr = httpmgr_new(errBuf);
    if (httpMgr == NULL) {
        km_err_free(errBuf);
        elog(ERROR, "new ogai http client failed.");
        return NULL;
    }
    if (provider == PROVIDER_OLLAMA) {
        SetOllamaRequestLine(httpMgr, url);
    } else {
        httpmgr_set_req_line(httpMgr, url, CURLOPT_POST, NULL);
    }
    
    if (apiKey) {
        int authLen = strlen("Authorization: Bearer ") + strlen(apiKey) + 1;
        char* auth = (char *)km_alloc(authLen);
        if (auth == NULL) {
            elog(ERROR, "malloc memory error");
            return NULL;
        }
        errno_t rc = sprintf_s(auth, authLen, "Authorization: Bearer %s", apiKey);
        km_securec_check_ss(rc, "", "");
        httpmgr_set_req_header(httpMgr, auth);
    }
    httpmgr_set_req_header(httpMgr, "Content-Type:application/json");
    httpmgr_set_response(httpMgr, remain, NULL);
    return httpMgr;
}
