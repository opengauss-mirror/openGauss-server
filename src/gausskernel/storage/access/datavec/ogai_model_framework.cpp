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
 * ogai_model_framework.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_model_framework.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_httpscan.h"
#include "access/datavec/ogai_utils.h"
#include "access/datavec/ogai_model_framework.h"

Vector** EmbeddingClient::BatchEmbed(OGAIString* texts, size_t textNum, int* dim)
{
    size_t i = 0;
    size_t batch = 0;
    KmErr* errBuf = NULL;
    HttpMgr* httpClient = NULL;
    char* reqBody = NULL;
    char* respBody = NULL;
    Vector** results = NULL;

    if (texts == NULL || textNum <= 0 || dim == NULL || *dim <= 0) {
        elog(ERROR, "Invalid params");
    }

    errBuf = km_err_new(OGAI_ERROR_BUF_SZ);
    if (errBuf == NULL) {
        elog(ERROR, "new ogai http error buf failed.");
    }

    results = (Vector**)palloc0(sizeof(Vector*) * textNum);
    for (i = 0; i < textNum; i += config->maxBatch) {
        /* todo optimize */
        httpClient = GetHttpMgr(errBuf, config->baseUrl, config->apiKey, config->provider);
        batch = (i + config->maxBatch >= textNum) ? (textNum - i) : config->maxBatch;
        reqBody = BuildEmbeddingReqBody(&texts[i], batch);
        httpmgr_set_req_header(httpClient, httpmgr_get_req_body_len(httpClient, reqBody));
        httpmgr_set_req_body(httpClient, reqBody);
        httpmgr_receive(httpClient);
        if (km_err_catch(errBuf)) {
            elog(WARNING, "request failed: %s.", errBuf->buf);
            km_err_free(errBuf);
            httpmgr_free(httpClient);
            pfree_ext(results);
            elog(ERROR, "call embedding model request failed.");
        }
        respBody = httpmgr_get_res_body(httpClient);
        if (respBody == NULL) {
            km_err_free(errBuf);
            httpmgr_free(httpClient);
            pfree_ext(results);
            elog(ERROR, "call embedding model respond failed.");
        }
        ParseEmbeddingRespBody(respBody, results, i, batch);
        km_err_free(errBuf);
        httpmgr_free(httpClient);
    }
    return results;
}

OGAIString GenerateClient::Generate(OGAIString query)
{
    size_t i = 0;
    size_t batch = 0;
    KmErr* errBuf = NULL;
    HttpMgr* httpClient = NULL;
    char* reqBody = NULL;
    char* respBody = NULL;
    OGAIString results = NULL;

    errBuf = km_err_new(OGAI_ERROR_BUF_SZ);
    if (errBuf == NULL) {
        elog(ERROR, "new ogai http error buf failed.");
    }

    httpClient = GetHttpMgr(errBuf, config->baseUrl, config->apiKey, config->provider);
    reqBody = BuildGenerateReqBody(query);
    httpmgr_set_req_header(httpClient, httpmgr_get_req_body_len(httpClient, reqBody));
    httpmgr_set_req_body(httpClient, reqBody);
    httpmgr_receive(httpClient);
    if (km_err_catch(errBuf)) {
        elog(WARNING, "request failed: %s.", errBuf->buf);
        km_err_free(errBuf);
        httpmgr_free(httpClient);
        elog(ERROR, "call generate model request failed.");
    }
    respBody = httpmgr_get_res_body(httpClient);
    if (respBody == NULL) {
        km_err_free(errBuf);
        httpmgr_free(httpClient);
        elog(ERROR, "call generate model failed.");
    }
    results = ParseGenerateRespBody(respBody);
    km_err_free(errBuf);
    httpmgr_free(httpClient);
    return results;
}

RerankResults* RerankClient::Rerank(OGAIString query, InputDocuments* documents)
{
    KmErr* errBuf = NULL;
    HttpMgr* httpClient = NULL;
    char* reqBody = NULL;
    char* respBody = NULL;
    RerankResults* results = NULL;

    errBuf = km_err_new(OGAI_ERROR_BUF_SZ);
    if (errBuf == NULL) {
        elog(ERROR, "new ogai http error buf failed.");
    }

    httpClient = GetHttpMgr(errBuf, config->baseUrl, config->apiKey, config->provider);
    reqBody = BuildRerankReqBody(query, documents);
    httpmgr_set_req_header(httpClient, httpmgr_get_req_body_len(httpClient, reqBody));
    httpmgr_set_req_body(httpClient, reqBody);
    httpmgr_receive(httpClient);
    if (km_err_catch(errBuf)) {
        elog(WARNING, "request failed: %s.", errBuf->buf);
        km_err_free(errBuf);
        httpmgr_free(httpClient);
        elog(ERROR, "call rerank model failed.");
    }
    respBody = httpmgr_get_res_body(httpClient);
    if (respBody == NULL) {
        km_err_free(errBuf);
        httpmgr_free(httpClient);
        elog(ERROR, "call generate model failed.");
    }
    results = ParseRerankRespBody(respBody, documents);
    km_err_free(errBuf);
    httpmgr_free(httpClient);
    return results;
}
