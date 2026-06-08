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
 * ogai_onnx_embedding.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_onnx_embedding.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "access/datavec/ogai_model_framework.h"
#include "access/datavec/ogai_onnx_mgr.h"
#include "access/datavec/ogai_onnx_embedding.h"


EmbeddingClient* CreateONNXEmbeddingClient(ModelConfig* config)
{
    if (!config) return NULL;
    return New(CurrentMemoryContext) ONNXEmbeddingClient(config);
}

static ONNXModelDesc* AcquireONNXModelDesc(ModelConfig* config)
{
    ONNXModelDesc* modelDesc = NULL;

    if (config->modelKey != NULL && config->ownerName != NULL) {
        modelDesc = ONNX_MODEL_MGR->AcquireONNXModelDescByKey(config->modelKey, config->ownerName);
        if (modelDesc == NULL) {
            (void)ONNX_MODEL_MGR->LoadONNXModelByKey(config->modelKey, config->ownerName, config->baseUrl);
            modelDesc = ONNX_MODEL_MGR->AcquireONNXModelDescByKey(config->modelKey, config->ownerName);
        }
    }

    if (modelDesc == NULL || modelDesc->handle == NULL) {
        elog(ERROR, "Failed to get or load ONNX model");
    }
    return modelDesc;
}

static void CheckONNXEmbeddingDim(ModelConfig* config, int actualDim, int* dim)
{
    size_t actualDimSize = static_cast<size_t>(actualDim);
    if (config->dimension > 0 && config->dimension != actualDimSize) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("ONNX embedding dimension mismatch: model_key=%s, requested_dim=%zu, actual_dim=%d",
                        config->modelKey, config->dimension, actualDim)));
    }
    if (dim != NULL) {
        *dim = actualDim;
    }
}

static float** AllocateEmbeddingBuffer(size_t textNum, int actualDim)
{
    float** embeddings = (float**)palloc0(textNum * sizeof(float*));
    for (size_t i = 0; i < textNum; i++) {
        embeddings[i] = (float*)palloc0(actualDim * sizeof(float));
    }
    return embeddings;
}

static Vector** CopyEmbeddingResult(float** embeddings, size_t textNum, int actualDim)
{
    Vector** results = (Vector**)palloc0(textNum * sizeof(Vector*));
    for (size_t i = 0; i < textNum; i++) {
        Vector* vec = InitVector(actualDim);
        errno_t rc = memcpy_s(vec->x, actualDim * sizeof(float), embeddings[i], actualDim * sizeof(float));
        securec_check_c(rc, "\0", "\0");
        results[i] = vec;
    }
    return results;
}

Vector** ONNXEmbeddingClient::BatchEmbed(OGAIString* texts, size_t textNum, int* dim)
{
    ONNXModelDesc* modelDesc = AcquireONNXModelDesc(config);
    int actualDim = modelDesc->dim;
    float** embeddings = NULL;
    char errbuf[OGAI_ONNX_ERRMSG_LEN] = {0};
    OgaiOnnxStatus ret = OGAI_ONNX_ERR;

    PG_TRY();
    {
        CheckONNXEmbeddingDim(config, actualDim, dim);
        embeddings = AllocateEmbeddingBuffer(textNum, actualDim);
        OgaiOnnxEmbeddingRequest request = {texts, textNum, embeddings, actualDim, errbuf, sizeof(errbuf)};
        ret = OgaiOnnxEmbeddingInferBatch(modelDesc->handle, &request);
    }
    PG_CATCH();
    {
        ONNX_MODEL_MGR->ReleaseONNXModelDesc(modelDesc);
        PG_RE_THROW();
    }
    PG_END_TRY();

    ONNX_MODEL_MGR->ReleaseONNXModelDesc(modelDesc);
    if (ret != OGAI_ONNX_OK) {
        ereport(ERROR, (errmsg("ONNX embedding inference failed: %s", errbuf)));
    }

    return CopyEmbeddingResult(embeddings, textNum, actualDim);
}

char* ONNXEmbeddingClient::BuildEmbeddingReqBody(OGAIString* texts, size_t textNum)
{
    elog(ERROR, "ONNXEmbeddingClient::BuildEmbeddingReqBody not implemented");
}

void ONNXEmbeddingClient::ParseEmbeddingRespBody(char* respBody, Vector** results, size_t start, size_t textNum)
{
    elog(ERROR, "ONNXEmbeddingClient::BuildEmbeddingReqBody not implemented");
}
