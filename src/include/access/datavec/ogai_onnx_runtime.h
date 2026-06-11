/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * ogai_onnx_runtime.h
 *        Server-side ONNX dense embedding runtime.
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ogai_onnx_runtime.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef OGAI_ONNX_RUNTIME_H
#define OGAI_ONNX_RUNTIME_H

#include "access/datavec/ogai_model_framework.h"

#define OGAI_ONNX_ERRMSG_LEN 1024

typedef void* OgaiOnnxEnvHandle;
typedef void* OgaiOnnxModelHandle;

typedef enum OgaiOnnxStatus {
    OGAI_ONNX_OK = 0,
    OGAI_ONNX_ERR = 1
} OgaiOnnxStatus;

OgaiOnnxEnvHandle OgaiOnnxEnvCreate(char* errbuf, size_t errbufLen);
void OgaiOnnxEnvRelease(OgaiOnnxEnvHandle envHandle);
OgaiOnnxModelHandle OgaiOnnxLoadModel(OgaiOnnxEnvHandle envHandle, const char* modelPath,
    int* dim, char* errbuf, size_t errbufLen);
void OgaiOnnxUnloadModel(OgaiOnnxModelHandle modelHandle);
typedef struct OgaiOnnxEmbeddingRequest {
    OGAIString* texts;
    size_t textNum;
    float** embeddings;
    int dim;
    char* errbuf;
    size_t errbufLen;
} OgaiOnnxEmbeddingRequest;
OgaiOnnxStatus OgaiOnnxEmbeddingInferBatch(OgaiOnnxModelHandle modelHandle, OgaiOnnxEmbeddingRequest* request);
int OgaiOnnxGetEmbeddingDim(OgaiOnnxModelHandle modelHandle);
#endif // OGAI_ONNX_RUNTIME_H
