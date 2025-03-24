
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
 * -------------------------------------------------------------------------
 *
 * tokenizer.h
 *
 * IDENTIFICATION
 *
 *        contrib/jieba_tokenizer/tokenizer.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TOKENIZER_H
#define TOKENIZER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct EmbeddingPair {
    uint32_t key;
    float value;
} EmbeddingPair;

typedef struct {
    EmbeddingPair *pairs;
    size_t size;
} EmbeddingMap;

bool CreateTokenizer();
void DestroyTokenizer();
bool ConvertString2Embedding(const char* srcStr, EmbeddingMap *embeddingMap, bool isKeywordExtractor);

#ifdef __cplusplus
}
#endif

#endif /* TOKENIZER_H */