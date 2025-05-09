
/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 * Note: Provides interface for openGauss as tokenizer by using cppjieba api.
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

#include <cstdint>

const uint32_t MAX_TOKEN_LEN = 100;

#ifdef __cplusplus
extern "C" {
#endif

typedef struct EmbeddingTokenInfo {
    uint32_t key;
    float value;
    char token[MAX_TOKEN_LEN];
} EmbeddingTokenInfo;

typedef struct {
    EmbeddingTokenInfo *tokens;
    size_t size;
} EmbeddingMap;

bool CreateTokenizer();
void DestroyTokenizer();
bool ConvertString2Embedding(const char* srcStr, EmbeddingMap *embeddingMap, bool isKeywordExtractor,
    bool cutForSearch = false);

#ifdef __cplusplus
}
#endif

#endif /* TOKENIZER_H */