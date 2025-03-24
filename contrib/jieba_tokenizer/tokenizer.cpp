/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * tokenizer.cpp
 *
 * IDENTIFICATION
 *        contrib/jieba_tokenizer/tokenizer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>
#include <cctype>
#include <stdlib.h>
#include <errno.h>
#include <securec.h>
#include "zlib.h"
#include "cppjieba/Jieba.hpp"
#include "tokenizer.h"

const size_t MAX_LENGTH_CRC = 100;
const size_t MAX_KEYWORD_NUM = 100;
const size_t MAX_PATH_LEN = 1024;

const char* const DICT_PATH = "lib/jieba_dict/jieba.dict.utf8";
const char* const HMM_PATH = "lib/jieba_dict/hmm_model.utf8";
const char* const USER_DICT_PATH = "lib/jieba_dict/user.dict.utf8";
const char* const IDF_PATH = "lib/jieba_dict/idf.utf8";
const char* const STOP_WORD_PATH = "lib/jieba_dict/stop_words.utf8";

cppjieba::Jieba *jiebaTokenizer = nullptr;
inline static bool IsWhitespace(const std::string& str)
{
    return std::all_of(str.begin(), str.end(), ::isspace);
}

inline static std::string Convert2LowerCase(const std::string& str)
{
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return result;
}

inline static uint32_t HashString2Uint32(const std::string& srcStr)
{
    std::string subStr = srcStr;
    if (srcStr.length() > MAX_LENGTH_CRC) {
        subStr = srcStr.substr(0, MAX_LENGTH_CRC);
    }

    uint32_t crc = crc32(0, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(subStr.data()), subStr.length());
    return crc;
}

inline static void ConvertEmbeddingMap(std::unordered_map<uint32_t, float> tempMap, EmbeddingMap *embeddingMap)
{
    embeddingMap->size = tempMap.size();
    if  (embeddingMap->size == 0) {
        return;
    }
    embeddingMap->pairs = (EmbeddingPair *)malloc(embeddingMap->size * sizeof(EmbeddingPair));
    if (embeddingMap->pairs == nullptr) {
        return;
    }

    size_t index = 0;
    for (const auto& pair : tempMap) {
        embeddingMap->pairs[index].key = pair.first;
        embeddingMap->pairs[index].value = pair.second;
        index++;
    }
}

#ifdef __cplusplus
extern "C" {
#endif

bool CreateTokenizer()
{
    if (jiebaTokenizer != nullptr) {
        return true;
    }
    char *installPath = getenv("GAUSSHOME");
    if (installPath == nullptr) {
        return false;
    }
    char path[MAX_PATH_LEN] = {0};
    if (!realpath(installPath, path)) {
        if (errno != ENOENT && errno != EACCES) {
            return false;
        }
    }
    char dictPath[MAX_PATH_LEN] = {0};
    char hmmPath[MAX_PATH_LEN] = {0};
    char userDictPath[MAX_PATH_LEN] = {0};
    char idfPath[MAX_PATH_LEN] = {0};
    char stopWordPath[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(dictPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, DICT_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(hmmPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, HMM_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(userDictPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, USER_DICT_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(idfPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, IDF_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(stopWordPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, STOP_WORD_PATH);
    if (ret < 0) {
        return false;
    }
    jiebaTokenizer = new(std::nothrow) cppjieba::Jieba(std::string(dictPath), std::string(hmmPath),
        std::string(userDictPath), std::string(idfPath), std::string(stopWordPath));
    return (jiebaTokenizer == nullptr) ? false : true;
}

void DestroyTokenizer()
{
    if (jiebaTokenizer == nullptr) {
        return;
    }
    delete jiebaTokenizer;
    jiebaTokenizer = nullptr;
}

bool ConvertString2Embedding(const char* srcStr, EmbeddingMap *embeddingMap, bool isKeywordExtractor)
{
    if (jiebaTokenizer == nullptr || srcStr == nullptr || embeddingMap == nullptr) {
        return false;
    }
    std::string srcStrInput(srcStr);
    std::unordered_map<uint32_t, float> tempMap;
    if (isKeywordExtractor) {
        std::vector<cppjieba::KeywordExtractor::Word> keywords;
        jiebaTokenizer->extractor.Extract(srcStrInput, keywords, MAX_KEYWORD_NUM);
        for (const auto& keyword : keywords) {
            uint32_t hashValue = HashString2Uint32(Convert2LowerCase(keyword.word));
            tempMap[hashValue] += keyword.weight;
        }
        if (!tempMap.empty()) {
            ConvertEmbeddingMap(tempMap, embeddingMap);
            return true;
        }
    }

    std::vector<std::string> tokens;
    jiebaTokenizer->Cut(srcStrInput, tokens, true);
    for (const auto& token : tokens) {
        if (IsWhitespace(token)) {
            continue;
        }
        uint32_t hashValue = HashString2Uint32(Convert2LowerCase(token));
        tempMap[hashValue] += 1.0f;
    }
    ConvertEmbeddingMap(tempMap, embeddingMap);
    return true;
}

#ifdef __cplusplus
}
#endif
