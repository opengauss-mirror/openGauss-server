/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 * Note: Provides interface for openGauss as tokenizer by using cppjieba api.
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
#include <cstdlib>
#include <thread>
#include <mutex>
#include <cerrno>
#include <securec.h>
#include "zlib.h"
#include "cppjieba/Jieba.hpp"
#include "tokenizer.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

const size_t MAX_LENGTH_CRC = 100;
const size_t MAX_KEYWORD_NUM = 100000;

const char* const DICT_PATH = "lib/jieba_dict/jieba.dict.utf8";
const char* const HMM_PATH = "lib/jieba_dict/hmm_model.utf8";
const char* const USER_DICT_PATH = "lib/jieba_dict/user.dict.utf8";
const char* const IDF_PATH = "lib/jieba_dict/idf.utf8";
const char* const STOP_WORD_PATH = "lib/jieba_dict/stop_words.utf8";

std::unique_ptr<cppjieba::Jieba> jiebaTokenizer = nullptr;
std::once_flag g_initFlag;

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

static void ConvertEmbeddingMap(std::unordered_map<std::string, std::pair<uint32_t, float>> tokensMap,
    EmbeddingMap *embeddingMap)
{
    embeddingMap->size = tokensMap.size();
    if  (embeddingMap->size == 0) {
        return;
    }
    embeddingMap->tokens = (EmbeddingTokenInfo *)malloc(embeddingMap->size * sizeof(EmbeddingTokenInfo));
    if (embeddingMap->tokens == nullptr) {
        embeddingMap->size = 0;
        return;
    }

    size_t idx = 0;
    for (const auto& token : tokensMap) {
        embeddingMap->tokens[idx].key = token.second.first;
        embeddingMap->tokens[idx].value = token.second.second;
        errno_t rc = strncpy_s(embeddingMap->tokens[idx].token, MAX_TOKEN_LEN, token.first.c_str(), MAX_TOKEN_LEN - 1);
        if (rc != EOK) {
            free(embeddingMap->tokens);
            embeddingMap->tokens = nullptr;
            embeddingMap->size = 0;
            return;
        }
        idx++;
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
    char path[PATH_MAX] = {0};
    if (!realpath(installPath, path)) {
        if (errno != ENOENT && errno != EACCES) {
            return false;
        }
    }
    char dictPath[PATH_MAX] = {0};
    char hmmPath[PATH_MAX] = {0};
    char userDictPath[PATH_MAX] = {0};
    char idfPath[PATH_MAX] = {0};
    char stopWordPath[PATH_MAX] = {0};
    int ret = snprintf_s(dictPath, PATH_MAX, PATH_MAX - 1, "%s/%s", path, DICT_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(hmmPath, PATH_MAX, PATH_MAX - 1, "%s/%s", path, HMM_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(userDictPath, PATH_MAX, PATH_MAX - 1, "%s/%s", path, USER_DICT_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(idfPath, PATH_MAX, PATH_MAX - 1, "%s/%s", path, IDF_PATH);
    if (ret < 0) {
        return false;
    }
    ret = snprintf_s(stopWordPath, PATH_MAX, PATH_MAX - 1, "%s/%s", path, STOP_WORD_PATH);
    if (ret < 0) {
        return false;
    }
    jiebaTokenizer.reset(new cppjieba::Jieba(std::string(dictPath), std::string(hmmPath),
        std::string(userDictPath), std::string(idfPath), std::string(stopWordPath)));
    return (jiebaTokenizer == nullptr) ? false : true;
}

void DestroyTokenizer()
{
    if (jiebaTokenizer == nullptr) {
        return;
    }
    jiebaTokenizer.reset();
    return;
}

bool ConvertString2Embedding(const char* srcStr, EmbeddingMap *embeddingMap, bool isKeywordExtractor, bool cutForSearch)
{
    std::call_once(g_initFlag, CreateTokenizer);
    if (jiebaTokenizer == nullptr || srcStr == nullptr || embeddingMap == nullptr) {
        return false;
    }

    std::string sentence(srcStr);
    std::unordered_map<std::string, std::pair<uint32_t, float>> tokensMap;
    if (isKeywordExtractor && !cutForSearch) {
        std::vector<cppjieba::KeywordExtractor::Word> keywords;
        jiebaTokenizer->extractor.Extract(sentence, keywords, MAX_KEYWORD_NUM);
        for (const auto& keyword : keywords) {
            std::string lowerCaseKeyword = Convert2LowerCase(keyword.word);
            uint32_t hashValue = HashString2Uint32(lowerCaseKeyword);
            tokensMap[lowerCaseKeyword] = std::make_pair(hashValue, keyword.weight);
        }
        if (!tokensMap.empty()) {
            ConvertEmbeddingMap(tokensMap, embeddingMap);
            return true;
        }
    }

    // if the keywords extracted by 'Extract' are empty, then use 'Cut' for tokenization.
    std::vector<std::string> tokens;
    if (cutForSearch) {
        jiebaTokenizer->CutForSearch(sentence, tokens, true);
    } else {
        jiebaTokenizer->Cut(sentence, tokens, true);
    }

    for (const auto& token : tokens) {
        if (IsWhitespace(token)) {
            continue;
        }
        std::string lowerCaseToken = Convert2LowerCase(token);
        uint32_t hashValue = HashString2Uint32(lowerCaseToken);
        if (tokensMap.find(lowerCaseToken) == tokensMap.end()) {
            tokensMap[lowerCaseToken] = std::make_pair(hashValue, 1.0f);
        } else {
            tokensMap[lowerCaseToken].second += 1.0f;
        }
    }
    ConvertEmbeddingMap(tokensMap, embeddingMap);
    return true;
}

#ifdef __cplusplus
}
#endif
