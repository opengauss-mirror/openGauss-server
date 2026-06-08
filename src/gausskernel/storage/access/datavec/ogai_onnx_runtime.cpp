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
 * ogai_onnx_runtime.cpp
 *        Server-side ONNX dense embedding runtime.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai_onnx_runtime.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cjson/cJSON.h"
#include "storage/smgr/fd.h"
#include "access/datavec/ogai_onnx_runtime.h"

/*
 * ONNX Runtime C++ headers and some C++ standard headers use names that collide
 * with openGauss compatibility macros from c.h.
 */
#ifdef Abs
#undef Abs
#endif
#ifdef gettext
#undef gettext
#endif
#ifdef dgettext
#undef dgettext
#endif
#ifdef ngettext
#undef ngettext
#endif
#ifdef dngettext
#undef dngettext
#endif

#include "tokenizers_ffi.h"
#include "onnxruntime_cxx_api.h"

#include <climits>
#include <cstdarg>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <vector>
namespace {
/*
 * Keep STL usage local to this ONNX Runtime C++ adapter. Callers see only the
 * C-style handles declared in ogai_onnx_runtime.h.
 */
static const int OGAI_ONNX_DEFAULT_MAX_LEN = 256;
enum TensorShapeMeta { BATCH_AXIS, SEQUENCE_AXIS, HIDDEN_AXIS, RANK_TWO = HIDDEN_AXIS, RANK_THREE };
enum class Pooling {
    MEAN,
    CLS,
    MAX,
    LAST_TOKEN
};
struct TensorSpec {
    std::string name;
    ONNXTensorElementDataType elemType = ONNX_TENSOR_ELEMENT_DATA_TYPE_UNDEFINED;
    std::vector<int64_t> shape;
};
struct RuntimeConfig {
    int maxLen = OGAI_ONNX_DEFAULT_MAX_LEN;
    int padTokenId = 0;
    int dimension = 0;
    bool normalize = true;
    Pooling pooling = Pooling::MEAN;
    std::string outputName;
};
struct InputBuffer {
    const TensorSpec* spec = nullptr;
    std::vector<int64_t> shape;
    std::vector<int64_t> i64;
    std::vector<int32_t> i32;
};
struct TokenizedInput {
    std::vector<int64_t> ids;
    std::vector<int64_t> attentionMask;
};
struct TokenBatch {
    std::vector<std::vector<int64_t>> ids;
    std::vector<int64_t> attentionMask;
    size_t textNum = 0;
    size_t seqLen = 0;
};
struct EmbeddingOutput {
    size_t batch;
    const std::vector<int64_t>* attentionMask;
    size_t seqLen;
    Pooling pooling;
    float** embeddings;
    int dim;
    bool normalize;
};
struct PoolInput {
    const float* data;
    size_t outSeq;
    int hidden;
    const int64_t* attentionMask;
    size_t maskSeq;
};
struct OgaiOnnxEnv {
    Ort::Env env;
    Ort::SessionOptions sessionOptions;

    OgaiOnnxEnv() : env(ORT_LOGGING_LEVEL_WARNING, "ogai_onnx")
    {
        sessionOptions.SetIntraOpNumThreads(1);
        sessionOptions.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_BASIC);
    }
};
class TokenizerWrapper {
public:
    TokenizerWrapper() = default;
    ~TokenizerWrapper()
    {
        if (handle_ != nullptr) {
            tokenizer_free(handle_);
        }
    }

    void Load(const std::string& path)
    {
        handle_ = tokenizer_from_file(path.c_str());
        if (handle_ == nullptr) {
            const char* err = tokenizer_get_last_error();
            throw std::runtime_error(std::string("failed to load tokenizer: ") + (err == nullptr ? "unknown" : err));
        }
    }

    TokenizedInput Encode(const char* text, int maxLen) const
    {
        if (text == nullptr) {
            throw std::runtime_error("input text is null");
        }
        if (maxLen <= 0) {
            throw std::runtime_error("max input length must be greater than zero");
        }
        size_t maxIds = static_cast<size_t>(maxLen);
        std::vector<uint32_t> rawIds(maxIds);
        std::vector<uint32_t> rawMask(maxIds);
        size_t n = 0;
        int rc = tokenizer_encode_with_mask(handle_, text, rawIds.data(), rawMask.data(), maxIds, &n);
        if (rc != 0 || n == 0) {
            const char* err = tokenizer_get_last_error();
            throw std::runtime_error(std::string("tokenizer_encode_with_mask failed: ") +
                (err == nullptr ? "unknown" : err));
        }
        if (n > maxIds) {
            n = maxIds;
        }
        TokenizedInput result;
        result.ids.resize(n);
        result.attentionMask.resize(n);
        for (size_t i = 0; i < n; ++i) {
            result.ids[i] = static_cast<int64_t>(rawIds[i]);
            result.attentionMask[i] = static_cast<int64_t>(rawMask[i]);
        }
        return result;
    }

private:
    TokenizerHandle handle_ = nullptr;
};

struct OgaiOnnxModel {
    Ort::Session* session = nullptr;
    TokenizerWrapper* tokenizer = nullptr;
    std::vector<TensorSpec> inputs;
    std::vector<TensorSpec> outputs;
    RuntimeConfig config;
    int selectedOutputIndex = -1;
    int embeddingDim = 0;

    ~OgaiOnnxModel()
    {
        delete tokenizer;
        delete session;
    }
};
static void SetError(char* errbuf, size_t errbufLen, const char* fmt, ...)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
static void SetError(char* errbuf, size_t errbufLen, const char* fmt, ...)
{
    if (errbuf == nullptr || errbufLen == 0) {
        return;
    }

    va_list args;
    va_start(args, fmt);
    int rc = vsnprintf_s(errbuf, errbufLen, errbufLen - 1, fmt, args);
    va_end(args);
    if (rc < 0) {
        errbuf[0] = '\0';
    } else {
        errbuf[errbufLen - 1] = '\0';
    }
}
static bool FileExists(const std::string& path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}
static bool DirExists(const std::string& path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}
static std::string JoinPath(const std::string& lhs, const std::string& rhs)
{
    return (lhs.empty() || lhs[lhs.size() - 1] == '/') ? lhs + rhs : lhs + "/" + rhs;
}
static std::string DirName(const std::string& path)
{
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return ".";
    }
    if (pos == 0) {
        return "/";
    }
    return path.substr(0, pos);
}
static std::string BaseName(const std::string& path)
{
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return path;
    }
    return path.substr(pos + 1);
}
static std::string ReadFileIfExists(const std::string& path)
{
    if (!FileExists(path)) {
        return "";
    }

    FILE* file = AllocateFile(path.c_str(), PG_BINARY_R);
    if (file == nullptr) {
        return "";
    }

    if (fseek(file, 0, SEEK_END) != 0) {
        FreeFile(file);
        return "";
    }

    long len = ftell(file);
    if (len <= 0) {
        FreeFile(file);
        return "";
    }
    rewind(file);

    std::string content;
    content.resize(static_cast<size_t>(len));
    size_t readLen = fread(&content[0], 1, static_cast<size_t>(len), file);
    FreeFile(file);

    if (readLen < static_cast<size_t>(len)) {
        content.resize(readLen);
    }
    return content;
}
static bool ExtractString(cJSON* root, const char* key, std::string* out)
{
    cJSON* item = cJSON_GetObjectItem(root, key);
    if (item != nullptr && cJSON_IsString(item) && item->valuestring != nullptr) {
        *out = item->valuestring;
        return true;
    }
    return false;
}
static bool ExtractInt(cJSON* root, const char* key, int* out)
{
    cJSON* item = cJSON_GetObjectItem(root, key);
    if (item != nullptr && cJSON_IsNumber(item)) {
        *out = item->valueint;
        return true;
    }
    return false;
}
static bool ExtractBool(cJSON* root, const char* key, bool* out)
{
    cJSON* item = cJSON_GetObjectItem(root, key);
    if (item != nullptr && cJSON_IsBool(item)) {
        *out = cJSON_IsTrue(item);
        return true;
    }
    return false;
}
static Pooling ParsePooling(const std::string& value)
{
    if (value == "cls" || value == "CLS") {
        return Pooling::CLS;
    }
    if (value == "max" || value == "MAX") {
        return Pooling::MAX;
    }
    if (value == "last_token" || value == "lasttoken" || value == "LAST_TOKEN") {
        return Pooling::LAST_TOKEN;
    }
    return Pooling::MEAN;
}

static std::string ResolveModelPath(const std::string& arg)
{
    if (FileExists(arg)) {
        return arg;
    }
    if (!DirExists(arg)) {
        throw std::runtime_error("model path is neither a file nor a directory: " + arg);
    }
    std::string flat = JoinPath(arg, "model.onnx");
    if (FileExists(flat)) {
        return flat;
    }

    std::string nested = JoinPath(JoinPath(arg, "onnx"), "model.onnx");
    if (FileExists(nested)) {
        return nested;
    }

    throw std::runtime_error("cannot find model.onnx under: " + arg);
}

static std::string ResolveConfigRoot(const std::string& modelPath)
{
    std::string modelDir = DirName(modelPath);
    if (BaseName(modelDir) == "onnx" && FileExists(JoinPath(DirName(modelDir), "tokenizer.json"))) {
        return DirName(modelDir);
    }
    return modelDir;
}

static std::string ResolveTokenizerPath(const std::string& configRoot, const std::string& modelPath)
{
    std::string fromRoot = JoinPath(configRoot, "tokenizer.json");
    if (FileExists(fromRoot)) {
        return fromRoot;
    }
    std::string fromModelDir = JoinPath(DirName(modelPath), "tokenizer.json");
    if (FileExists(fromModelDir)) {
        return fromModelDir;
    }
    throw std::runtime_error("cannot find tokenizer.json near model");
}

static void LoadMetadataConfig(const std::string& configRoot, RuntimeConfig* cfg)
{
    std::string metadata = ReadFileIfExists(JoinPath(configRoot, "metadata.json"));
    if (metadata.empty()) {
        return;
    }

    cJSON* root = cJSON_Parse(metadata.c_str());
    if (root == nullptr) {
        return;
    }

    ExtractString(root, "output", &cfg->outputName);
    ExtractInt(root, "dimension", &cfg->dimension);
    ExtractInt(root, "max_length", &cfg->maxLen);

    std::string poolingType;
    if (ExtractString(root, "type", &poolingType)) {
        cfg->pooling = ParsePooling(poolingType);
    }

    bool enabled = true;
    if (ExtractBool(root, "enabled", &enabled)) {
        cfg->normalize = enabled;
    }
    cJSON_Delete(root);
}

static void LoadSentenceBertConfig(const std::string& configRoot, RuntimeConfig* cfg)
{
    std::string stCfg = ReadFileIfExists(JoinPath(configRoot, "sentence_bert_config.json"));
    if (stCfg.empty()) {
        return;
    }

    cJSON* root = cJSON_Parse(stCfg.c_str());
    if (root == nullptr) {
        return;
    }
    ExtractInt(root, "max_seq_length", &cfg->maxLen);
    cJSON_Delete(root);
}

static void LoadPoolingConfig(const std::string& configRoot, RuntimeConfig* cfg)
{
    std::string poolCfg = ReadFileIfExists(JoinPath(JoinPath(configRoot, "1_Pooling"), "config.json"));
    if (poolCfg.empty()) {
        return;
    }

    cJSON* root = cJSON_Parse(poolCfg.c_str());
    if (root == nullptr) {
        return;
    }

    bool enabled = false;
    if (ExtractBool(root, "pooling_mode_cls_token", &enabled) && enabled) {
        cfg->pooling = Pooling::CLS;
    } else if (ExtractBool(root, "pooling_mode_max_tokens", &enabled) && enabled) {
        cfg->pooling = Pooling::MAX;
    } else if (ExtractBool(root, "pooling_mode_lasttoken", &enabled) && enabled) {
        cfg->pooling = Pooling::LAST_TOKEN;
    } else if (ExtractBool(root, "pooling_mode_mean_tokens", &enabled) && enabled) {
        cfg->pooling = Pooling::MEAN;
    }
    ExtractInt(root, "word_embedding_dimension", &cfg->dimension);
    cJSON_Delete(root);
}

static void LoadHuggingFaceConfig(const std::string& configRoot, RuntimeConfig* cfg)
{
    std::string hfCfg = ReadFileIfExists(JoinPath(configRoot, "config.json"));
    if (hfCfg.empty()) {
        return;
    }

    cJSON* root = cJSON_Parse(hfCfg.c_str());
    if (root == nullptr) {
        return;
    }
    if (cfg->dimension == 0) {
        ExtractInt(root, "hidden_size", &cfg->dimension);
    }
    ExtractInt(root, "pad_token_id", &cfg->padTokenId);
    cJSON_Delete(root);
}

static void LoadTokenizerConfig(const std::string& configRoot, RuntimeConfig* cfg)
{
    std::string tokCfg = ReadFileIfExists(JoinPath(configRoot, "tokenizer.json"));
    if (tokCfg.empty()) {
        return;
    }

    cJSON* root = cJSON_Parse(tokCfg.c_str());
    if (root == nullptr) {
        return;
    }
    if (!ExtractInt(root, "pad_id", &cfg->padTokenId)) {
        cJSON* padding = cJSON_GetObjectItem(root, "padding");
        if (padding != nullptr) {
            ExtractInt(padding, "pad_id", &cfg->padTokenId);
        }
    }
    cJSON_Delete(root);
}

static RuntimeConfig LoadConfig(const std::string& configRoot)
{
    RuntimeConfig cfg;

    LoadMetadataConfig(configRoot, &cfg);
    LoadSentenceBertConfig(configRoot, &cfg);
    LoadPoolingConfig(configRoot, &cfg);
    LoadHuggingFaceConfig(configRoot, &cfg);
    LoadTokenizerConfig(configRoot, &cfg);
    return cfg;
}

static std::vector<TensorSpec> GetInputs(Ort::Session& session)
{
    Ort::AllocatorWithDefaultOptions allocator;
    std::vector<TensorSpec> specs;
    size_t count = session.GetInputCount();
    specs.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        TensorSpec spec;
        auto name = session.GetInputNameAllocated(i, allocator);
        spec.name = name.get();
        auto typeInfo = session.GetInputTypeInfo(i);
        auto info = typeInfo.GetTensorTypeAndShapeInfo();
        spec.elemType = info.GetElementType();
        spec.shape = info.GetShape();
        specs.push_back(spec);
    }
    return specs;
}

static std::vector<TensorSpec> GetOutputs(Ort::Session& session)
{
    Ort::AllocatorWithDefaultOptions allocator;
    std::vector<TensorSpec> specs;
    size_t count = session.GetOutputCount();
    specs.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        TensorSpec spec;
        auto name = session.GetOutputNameAllocated(i, allocator);
        spec.name = name.get();
        auto typeInfo = session.GetOutputTypeInfo(i);
        auto info = typeInfo.GetTensorTypeAndShapeInfo();
        spec.elemType = info.GetElementType();
        spec.shape = info.GetShape();
        specs.push_back(spec);
    }
    return specs;
}

static bool NameContains(const std::string& name, const std::string& needle)
{
    return name.find(needle) != std::string::npos;
}

static int SelectOutputIndex(const std::vector<TensorSpec>& outputs, const RuntimeConfig& cfg)
{
    if (!cfg.outputName.empty()) {
        for (size_t i = 0; i < outputs.size(); ++i) {
            if (outputs[i].name == cfg.outputName) {
                return static_cast<int>(i);
            }
        }
    }

    static const char* preferredOutputs[] = {"sentence_embedding", "embedding"};
    for (size_t p = 0; p < sizeof(preferredOutputs) / sizeof(preferredOutputs[0]); ++p) {
        for (size_t i = 0; i < outputs.size(); ++i) {
            if (outputs[i].name == preferredOutputs[p]) {
                return static_cast<int>(i);
            }
        }
    }

    for (size_t i = 0; i < outputs.size(); ++i) {
        if (outputs[i].shape.size() == RANK_TWO && outputs[i].elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT) {
            return static_cast<int>(i);
        }
    }

    for (size_t i = 0; i < outputs.size(); ++i) {
        if (NameContains(outputs[i].name, "last_hidden_state")) {
            return static_cast<int>(i);
        }
    }

    return outputs.empty() ? -1 : 0;
}

static int ResolveEmbeddingDim(const TensorSpec& output, const RuntimeConfig& cfg)
{
    if (output.shape.empty()) {
        return cfg.dimension;
    }
    int64_t dim = output.shape.back();
    return (dim > 0 && dim <= INT_MAX) ? static_cast<int>(dim) : cfg.dimension;
}

template <typename T>
static void FillInputValues(const TensorSpec& spec, const TokenBatch& tokenBatch, T* values)
{
    size_t valueNum = tokenBatch.textNum * tokenBatch.seqLen;
    for (size_t i = 0; i < valueNum; ++i) {
        values[i] = 0;
    }

    if (NameContains(spec.name, "input_ids")) {
        for (size_t b = 0; b < tokenBatch.textNum; ++b) {
            for (size_t s = 0; s < tokenBatch.seqLen; ++s) {
                values[b * tokenBatch.seqLen + s] = static_cast<T>(tokenBatch.ids[b][s]);
            }
        }
    } else if (NameContains(spec.name, "attention_mask")) {
        for (size_t i = 0; i < valueNum; ++i) {
            values[i] = static_cast<T>(tokenBatch.attentionMask[i]);
        }
    } else if (NameContains(spec.name, "token_type_ids") || NameContains(spec.name, "segment_ids")) {
        return;
    } else if (NameContains(spec.name, "position_ids")) {
        for (size_t b = 0; b < tokenBatch.textNum; ++b) {
            for (size_t s = 0; s < tokenBatch.seqLen; ++s) {
                values[b * tokenBatch.seqLen + s] = static_cast<T>(s);
            }
        }
    } else {
        throw std::runtime_error("unsupported required ONNX input: " + spec.name);
    }
}

static void FillInputBuffer(const TensorSpec& spec, const TokenBatch& tokenBatch, InputBuffer* buf)
{
    buf->spec = &spec;
    buf->shape.clear();
    buf->shape.push_back(static_cast<int64_t>(tokenBatch.textNum));
    buf->shape.push_back(static_cast<int64_t>(tokenBatch.seqLen));

    if (spec.elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64) {
        buf->i64.resize(tokenBatch.textNum * tokenBatch.seqLen);
        FillInputValues(spec, tokenBatch, buf->i64.data());
    } else if (spec.elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32) {
        buf->i32.resize(tokenBatch.textNum * tokenBatch.seqLen);
        FillInputValues(spec, tokenBatch, buf->i32.data());
    } else {
        throw std::runtime_error("unsupported input type for " + spec.name);
    }
}

static void CopyFloatArray(const float* src, float* dst, int dim)
{
    for (int i = 0; i < dim; ++i) {
        dst[i] = src[i];
    }
}

static void FillFloatArray(float* dst, int dim, float value)
{
    for (int i = 0; i < dim; ++i) {
        dst[i] = value;
    }
}

static void Normalize(float* embedding, int dim)
{
    double sum = 0.0;
    for (int i = 0; i < dim; ++i) {
        sum += static_cast<double>(embedding[i]) * static_cast<double>(embedding[i]);
    }
    double norm = std::sqrt(sum);
    if (norm > 0.0) {
        for (int i = 0; i < dim; ++i) {
            embedding[i] = static_cast<float>(static_cast<double>(embedding[i]) / norm);
        }
    }
}

static void CopyRank2Embedding(const float* data, int hidden, const EmbeddingOutput& output)
{
    if (hidden != output.dim) {
        throw std::runtime_error("output dimension does not match loaded model dimension");
    }

    for (size_t b = 0; b < output.batch; ++b) {
        const float* src = data + b * hidden;
        CopyFloatArray(src, output.embeddings[b], hidden);
        if (output.normalize) {
            Normalize(output.embeddings[b], output.dim);
        }
    }
}

static void PoolMaskedRank3Embedding(const PoolInput& input, bool useMax, float* dst)
{
    size_t used = 0;
    FillFloatArray(dst, input.hidden, useMax ? -std::numeric_limits<float>::infinity() : 0.0f);
    for (size_t s = 0; s < input.outSeq && s < input.maskSeq; ++s) {
        if (input.attentionMask[s] == 0) {
            continue;
        }
        const float* row = input.data + s * input.hidden;
        for (int h = 0; h < input.hidden; ++h) {
            if (useMax) {
                dst[h] = (row[h] > dst[h]) ? row[h] : dst[h];
            } else {
                dst[h] += row[h];
            }
        }
        ++used;
    }
    if (useMax) {
        if (used == 0) {
            FillFloatArray(dst, input.hidden, 0.0f);
        }
        return;
    }
    used = (used == 0) ? 1 : used;
    for (int h = 0; h < input.hidden; ++h) {
        dst[h] /= static_cast<float>(used);
    }
}

static void PoolOneRank3Embedding(const PoolInput& input, Pooling pooling, float* dst)
{
    if (pooling == Pooling::CLS) {
        CopyFloatArray(input.data, dst, input.hidden);
    } else if (pooling == Pooling::LAST_TOKEN) {
        size_t last = 0;
        for (size_t s = 0; s < input.outSeq && s < input.maskSeq; ++s) {
            if (input.attentionMask[s] != 0) {
                last = s;
            }
        }
        const float* row = input.data + last * input.hidden;
        CopyFloatArray(row, dst, input.hidden);
    } else {
        PoolMaskedRank3Embedding(input, pooling == Pooling::MAX, dst);
    }
}

static void PoolRank3Embedding(const float* data, size_t outSeq, int hidden, const EmbeddingOutput& output)
{
    if (hidden != output.dim) {
        throw std::runtime_error("output dimension does not match loaded model dimension");
    }

    for (size_t b = 0; b < output.batch; ++b) {
        PoolInput input = {data + b * outSeq * hidden, outSeq, hidden,
            output.attentionMask->data() + b * output.seqLen, output.seqLen};
        PoolOneRank3Embedding(input, output.pooling, output.embeddings[b]);
        if (output.normalize) {
            Normalize(output.embeddings[b], output.dim);
        }
    }
}

static void ExtractEmbeddingBatch(const Ort::Value& tensor, const EmbeddingOutput& output)
{
    auto info = tensor.GetTensorTypeAndShapeInfo();
    if (info.GetElementType() != ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT) {
        throw std::runtime_error("selected output is not float");
    }

    std::vector<int64_t> shape = info.GetShape();
    const float* data = tensor.GetTensorData<float>();
    if (shape.size() == RANK_TWO) {
        if (shape[BATCH_AXIS] > 0 && static_cast<size_t>(shape[BATCH_AXIS]) != output.batch) {
            throw std::runtime_error("rank-2 output batch size mismatch");
        }
        CopyRank2Embedding(data, static_cast<int>(shape[SEQUENCE_AXIS]), output);
        return;
    }

    if (shape.size() == RANK_THREE) {
        if (shape[BATCH_AXIS] > 0 && static_cast<size_t>(shape[BATCH_AXIS]) != output.batch) {
            throw std::runtime_error("rank-3 output batch size mismatch");
        }
        PoolRank3Embedding(data, static_cast<size_t>(shape[SEQUENCE_AXIS]),
            static_cast<int>(shape[HIDDEN_AXIS]), output);
        return;
    }

    throw std::runtime_error("unsupported output rank");
}

static void TokenizeBatch(const OgaiOnnxModel& model, const OgaiOnnxEmbeddingRequest& request, TokenBatch* tokenBatch)
{
    std::vector<std::vector<int64_t>> maskBatch(request.textNum);
    tokenBatch->textNum = request.textNum;
    tokenBatch->seqLen = 0;
    tokenBatch->ids.resize(request.textNum);
    for (size_t i = 0; i < request.textNum; ++i) {
        TokenizedInput encoded = model.tokenizer->Encode(request.texts[i], model.config.maxLen);
        tokenBatch->ids[i].swap(encoded.ids);
        maskBatch[i].swap(encoded.attentionMask);
        if (tokenBatch->ids[i].size() > tokenBatch->seqLen) {
            tokenBatch->seqLen = tokenBatch->ids[i].size();
        }
    }
    if (tokenBatch->seqLen == 0) {
        throw std::runtime_error("tokenizer returned empty input");
    }
    tokenBatch->attentionMask.assign(request.textNum * tokenBatch->seqLen, 0);
    for (size_t b = 0; b < request.textNum; ++b) {
        tokenBatch->ids[b].resize(tokenBatch->seqLen, model.config.padTokenId);
        maskBatch[b].resize(tokenBatch->seqLen, 0);
        for (size_t s = 0; s < tokenBatch->seqLen; ++s) {
            tokenBatch->attentionMask[b * tokenBatch->seqLen + s] = maskBatch[b][s];
        }
    }
}

static void BuildInputTensors(const std::vector<TensorSpec>& inputs, const TokenBatch& tokenBatch,
    std::vector<InputBuffer>& buffers,
    std::vector<const char*>& inputNames, std::vector<Ort::Value>& inputTensors)
{
    Ort::MemoryInfo memory = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
    buffers.reserve(inputs.size());
    inputNames.reserve(inputs.size());
    inputTensors.reserve(inputs.size());
    for (const auto& spec : inputs) {
        buffers.emplace_back();
        InputBuffer& buf = buffers.back();
        FillInputBuffer(spec, tokenBatch, &buf);
        inputNames.push_back(buf.spec->name.c_str());
        if (buf.spec->elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64) {
            inputTensors.push_back(Ort::Value::CreateTensor<int64_t>(memory, buf.i64.data(),
                buf.i64.size(), buf.shape.data(), buf.shape.size()));
        } else if (buf.spec->elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32) {
            inputTensors.push_back(Ort::Value::CreateTensor<int32_t>(memory, buf.i32.data(),
                buf.i32.size(), buf.shape.data(), buf.shape.size()));
        }
    }
}

} // namespace

OgaiOnnxEnvHandle OgaiOnnxEnvCreate(char* errbuf, size_t errbufLen)
{
    try {
        return new OgaiOnnxEnv();
    } catch (const std::exception& e) {
        SetError(errbuf, errbufLen, "failed to create ONNX env: %s", e.what());
        return nullptr;
    } catch (...) {
        SetError(errbuf, errbufLen, "failed to create ONNX env: unknown error");
        return nullptr;
    }
}

void OgaiOnnxEnvRelease(OgaiOnnxEnvHandle envHandle)
{
    if (envHandle != nullptr) {
        delete static_cast<OgaiOnnxEnv*>(envHandle);
    }
}

OgaiOnnxModelHandle OgaiOnnxLoadModel(OgaiOnnxEnvHandle envHandle, const char* modelPath, int* dim,
    char* errbuf, size_t errbufLen)
{
    if (envHandle == nullptr || modelPath == nullptr || dim == nullptr) {
        SetError(errbuf, errbufLen, "invalid argument when loading ONNX model");
        return nullptr;
    }

    OgaiOnnxModel* model = nullptr;

    try {
        OgaiOnnxEnv* env = static_cast<OgaiOnnxEnv*>(envHandle);
        model = new OgaiOnnxModel();
        std::string resolvedModelPath = ResolveModelPath(modelPath);
        std::string configRoot = ResolveConfigRoot(resolvedModelPath);
        std::string tokenizerPath = ResolveTokenizerPath(configRoot, resolvedModelPath);

        model->config = LoadConfig(configRoot);

        model->session = new Ort::Session(env->env, resolvedModelPath.c_str(), env->sessionOptions);
        model->inputs = GetInputs(*model->session);
        model->outputs = GetOutputs(*model->session);
        model->selectedOutputIndex = SelectOutputIndex(model->outputs, model->config);
        if (model->selectedOutputIndex < 0) {
            throw std::runtime_error("model has no output");
        }

        model->embeddingDim = ResolveEmbeddingDim(model->outputs[static_cast<size_t>(model->selectedOutputIndex)],
            model->config);
        if (model->embeddingDim <= 0) {
            throw std::runtime_error("cannot determine embedding dimension");
        }

        model->tokenizer = new TokenizerWrapper();
        model->tokenizer->Load(tokenizerPath);

        *dim = model->embeddingDim;
        return model;
    } catch (const std::exception& e) {
        delete model;
        SetError(errbuf, errbufLen, "failed to load ONNX model '%s': %s", modelPath, e.what());
        return nullptr;
    } catch (...) {
        delete model;
        SetError(errbuf, errbufLen, "failed to load ONNX model '%s': unknown error", modelPath);
        return nullptr;
    }
}

void OgaiOnnxUnloadModel(OgaiOnnxModelHandle modelHandle)
{
    if (modelHandle != nullptr) {
        delete static_cast<OgaiOnnxModel*>(modelHandle);
    }
}

OgaiOnnxStatus OgaiOnnxEmbeddingInferBatch(OgaiOnnxModelHandle modelHandle, OgaiOnnxEmbeddingRequest* request)
{
    if (request == nullptr) {
        return OGAI_ONNX_ERR;
    }
    if (modelHandle == nullptr || request->texts == nullptr || request->embeddings == nullptr ||
        request->textNum == 0) {
        SetError(request->errbuf, request->errbufLen, "invalid argument when running ONNX embedding inference");
        return OGAI_ONNX_ERR;
    }

    try {
        OgaiOnnxModel* model = static_cast<OgaiOnnxModel*>(modelHandle);
        if (request->dim != model->embeddingDim) {
            throw std::runtime_error("requested dimension does not match loaded model dimension");
        }

        TokenBatch tokenBatch;
        TokenizeBatch(*model, *request, &tokenBatch);
        std::vector<InputBuffer> buffers;
        std::vector<const char*> inputNames;
        std::vector<Ort::Value> inputTensors;
        BuildInputTensors(model->inputs, tokenBatch, buffers, inputNames, inputTensors);
        const char* outputName = model->outputs[static_cast<size_t>(model->selectedOutputIndex)].name.c_str();
        std::vector<Ort::Value> outputTensors = model->session->Run(Ort::RunOptions{nullptr},
            inputNames.data(), inputTensors.data(), inputTensors.size(), &outputName, 1);
        if (outputTensors.empty()) {
            throw std::runtime_error("ONNX Runtime returned no output");
        }

        EmbeddingOutput output = {request->textNum, &tokenBatch.attentionMask, tokenBatch.seqLen,
            model->config.pooling, request->embeddings, request->dim, model->config.normalize};
        ExtractEmbeddingBatch(outputTensors[0], output);
        return OGAI_ONNX_OK;
    } catch (const std::exception& e) {
        SetError(request->errbuf, request->errbufLen, "%s", e.what());
        return OGAI_ONNX_ERR;
    } catch (...) {
        SetError(request->errbuf, request->errbufLen, "unknown ONNX embedding inference error");
        return OGAI_ONNX_ERR;
    }
}

int OgaiOnnxGetEmbeddingDim(OgaiOnnxModelHandle modelHandle)
{
    return (modelHandle == nullptr) ? -1 : static_cast<OgaiOnnxModel*>(modelHandle)->embeddingDim;
}
