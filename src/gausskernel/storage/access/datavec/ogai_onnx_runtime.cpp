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

#include <algorithm>
#include <climits>
#include <cstdarg>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <memory>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <vector>
namespace {
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
    std::string inputType;
};
struct InputBuffer {
    TensorSpec spec;
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
    std::unique_ptr<Ort::Session> session;
    std::unique_ptr<TokenizerWrapper> tokenizer;
    std::vector<TensorSpec> inputs;
    std::vector<TensorSpec> outputs;
    RuntimeConfig config;
    int selectedOutputIndex = -1;
    int embeddingDim = 0;
    std::string modelPath;
    std::string configRoot;
    std::string tokenizerPath;
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
    std::ifstream in(path.c_str(), std::ios::in | std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}
static bool ExtractString(const std::string& json, const std::string& key, std::string* out)
{
    std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"");
    std::smatch match;
    if (std::regex_search(json, match, re)) {
        *out = match[1].str();
        return true;
    }
    return false;
}
static bool ExtractInt(const std::string& json, const std::string& key, int* out)
{
    std::regex re("\"" + key + "\"\\s*:\\s*([0-9]+)");
    std::smatch match;
    if (std::regex_search(json, match, re)) {
        *out = std::atoi(match[1].str().c_str());
        return true;
    }
    return false;
}
static bool ExtractBool(const std::string& json, const std::string& key, bool* out)
{
    std::regex re("\"" + key + "\"\\s*:\\s*(true|false)");
    std::smatch match;
    if (std::regex_search(json, match, re)) {
        *out = (match[1].str() == "true");
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

static RuntimeConfig LoadConfig(const std::string& configRoot)
{
    RuntimeConfig cfg;

    std::string metadata = ReadFileIfExists(JoinPath(configRoot, "metadata.json"));
    if (!metadata.empty()) {
        ExtractString(metadata, "input_type", &cfg.inputType);
        ExtractString(metadata, "output", &cfg.outputName);
        ExtractInt(metadata, "dimension", &cfg.dimension);
        ExtractInt(metadata, "max_length", &cfg.maxLen);

        std::string poolingType;
        if (ExtractString(metadata, "type", &poolingType)) {
            cfg.pooling = ParsePooling(poolingType);
        }

        bool enabled = true;
        if (ExtractBool(metadata, "enabled", &enabled)) {
            cfg.normalize = enabled;
        }
    }

    std::string stCfg = ReadFileIfExists(JoinPath(configRoot, "sentence_bert_config.json"));
    if (!stCfg.empty()) {
        ExtractInt(stCfg, "max_seq_length", &cfg.maxLen);
    }

    std::string poolCfg = ReadFileIfExists(JoinPath(JoinPath(configRoot, "1_Pooling"), "config.json"));
    if (!poolCfg.empty()) {
        bool enabled = false;
        if (ExtractBool(poolCfg, "pooling_mode_cls_token", &enabled) && enabled) {
            cfg.pooling = Pooling::CLS;
        } else if (ExtractBool(poolCfg, "pooling_mode_max_tokens", &enabled) && enabled) {
            cfg.pooling = Pooling::MAX;
        } else if (ExtractBool(poolCfg, "pooling_mode_lasttoken", &enabled) && enabled) {
            cfg.pooling = Pooling::LAST_TOKEN;
        } else if (ExtractBool(poolCfg, "pooling_mode_mean_tokens", &enabled) && enabled) {
            cfg.pooling = Pooling::MEAN;
        }
        ExtractInt(poolCfg, "word_embedding_dimension", &cfg.dimension);
    }

    std::string hfCfg = ReadFileIfExists(JoinPath(configRoot, "config.json"));
    if (!hfCfg.empty() && cfg.dimension == 0) {
        ExtractInt(hfCfg, "hidden_size", &cfg.dimension);
    }
    if (!hfCfg.empty()) {
        ExtractInt(hfCfg, "pad_token_id", &cfg.padTokenId);
    }

    std::string tokCfg = ReadFileIfExists(JoinPath(configRoot, "tokenizer.json"));
    if (!tokCfg.empty()) {
        ExtractInt(tokCfg, "pad_id", &cfg.padTokenId);
    }

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

    for (const char* preferred : {"sentence_embedding", "embedding"}) {
        for (size_t i = 0; i < outputs.size(); ++i) {
            if (outputs[i].name == preferred) {
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

static void FillInputBuffer(const TensorSpec& spec, const TokenBatch& tokenBatch, InputBuffer* buf)
{
    buf->spec = spec;
    buf->shape = {static_cast<int64_t>(tokenBatch.textNum), static_cast<int64_t>(tokenBatch.seqLen)};

    std::vector<int64_t> values(tokenBatch.textNum * tokenBatch.seqLen, 0);
    if (NameContains(spec.name, "input_ids")) {
        for (size_t b = 0; b < tokenBatch.textNum; ++b) {
            for (size_t s = 0; s < tokenBatch.seqLen; ++s) {
                values[b * tokenBatch.seqLen + s] = tokenBatch.ids[b][s];
            }
        }
    } else if (NameContains(spec.name, "attention_mask")) {
        values = tokenBatch.attentionMask;
    } else if (NameContains(spec.name, "token_type_ids") || NameContains(spec.name, "segment_ids")) {
        std::fill(values.begin(), values.end(), 0);
    } else if (NameContains(spec.name, "position_ids")) {
        for (size_t b = 0; b < tokenBatch.textNum; ++b) {
            for (size_t s = 0; s < tokenBatch.seqLen; ++s) {
                values[b * tokenBatch.seqLen + s] = static_cast<int64_t>(s);
            }
        }
    } else {
        throw std::runtime_error("unsupported required ONNX input: " + spec.name);
    }

    if (spec.elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64) {
        buf->i64 = values;
    } else if (spec.elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32) {
        buf->i32.resize(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            buf->i32[i] = static_cast<int32_t>(values[i]);
        }
    } else {
        throw std::runtime_error("unsupported input type for " + spec.name);
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
        std::copy(src, src + hidden, output.embeddings[b]);
        if (output.normalize) {
            Normalize(output.embeddings[b], output.dim);
        }
    }
}

static void PoolMaskedRank3Embedding(const PoolInput& input, bool useMax, float* dst)
{
    size_t used = 0;
    std::fill(dst, dst + input.hidden, useMax ? -std::numeric_limits<float>::infinity() : 0.0f);
    for (size_t s = 0; s < input.outSeq && s < input.maskSeq; ++s) {
        if (input.attentionMask[s] == 0) {
            continue;
        }
        const float* row = input.data + s * input.hidden;
        for (int h = 0; h < input.hidden; ++h) {
            if (useMax) {
                dst[h] = std::max(dst[h], row[h]);
            } else {
                dst[h] += row[h];
            }
        }
        ++used;
    }
    if (useMax) {
        if (used == 0) {
            std::fill(dst, dst + input.hidden, 0.0f);
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
        std::copy(input.data, input.data + input.hidden, dst);
    } else if (pooling == Pooling::LAST_TOKEN) {
        size_t last = 0;
        for (size_t s = 0; s < input.outSeq && s < input.maskSeq; ++s) {
            if (input.attentionMask[s] != 0) {
                last = s;
            }
        }
        std::copy(input.data + last * input.hidden, input.data + last * input.hidden + input.hidden, dst);
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
        tokenBatch->seqLen = std::max(tokenBatch->seqLen, tokenBatch->ids[i].size());
    }
    if (tokenBatch->seqLen == 0) {
        throw std::runtime_error("tokenizer returned empty input");
    }
    tokenBatch->attentionMask.assign(request.textNum * tokenBatch->seqLen, 0);
    for (size_t b = 0; b < request.textNum; ++b) {
        tokenBatch->ids[b].resize(tokenBatch->seqLen, model.config.padTokenId);
        maskBatch[b].resize(tokenBatch->seqLen, 0);
        std::copy(maskBatch[b].begin(), maskBatch[b].end(),
            tokenBatch->attentionMask.begin() + b * tokenBatch->seqLen);
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
        inputNames.push_back(buf.spec.name.c_str());
        if (buf.spec.elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64) {
            inputTensors.push_back(Ort::Value::CreateTensor<int64_t>(memory, buf.i64.data(),
                buf.i64.size(), buf.shape.data(), buf.shape.size()));
        } else if (buf.spec.elemType == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32) {
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

    try {
        OgaiOnnxEnv* env = static_cast<OgaiOnnxEnv*>(envHandle);
        std::unique_ptr<OgaiOnnxModel> model(new OgaiOnnxModel());

        model->modelPath = ResolveModelPath(modelPath);
        model->configRoot = ResolveConfigRoot(model->modelPath);
        model->tokenizerPath = ResolveTokenizerPath(model->configRoot, model->modelPath);
        model->config = LoadConfig(model->configRoot);

        model->session.reset(new Ort::Session(env->env, model->modelPath.c_str(), env->sessionOptions));
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

        model->tokenizer.reset(new TokenizerWrapper());
        model->tokenizer->Load(model->tokenizerPath);

        *dim = model->embeddingDim;
        return model.release();
    } catch (const std::exception& e) {
        SetError(errbuf, errbufLen, "failed to load ONNX model '%s': %s", modelPath, e.what());
        return nullptr;
    } catch (...) {
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
