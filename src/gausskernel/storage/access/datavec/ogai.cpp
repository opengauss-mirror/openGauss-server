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
 * ogai.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ogai.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/spi.h"
#include "access/htup.h"
#include "access/hash.h"
#include "funcapi.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "cipher.h"
#include "access/datavec/ogai_model_framework.h"
#include "access/datavec/ogai_model_manager.h"
#include "access/datavec/ogai_text_splitter.h"
#include "access/datavec/ogai_onnx_mgr.h"
#include "access/datavec/vector.h"
#include "access/datavec/ogai_worker.h"
#include "access/datavec/ogai.h"

static const char* OGAI_ONNX_USAGE_LOCK_NAMESPACE = "ogai_onnx_model_usage";
static const int ONNX_MODEL_REF_PARAM_NUM = 2;

static int32 HashCStringToInt32(const char* value)
{
    text* valueText = cstring_to_text(value);
    int32 hashValue = DatumGetInt32(DirectFunctionCall1(hashtext, PointerGetDatum(valueText)));
    pfree(valueText);
    return hashValue;
}

static int32 GetONNXUsageLockKey(const char* modelKey, const char* ownerName)
{
    size_t keyLen = strlen(ownerName) + strlen(modelKey) + 2;
    char* key = (char*)palloc0(keyLen);
    errno_t rc = snprintf_s(key, keyLen, keyLen - 1, "%s:%s", ownerName, modelKey);
    securec_check_ss_c(rc, "\0", "\0");

    int32 hashValue = HashCStringToInt32(key);
    pfree(key);
    return hashValue;
}

static void AcquireONNXModelUsageShareLock(const char* modelKey, const char* ownerName)
{
    int32 namespaceKey = HashCStringToInt32(OGAI_ONNX_USAGE_LOCK_NAMESPACE);
    int32 modelKeyHash = GetONNXUsageLockKey(modelKey, ownerName);

    (void)DirectFunctionCall2(pg_advisory_xact_lock_shared_int4,
                              Int32GetDatum(namespaceKey),
                              Int32GetDatum(modelKeyHash));
}

static bool TryAcquireONNXModelUsageExclusiveLock(const char* modelKey, const char* ownerName)
{
    int32 namespaceKey = HashCStringToInt32(OGAI_ONNX_USAGE_LOCK_NAMESPACE);
    int32 modelKeyHash = GetONNXUsageLockKey(modelKey, ownerName);

    return DatumGetBool(DirectFunctionCall2(pg_try_advisory_xact_lock_int4,
                                            Int32GetDatum(namespaceKey),
                                            Int32GetDatum(modelKeyHash)));
}

Datum ogai_embedding(PG_FUNCTION_ARGS)
{
    char* text = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
    char* model = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(1)));
    int dim = PG_GETARG_INT32(2);

    ModelConfig config;
    config.dimension = dim;
    config.maxBatch = 1;
    GenerateModelConfig(&config, model);
    if (config.provider == PROVIDER_ONNX) {
        AcquireONNXModelUsageShareLock(config.modelKey, config.ownerName);
    }
    EmbeddingClient* client = CreateEmbeddingClient(&config);
    Vector** vectors = client->BatchEmbed(&text, 1, &dim);
    PG_RETURN_POINTER(vectors[0]);
}

Datum ogai_generate(PG_FUNCTION_ARGS)
{
    char* answer = NULL;
    char* query = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
    char* model = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(1)));

    ModelConfig config;
    GenerateModelConfig(&config, model);
    GenerateClient* client = CreateGenerateClient(&config);
    if (!client) {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("failed to create generate client for model: %s", model)));
    }
    
    answer = client->Generate(query);
    if (!answer) {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("failed to generate answer for query")));
    }
    
    PG_RETURN_TEXT_P(cstring_to_text(answer));
}

Datum ogai_rerank(PG_FUNCTION_ARGS)
{
    FuncCallContext  *funcctx;
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        Datum      *elements;
        bool       *nulls;
        int         numDocs;
        TupleDesc tupdesc = NULL;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(3, false, TableAmHeap);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "origin_index", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "document", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "rerank_score", FLOAT8OID, -1, 0);

        char *query = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
        ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
        char *model = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(2)));

        if (ARR_NDIM(arr) != 1) {
            MemoryContextSwitchTo(oldcontext);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("array must be 1-dimensional")));
        }

        deconstruct_array(arr, TEXTOID, -1, false, 'i',
                          &elements, &nulls, &numDocs);

        if (numDocs == 0) {
            MemoryContextSwitchTo(oldcontext);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("document array cannot be empty")));
        }

        OGAIString *docArray = (OGAIString*) palloc(sizeof(OGAIString) * numDocs);
        for (int i = 0; i < numDocs; i++) {
            if (nulls[i]) {
                MemoryContextSwitchTo(oldcontext);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("null document at index %d", i)));
            }
            docArray[i] = text_to_cstring(DatumGetVarCharPP(elements[i]));
        }

        InputDocuments input_docs = { .docArray = docArray, .docCount = numDocs };

        ModelConfig config;
        GenerateModelConfig(&config, model);
        RerankClient *client = CreateRerankClient(&config);
        if (!client) {
            MemoryContextSwitchTo(oldcontext);
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("failed to create rerank client for model: %s", model)));
        }

        RerankResults *results = client->Rerank(query, &input_docs);
        if (!results) {
            MemoryContextSwitchTo(oldcontext);
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("failed to rerank documents")));
        }

        funcctx->user_fctx = results;
        funcctx->max_calls = results->docCount;
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[3];
        bool nulls[3] = {false, false, false};

        RerankResults *results = (RerankResults*) funcctx->user_fctx;
        RerankDocument *doc = &results->documents[funcctx->call_cntr];

        values[0] = Int32GetDatum(doc->originIndex);
        values[1] = CStringGetTextDatum(doc->document);
        values[2] = Float8GetDatum(doc->rerankScore);

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        Datum result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}

Datum ogai_chunk(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc = NULL;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(2, false, TableAmHeap);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "chunk_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "chunk", TEXTOID, -1, 0);

        char* document = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
        int maxChunkSize = PG_GETARG_INT32(1);

        int maxChunkOverlap = 0;
        if (PG_NARGS() > 2 && !PG_ARGISNULL(2)) {
            maxChunkOverlap = PG_GETARG_INT32(2);
        }

        if (maxChunkSize <= 0) {
            MemoryContextSwitchTo(oldcontext);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("maxChunkSize must be greater than 0")));
        }

        if (maxChunkOverlap < 0 || maxChunkOverlap >= maxChunkSize) {
            MemoryContextSwitchTo(oldcontext);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("maxChunkOverlap must be between 0 and maxChunkSize")));
        }

        TextSplitConfig split_config = BuildDefaultTextSplitConfig(maxChunkSize, maxChunkOverlap);

        TextSplitResult split_result;
        SplitTextByConfig(document, &split_config, &split_result);

        funcctx->user_fctx = split_result.chunks;
        funcctx->max_calls = split_result.chunkCount;
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[2];
        bool nulls[2] = {false, false};

        TextSplitChunk* chunks = (TextSplitChunk*) funcctx->user_fctx;
        TextSplitChunk* chunk = &chunks[funcctx->call_cntr];

        values[0] = Int32GetDatum(funcctx->call_cntr);
        values[1] = CStringGetTextDatum(chunk->chunk);

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        Datum result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    SRF_RETURN_DONE(funcctx);
}

Datum ogai_notify(PG_FUNCTION_ARGS)
{
    Oid dboid = u_sess->proc_cxt.MyDatabaseId;
    int actualOgaiWorkers = MAX_OGAI_WORKERS;
    int targetSlot = 0;
    bool hasTargetSlot = false;

    if (!g_instance.attr.attr_storage.enable_async_ogai ||
        t_thrd.ogailauncher_cxt.ogaiWorkerShmem == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("async OGAI is not enabled"),
                 errhint("Set enable_async_ogai=on and restart the database before using async vectorize tasks.")));
    }

    for (int i = 0; i < actualOgaiWorkers; i++) {
        if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem->target_dbs[i] == dboid) {
            targetSlot = i;
            hasTargetSlot = true;
            break;
        }
        if (!hasTargetSlot && !OidIsValid(t_thrd.ogailauncher_cxt.ogaiWorkerShmem->target_dbs[i])) {
            targetSlot = i;
            hasTargetSlot = true;
        }
    }

    if (!hasTargetSlot) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("too many pending OGAI async target databases")));
    }

    t_thrd.ogailauncher_cxt.ogaiWorkerShmem->target_dbs[targetSlot] = dboid;
    SetLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);
    PG_RETURN_VOID();
}

/*
 * load_onnx_model - Load ONNX model into cache
 *
 * Usage: SELECT load_onnx_model('model_key');
 */
Datum load_onnx_model(PG_FUNCTION_ARGS)
{
    char* modelKey = NULL;
    ONNXModelDesc* modelDesc = NULL;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("model_key cannot be NULL")));
    }

    modelKey = text_to_cstring(PG_GETARG_TEXT_PP(0));

    ModelConfig config;
    GenerateModelConfig(&config, modelKey);

    if (config.provider != PROVIDER_ONNX) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("model_key '%s' is not an ONNX model", modelKey)));
    }

    const char* modelPath = config.baseUrl;
    const char* ownerName = config.ownerName;

    if (modelPath == NULL || ownerName == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("model_name or url cannot be NULL for model_key: %s", modelKey)));
    }

    elog(LOG, "load_onnx_model: loading model '%s' from path '%s'", modelKey, modelPath);
    PG_TRY();
    {
        modelDesc = ONNX_MODEL_MGR->LoadONNXModelByKey(modelKey, ownerName, modelPath);
        if (modelDesc == NULL) {
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                         errmsg("failed to load ONNX model: %s", modelKey)));
        }
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_BOOL(true);
}

static void CheckONNXModelUnloadAllowed(const char* modelKey, const char* ownerName)
{
    const char* query =
        "SELECT task_name FROM ogai.vectorize_tasks "
        "WHERE model_key = $1 AND owner_name::text = $2 LIMIT 1";
    Datum params[ONNX_MODEL_REF_PARAM_NUM];
    Oid paramTypes[ONNX_MODEL_REF_PARAM_NUM] = {TEXTOID, TEXTOID};
    int ret;
    MemoryContext originCtx = CurrentMemoryContext;

    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(ERROR, "Failed to connect to SPI");
    }

    params[0] = CStringGetTextDatum(modelKey);
    params[1] = CStringGetTextDatum(ownerName);

    ret = SPI_execute_with_args(query,
                                ONNX_MODEL_REF_PARAM_NUM,
                                paramTypes,
                                params,
                                NULL,
                                true,
                                1,
                                NULL,
                                NULL);
    if (ret != SPI_OK_SELECT) {
        SPI_finish();
        elog(ERROR, "SPI_exec failed: %d", ret);
    }

    if (SPI_processed > 0) {
        HeapTuple tuple = SPI_tuptable->vals[0];
        char* taskName = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
        MemoryContext oldcontext = MemoryContextSwitchTo(originCtx);
        char* taskNameCopy = pstrdup(taskName ? taskName : "");
        MemoryContextSwitchTo(oldcontext);
        SPI_finish();

        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_IN_USE),
                 errmsg("cannot unload ONNX model \"%s\" because vectorize task \"%s\" references it",
                        modelKey, taskNameCopy),
                 errhint("Run ogai.ai_unvectorize('%s') or remove the vectorize task before unloading the model.",
                         taskNameCopy)));
    }

    SPI_finish();
}

/*
 * unload_onnx_model - Unload ONNX model from cache
 *
 * Usage: SELECT unload_onnx_model('model_key');
 */
Datum unload_onnx_model(PG_FUNCTION_ARGS)
{
    char* modelKey = NULL;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("model_key cannot be NULL")));
    }

    modelKey = text_to_cstring(PG_GETARG_TEXT_PP(0));

    ModelConfig config;
    GenerateModelConfig(&config, modelKey);

    if (config.provider != PROVIDER_ONNX) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("model_key '%s' is not an ONNX model", modelKey)));
    }
    const char* ownerName = config.ownerName;
    if (!TryAcquireONNXModelUsageExclusiveLock(modelKey, ownerName)) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_IN_USE),
                 errmsg("cannot unload ONNX model \"%s\" because it is being used by another transaction",
                        modelKey),
                 errhint("Wait for the active embedding or vectorize transaction to finish, then retry.")));
    }
    CheckONNXModelUnloadAllowed(modelKey, ownerName);
    elog(DEBUG1, "unload_onnx_model: unloading model '%s'", modelKey);
    PG_TRY();
    {
        ONNX_MODEL_MGR->UnloadONNXModelByKey(modelKey, ownerName);
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_BOOL(true);
}

/*
 * ogai_encrypt_api_key - Encrypt API key for secure storage
 *
 * This function encrypts an API key using OGAI_MODE encryption.
 * The encrypted string can be safely stored in ogai.model_sources table.
 *
 * Usage: SELECT ogai_encrypt_api_key('your-api-key');
 */
Datum ogai_encrypt_api_key(PG_FUNCTION_ARGS)
{
    char* plainApiKey = NULL;
    char* encryptedApiKey = NULL;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    plainApiKey = text_to_cstring(PG_GETARG_TEXT_PP(0));
    if (plainApiKey == NULL || strlen(plainApiKey) == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("api_key cannot be empty")));
    }

    /* Encrypt using OGAI_MODE */
    encryptedApiKey = encryptECString(plainApiKey, OGAI_MODE);
    if (encryptedApiKey == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                 errmsg("Failed to encrypt api_key")));
    }

    /* Clear the plain text for security */
    errno_t rc = memset_s(plainApiKey, strlen(plainApiKey), 0, strlen(plainApiKey));
    securec_check(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(encryptedApiKey));
}

/*
 * ogai_decrypt_api_key - Decrypt API key for viewing
 *
 * This function decrypts an encrypted API key using OGAI_MODE decryption.
 * Use this to verify or view the actual API key stored in ogai.model_sources table.
 *
 * Usage: SELECT ogai_decrypt_api_key(api_key) FROM ogai.model_sources WHERE model_key = 'xxx';
 *
 * Note: If the input is not encrypted (plain text), it will be returned as-is.
 */
Datum ogai_decrypt_api_key(PG_FUNCTION_ARGS)
{
    char* inputApiKey = NULL;
    char* decryptedApiKey = NULL;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    inputApiKey = text_to_cstring(PG_GETARG_TEXT_PP(0));
    if (inputApiKey == NULL || strlen(inputApiKey) == 0) {
        PG_RETURN_NULL();
    }

    /* Check if the api_key is encrypted */
    if (IsECEncryptedString(inputApiKey)) {
        /* Decrypt using OGAI_MODE */
        if (!decryptECString(inputApiKey, &decryptedApiKey, OGAI_MODE)) {
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                     errmsg("Failed to decrypt api_key"),
                     errhint("Make sure the ogai.key.cipher file exists and is valid.")));
        }
        PG_RETURN_TEXT_P(cstring_to_text(decryptedApiKey));
    }

    /* Not encrypted, return as-is */
    PG_RETURN_TEXT_P(cstring_to_text(inputApiKey));
}
