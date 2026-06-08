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
 * ogai_vector_processor.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ogai_vector_processor.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef OGAI_VECTOR_PROCESSOR_H
#define OGAI_VECTOR_PROCESSOR_H

#include "utils/numeric.h"
#include "utils/builtins.h"
#include "fmgr.h"

/* Forward declaration for bgworker context */
struct BgWorkerContext;

/* Maximum number of retries */
#define VECTOR_PROCESSOR_MAX_RETRY_COUNT 3

/* Parallel vectorize configuration */
#define OGAI_MAX_BATCH_TASKS 10   /* Max tasks per batch */
#define OGAI_DEFAULT_WORKERS 3    /* Default number of parallel workers */

/* Task result status (set by worker, read by leader) */
#define TASK_RESULT_PENDING    0
#define TASK_RESULT_SUCCESS    1
#define TASK_RESULT_FAIL       2

/* Fail reason buffer size */
#define OGAI_FAIL_REASON_SIZE 1024

/*
 * Embedding chunk result for join mode.
 * Each chunk has its own text and embedding vector.
 * Memory allocated from INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
 * freed by the leader after DML processing.
 */
typedef struct OgaiEmbeddingChunk {
    char *chunkText;      /* chunk text, allocated in instance memory */
    void *vectorData;     /* serialized Vector datum copy, instance memory */
    int vectorSize;       /* VARSIZE of the serialized Vector datum */
} OgaiEmbeddingChunk;

/*
 * Per-task config needed by leader for DML writes.
 * Populated by worker (read-only SPI), consumed by leader.
 */
typedef struct OgaiTaskConfigResult {
    char srcSchema[64];
    char srcTable[64];
    char primaryKey[64];
    char tableMethod[32];
    bool useChunking;
} OgaiTaskConfigResult;

/*
 * Per-task data in the shared context.
 * The leader reads the output fields after workers finish, then does all
 * DML (INSERT/UPDATE/DELETE) in its own transaction.
 */
typedef struct OgaiVectorizeTask {
    /* ---- Input: set by leader in OgaiInitSharedContext ---- */
    int64 msgId;
    int taskId;
    int pkValue;
    int retryCount;

    /* ---- Output: set by worker ---- */
    volatile int resultStatus;    /* TASK_RESULT_PENDING / SUCCESS / FAIL */
    int failType;                 /* OgaiFailType cast to int */
    char failReason[OGAI_FAIL_REASON_SIZE];

    /* Task config subset needed for leader DML */
    OgaiTaskConfigResult config;

    /* Append mode: single embedding result */
    void *vectorData;             /* serialized Vector datum copy, instance memory */
    int vectorSize;               /* VARSIZE of the serialized Vector datum */

    /* Join mode: array of chunk embedding results */
    int chunkCount;
    OgaiEmbeddingChunk *chunks;   /* array allocated in instance memory */
} OgaiVectorizeTask;

/* Shared context passed to bgworkers for parallel vectorization. */
typedef struct OgaiVectorizeSharedContext {
    pg_atomic_uint32 nextTask;     /* Atomic counter for worker task assignment */
    int taskCount;                  /* Total number of tasks in this batch */
    OgaiVectorizeTask tasks[OGAI_MAX_BATCH_TASKS];
} OgaiVectorizeSharedContext;

/**
 * @brief Initialize the vector processor (checks dependent tables exist)
 * @return true if initialization succeeds; false if it fails
 */
bool OgaiVectorProcessorInit();

/**
 * @brief Launch bgworkers to process the vectorization queue in parallel.
 */
void OgaiParallelVectorize(void);

/**
 * @brief Worker main function for bgworker-based parallel vectorization.
 * @param bwc  Background worker context (contains bgshared pointer)
 */
void OgaiVectorizeWorkerMain(const BgWorkerContext *bwc);

#endif // OGAI_VECTOR_PROCESSOR_H
