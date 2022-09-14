/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * logical_queue.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/logical_queue.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LOGICAL_QUEUE_H
#define LOGICAL_QUEUE_H

#include "postgres.h"
#include "knl/knl_variable.h"

#define DEFAULT_PARALLEL_QUEUE_SIZE 128
#define MAX_PARALLEL_QUEUE_SIZE 1024
#define MIN_PARALLEL_QUEUE_SIZE 2

#define POWER_OF_TWO(x) (((x) & ((x)-1)) == 0)
#define COUNT(head, tail, mask) ((uint32)(((head) - (tail)) & (mask)))
#define SPACE(head, tail, mask) ((uint32)(((tail) - ((head) + 1)) & (mask)))

typedef void (*CallBackFunc)();

typedef struct LogicalQueue {
    pg_atomic_uint32 writeHead; /* Array index for the next write. */
    pg_atomic_uint32 readTail;  /* Array index for the next read. */
    uint32 capacity;            /* Queue capacity, must be power of 2. */
    uint32 mask;                /* Bit mask for computing index. */
    pg_atomic_uint32 maxUsage;
    pg_atomic_uint64 totalCnt;
    CallBackFunc callBackFunc;
    void* buffer[1]; /* Queue buffer, the actual size is capacity. */
} LogicalQueue;

LogicalQueue *LogicalQueueCreate(int slotId, CallBackFunc func = NULL);

void LogicalQueuePut(LogicalQueue* queue, void* element);
void* LogicalQueueTop(LogicalQueue* queue);
void LogicalQueuePop(LogicalQueue* queue);

#endif

