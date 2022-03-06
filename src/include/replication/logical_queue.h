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

#include "postgres.h"
#include "knl/knl_variable.h"
#define POWER_OF_TWO(x) (((x) & ((x)-1)) == 0)
#define COUNT(head, tail, mask) ((uint32)(((head) - (tail)) & (mask)))
#define SPACE(head, tail, mask) ((uint32)(((tail) - ((head) + 1)) & (mask)))

const int QUEUE_CAPACITY_MIN_LIMIT = 2;

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
}LogicalQueue;

LogicalQueue *LogicalQueueCreate(uint32 capacity, uint32 slotId, CallBackFunc func = NULL);

bool LogicalQueuePut(LogicalQueue* queue, void* element);
void* LogicalQueueTop(LogicalQueue* queue);
void LogicalQueuePop(LogicalQueue* queue);

