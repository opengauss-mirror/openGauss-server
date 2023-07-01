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
 * spsc_blocking_queue.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/ondemand_extreme_rto/spsc_blocking_queue.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_SPSC_BLOCKING_QUEUE_H
#define ONDEMAND_EXTREME_RTO_SPSC_BLOCKING_QUEUE_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/parallel_recovery/posix_semaphore.h"


namespace ondemand_extreme_rto {
typedef void (*CallBackFunc)();

struct SPSCBlockingQueue {
    pg_atomic_uint32 writeHead; /* Array index for the next write. */
    pg_atomic_uint32 readTail;  /* Array index for the next read. */
    uint32 capacity;            /* Queue capacity, must be power of 2. */
    uint32 mask;                /* Bit mask for computing index. */
    pg_atomic_uint32 maxUsage;
    pg_atomic_uint64 totalCnt;
    CallBackFunc callBackFunc;
    uint64 lastTotalCnt;
    void *buffer[1]; /* Queue buffer, the actual size is capacity. */
};

SPSCBlockingQueue *SPSCBlockingQueueCreate(uint32 capacity, CallBackFunc func = NULL);
void SPSCBlockingQueueDestroy(SPSCBlockingQueue *queue);

bool SPSCBlockingQueuePut(SPSCBlockingQueue *queue, void *element);
void *SPSCBlockingQueueTake(SPSCBlockingQueue *queue);
bool SPSCBlockingQueueIsEmpty(SPSCBlockingQueue *queue);
void *SPSCBlockingQueueTop(SPSCBlockingQueue *queue);
void SPSCBlockingQueuePop(SPSCBlockingQueue *queue);
void DumpQueue(const SPSCBlockingQueue *queue);
uint32 SPSCGetQueueCount(SPSCBlockingQueue *queue);
bool SPSCBlockingQueueGetAll(SPSCBlockingQueue *queue, void ***eleArry, uint32 *eleNum);
void SPSCBlockingQueuePopN(SPSCBlockingQueue *queue, uint32 n);
}  // namespace ondemand_extreme_rto
#endif
