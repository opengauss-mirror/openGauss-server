/* ---------------------------------------------------------------------------------------
 *
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
 * logical_queue.cpp
 *      A bounded queue that supports operations that wait for the queue to
 *      become non-empty when retrieving an element, and wait for space to
 *      become available in the queue when storing an element.
 *
 *      This structure uses memory barrier in case of disorder.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/logical/logical_queue.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <assert.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/atomic.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "replication/logical_queue.h"
#include "storage/ipc.h"

static const int gMaxCnt = 10;

LogicalQueue *LogicalQueueCreate(uint32 capacity, uint32 slotId, CallBackFunc func)
{
    /*
     * We require the capacity to be a power of 2, so index wrap can be
     * handled by a bit-wise and.  The actual capacity is one less than
     * the specified, so the minimum capacity is 2.
     */
    Assert(capacity >= QUEUE_CAPACITY_MIN_LIMIT && POWER_OF_TWO(capacity));

    size_t allocSize = sizeof(LogicalQueue) + sizeof(void *) * capacity;
    MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    LogicalQueue *queue = (LogicalQueue *)palloc0(allocSize);
    MemoryContextSwitchTo(oldCtx);

    uint32 mask = capacity - 1;
    pg_atomic_init_u32(&queue->writeHead, 0);
    pg_atomic_init_u32(&queue->readTail, 0);

    queue->mask = mask;
    queue->maxUsage = 0;
    queue->totalCnt = 0;
    queue->capacity = capacity;
    queue->callBackFunc = func;
    return queue;
}

bool LogicalQueuePut(LogicalQueue *queue, void *element)
{
    uint32 head = 0;
    uint32 tail = 0;
    uint64 cnt = 0;

    head = pg_atomic_read_u32(&queue->writeHead);
    do {
        if (t_thrd.logicalreadworker_cxt.shutdown_requested ||
            t_thrd.parallel_decode_cxt.shutdown_requested) {
            ereport(LOG, (errmsg("Parallel Decode Worker stop")));
            proc_exit(0);
        }
        tail = pg_atomic_read_u32(&queue->readTail);
        cnt++;

        if (cnt >= gMaxCnt) {
            pg_usleep(500L);
            cnt = 0;
        }
    } while (SPACE(head, tail, queue->mask) == 0);

    /*
     * Make sure the following write to the buffer happens after the read
     * of the tail.  Combining this with the corresponding barrier in Take()
     * which guarantees that the tail is updated after reading the buffer,
     * we can be sure that we cannot update a slot's value before it has
     * been read.
     */
    pg_memory_barrier();
    uint32 tmpCount = COUNT(head, tail, queue->mask);
    if (tmpCount > queue->maxUsage) {
        pg_atomic_write_u32(&queue->maxUsage, tmpCount);
    }

    queue->buffer[head] = element;

    /* Make sure the index is updated after the buffer has been written. */
    pg_write_barrier();

    pg_atomic_write_u32(&queue->writeHead, (head + 1) & queue->mask);

    return true;
}

void *LogicalQueueTop(LogicalQueue *queue)
{
    uint32 head = 0;
    uint32 tail = 0;
    uint32 count = 0;
    tail = pg_atomic_read_u32(&queue->readTail);
    do {
        head = pg_atomic_read_u32(&queue->writeHead);
        ++count;
        /* here we sleep, let the cpu to do other important work */

        if (count >= gMaxCnt) {
            pg_usleep(1000L);
            return NULL;
        }

        if (queue->callBackFunc != NULL) {
            queue->callBackFunc();
        }
    } while (COUNT(head, tail, queue->mask) == 0);

    pg_read_barrier();
    void *elem = queue->buffer[tail];
    return elem;
}

void LogicalQueuePop(LogicalQueue *queue)
{
    uint32 head;
    uint32 tail;
    uint64 totalCnt = pg_atomic_read_u64(&queue->totalCnt);
    tail = pg_atomic_read_u32(&queue->readTail);
    head = pg_atomic_read_u32(&queue->writeHead);
    if (COUNT(head, tail, queue->mask) == 0) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("LogicalQueuePop queue error!")));
        return;
    }

    /* Make sure the read of the buffer finishes before updating the tail. */
    pg_memory_barrier();
    pg_atomic_write_u64(&queue->totalCnt, (totalCnt + 1));
    pg_atomic_write_u32(&queue->readTail, (tail + 1) & queue->mask);
}

