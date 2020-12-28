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
 * -------------------------------------------------------------------------
 *
 * spsc_blocking_queue.cpp
 *      A bounded queue that supports operations that wait for the queue to
 *      become non-empty when retrieving an element, and wait for space to
 *      become available in the queue when storing an element.
 *
 *      This structure is limited to Single-Producer/Single-Consumer, so the
 *      internal data can be accesses without locks.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/parallel_recovery/spsc_blocking_queue.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <assert.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/atomic.h"
#include "utils/palloc.h"

#include "access/parallel_recovery/spsc_blocking_queue.h"
#include "access/parallel_recovery/page_redo.h"
#include "utils/elog.h"

namespace parallel_recovery {

#define POWER_OF_TWO(x) (((x) & ((x)-1)) == 0)
#define COUNT(head, tail, mask) ((uint32)(((head) - (tail)) & (mask)))
#define SPACE(head, tail, mask) ((uint32)(((tail) - ((head) + 1)) & (mask)))

const uint32 MAX_REDO_QUE_TAKE_DELAY = 200; /* 100 us */
const uint32 MAX_REDO_QUE_IDEL_TAKE_DELAY = 1000;
const uint32 MAX_TSDB_QUE_TAKE_DELAY = 200;
const uint32 SLEEP_COUNT_QUE_TAKE = 0xFFF;

const int QUEUE_CAPACITY_MIN_LIMIT = 2;
const int QUEUE_PUT_WAIT_LIMIT = 3;

SPSCBlockingQueue *SPSCBlockingQueueCreate(uint32 capacity, CallBackFunc func)
{
    /*
     * We require the capacity to be a power of 2, so index wrap can be
     * handled by a bit-wise and.  The actual capacity is one less than
     * the specified, so the minimum capacity is 2.
     */
    Assert(capacity >= QUEUE_CAPACITY_MIN_LIMIT && POWER_OF_TWO(capacity));

    size_t allocSize = sizeof(SPSCBlockingQueue) + sizeof(void *) * capacity;
    SPSCBlockingQueue *queue = (SPSCBlockingQueue *)palloc0(allocSize);

    uint32 mask = capacity - 1;
    pg_atomic_init_u32(&queue->writeHead, 0);
    pg_atomic_init_u32(&queue->readTail, 0);
    queue->capacity = capacity;
    queue->mask = mask;
    queue->maxUsage = 0;
    queue->totalCnt = 0;
    queue->callBackFunc = func;
    return queue;
}

void SPSCBlockingQueueDestroy(SPSCBlockingQueue *queue)
{
    pfree(queue);
}

bool SPSCBlockingQueuePut(SPSCBlockingQueue *queue, void *element)
{
    uint32 head;
    uint32 tail;
    uint64 cnt = 0;
    uint32 tmpCnt = 0;
    head = pg_atomic_read_u32(&queue->writeHead);
    do {
        if (cnt > QUEUE_PUT_WAIT_LIMIT)
            return false; /* avoid deadlock with pageworker for syncrecordlog */
        tail = pg_atomic_read_u32(&queue->readTail);
        cnt++;
    } while (SPACE(head, tail, queue->mask) == 0);

    /*
     * Make sure the following write to the buffer happens after the read
     * of the tail.  Combining this with the corresponding barrier in Take()
     * which guarantees that the tail is updated after reading the buffer,
     * we can be sure that we cannot update a slot's value before it has
     * been read.
     */
    pg_memory_barrier();
    tmpCnt = COUNT(head, tail, queue->mask);
    if (tmpCnt > queue->maxUsage) {
        pg_atomic_write_u32(&queue->maxUsage, tmpCnt);
    }

    queue->buffer[head] = element;

    /* Make sure the index is updated after the buffer has been written. */
    pg_write_barrier();

    pg_atomic_write_u32(&queue->writeHead, (head + 1) & queue->mask);

    return true;
}

uint32 SPSCGetQueueCount(SPSCBlockingQueue *queue)
{
    uint32 head = pg_atomic_read_u32(&queue->writeHead);
    uint32 tail = pg_atomic_read_u32(&queue->readTail);
    return (COUNT(head, tail, queue->mask));
}

void *SPSCBlockingQueueTake(SPSCBlockingQueue *queue)
{
    uint32 head;
    uint32 tail;
    uint32 count = 0;
    long sleeptime;
    tail = pg_atomic_read_u32(&queue->readTail);
    do {
        head = pg_atomic_read_u32(&queue->writeHead);
        ++count;
        /* here we sleep, let the cpu to do other important work */
        if ((count & SLEEP_COUNT_QUE_TAKE) == SLEEP_COUNT_QUE_TAKE) {
            if (t_thrd.page_redo_cxt.sleep_long)
                sleeptime = MAX_REDO_QUE_IDEL_TAKE_DELAY;
            else
                sleeptime = MAX_REDO_QUE_TAKE_DELAY;
            pg_usleep(sleeptime);
        }

        if (queue->callBackFunc != NULL) {
            queue->callBackFunc();
        }
    } while (COUNT(head, tail, queue->mask) == 0);
    t_thrd.page_redo_cxt.sleep_long = false;
    /* Make sure the buffer is read after the index. */
    pg_read_barrier();

    void *elem = queue->buffer[tail];

    /* Make sure the read of the buffer finishes before updating the tail. */
    pg_memory_barrier();

    pg_atomic_write_u32(&queue->readTail, (tail + 1) & queue->mask);
    return elem;
}

bool SPSCBlockingQueueIsEmpty(SPSCBlockingQueue *queue)
{
    uint32 head = pg_atomic_read_u32(&queue->writeHead);
    uint32 tail = pg_atomic_read_u32(&queue->readTail);
    return (COUNT(head, tail, queue->mask) == 0);
}

void *SPSCBlockingQueueTop(SPSCBlockingQueue *queue)
{
    uint32 head;
    uint32 tail;
    uint32 count = 0;
    long sleeptime;
    tail = pg_atomic_read_u32(&queue->readTail);
    do {
        head = pg_atomic_read_u32(&queue->writeHead);
        ++count;
        /* here we sleep, let the cpu to do other important work */
        if ((count & SLEEP_COUNT_QUE_TAKE) == SLEEP_COUNT_QUE_TAKE) {
            if (t_thrd.page_redo_cxt.sleep_long)
                sleeptime = MAX_REDO_QUE_IDEL_TAKE_DELAY;
            else
                sleeptime = MAX_REDO_QUE_TAKE_DELAY;
            pg_usleep(sleeptime);
        }

        if (queue->callBackFunc != NULL) {
            queue->callBackFunc();
        }
    } while (COUNT(head, tail, queue->mask) == 0);
    t_thrd.page_redo_cxt.sleep_long = false;
    pg_read_barrier();
    void *elem = queue->buffer[tail];
    return elem;
}

void SPSCBlockingQueuePop(SPSCBlockingQueue *queue)
{
    uint32 head;
    uint32 tail;
    uint64 totalCnt = pg_atomic_read_u64(&queue->totalCnt);
    tail = pg_atomic_read_u32(&queue->readTail);
    head = pg_atomic_read_u32(&queue->writeHead);
    if (COUNT(head, tail, queue->mask) == 0) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("SPSCBlockingQueuePop queue error!")));
        return;
    }

    /* Make sure the read of the buffer finishes before updating the tail. */
    pg_memory_barrier();
    pg_atomic_write_u64(&queue->totalCnt, (totalCnt + 1));
    pg_atomic_write_u32(&queue->readTail, (tail + 1) & queue->mask);
}

void DumpQueue(SPSCBlockingQueue *queue)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("[REDO_LOG_TRACE]queue info: writeHead %u, readTail %u, capacity %u, mask %u",
                         queue->writeHead, queue->readTail, queue->capacity, queue->mask)));
}

void *SPSCTimeseriesQueueTop(SPSCBlockingQueue *queue)
{
    uint32 head = pg_atomic_read_u32(&queue->writeHead);
    uint32 tail = pg_atomic_read_u32(&queue->readTail);
    if (COUNT(head, tail, queue->mask) == 0) {
        return NULL;
    }
    pg_read_barrier();
    void *elem = queue->buffer[tail];
    return elem;
}

}  // namespace parallel_recovery
