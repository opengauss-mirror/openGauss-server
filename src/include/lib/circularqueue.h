/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * circularQueue.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/lib/Circularqueue.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef Circular_QUEUE_H
#define Circular_QUEUE_H

#include "postgres.h"

class CircularQueue : public BaseObject {
public:
    CircularQueue(int size, MemoryContext context);
    ~CircularQueue();

    void Init();
    void Destroy();
    bool LockFreeEnQueue(void* elem); /* append elem to the queue */

    const uint32 GetStart() const;              /* get the start  */
    void SetStart(uint32 index);                /* set the start */
    const uint32 GetEnd() const;                /* get the end  */
    const bool IsFull();                        /* if the queue is full */
    uint32 GetLength(uint32 start, uint32 end); /* get the length of queue */
    void QueueTraverse();                       /* traverse the queue */
    void* GetElem(int n);                       /* get nth element in the queue */
    MemoryContext GetContext();                 /* get the memory context of this queue */
public:
    uint32 head;
    volatile uint32 tail;
    uint32 capacity;
    void** queue;
    MemoryContext cxt;
};

#endif /* Circular_QUEUE_H */