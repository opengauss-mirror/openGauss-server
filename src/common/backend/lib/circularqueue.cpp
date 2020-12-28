/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
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
 * circularqueue.cpp
 *      simple Circular queue primitives
 *      the elements of the list are void* so the queue can contain anything
 *
 *
 * IDENTIFICATION
 *      src/common/backend/lib/circularqueue.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "lib/circularqueue.h"
#include "miscadmin.h"
#include "utils/hotkey.h"
#include "utils/memutils.h"

const int SIZE_OF_UINT64 = 8;
const int SIZE_OF_TWO_UINT64 = 16;

/* lock free circular queue for writing */
CircularQueue::CircularQueue(int size, MemoryContext context)
    : head(0), tail(0), capacity(size), queue(NULL), cxt(context)
{}

CircularQueue::~CircularQueue()
{}

void CircularQueue::Init()
{
    MemoryContext oldcxt = MemoryContextSwitchTo(cxt);
    queue = (void**)palloc0(sizeof(void*) * capacity);
    (void)MemoryContextSwitchTo(oldcxt);
}

void CircularQueue::Destroy()
{
    for (uint32 i = 0; i < capacity; i++) {
        CleanHotkeyInfo((HotkeyInfo*)queue[i]);
    }
    pfree_ext(queue);
    cxt = NULL;
}

bool CircularQueue::LockFreeEnQueue(void* elem)
{
    while (true) {
        uint32 valid_slot = tail;
        uint32 update_tail = (valid_slot + 1) % capacity;
        if (IsFull()) {
            return false;
        }
        if (!pg_atomic_compare_exchange_u32(&tail, &valid_slot, update_tail)) {
            continue;
        } else {
            Assert(queue[valid_slot] == NULL);
            queue[valid_slot] = elem;
            return true;
        }
    }
}

const uint32 CircularQueue::GetStart() const
{
    return head;
}

void CircularQueue::SetStart(uint32 index)
{
    head = index % capacity;
}

const uint32 CircularQueue::GetEnd() const
{
    return tail;
}

uint32 CircularQueue::GetLength(uint32 start, uint32 end)
{
    return (end + capacity - start) % capacity;
}

const bool CircularQueue::IsFull()
{
    return head == (tail + 1) % capacity;
}

void* CircularQueue::GetElem(int n)
{
    void* temp = queue[(n + head) % capacity];
    if (temp == NULL) {
        return NULL;
    }
    queue[(n + head) % capacity] = NULL;
    return temp;
}

MemoryContext CircularQueue::GetContext()
{
    return cxt;
}
