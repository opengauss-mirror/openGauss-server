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
 * -------------------------------------------------------------------------
 *
 * bm25_heap.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/bm25_heap.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BM25HEAP_H
#define BM25HEAP_H

#include "postgres.h"
#include "utils/palloc.h"

struct IdVal {
    uint32 id;
    float val;

    IdVal() : id(0), val(0.0f) {}
    IdVal(uint32 id, float val) : id(id), val(val) {}

    friend bool operator<(const IdVal& lhs, const IdVal& rhs)
    {
        return lhs.val < rhs.val || (lhs.val == rhs.val && lhs.id < rhs.id);
    }

    friend bool operator>(const IdVal& lhs, const IdVal& rhs)
    {
        return rhs < lhs;
    }

    friend bool operator==(const IdVal& lhs, const IdVal& rhs)
    {
        return lhs.id == rhs.id && lhs.val == rhs.val;
    }
};

class MaxMinHeap {
public:
    explicit MaxMinHeap()
        : capacity_(0), size_(0), pool_(nullptr)
        { }

    void InitHeap(size_t capacity)
    {
        capacity_ = capacity;
        pool_ = (IdVal *)palloc0(capacity_ * sizeof(IdVal));
    }
    ~MaxMinHeap()
    {
        pfree_ext(pool_);
    }

    MaxMinHeap(const MaxMinHeap&) = delete;
    MaxMinHeap& operator=(const MaxMinHeap&) = delete;

    void push(uint32 id, float val)
    {
        if (size_ < capacity_) {
            pool_[size_] = IdVal(id, val);
            shiftUp(size_);
            size_++;
        } else if (val > pool_[0].val) {
            siftDown(id, val);
        }
    }

    uint32 pop()
    {
        uint32 ret = pool_[0].id;
        pool_[0] = pool_[--size_];
        shiftDown(0);
        return ret;
    }

    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }
    const IdVal& top() const { return pool_[0]; }
    bool full() const { return size_ == capacity_; }

private:
    void shiftUp(size_t index)
    {
        while (index > 0) {
            size_t parent = (index - 1) / 2;
            if (pool_[index] < pool_[parent]) {
                swap(index, parent);
                index = parent;
            } else {
                break;
            }
        }
    }

    void shiftDown(size_t index)
    {
        size_t smallest;
        while (true) {
            smallest = index;
            size_t left = 2 * index + 1;
            size_t right = 2 * index + 2;

            if (left < size_ && pool_[left] < pool_[smallest]) {
                smallest = left;
            }
            if (right < size_ && pool_[right] < pool_[smallest]) {
                smallest = right;
            }
            if (smallest != index) {
                swap(index, smallest);
                index = smallest;
            } else {
                break;
            }
        }
    }

    void siftDown(uint32 id, float val)
    {
        size_t i = 0;
        while (2 * i + 1 < size_) {
            size_t left = 2 * i + 1;
            size_t right = 2 * i + 2;
            size_t smallest = left;

            if (right < size_ && pool_[right] < pool_[left]) {
                smallest = right;
            }

            if (IdVal(id, val) < pool_[smallest]) {
                break;
            }

            pool_[i] = pool_[smallest];
            i = smallest;
        }
        pool_[i] = IdVal(id, val);
    }

    void swap(size_t a, size_t b)
    {
        IdVal temp = pool_[a];
        pool_[a] = pool_[b];
        pool_[b] = temp;
    }

    size_t capacity_;
    size_t size_;
    IdVal* pool_;
}; // class MaxMinHeap

#endif // BM25HEAP_H
