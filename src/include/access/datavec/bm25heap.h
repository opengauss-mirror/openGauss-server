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
#include <algorithm>
#include <unordered_map>
#include <vector>

template <typename I, typename T>
struct IdVal {
    I id;
    T val;

    IdVal() = default;
    IdVal(I id, T val) : id(id), val(val) {
    }

    inline friend bool
    operator<(const IdVal<I, T>& lhs, const IdVal<I, T>& rhs) {
        return lhs.val < rhs.val || (lhs.val == rhs.val && lhs.id < rhs.id);
    }

    inline friend bool
    operator>(const IdVal<I, T>& lhs, const IdVal<I, T>& rhs) {
        return !(lhs < rhs) && !(lhs == rhs);
    }

    inline friend bool
    operator==(const IdVal<I, T>& lhs, const IdVal<I, T>& rhs) {
        return lhs.id == rhs.id && lhs.val == rhs.val;
    }
};

template <typename T>
using SparseIdVal = IdVal<uint32, T>;

template <typename T>
class MaxMinHeap {
public:
    explicit MaxMinHeap(int capacity) : capacity_(capacity), pool_(capacity) {
    }
    void
    push(uint32 id, T val) {
        if (size_ < capacity_) {
            pool_[size_] = {id, val};
            size_ += 1;
            std::push_heap(pool_.begin(), pool_.begin() + size_, std::greater<SparseIdVal<T>>());
        } else if (val > pool_[0].val) {
            sift_down(id, val);
        }
    }
    uint32
    pop() {
        std::pop_heap(pool_.begin(), pool_.begin() + size_, std::greater<SparseIdVal<T>>());
        size_ -= 1;
        return pool_[size_].id;
    }
    size_t size() const {
        return size_;
    }
    bool empty() const {
        return size() == 0;
    }
    SparseIdVal<T>
    top() const {
        return pool_[0];
    }
    bool
    full() const {
        return size_ == capacity_;
    }

private:
    void
    sift_down(uint32 id, T val) {
        size_t i = 0;
        for (; 2 * i + 1 < size_;) {
            size_t j = i;
            size_t l = 2 * i + 1, r = 2 * i + 2;
            if (pool_[l].val < val) {
                j = l;
            }
            if (r < size_ && pool_[r].val < std::min(pool_[l].val, val)) {
                j = r;
            }
            if (i == j) {
                break;
            }
            pool_[i] = pool_[j];
            i = j;
        }
        pool_[i] = {id, val};
    }

    size_t size_ = 0, capacity_;
    std::vector<SparseIdVal<T>> pool_;
};  // class MaxMinHeap

#endif //BM25HEAP_H
