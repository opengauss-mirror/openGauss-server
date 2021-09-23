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
 * txn_local_allocators.h
 *    Local row allocator for current transaction manager.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn_local_allocators.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef TXN_LOCAL_ALLOCATORS_H
#define TXN_LOCAL_ALLOCATORS_H

#include <stdint.h>
#include <cmath>
#include "table.h"
#include "row.h"
#include "index.h"
#include "access_params.h"
#include "access.h"

namespace MOT {
/**
 * @class DummyTable
 * @brief local row allocator for current transaction manager
 */
class DummyTable {
public:
    static constexpr double BITMAP_BITS_IN_BYTE = 8.0;

    DummyTable() : m_slab(nullptr)
    {}

    ~DummyTable()
    {
        if (m_slab != nullptr) {
            delete m_slab;
            m_slab = nullptr;
        }
    }

    /**
     * @brief Initialize the slab allocator for sizes ranging from 64B to size of 32K
     */
    bool inline Init()
    {
        m_slab = new (std::nothrow) SlabAllocator(SLAB_MIN_BIN, std::ceil(std::log2(MAX_TUPLE_SIZE) + 1), true);
        if (m_slab == nullptr) {
            return false;
        }
        return m_slab->IsSlabInitialized();
    }

    /** @brief return bitmap buffer   */
    uint8_t* CreateBitMapBuffer(int fieldCount)
    {
        int size = std::ceil(fieldCount / BITMAP_BITS_IN_BYTE);
        void* buf = m_slab->Alloc(size);
        if (buf == nullptr) {
            return nullptr;
        }
        errno_t erc = memset_s(buf, size, 0, size);
        securec_check(erc, "\0", "\0");
        return reinterpret_cast<uint8_t*>(buf);
    }

    /** @brief return row match the current table schema   */
    Row* CreateNewRow(Table* t, Access* ac)
    {
        int size = t->GetTupleSize() + sizeof(Row);
        ac->m_localRowSize = size;
        void* buf = m_slab->Alloc(size);
        if (buf == nullptr) {
            return nullptr;
        }
        return new (buf) Row(t);
    }

    Row* CreateMaxRow(Table* t = nullptr)
    {
        int size = MAX_TUPLE_SIZE + sizeof(Row);
        void* buf = m_slab->Alloc(size);
        if (buf == nullptr) {
            return nullptr;
        }
        return new (buf) Row(t);
    }

    void DestroyRow(Row* row, Access* ac)
    {
        m_slab->Release(row, ac->m_localRowSize);
    }

    void DestroyBitMapBuffer(void* buffer, int fieldCount)
    {
        int sizeInBytes = std::ceil(fieldCount / BITMAP_BITS_IN_BYTE);
        m_slab->Release(buffer, sizeInBytes);
    }

    void DestroyMaxRow(Row* row)
    {
        int size = MAX_TUPLE_SIZE + sizeof(Row);
        m_slab->Release(row, size);
    }

    void ClearRowCache()
    {
        m_slab->ClearFreeCache();
    }

private:
    static constexpr int SLAB_MIN_BIN = 3;  // 2^3 = 8Bytes
    SlabAllocator* m_slab;
};

/**
 * @class DummyIndex
 * @brief Implement local key pool bases on slabAlocator
 */
class DummyIndex {
public:
    DummyIndex() : m_slab(nullptr)
    {}

    ~DummyIndex()
    {
        if (m_slab != nullptr) {
            delete m_slab;
            m_slab = nullptr;
        }
    }

    /**
     * @brief Initialiaze the local slabAllocator
     */
    bool inline Init()
    {
        m_slab = new (std::nothrow) SlabAllocator(SLAB_MIN_BIN, std::ceil(std::log2(MAX_KEY_SIZE) + 1), true);
        if (m_slab == nullptr) {
            return false;
        }
        return m_slab->IsSlabInitialized();
    }

    /** @brief Create a new local key matches the size from the current index   */
    Key* CreateNewKey(Index* i_)
    {
        int size = i_->GetAlignedKeyLength() + sizeof(Key);
        void* buf = m_slab->Alloc(size);
        if (buf == nullptr) {
            return nullptr;
        }
        return new (buf) Key(i_->GetAlignedKeyLength());
    }

    Key* CreateMaxKey(Index* i = nullptr)
    {
        int size = ALIGN8(MAX_KEY_SIZE) + sizeof(Key);
        void* buf = m_slab->Alloc(size);
        if (buf == nullptr) {
            return nullptr;
        }
        return new (buf) Key(ALIGN8(MAX_KEY_SIZE));
    }

    void DestroyKey(Key* key)
    {
        int size = key->GetAlignedKeyLength() + sizeof(Key);
        m_slab->Release(key, size);
    }

    void ClearKeyCache()
    {
        m_slab->ClearFreeCache();
    }

private:
    static constexpr int SLAB_MIN_BIN = 3;  // 2^3 = 8Bytes

    SlabAllocator* m_slab;

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif  // TXN_LOCAL_ALLOCATORS_H
