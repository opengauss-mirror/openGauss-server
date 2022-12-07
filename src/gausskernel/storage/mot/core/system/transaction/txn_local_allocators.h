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

#include <cstdint>
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
    static constexpr uint32_t BITMAP_BITS_IN_BYTE = 8;

    /* Max supported by the envelope (MaxHeapAttributeNumber in htup.h) */
    static constexpr uint32_t MAX_COLUMNS_PER_TABLE = 1600;

    static constexpr uint32_t MAX_BITMAP_BUFFER_SIZE = MAX_COLUMNS_PER_TABLE / BITMAP_BITS_IN_BYTE;

    DummyTable() : m_slab(nullptr), m_recovery(false)
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
    bool inline Init(bool recovery, bool global = false)
    {
        m_recovery = recovery;
        m_slab = new (std::nothrow) SlabAllocator(
            SlabAllocator::SLAB_MIN_BIN, static_cast<int>(std::ceil(std::log2(MAX_BITMAP_BUFFER_SIZE) + 1)), !global);
        if (m_slab == nullptr) {
            return false;
        }
        return m_slab->IsSlabInitialized();
    }

    /** @brief return bitmap buffer   */
    uint8_t* CreateBitMapBuffer(int fieldCount)
    {
        int size = BitmapSet::GetLength(fieldCount);
        void* buf = m_slab->Alloc(size);
        if (buf == nullptr) {
            return nullptr;
        }
        errno_t erc = memset_s(buf, size, 0, size);
        securec_check(erc, "\0", "\0");
        return reinterpret_cast<uint8_t*>(buf);
    }

    void* AllocBuf(size_t size)
    {
        int temp = size + sizeof(Row);
        return m_slab->Alloc(temp);
    }

    void DestroyBuf(void* ptr, size_t size)
    {
        m_slab->Release(ptr, size);
    }

    /** @brief Destroy Bitmap Buffer   */
    void DestroyBitMapBuffer(void* buffer, int fieldCount)
    {
        int sizeInBytes = BitmapSet::GetLength(fieldCount);
        m_slab->Release(buffer, sizeInBytes);
    }

    /** @brief Clear row cache   */
    void ClearRowCache()
    {
        m_slab->ClearFreeCache();
    }

    void PrintStats()
    {
        uint64_t gross = 0;
        uint64_t netTotal = 0;
        m_slab->PrintSize(gross, netTotal, "DummyTable Slab");
    }

private:
    SlabAllocator* m_slab;
    bool m_recovery;
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
     * @brief Initialize the local slabAllocator
     */
    bool inline Init(bool global = false)
    {
        m_slab = new (std::nothrow) SlabAllocator(
            SlabAllocator::SLAB_MIN_BIN, static_cast<int>(std::ceil(std::log2(MAX_KEY_SIZE) + 1)), !global);
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

    void PrintStats()
    {
        uint64_t gross = 0;
        uint64_t netTotal = 0;
        m_slab->PrintSize(gross, netTotal, "DummyIndex Slab");
    }

private:
    SlabAllocator* m_slab;

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif  // TXN_LOCAL_ALLOCATORS_H
