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
 * txn_insert_action.h
 *    Holds data for a row insertion request.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn_insert_action.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef TXN_INSERT_ACTION_H
#define TXN_INSERT_ACTION_H

#include <stdint.h>

namespace MOT {
/**
 * @class ins_item
 * @brief Holds data for a row insertion request.
 */
class InsItem {
public:
    /** @brief Default constructor. */
    InsItem() : m_index(nullptr), m_row(nullptr), m_key(nullptr), m_indexOrder(IndexOrder::INDEX_ORDER_PRIMARY)
    {}

    /**
     * @brief Constructor.
     * @param row The row to insert.
     * @param index The primary index to insert the row into.
     * @param pid The logical identifier of the requesting process/thread.
     */
    inline InsItem(Row* row, Index* index);

    /**
     * @brief Destructor.
     */
    inline ~InsItem()
    {}

    inline __attribute__((always_inline)) void SetItem(Row* row, Index* index, Key* key)
    {
        m_row = row;
        m_index = index;
        m_indexOrder = index->GetIndexOrder();
        m_key = key;
    }

    inline Key* GetKey() const
    {
        return m_key;
    }

    /**
     * @brief Retrieves the index order for the stored index.
     * @return The index order.
     */
    inline IndexOrder getIndexOrder() const
    {
        return m_indexOrder;
    }

    /** @var The index to insert the row into. */
    Index* m_index;

    /** @var The new row to insert. */
    Row* m_row;

    /** @var The key by which to insert the row. */
    MOT::Key* m_key;

    /** @var The order of the stored index. */
    IndexOrder m_indexOrder;

private:
    DECLARE_CLASS_LOGGER()
};

/**
 * @class txn_insert_action
 * @brief Holds all parameters required for a set of row insertion requests.
 */
class TxnInsertAction {
public:
    /** @brief Constructor. */
    TxnInsertAction()
    {}

    /** @brief Destructor. */
    ~TxnInsertAction();

    /**
     * @brief Initializes the object.
     * @param _manager The owning transaction manager object.
     * @return Boolean value denoting success or failure.
     */
    bool Init(TxnManager* _manager);

    /** @brief Clears the insertion request set. */
    inline void ClearSet()
    {
        m_insertSetSize = 0;
        if (unlikely(m_insertArraySize > INSERT_ARRAY_DEFAULT_SIZE)) {
            // shrink array
            ShrinkInsertSet();
        }
    };

    /**
     * @brief Executes all stored row insertion requests.
     * @param row
     * @return Return code denoting the execution result.
     */
    RC ExecuteOptimisticInsert(Row* row);

    /**
     * @bruef Retrieves the first insertion request.
     * @return The first insertion request.
     */
    inline InsItem* BeginCursor() const
    {
        return &(m_insertSet[0]);
    };

    inline InsItem* EndCursor() const
    {
        return (&m_insertSet[m_insertSetSize]);
    };

    void ReportError(RC rc, InsItem* currentItem = nullptr);

private:
    static constexpr uint32_t INSERT_ARRAY_EXTEND_FACTOR = 2;

    /** @var The row insertion request array. */
    InsItem* m_insertSet = nullptr;

    /** @var Number of stored row insertion requests. */
    uint32_t m_insertSetSize = 0;

    /** @var The capacity of the row insertion request array. */
    uint32_t m_insertArraySize = 0;

    /** @var The owning transaction manager object. */
    TxnManager* m_manager = nullptr;

    /** @var The row insertion request array growth factor.  */
    static constexpr uint64_t INSERT_ARRAY_DEFAULT_SIZE = 64;

    inline InsItem* GetInsertItem(Index* index = nullptr)
    {

        if (__builtin_expect(m_insertSetSize == m_insertArraySize, 0)) {
            ReallocInsertSet();
        }

        return &(m_insertSet[m_insertSetSize++]);
    }

    /**
     * @brief Increases the size of the row insertion request array.
     */
    bool ReallocInsertSet();

    void ShrinkInsertSet();

    /**
     * @brief Cleans up the current aborted row.
     */
    void CleanupOptimisticInsert(
        InsItem* currentItem, Sentinel* pIndexInsertResult, bool isInserted, bool isMappedToCache);

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    TxnInsertAction(const TxnInsertAction&) = delete;

    TxnInsertAction(TxnInsertAction&&) = delete;

    TxnInsertAction& operator=(const TxnInsertAction&) = delete;

    TxnInsertAction& operator=(TxnInsertAction&&) = delete;
    /** @endcond */

    // allow privileged access
    friend TxnManager;

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif  // TXN_INSERT_ACTION_H
