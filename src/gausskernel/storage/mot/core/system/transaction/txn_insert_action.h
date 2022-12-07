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

#include <cstdint>

namespace MOT {
/**
 * @class ins_item
 * @brief Holds data for a row insertion request.
 */
class InsItem {
public:
    /** @brief Default constructor. */
    InsItem() : m_index(nullptr), m_row(nullptr)
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
    {
        m_row = nullptr;
        m_index = nullptr;
    }

    inline __attribute__((always_inline)) void SetItem(Row* row, Index* index)
    {
        m_row = row;
        m_index = index;
    }

    /**
     * @brief Retrieves the index order for the stored index.
     * @return The index order.
     */
    inline IndexOrder getIndexOrder() const
    {
        return m_index->GetIndexOrder();
    }

    /** @var The index to insert the row into. */
    Index* m_index;

    /** @var The new row to insert. */
    Row* m_row;

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
     * @param isUpdateColumn Indicate whether the insert source is update column
     * @return Return code denoting the execution result.
     */
    RC ExecuteOptimisticInsert(Row* row, Key* updateColumnKey = nullptr);

    /**
     * @brief Executes all stored row insertion requests in recovery.
     * @param row
     * @return Return code denoting the execution result.
     */
    RC ExecuteRecoveryOCCInsert(Row* row);

    /**
     * @bruef Retrieves the first insertion request.
     * @return The first insertion request.
     */
    inline InsItem* BeginCursor()
    {
        return &(m_insertSet[0]);
    };

    inline InsItem* EndCursor()
    {
        return (&m_insertSet[m_insertSetSize]);
    };

    void ReportError(RC rc, InsItem* currentItem = nullptr);

    uint32_t GetInsertSetSize() const
    {
        return m_insertSetSize;
    }

    bool IsInsertSetEmpty() const
    {
        return (m_insertSetSize == 0);
    }

private:
    static constexpr uint32_t INSERT_ARRAY_EXTEND_FACTOR = 2;

    /** @var The row insertion request array. */
    InsItem* m_insertSet = nullptr;

    /** @var The owning transaction manager object. */
    TxnManager* m_manager = nullptr;

    /** @var Number of stored row insertion requests. */
    uint32_t m_insertSetSize = 0;

    /** @var The capacity of the row insertion request array. */
    uint32_t m_insertArraySize = 0;

    /** @var The row insertion request array growth factor.  */
    static constexpr uint64_t INSERT_ARRAY_DEFAULT_SIZE = 64;

    inline InsItem* GetInsertItem(Index* index = nullptr)
    {
        if (__builtin_expect(m_insertSetSize == m_insertArraySize, 0)) {
            if (!ReallocInsertSet()) {
                MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Transaction Processing", "Cannot get insert item");
                return nullptr;
            }
        }

        return &(m_insertSet[m_insertSetSize++]);
    }

    /**
     * @brief Increases the size of the row insertion request array.
     */
    bool ReallocInsertSet();

    void ShrinkInsertSet();

    RC AddInsertToLocalAccess(Row* row, InsItem* currentItem, Sentinel* pIndexInsertResult, bool& isMappedToCache);

    /**
     * @brief Cleans up the current aborted row.
     */
    void CleanupOptimisticInsert(
        InsItem* currentItem, Sentinel* pIndexInsertResult, bool isInserted, bool isMappedToCache);

    void CleanupRecoveryOCCInsert(Row* row, std::vector<Sentinel*>& sentinels);

    void CleanupInsertReclaimKey(Row* row, Sentinel* sentinel);

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
