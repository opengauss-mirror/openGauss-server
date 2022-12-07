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
 * row.h
 *    The Row class holds all that is required to manage an in-memory row in a table.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/row.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_ROW_H
#define MOT_ROW_H

#include <cstring>
#include <iosfwd>
#include <string>
#include <type_traits>

#include "table.h"
#include "column.h"
#include "key.h"
#include "sentinel.h"
#include "object_pool.h"

// forward declaration
namespace MOT {
class OccTransactionManager;
class CheckpointWorkerPool;
class RecoveryOps;

enum class RowType : uint8_t { ROW, TOMBSTONE, STABLE, ROW_ZERO };

/**
 * @class Row
 * @brief The Row class holds all that is required to manage an in-memory row in
 * a table.
 */
class PACKED Row {
public:
    /**
     * @brief Non-default constructor.
     * @param host_table The table to which the row belongs.
     */
    explicit Row(Table* hostTable);

    Row(const Row& src);

    Row(const Row& src, Table* tab);
    Row(const Row& src, bool nullBitsChanged, size_t srcBitsSize, size_t newBitsSize, size_t dataSize, Column* col);
    Row(const Row& src, Column** newCols, uint32_t newColCnt, Column** oldCols, uint32_t oldColCnt);

    /**
     * @brief Destructor.
     */
    inline __attribute__((always_inline)) ~Row()
    {
        m_table = nullptr;
        m_pSentinel = nullptr;
        m_next = nullptr;
    };

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    Row(Row&&) = delete;

    Row& operator=(const Row&) = delete;

    Row& operator=(Row&&) = delete;
    /** @endcond */

    void CopyRowZero(const Row* src, Table* txnTable);

    void CopyHeader(const Row& src, RowType rowType = RowType::ROW);
    /**
     * @brief Retrieves the table containing the row.
     * @return The owning table.
     */
    inline Table* GetTable() const
    {
        return m_table;
    }

    inline Table* GetOrigTable() const
    {
        return m_table->GetOrigTable();
    }

    inline void SetTable(Table* tab)
    {
        m_table = tab;
    }

    /**
     * @brief Sets the value of a field (column) in the row.
     * @param id The field id (ordinal number) in the row.
     * @param ptr A pointer to the value to set.
     * @param size The size in bytes of the value to set.
     */
    void SetValueVariable(int id, const void* ptr, uint32_t size);

    /**
     * @brief Retrieves the value of a field (column) in the row by id.
     * @param id The field id (ordinal number) in the row.
     * @return A pointer to the retrieved value.
     */
    inline uint8_t* GetValue(int id)
    {
        uint64_t pos = m_table->GetFieldOffset(id);
        return &m_data[pos];
    }

    /**
     * @brief Retrieves the value of a field (column) in the row by name.
     * @param colName The name of field in the row.
     * @return A pointer to the retrieved value.
     */
    inline uint8_t* GetValue(const char* colName)
    {
        uint64_t pos = m_table->GetFieldOffset(colName);
        return &m_data[pos];
    }

    /**
     * @brief Retrieves a typed value of a field (column) in the row by id.
     * @param colId The column identifier.
     * @param value The variable to set its value.
     */
    template <typename T>
    inline void GetValue(int colId, T& value) const
    {
        uint64_t pos = m_table->GetFieldOffset(colId);
        uint8_t* p = (uint8_t*)&m_data[pos];
        value = *(reinterpret_cast<T*>(p));
    }

    /**
     * @brief A synonym for #Row::setValue.
     * @param columnId The identifier of the column to update.
     * @param value The typed value to set.
     */
    template <typename T>
    inline void SetAttribute(int columnId, T value)
    {
        SetValue(columnId, value);
    }

    /**
     * @brief Sets the raw data of this Row object from an external buffer.
     * @param data The buffer holding the raw data of the row.
     * @param size The buffer size
     */
    inline void CopyData(uint8_t const* data, uint64_t size)
    {
        // We test to make sure pointer was not return into the pull;
        // If so we copy garbage and we fall in validation.
        if (data != nullptr) {
            errno_t erc = memcpy_s(m_data, GetTupleSize(), data, size);
            securec_check(erc, "\0", "\0");
        }
    }

    /**
     * @brief Copies the raw data of this Row object from another Row object.
     * @param src The source row from which to copy the raw data.
     */
    inline void Copy(const Row* src)
    {
        if (unlikely(GetTable()->GetHasColumnChanges())) {
            CopyVersion(*src,
                GetTable()->m_columns,
                GetTable()->m_fieldCnt,
                src->GetTable()->m_columns,
                src->GetTable()->m_fieldCnt);
        } else {
            CopyData(src->GetData(), src->GetTupleSize());
            m_table = src->m_table;
        }
    }

    /**
     * @brief Deep copies the row data (including CSN, rowid, etc) from another Row object.
     * @param src The source row from which to copy the data.
     */
    inline void DeepCopy(const Row* src)
    {
        CopyData(src->GetData(), src->GetTupleSize());
        CopyHeader(*src, src->GetRowType());
    }

    /**
     * @brief Copies partial raw data from another row object.
     * This method is used only for internal tests.
     * @param src A source row.
     */
    void CopyOpt(const Row* src);

    /**
     * @brief full row copy, src row is an old version.
     * @param src A source row.
     * @param newCols A new data version
     * @param oldCols An old data version
     */
    void CopyVersion(const Row& src, Column** newCols, uint32_t newColCnt, Column** oldCols, uint32_t oldColCnt);

    /**
     * @brief Retrieves a pointer to the raw data of the row.
     * @return The row data buffer.
     */
    inline const uint8_t* GetData() const
    {
        return m_data;
    }

    /**
     * @brief Retrieves the size in bytes of the raw data of the row.
     * @return The row data size in bytes.
     */
    inline uint32_t GetTupleSize() const
    {
        return m_table->GetTupleSize();
    };

    RowType GetRowType() const
    {
        return m_rowType;
    }

    /**
     * @brief Sets (turns on) the absent bit for the row.
     */
    inline void SetDeletedRow()
    {
        m_rowType = RowType::TOMBSTONE;
    }

    /**
     * @brief Sets stable flag on the row.
     */
    inline void SetStableRow()
    {
        m_rowType = RowType::STABLE;
    }

    /**
     * @brief Queries the status of the row header absent bit for the row.
     * @return Boolean value denoting whether the absent bit is set or not.
     */
    inline bool IsRowDeleted() const
    {
        return (m_rowType == RowType::TOMBSTONE);
    }

    /**
     * @brief Retrieves the commit sequnce number (CSN) of the row.
     * @return The row commit sequnence number.
     */
    inline uint64_t GetCommitSequenceNumber() const
    {
        return m_rowCSN;
    }

    /**
     * @brief Sets the commit sequence number (CSN) of the row.
     * @param csn The row commit sequence number.
     */
    inline void SetCommitSequenceNumber(uint64_t csn)
    {
        m_rowCSN = csn;
    }

    /**
     * @brief Class specific in-place new operator.
     * @param size Object size in bytes.
     * @param place Object allocation address.
     * @return A pointer to the resulting allocated object.
     */
    static inline __attribute__((always_inline)) void* operator new(std::size_t size, void* place) noexcept
    {
        (void)size;
        return place;
    }

    /**
     * @brief Class specific in-place delete operator.
     * @param ptr The pointer to the object being de-allocated.
     * @param place The object's allocation address.
     */
    static inline __attribute__((always_inline)) void operator delete(void* ptr, void* place) noexcept
    {
        MOT_ASSERT(ptr != nullptr);
        (void)ptr;
        (void)place;
    }

    // allow privileged access to global insertion operator
    friend std::ostream& operator<<(std::ostream& out, const Row& row);

    /**
     * @brief Generates a copy of the row (for CALC)
     * @return the ptr of the new row
     */
    Row* CreateCopy();

    /**
     * @brief Retrieves the stable row
     * @return The ptr of the stable row.
     */
    Row* GetStable()
    {
        return m_pSentinel->GetStable();
    };

    Row* GetNextVersion() const
    {
        return m_next;
    }

    void SetNextVersion(Row* r)
    {
        m_next = r;
    }

    /**
     * @brief Obtains the original row's transaction id from the stable row.
     *  We reuse the m_next member in order to avoid adding more members.
     * @return the transaction id.
     */
    uint64_t GetStableTid() const
    {
        MOT_ASSERT(m_rowType == RowType::STABLE);
        return reinterpret_cast<uint64_t>(m_next);
    }

    /**
     * @brief Sets the original row's transaction id on the stable row.
     *  We reuse the m_next member in order to avoid adding more members.
     * @param the transaction id.
     */
    void SetStableTid(uint64_t tid)
    {
        MOT_ASSERT(m_rowType == RowType::STABLE);
        m_next = reinterpret_cast<Row*>(tid);
    }

    /**
     * @brief Sets the row primary sentinel
     * @param s A pointer to the primary sentinel
     */
    void SetPrimarySentinel(Sentinel* s)
    {
        m_pSentinel = reinterpret_cast<PrimarySentinel*>(s);
    }

    /**
     * @brief Gets the row's primary sentinel.
     * @return pointer to the row's primary sentinel.
     */
    PrimarySentinel* GetPrimarySentinel()
    {
        return m_pSentinel;
    }

    /**
     * @brief Sets the row's key type
     * @param type The key type
     */
    void SetKeytype(KeyType type)
    {
        m_keyType = type;
    }

    /**
     * @brief Sets a row with internal key.
     * Used for internal tests
     * @param id A column id.
     */
    void SetInternalKey(int id, uint64_t key)
    {
        m_keyType = KeyType::INTERNAL_KEY;
        SetValue(id, key);
    }

    /**
     * @brief Queries if the row has surrogate key.
     * @return Boolean value denoting whether the row has surrogate key or not.
     */
    bool IsSurrogateKey() const
    {
        return (m_keyType == KeyType::SURROGATE_KEY);
    }

    /**
     * @brief Queries if the row has internal key.
     * @return Boolean value denoting whether the row has internal key or not.
     */
    bool IsInternalKey() const
    {
        return (m_keyType == KeyType::INTERNAL_KEY);
    }

    /**
     * @brief Get a reference to the surrogate key.
     * @return refernece to the surrogate key
     */
    uint8_t* GetSurrogateKeyBuff() const
    {
        return (uint8_t*)&m_rowId;
    }

    /**
     * @brief Get the internal key buffer.
     * @param order The index order type.
     * @return Pointer to the internal key buffer.
     */
    uint8_t* GetInternalKeyBuff(IndexOrder order)
    {
        int id = GetTable()->GetFieldCount();
        int index_count = GetTable()->GetNumIndexes();
        if (index_count > 1) {
            if (order == IndexOrder::INDEX_ORDER_PRIMARY) {
                id--;
            }
        }
        return GetValue(id - 1);
    }

    /**
     * @brief Get key type.
     * @return key type.
     */
    KeyType GetKeyType() const
    {
        return m_keyType;
    }

    /**
     * @brief Sets surrogate key.
     * @param key The surrogate key value.
     */
    void SetSurrogateKey()
    {
        m_keyType = KeyType::SURROGATE_KEY;
    }

    /**
     * @brief Reset the key type.
     */
    void ResetKeyType()
    {
        m_keyType = KeyType::EMPTY_KEY;
    }

    /**
     * @brief Get the row Id.
     * @return The row Id
     */
    uint64_t GetRowId() const
    {
        return m_rowId;
    }

    /**
     * @brief Get the row surrogate key.
     * @return The row surrogate key
     */
    uint64_t GetSurrogateKey() const
    {
        return htobe64(m_rowId);
    }

    /**
     * @brief Sets the row Id.
     * @param id The row id.
     */
    void SetRowId(uint64_t id)
    {
        m_rowId = id;
    }

    /**
     * @brief Copy surrogate key from a source row.
     * @param r A source row.
     */
    void CopySurrogateKey(const Row* r)
    {
        m_keyType = r->GetKeyType();
    }

    /**
     * @brief Sets a value for a specified field (column) id.
     * @param colId A column id.
     * @param value A typed value of the column.
     */
    template <typename T>
    inline void SetValue(int colId, T value)
    {
        static_assert(
            !std::is_pointer<T>::value, "Template setValue method must be passed non-pointer second argument");
        uint64_t dataSize;
        uint64_t pos = GetSetValuePosition(colId, dataSize);
        uint8_t* p = &m_data[pos];
        if (__builtin_expect(dataSize == sizeof(T), 1)) {
            *(reinterpret_cast<T*>(p)) = value;
        } else {
            errno_t erc = memcpy_s(p, dataSize, (const void*)&value, dataSize);
            securec_check(erc, "\0", "\0");
        }
    }

    void Print();

    /**
     * @brief a callback function to remove and destroy a version chain.
     * @param gcElement A place holder for the gc element metadata.
     * @param oper GC operation for the current element
     * @param aux a pointer to the GC delete vector
     * @return size of cleaned element
     */
    static uint32_t RowVersionDtor(void* gcElement, void* oper, void* aux)
    {
        GC_OPERATION_TYPE gcOperType = (*(GC_OPERATION_TYPE*)oper);
        GcQueue::DeleteVector* deleteVector = static_cast<GcQueue::DeleteVector*>(aux);
        LimboElement* elem = reinterpret_cast<LimboElement*>(gcElement);
        PrimarySentinel* ps = reinterpret_cast<PrimarySentinel*>(elem->m_objectPool);
        uint32_t size = ps->GetIndex()->GetTable()->GetRowSizeFromPool();
        if (unlikely(gcOperType == GC_OPERATION_TYPE::GC_OPER_DROP_INDEX)) {
            return size;
        }
        // We want to destroy the row even if this is a drop_index flow
        GcSharedInfo& gcInfo = ps->GetGcInfo();
        MOT_ASSERT(gcInfo.GetCounter() > 0);
        // Decrement reference count
        uint32_t gcSlot = gcInfo.RefCountUpdate(DEC);
        gcSlot--;
        // Check if we are the last owners of the sentinel
        // if so reclaim it
        if ((gcSlot == 0) and ps->IsSentinelRemovable()) {
            MOT_LOG_DEBUG("RowVersionDtor Deleting ps %p minActiveCSN %lu ", ps, MOT::g_gcActiveEpoch);
            ps->ReclaimSentinel(ps);
            return size;
        }
        uint64_t reclaimCSN = elem->m_csn;
        // If row was reclaimed skip this element
        if (reclaimCSN <= gcInfo.GetMinCSN()) {
            return size;
        }
        // We try to reclaim every N element
        if (gcSlot and gcSlot % GC_MAX_ELEMENTS_TO_SKIP) {
            // At this point we can check if the row was updated
            // If so we can skip
            if (ps->GetData() != elem->m_objectPtr) {
                return size;
            }
        }
        bool res = gcInfo.TryLock();
        if (res == false) {
            return size;
        }
        uint64_t min_csn = gcInfo.GetMinCSN();
        // Locked Re-verify element is still valid
        if (reclaimCSN <= min_csn) {
            gcInfo.Release();
            return size;
        }
        // Set MIN_CSN in the gcInfo to inform Concurrent GC's about the latest version.
        gcInfo.SetMinCSN(reclaimCSN);
        Row* tmp = nullptr;
        Row* versionRow = reinterpret_cast<Row*>(elem->m_objectPtr);
        // Add size of key and row
        MOT_ASSERT(versionRow != nullptr);
        Table* t = versionRow->GetTable();
        MOT_ASSERT(t != nullptr);

        // We reclaim all the versions below this versions
        tmp = versionRow;
        versionRow = versionRow->GetNextVersion();
        // Detach the list of rows
        tmp->SetNextVersion(nullptr);

        // Clean older version
        while (versionRow) {
            tmp = versionRow;
            versionRow = versionRow->GetNextVersion();
            if (tmp->IsRowDeleted() == false) {
                t->DestroyRow(tmp);
            } else {
                uint64_t tombstoneCSN = tmp->GetCommitSequenceNumber();
                if (tombstoneCSN > min_csn) {
                    (void)t->GCRemoveRow(deleteVector, tmp, gcOperType);
                }
                t->DestroyRow(tmp);
            }
        }
        // Unlock gcInfo
        gcInfo.Release();

        return size;
    }

    /**
     * @brief a callback function to remove and destroy a deleted version chain.
     * @param gcElement A place holder for the gc element metadata.
     * @param oper GC operation for the current element
     * @param aux a pointer to the GC delete vector
     * @return size of cleaned element
     */
    static uint32_t DeleteRowDtor(void* gcElement, void* oper, void* aux)
    {
        GC_OPERATION_TYPE gcOperType = (*(GC_OPERATION_TYPE*)oper);
        GcQueue::DeleteVector* deleteVector = static_cast<GcQueue::DeleteVector*>(aux);
        LimboElement* limboElem = reinterpret_cast<LimboElement*>(gcElement);
        PrimarySentinel* ps = reinterpret_cast<PrimarySentinel*>(limboElem->m_objectPool);
        uint32_t size = ps->GetIndex()->GetTable()->GetRowSizeFromPool();
        if (unlikely(gcOperType == GC_OPERATION_TYPE::GC_OPER_DROP_INDEX)) {
            return size;
        }
        // We want to destroy the row even if this is a drop_index flow
        GcSharedInfo& gcInfo = ps->GetGcInfo();
        MOT_ASSERT(gcInfo.GetCounter() > 0);
        // Decrement reference count
        uint32_t gcSlot = gcInfo.RefCountUpdate(DEC);
        // Check if we are the last owners of the sentinel
        // if so reclaim it
        if ((gcSlot == 1) and ps->IsSentinelRemovable()) {
            MOT_LOG_DEBUG("RowVersionDtor Deleting ps %p minActiveCSN %lu ", ps, MOT::g_gcActiveEpoch);
            ps->ReclaimSentinel(ps);
            return size;
        }
        uint64_t reclaimCSN = limboElem->m_csn;
        // If row was reclaimed skip this element
        if (reclaimCSN <= gcInfo.GetMinCSN()) {
            MOT_LOG_DEBUG("Skipping Element %s %d ", __func__, __LINE__);
            return size;
        }
        // Lock the gcInfo
        gcInfo.Lock();
        uint64_t min_csn = gcInfo.GetMinCSN();
        // Re-verify element is still valid
        if (reclaimCSN <= min_csn) {
            gcInfo.Release();
            return size;
        }
        //  If a key was inserted on top of us ignore this element
        //  Since we passed CSN check
        //  Serialize the operations from newest to oldest!
        if (ps->GetData() != limboElem->m_objectPtr) {
            MOT_LOG_DEBUG("Skipping Element %s %d ", __func__, __LINE__);
            gcInfo.Release();
            return size;
        }
        gcInfo.SetMinCSN(reclaimCSN);
        Row* tombstone = reinterpret_cast<Row*>(limboElem->m_objectPtr);
        Row* versionRow = tombstone->GetNextVersion();
        // Add size of key and row
        MOT_ASSERT(tombstone != nullptr);
        Table* table = tombstone->GetTable();
        MOT_ASSERT(table != nullptr);
        (void)table->GCRemoveRow(deleteVector, tombstone, gcOperType);

        if (versionRow) {
            Row* currentRow = versionRow;
            versionRow = versionRow->GetNextVersion();
            currentRow->SetNextVersion(nullptr);
        }

        // Clean older version
        while (versionRow) {
            Row* tmp = versionRow;
            versionRow = versionRow->GetNextVersion();
            if (tmp->IsRowDeleted() == false) {
                MOT_LOG_DEBUG("Deleting Version %s %d ", __func__, __LINE__);
                table->DestroyRow(tmp);
            } else {
                MOT_LOG_DEBUG("Deleting tombstone - Clean Secondary Only! %s %d ", __func__, __LINE__);
                uint64_t tombstoneCSN = tmp->GetCommitSequenceNumber();
                if (tombstoneCSN > min_csn) {
                    (void)table->GCRemoveRow(deleteVector, tmp, gcOperType);
                }
                table->DestroyRow(tmp);
            }
        }

        // Unlock gcInfo
        gcInfo.Release();

        return size;
    }

    static constexpr uint64_t INVALID_ROW_ID = 0;

private:
    /**
     * @brief Helper function for optimizing row updates.
     * @detail Accumulates all filed update requests into one memcpy operation.
     * @param id The identifier of the column to update.
     * @param dataSize[out] The number of bytes to update in the field (column).
     * @return The field offset in the row.
     */
    inline uint64_t GetSetValuePosition(int id, uint64_t& dataSize) const
    {
        uint64_t dSize = m_table->GetFieldSize(id);
        uint64_t pos = m_table->GetFieldOffset(id);
        dataSize = dSize;
        return pos;
    }

protected:
    /**
     * @brief Sets the value of a single field (column) in the row.
     * @param colId The identifier of the column to update.
     * @param value The raw value to set.
     */
    inline void SetValue(int colId, const char* value)
    {
        uint64_t dataSize;
        uint64_t pos = GetSetValuePosition(colId, dataSize);
        errno_t erc = memcpy_s(&m_data[pos], dataSize, value, dataSize);
        securec_check(erc, "\0", "\0");
    }

    /**
     * @brief Sets the value of a single field (column) in the row.
     * @param colId The identifier of the column to update.
     * @param value The raw value to set.
     */
    inline void SetValue(int colId, char* value)
    {
        SetValue(colId, const_cast<const char*>(value));
    }

    // disable override
    /** @cond EXCLUDE_DOC */
    void SetValue(int colId, const void* value) = delete;
    /** @endcond */

    /** @var The row header. */
    uint64_t m_rowCSN;

    /** @var The next version ordered from N2O */
    Row* m_next = nullptr;

    /** @var The table to which this row belongs. */
    Table* m_table = nullptr;

    /** @var The reference to the sentinel that points to this row. */
    PrimarySentinel* m_pSentinel = nullptr;

    /** @var the row id. */
    uint64_t m_rowId;

    /** @var The key type. */
    KeyType m_keyType;

    /** @var The row type. */
    RowType m_rowType;

    /** @var The raw buffer holding the row data. Starts at the end of the class
     * Must be last member */
    uint8_t m_data[0];

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    void* operator new(std::size_t size) = delete;

    void* operator new(std::size_t size, const std::nothrow_t&) = delete;

    void operator delete(void* ptr) = delete;

    void operator delete(void* ptr, const std::nothrow_t&) = delete;
    /** @endcond */

    // Access to setters
    friend TxnManager;
    friend TxnAccess;
    friend OccTransactionManager;
    friend CheckpointManager;
    friend CheckpointWorkerPool;
    friend Index;
    friend RecoveryOps;
    friend Table;
    friend TxnTable;

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif  // MOT_ROW_H
