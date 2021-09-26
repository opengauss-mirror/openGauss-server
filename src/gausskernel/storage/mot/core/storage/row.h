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

#include <string.h>
#include <iosfwd>
#include <string>
#include <type_traits>

#include "table.h"
#include "key.h"
#include "sentinel.h"
#include "row_header.h"
#include "object_pool.h"

// forward declaration
namespace MOT {
class OccTransactionManager;
class CheckpointWorkerPool;
class RecoveryOps;

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

    /**
     * @brief Destructor.
     */
    inline __attribute__((always_inline)) ~Row(){};

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    Row(Row&&) = delete;

    Row& operator=(const Row&) = delete;

    Row& operator=(Row&&) = delete;
    /** @endcond */

    /**
     * @brief Retrieves the table containing the row.
     * @return The owning table.
     */
    inline Table* GetTable() const
    {
        return m_table;
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
        CopyData(src->GetData(), src->GetTupleSize());
        m_table = src->m_table;
    }

    /**
     * @brief Deep copies the row data (including CSN, rowid, etc) from another Row object.
     * @param src The source row from which to copy the data.
     */
    inline void DeepCopy(const Row* src)
    {
        CopyData(src->GetData(), src->GetTupleSize());
        SetCommitSequenceNumber(src->GetCommitSequenceNumber());
        m_table = src->m_table;
        m_surrogateKey = src->m_surrogateKey;
        m_rowId = src->m_rowId;
        m_keyType = src->m_keyType;
    }

    /**
     * @brief Copies partial raw data from another row object.
     * This method is used only for internal tests.
     * @param src A source row.
     */
    void CopyOpt(const Row* src);

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

    /**
     * @brief Sets (turns on) the absent bit for the row.
     */
    inline void SetAbsentRow()
    {
        m_rowHeader.SetAbsentBit();
    }

    /**
     * @brief Sets (turns on) the row header absent lock bit for the row.
     */
    inline void SetAbsentLockedRow()
    {
        m_rowHeader.SetAbsentLockedBit();
    }

    /**
     * @brief Resets (turns off) the row header absent bit for the row.
     */
    inline void UnsetAbsentRow()
    {
        m_rowHeader.UnsetAbsentBit();
    }

    /**
     * @brief Queries the status of the row header absent bit for the row.
     * @return Boolean value denoting whether the absent bit is set or not.
     */
    inline bool IsAbsentRow() const
    {
        return m_rowHeader.IsAbsent();
    }

    /**
     * @brief Queries the validity of the row header.
     * @return Boolean value denoting whether the row is valid or not.
     */
    inline bool IsRowDeleted() const
    {
        return m_rowHeader.IsRowDeleted();
    }

    /**
     * @brief Retrieves the commit sequnce number (CSN) of the row.
     * @return The row commit sequnence number.
     */
    inline uint64_t GetCommitSequenceNumber() const
    {
        return m_rowHeader.GetCSN();
    }

    /**
     * @brief Sets the commit sequence number (CSN) of the row.
     * @param csn The row commit sequnence number.
     */
    inline void SetCommitSequenceNumber(uint64_t csn)
    {
        m_rowHeader.SetCSN(csn);
    }

    /**
     * @brief Accesses the row trough the concurrency contorl management.
     * @param type The concurrency contorl access type.
     * @param txn The local private transaction cache.
     * @param[out] row Receives a copy of this row.
     * @param[out] lastTid Receives the last tuple id version of the row.
     * @return Return code denoting the execution result.
     */
    RC GetRow(AccessType type, TxnAccess* txn, Row* row, TransactionId& lastTid) const;

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
     * @param ptr The pointer to the object being deallocated.
     * @param place The object's allocation address.
     */
    static inline __attribute__((always_inline)) void operator delete(void* ptr, void* place) noexcept
    {
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

    /**
     * @brief Returns if row in a two phase recovery mode.
     * @return Boolean value denoting whether the row is in a two phase recovery mode or not.
     */
    bool GetTwoPhaseMode()
    {
        return m_twoPhaseRecoverMode;
    }

    /**
     * @brief Sets the row to two phase recovery mode
     * @param val true/false
     */
    void SetTwoPhaseMode(bool val)
    {
        m_twoPhaseRecoverMode = val;
    }

    /**
     * @brief Sets the row primary sentinel
     * @param s A pointer to the primary sentinel
     */
    void SetPrimarySentinel(Sentinel* s)
    {
        m_pSentinel = s;
    }

    /**
     * @brief Gets the row's primary sentinel.
     * @return pointer to the row's primary sentinel.
     */
    Sentinel* GetPrimarySentinel()
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
        return (uint8_t*)&m_surrogateKey;
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
    void SetSurrogateKey(uint64_t key)
    {
        m_surrogateKey = key;
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
        return m_surrogateKey;
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
        m_surrogateKey = r->GetSurrogateKey();
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

    /**
     * @brief a callback function to remove and destroy a row.
     * @param gcParam1 A place holder for first paramater passed by the GC.
     * @param gcParam1 A place holder for second param passed by the GC.
     * @param dropIndex An indicator for drop index operator.
     */
    static uint32_t RowDtor(void* gcParam1, void* gcParam2, bool dropIndex)
    {
        // We want to destroy the row even if this is a drop_index flow
        uint32_t size = 0;
        Row* r = reinterpret_cast<Row*>(gcParam1);
        // Add size of key and row
        MOT_ASSERT(r != nullptr);
        Table* t = r->GetTable();
        MOT_ASSERT(t != nullptr);
        size += t->GetRowSizeFromPool();
        if (!dropIndex) {
            t->DestroyRow(r);
        }
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
    inline uint64_t GetSetValuePosition(int id, uint64_t& dataSize)
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

    // Header of the row reserved for the concurrency control method
    // NOTE: This member should always be first (DO NOT CHANGE)
    /** @var The row header. */
    RowHeader m_rowHeader;

    /** @var The table to which this row belongs. */
    Table* m_table = nullptr;

    /** @var The internal key number on cases where primary key doesn't exist. */
    uint64_t m_surrogateKey;

    /** @var The reference to the sentinel that points to this row. */
    Sentinel* m_pSentinel = nullptr;

    /** @var the row id. */
    uint64_t m_rowId;

    /** @var The key type. */
    KeyType m_keyType;

    /** @var A flag to identify if row is in recover mode state. */
    bool m_twoPhaseRecoverMode = false;

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

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif  // MOT_ROW_H
