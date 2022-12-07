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
 * column.h
 *    The Column class describes a single field in a row in a table.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/column.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_COLUMN_H
#define MOT_COLUMN_H

#include "catalog_column_types.h"
#include "utils/elog.h"
#include "securec.h"

#define MOT_MAXDATELEN 128

namespace MOT {
/**
 * @brief The Column class describes a single field in a row in a table.
 *
 * Each column has a name, type, size, byte offset and identifier (ordinal number)
 * in its containing column list (@see Catalog below).
 */
class alignas(CACHE_LINE_SIZE) Column {
public:
    /** @var Maximum size of column name. */
    static constexpr size_t MAX_COLUMN_NAME_LEN = 84;

    static const char* ColumnTypeToStr(MOT_CATALOG_FIELD_TYPES type);

    static MOT_CATALOG_FIELD_TYPES ColumnStrTypeToEnum(const char* type);

    static Column* AllocColumn(MOT_CATALOG_FIELD_TYPES type);

    static const char* ColumnErrorMsg(RC err);

    /** @brief Constructor. */
    Column();

    /** @brief Destructor. */
    virtual ~Column();

    /**
     * @brief stores column data into the provided data row at the columns offset.
     *
     * @param dest The pointer to a start of the Row data.
     * @param src The pointer to column data to be stored.
     * If the data to be stored is fixed length (like int etc.) it should be passed by value
     * @param len The length of the data to be stored.
     * @return Boolean value denoting success or failure.
     */
    virtual bool Pack(uint8_t* dest, uintptr_t src, size_t len) = 0;

    /**
     * @brief stores column data into the provided destination buffer.
     *
     * @param dest The pointer to a data buffer at the correct offset.
     * @param src The pointer to column data to be stored.
     * @param len The length of the data to be stored.
     * @return Boolean value denoting success or failure.
     */
    virtual bool PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill = 0x00) = 0;

    /**
     * @brief retrieves column data from the provided data row by the columns offset.
     *
     * @param data The pointer to a start of the Row data.
     * @param dest The pointer to a placeholder for data to be retrieved. If the data is fixed length (like int and
     * etc.) it's stored in the pointer if the data is variable length the dest will contain a pointer to a data
     * @param len The length of the data pointed by the dest.
     */
    virtual void Unpack(uint8_t* data, uintptr_t* dest, size_t& len) = 0;

    virtual void SetKeySize() = 0;

    inline size_t GetKeySize() const
    {
        return m_keySize;
    }

    virtual uint16_t PrintValue(uint8_t* data, char* destBuf, size_t len, bool useDefault = false)
    {
        if (len >= 3) {
            errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
            securec_check_ss(erc, "\0", "\0");
            return erc;
        }

        return 0;
    }

    inline bool IsUsedByIndex(void) const
    {
        return m_numIndexesUsage > 0;
    }

    inline void IncIndexUsage(void)
    {
        m_numIndexesUsage++;
    }

    inline void DecIndexUsage(void)
    {
        m_numIndexesUsage--;
    }

    const char* GetTypeStr() const
    {
        return ColumnTypeToStr(m_type);
    }

    RC SetDefaultValue(uintptr_t val, size_t size);

    void ResetDefaultValue();

    void SetDropped();

    inline void SetIsDropped(bool isDropped)
    {
        m_isDropped = isDropped;
    }

    inline bool GetIsDropped() const
    {
        return m_isDropped;
    }

    inline void SetIsCommitted(bool isCommited)
    {
        m_isCommitted = isCommited;
    }

    inline bool GetIsCommitted() const
    {
        return m_isCommitted;
    }

    RC Clone(Column* col);

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    Column(const Column&) = delete;

    Column(Column&&) = delete;

    Column& operator=(const Column&) = delete;

    Column& operator=(Column&&) = delete;
    /** @endcond */

    /** @var Column identifier. */
    uint64_t m_id = 0;

    /** @var Field size in bytes. */
    uint32_t m_size = 0;

    uint32_t m_keySize = 0;

    /** @var Field offset in the row. */
    uint64_t m_offset = 0;

    /** @var Column name. */
    char m_name[MAX_COLUMN_NAME_LEN] = {0};

    /** @var Column name length */
    uint16_t m_nameLen = 0;

    /** @var Number of indexes which using this column as part of their key. */
    uint16_t m_numIndexesUsage = 0;

    /** @var Column type name. */
    MOT_CATALOG_FIELD_TYPES m_type;

    /** @var Column does not allow null values. */
    bool m_isNotNull = false;

    /** @var Envelope column type. */
    unsigned int m_envelopeType = 0;

    /** @var Column is dropped. */
    bool m_isDropped = 0;

    /** @var Column has default value. */
    bool m_hasDefault = false;

    /** @var Column's default value. */
    uintptr_t m_defValue = 0;

    /** @var Column's default value size. */
    size_t m_defSize = 0;

    /** @var Column's commit state. */
    bool m_isCommitted = false;
};

// derived column classes
#define X(Enum, String)                                                                          \
    class alignas(CACHE_LINE_SIZE) Column##Enum : public Column {                                \
    public:                                                                                      \
        ~Column##Enum() override                                                                 \
        {}                                                                                       \
        bool Pack(uint8_t* dest, uintptr_t src, size_t len) override;                            \
        bool PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill) override;           \
        void Unpack(uint8_t* data, uintptr_t* dest, size_t& len) override;                       \
        void SetKeySize() override;                                                              \
        uint16_t PrintValue(uint8_t* data, char* destBuf, size_t len, bool useDefault) override; \
    };
TYPENAMES
#undef X
}  // namespace MOT

#endif  // MOT_COLUMN_H
