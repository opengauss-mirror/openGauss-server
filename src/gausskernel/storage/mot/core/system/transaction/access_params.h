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
 * access_params.h
 *    Holds parameters of current row.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/access_params.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef ACCESS_PARAMS_H
#define ACCESS_PARAMS_H

#include <cstdint>
#include <type_traits>
#include "logger.h"

namespace MOT {
enum AccessFlags : uint8_t {
    primary_sentinel_bit = (1U),
    unique_index_bit = (1U << 1),
    row_commited_bit = (1U << 2),
    upgrade_insert_bit = (1U << 3),
    dummy_deleted_bit = (1U << 4),
    index_update_bit = (1U << 5),
    update_deleted_bit = (1U << 6),
};

/**
 * @class AccessParams
 * @brief Holds parameters of current row
 */
template <typename T>
class AccessParams {
public:
    AccessParams() : m_value(0)
    {}

    ~AccessParams()
    {
        static_assert(sizeof(T) == sizeof(AccessFlags), "Sizes are different");
    };

    __attribute__((noinline)) void Print() const
    {
        MOT_LOG_INFO("PrimarySentinel: %s", IsPrimarySentinel() ? "TRUE" : "FALSE");
        MOT_LOG_INFO("UniqueIndex: %s", IsUniqueIndex() ? "TRUE" : "FALSE");
        MOT_LOG_INFO("RowCommited: %s", IsRowCommited() ? "TRUE" : "FALSE");
        MOT_LOG_INFO("UpgradeInsert: %s", IsUpgradeInsert() ? "TRUE" : "FALSE");
        MOT_LOG_INFO("InsertOnDeleted: %s", IsInsertOnDeletedRow() ? "TRUE" : "FALSE");
        MOT_LOG_INFO("IndexUpdate: %s", IsIndexUpdate() ? "TRUE" : "FALSE");
        MOT_LOG_INFO("UpdateDeleted: %s", IsUpdateDeleted() ? "TRUE" : "FALSE");
    }

    bool IsPrimarySentinel() const
    {
        return static_cast<bool>(m_value & primary_sentinel_bit);
    }

    bool IsUniqueIndex() const
    {
        return static_cast<bool>(m_value & unique_index_bit);
    }

    bool IsRowCommited() const
    {
        return static_cast<bool>(m_value & row_commited_bit);
    }

    bool IsUpgradeInsert() const
    {
        return static_cast<bool>(m_value & upgrade_insert_bit);
    }

    bool IsPrimaryUpgrade() const
    {
        return (IsPrimarySentinel() and IsUpgradeInsert());
    }

    bool IsSecondaryUniqueSentinel() const
    {
        return !IsPrimarySentinel() and IsUniqueIndex();
    }

    bool IsInsertOnDeletedRow() const
    {
        return static_cast<bool>(m_value & dummy_deleted_bit);
    }

    bool IsIndexUpdate() const
    {
        return static_cast<bool>(m_value & index_update_bit);
    }

    bool IsUpdateDeleted() const
    {
        return static_cast<bool>(m_value & update_deleted_bit);
    }

    void SetPrimarySentinel()
    {
        m_value |= (primary_sentinel_bit | unique_index_bit);
    }

    void SetSecondarySentinel()
    {
        m_value &= ~primary_sentinel_bit;
    }

    void SetUniqueIndex()
    {
        m_value |= unique_index_bit;
    }

    void SetRowCommited()
    {
        m_value |= row_commited_bit;
    }

    void UnsetRowCommited()
    {
        m_value &= ~row_commited_bit;
    }

    void SetUpgradeInsert()
    {
        m_value |= upgrade_insert_bit;
    }

    void UnsetUpgradeInsert()
    {
        m_value &= ~upgrade_insert_bit;
        UnsetInsertOnDeletedRow();
    }

    void SetInsertOnDeletedRow()
    {
        m_value |= dummy_deleted_bit;
    }

    void SetIndexUpdate()
    {
        m_value |= index_update_bit;
    }

    void UnsetIndexUpdate()
    {
        m_value &= ~index_update_bit;
    }

    void UnsetInsertOnDeletedRow()
    {
        m_value &= ~dummy_deleted_bit;
    }

    void SetUpdateDeleted()
    {
        m_value |= update_deleted_bit;
    }

    void UnsetUpdateDeleted()
    {
        m_value &= ~update_deleted_bit;
    }

    void AssignParams(T x)
    {
        m_value = x;
    }

private:
    /** @var container for bit parameters   */
    T m_value;

    AccessParams(T v) : m_value(v)
    {}

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif  // ACCESS_PARAMS_H
