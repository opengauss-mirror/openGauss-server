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

#include <stdint.h>
#include <type_traits>

namespace MOT {
enum AccessFlags : uint8_t {
    primary_sentinel_bit = (1U << 0),
    unique_index_bit = (1U << 1),
    row_commited_bit = (1U << 2),
    upgrade_insert_bit = (1U << 3),
    dummy_deleted_bit = (1U << 4),
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

    bool IsPrimarySentinel() const
    {
        return m_value & primary_sentinel_bit;
    }

    bool IsUniqueIndex() const
    {
        return m_value & unique_index_bit;
    }

    bool IsRowCommited() const
    {
        return m_value & row_commited_bit;
    }

    bool IsUpgradeInsert() const
    {
        return m_value & upgrade_insert_bit;
    }

    bool IsPrimaryUpgrade() const
    {
        return (IsPrimarySentinel() and IsUpgradeInsert());
    }

    bool IsDummyDeletedRow() const
    {
        return m_value & dummy_deleted_bit;
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
    }

    void SetDummyDeletedRow()
    {
        m_value |= dummy_deleted_bit;
    }

    void UnsetDeletedCommitedRow()
    {
        m_value &= ~dummy_deleted_bit;
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
};
}  // namespace MOT

#endif  // ACCESS_PARAMS_H
