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
 * access.h
 *    Holds data for single row access.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/access.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef MOT_ACCESS_H
#define MOT_ACCESS_H

#include <stdint.h>

namespace MOT {
/**
 * @class Access
 * @brief Holds data for single row access.
 */
class Access {

public:
    explicit Access(uint32_t id) : m_localRowSize(0), m_bufferId(id)
    {}

    ~Access()
    {}

    void ResetUsedParameters()
    {
        m_localInsertRow = nullptr;
        m_auxRow = nullptr;
        m_origSentinel = nullptr;
        m_stmtCount = 0;
        m_params.AssignParams(0);
    }

    /** @var Access type. */
    inline Sentinel* GetSentinel() const
    {
        return m_origSentinel;
    }

    /**
     * @brief Get row from header
     *  for inserts the local header if mapped is the local draft
     * @return Pointer to the currrent row
     */
    inline Row* GetRowFromHeader() const
    {
        if (m_type == INS and m_params.IsUpgradeInsert() == false) {
            return m_localInsertRow;
        }
        return m_origSentinel->GetData();
    }

    /** @var The original row */
    Row* m_localInsertRow = nullptr;

    /** @var The modified draft row. */
    Row* m_localRow = nullptr;

    /** @var The auxiliary row */
    Row* m_auxRow = nullptr;

    /** @var The original row header */
    Sentinel* m_origSentinel = nullptr;

    /** @var The bitmap set represents the updated columns. */
    BitmapSet m_modifiedColumns;

    /**
     * @brief Gets a consistent local copy of the row which is used by the OCC transaction.
     * If the access is not an insertion the copy will be used also as a draft for
     * writers. If it is an insertion, a clean insertion returns the actual,
     * unpublished and private row, while an upgrade, i.e. insert after delete,
     * returns the auxiliary row
     * @return latest draft row
     */
    inline Row* GetTxnRow() const
    {
        if (m_type != INS) {
            return m_localRow;
        } else {
            if (m_params.IsUpgradeInsert()) {
                return m_auxRow;
            } else
                return m_localInsertRow;
        }
    }

    /**
     * @brief When validating INS operation we want to verify no
     *  other commiter commited before us.
     *  for regular insert we start by pointing at a null pointer
     * @return row pointer
     */
    inline Row* GetRecordedSentinelRow() const
    {
        if (m_params.IsUpgradeInsert() == false) {
            return nullptr;
        } else {
            return GetSentinel()->GetData();
        }
    }

    inline uint32_t GetBufferId() const
    {
        return m_bufferId;
    }

    inline void SetBufferId(uint32_t id)
    {
        m_bufferId = id;
    }

    /** @var OCC transaction identifier. */
    TransactionId m_tid = 0;

    /** @var Local access parameters */
    AccessParams<uint8_t> m_params;

    /** @var Row state   */
    AccessType m_type = AccessType::INV;

    /** @var Transaction statement counter   */
    uint32_t m_stmtCount = 0;

    uint32_t m_localRowSize;

private:
    /**
     * @brief Swap current id with other Access
     * @param lhs - reference to another Access
     * @return void
     */
    inline void SwapId(Access& lhs)
    {
        uint32_t temp = m_bufferId;
        m_bufferId = lhs.GetBufferId();
        lhs.SetBufferId(temp);
    }

    /** @var buffer_id   */
    uint32_t m_bufferId;

    friend class TxnAccess;
};
}  // namespace MOT

#endif  // MOT_ACCESS_H
