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

#ifndef MOT_ACCESS_H
#define MOT_ACCESS_H

#include <cstdint>

namespace MOT {
/**
 * @class Access
 * @brief Holds data for single row access.
 */

class Access {
public:
    explicit Access(uint32_t id) : m_csn(0), m_snapshot(0), m_stmtCount(0), m_redoStmt(0), m_ops(0), m_bufferId(id)
    {
        m_modifiedColumns.Reset();
    }

    ~Access()
    {
        m_localInsertRow = nullptr;
        m_globalRow = nullptr;
        m_localRow = nullptr;
        m_origSentinel = nullptr;
        m_secondaryUniqueNode = nullptr;
        m_secondaryDelKey = nullptr;
    }

    void Print() const;

    void WriteGlobalChanges(uint64_t csn, uint64_t transaction_id);

    void ResetUsedParameters()
    {
        m_localInsertRow = nullptr;
        m_globalRow = nullptr;
        m_localRow = nullptr;
        m_origSentinel = nullptr;
        m_secondaryUniqueNode = nullptr;
        m_secondaryDelKey = nullptr;
        m_stmtCount = 0;
        m_redoStmt = 0;
        m_ops = 0;
        m_csn = 0;
        m_snapshot = 0;
        m_params.AssignParams(0);
    }

    inline Row* GetLocalVersion()
    {
        if (m_type != INS) {
            return m_localRow;
        } else {
            return m_localInsertRow;
        }
    }

    inline Row* GetGlobalVersion() const
    {
        return m_globalRow;
    }

    inline Sentinel* GetSentinel() const
    {
        return m_origSentinel;
    }

    /**
     * @brief Get row from header
     *  for inserts the local header if mapped is the local draft
     * @return Pointer to the current row
     */
    inline Row* GetRowFromHeader() const
    {
        if (m_type == INS and m_params.IsUpgradeInsert() == false) {
            return m_localInsertRow;
        }
        return m_origSentinel->GetData();
    }

    inline Row* GetLocalInsertRow() const
    {
        return m_localInsertRow;
    }

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
        if (m_type == RD or m_type == RD_FOR_UPDATE) {
            return m_globalRow;
        }
        if (m_type != INS) {
            return m_localRow;
        } else {
            return m_localInsertRow;
        }
    }

    inline AccessType GetType() const
    {
        return m_type;
    }

    inline uint32_t GetBufferId() const
    {
        return m_bufferId;
    }

    inline void SetBufferId(uint32_t id)
    {
        m_bufferId = id;
    }

    void IncreaseOps()
    {
        m_ops++;
    }

    uint16_t GetOpsCount() const
    {
        return m_ops;
    }

    uint32_t GetStmtCount() const
    {
        return m_stmtCount;
    }

    uint32_t GetRedoStmt() const
    {
        return m_redoStmt;
    }

    /** @var The global visible sentinel */
    Sentinel* m_origSentinel = nullptr;

    /** @var The global visible version   */
    Row* m_globalRow = nullptr;

    /** @var The inserted row for the INS operation */
    Row* m_localInsertRow = nullptr;

    /** @var The modified draft row. */
    Row* m_localRow = nullptr;

    /** @var Key used for delete from secondary index in case of update on indexed column */
    Key* m_secondaryDelKey = nullptr;

    /** @var the snapshot of the current operation */
    uint64_t m_csn = 0;

    uint64_t m_snapshot = 0;

    /** @var The bitmap set represents the updated columns. */
    BitmapSet m_modifiedColumns;

    /** @var The secondary-sentinel unique cached node */
    PrimarySentinelNode* m_secondaryUniqueNode = nullptr;

    /** @var Transaction statement counter   */
    uint32_t m_stmtCount = 0;

    /** @var redo statement index   */
    uint32_t m_redoStmt = 0;

    /** @var number of operation performed in a single query   */
    uint16_t m_ops = 0;

    /** @var Local access parameters */
    AccessParams<uint8_t> m_params;

    /** @var Row state   */
    AccessType m_type = AccessType::INV;

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

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif  // MOT_ACCESS_H
