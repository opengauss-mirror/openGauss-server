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
 * secondary_sentinel_unique.h
 *    Secondary Unique Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/secondary_sentinel_unique.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SECONDARY_SENTINEL_UNIQUE_H
#define SECONDARY_SENTINEL_UNIQUE_H

#include "sentinel.h"
#include "primary_sentinel.h"

namespace MOT {
/**
 * @class SecondarySentinelUnique
 * @brief Secondary unique index sentinel.
 */
class SecondarySentinelUnique : public Sentinel {
public:
    ~SecondarySentinelUnique() override
    {}

    inline PrimarySentinelNode* GetTopNode() const
    {
        return reinterpret_cast<PrimarySentinelNode*>((uint64_t)(GetStatus() & S_OBJ_ADDRESS_MASK));
    }

    void SetEndCSN(uint64_t endCSN) override
    {
        PrimarySentinelNode* node = GetTopNode();
        MOT_ASSERT(node != nullptr);
        node->SetEndCSN(endCSN);
    }

    /**
     * @brief Retrieves the row for the current CSN
     * @param csn the current snapshot
     * @return The sentinel data.
     */
    Row* GetVisibleRowVersion(uint64_t csn) override
    {
        while (IsLocked()) {
            CpuCyclesLevelTime::Sleep(1);
        }
        if (IsCommited()) {
            PrimarySentinel* ps = GetVisiblePrimaryHeader(csn);
            if (!ps) {
                return nullptr;
            }
            return ps->GetVisibleRowVersion(csn);
        }
        return nullptr;
    }

    /**
     * @brief Retrieves the Correct primary-sentinel for the current CSN
     * @param csn the current snapshot
     * @return The primary-sentinel.
     */
    PrimarySentinel* GetVisiblePrimaryHeader(uint64_t csn) override;

    /**
     * @brief Retrieves the Correct primary-sentinel node for the current CSN
     * @param csn the current snapshot
     * @return The primary-sentinel node.
     */
    PrimarySentinelNode* GetNodeByCSN(uint64_t csn);

    /**
     * @brief Return all nodes to the index memory pool
     */
    void ReleaseAllNodes();

    void DetachUnusedNodes(uint64_t snapshot, PrimarySentinelNode*& detachedChain);

    void Print() override;

    static void ReleaseNodeChain(PrimarySentinelNode* node, Index* index);

private:
    DECLARE_CLASS_LOGGER()
};

}  // namespace MOT

#endif  // SECONDARY_SENTINEL_UNIQUE_H
