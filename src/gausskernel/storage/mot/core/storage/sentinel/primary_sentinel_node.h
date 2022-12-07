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
 * primary_sentinel_node.h
 *    Primary Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/primary_sentinel_node.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PRIMARY_SENTINEL_NODE_H
#define PRIMARY_SENTINEL_NODE_H

namespace MOT {

class PrimarySentinel;

/**
 * @class PrimarySentinelNode
 * @brief Node specifying the current primary sentinel the secondary-unique sentinel points to.
 */

class PrimarySentinelNode {
public:
    PrimarySentinelNode() : m_startCSN(0), m_endCSN(NODE_INIT_CSN), m_primarySentinel(nullptr), m_next(nullptr)
    {}

    void Init(uint64_t start_csn, uint64_t end_csn, PrimarySentinel* p, PrimarySentinelNode* next = nullptr)
    {
        m_startCSN = start_csn;
        m_endCSN = end_csn;
        m_primarySentinel = p;
        m_next = next;
    }

    void SetStartCSN(uint64_t start_csn)
    {
        m_startCSN = start_csn;
    }

    void SetEndCSN(uint64_t end_csn)
    {
        m_endCSN = end_csn;
    }

    void SetPrimarySentinel(PrimarySentinel* ps)
    {
        m_primarySentinel = ps;
    }

    void SetNextVersion(PrimarySentinelNode* v)
    {
        m_next = v;
    }

    uint64_t GetStartCSN() const
    {
        return m_startCSN;
    }

    uint64_t GetEndCSN() const
    {
        return m_endCSN;
    }

    std::string GetEndCSNStr() const
    {
        if (m_endCSN == NODE_INIT_CSN) {
            return std::string("INF");
        } else {
            return std::to_string(m_endCSN);
        }
    }

    PrimarySentinel* GetPrimarySentinel() const
    {
        return m_primarySentinel;
    }

    PrimarySentinelNode* GetNextVersion() const
    {
        return m_next;
    }

    inline bool IsNodeDeleted() const
    {
        return m_endCSN != NODE_INIT_CSN;
    }

    static constexpr uint64_t NODE_INIT_CSN = static_cast<uint64_t>(-1);

private:
    /** @var The Start CSN of the sentinel. */
    uint64_t m_startCSN;

    /** @var The End CSN of the sentinel. */
    uint64_t m_endCSN;

    /** @var Pointer to the primary sentinel */
    PrimarySentinel* m_primarySentinel;

    /** @var Pointer to the next node */
    PrimarySentinelNode* m_next;
};

}  // namespace MOT

#endif  // PRIMARY_SENTINEL_NODE_H
