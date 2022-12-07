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
 * surrogate_state.cpp
 *    Surrogate key info helper class.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/surrogate_state.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "surrogate_state.h"

namespace MOT {
DECLARE_LOGGER(SurrogateState, Recovery);

SurrogateState::SurrogateState()
    : m_insertsArray(nullptr), m_empty(true), m_maxConnections(GetGlobalConfiguration().m_maxConnections)
{}

bool SurrogateState::Init()
{
    m_insertsArray = new (std::nothrow) uint64_t[m_maxConnections];
    if (m_insertsArray == nullptr) {
        return false;
    }
    errno_t erc = memset_s(m_insertsArray, m_maxConnections * sizeof(uint64_t), 0, m_maxConnections * sizeof(uint64_t));
    securec_check(erc, "\0", "\0");
    return true;
}

SurrogateState::~SurrogateState()
{
    if (m_insertsArray != nullptr) {
        delete[] m_insertsArray;
        m_insertsArray = nullptr;
    }
    m_empty = true;
}

void SurrogateState::UpdateMaxInsertions(uint64_t insertions, uint32_t pid)
{
    if (pid < m_maxConnections && m_insertsArray[pid] < insertions) {
        m_insertsArray[pid] = insertions;
        if (m_empty) {
            m_empty = false;
        }
    }
}

bool SurrogateState::UpdateMaxKey(uint64_t key)
{
    uint64_t pid = 0;
    uint64_t insertions = 0;

    ExtractInfoFromKey(key, pid, insertions);
    if (pid >= m_maxConnections) {
        MOT_LOG_WARN(
            "SurrogateState::UpdateMaxKey: ConnectionId %lu exceeds max_connections %u", pid, m_maxConnections);
        return false;
    }

    UpdateMaxInsertions(insertions, pid);
    return true;
}

void SurrogateState::Merge(std::list<uint64_t*>& arrays, SurrogateState& global)
{
    for (auto i = arrays.begin(); i != arrays.end();) {
        uint64_t* ptr = (*i);
        if (ptr) {
            for (uint32_t j = 0; j < global.GetMaxConnections(); ++j) {
                global.UpdateMaxInsertions(ptr[j], j);
            }
            i = arrays.erase(i);
            free(ptr);
        } else {
            ++i;
        }
    }
}
}  // namespace MOT
