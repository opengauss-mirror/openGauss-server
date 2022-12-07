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
 * surrogate_state.h
 *    Surrogate key info helper class.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/surrogate_state.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SURROGATE_STATE_H
#define SURROGATE_STATE_H

#include <cstdint>
#include <list>

namespace MOT {
/**
 * @class SurrogateState
 * @brief Implements a surrogate state array in order to
 * properly recover tables that were created with surrogate
 * primary keys.
 */
class SurrogateState {
public:
    SurrogateState();

    ~SurrogateState();

    /**
     * @brief Initializes the surrogate state.
     * @return Boolean value denoting success or failure.
     */
    bool Init();

    /**
     * @brief Extracts the surrogate counter from the key and updates
     * the array according to the connection id
     * @param key The key to extract.
     * @return Boolean value denoting success or failure.
     */
    bool UpdateMaxKey(uint64_t key);

    /**
     * @brief A helper that updates the max insertions of a thread id.
     * @param insertions The insertions count.
     * @param tid The thread id.
     */
    void UpdateMaxInsertions(uint64_t insertions, uint32_t tid);

    /**
     * @brief A helper that extracts the insertions count and
     * thread id from a key
     * @param key The key to extract the info from.
     * @param pid The returned thread id.
     * @param insertions The returned number of insertions.
     */
    inline void ExtractInfoFromKey(uint64_t key, uint64_t& pid, uint64_t& insertions) const
    {
        pid = key >> SurrogateKeyGenerator::KEY_BITS;
        insertions = key & 0x0000FFFFFFFFFFFFULL;
        insertions++;
    }

    /**
     * @brief merges some max insertions arrays into a one single state
     * @param arrays an arrays list.
     * @param global the returned merged SurrogateState.
     */
    static void Merge(std::list<uint64_t*>& arrays, SurrogateState& global);

    bool IsEmpty() const
    {
        return m_empty;
    }

    const uint64_t* GetArray() const
    {
        return m_insertsArray;
    }

    uint32_t GetMaxConnections() const
    {
        return m_maxConnections;
    }

private:
    uint64_t* m_insertsArray;

    bool m_empty;

    uint32_t m_maxConnections;
};
}  // namespace MOT

#endif /* SURROGATE_STATE_H */
