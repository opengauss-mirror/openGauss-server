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
 * surrogate_key_manager.h
 *    Manages the array of surrogate key generators for all possible connections.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/surrogate_key_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SURROGATE_KEY_MANAGER_H
#define SURROGATE_KEY_MANAGER_H

#include "surrogate_key_generator.h"
#include "connection_id.h"

#include <vector>

namespace MOT {
/** @brief Manages the array of surrogate key generators for all possible connections. */
class SurrogateKeyManager {
public:
    SurrogateKeyManager()
    {}
    ~SurrogateKeyManager()
    {}

    /** @brief Initializes the surrogate key manager with maximum number of connections. */
    bool Initialize(uint16_t maxConnectionCount);

    /** @brief Destroys the surrogate key manager. */
    void Destroy();

    /** @brief Retrieves the surrogate key generator by a connection identifier. */
    inline SurrogateKeyGenerator& GetSurrogateSlot(ConnectionId connectionId)
    {
        return m_surrogateArray[connectionId];
    }

    /**
     * @brief Sets the surrogate key counter for a given connection (save counter during session closure for
     * continuation in a new session with the same connection identifier).
     * @param connectionId The identifier for which the surrogate key counter is to be saved.
     * @param counter The new surrogate key counter value.
     * @param[opt] overrideCounter Specifies whether to override the existing counter value even if it is higher than
     * the new counter value.
     */
    inline void SetSurrogateSlot(ConnectionId connectionId, uint64_t counter, bool overrideCounter = false)
    {
        if (overrideCounter || counter > m_surrogateArray[connectionId].GetCurrentCount())
            m_surrogateArray[connectionId].Set(counter);
    }

    /** @brief Resets to zero all counters for all surrogate key generators. */
    inline void ClearSurrogateArray()
    {
        for (uint32_t i = 0; i < m_surrogateArray.size(); ++i) {
            m_surrogateArray[i].Set(SurrogateKeyGenerator::INITIAL_KEY);
        }
    }

    /** @brief Prints the surrogate key map. */
    void PrintSurrogateMap();

private:
    /** @var The surrogate key generator array. */
    std::vector<SurrogateKeyGenerator> m_surrogateArray;
};
}  // namespace MOT
#endif /* SURROGATE_KEY_MANAGER_H */
